//! MMO 大厅/房间制玩法服务。
//!
//! 范式特征：
//!  - 通信：请求/响应 + 房间内全量状态广播（低频事件）
//!  - 状态：服务端权威全量状态，房间内成员持有 RoomInfo 快照
//!  - 广播：房间状态变化时向房间内所有成员 push 全量 RoomStatePush
//!  - 扩展：可依据 room_id 哈希分片（多节点部署时把不同房间路由到不同节点）

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use grimoire_common::{msg, svc};
use grimoire_pb::pb::{
    service_bridge_server::{ServiceBridge, ServiceBridgeServer},
    ForwardReply, ForwardRequest, PlayerEvent, RoomChatPush, RoomCreateReq, RoomCreateResp,
    RoomInfo, RoomJoinReq, RoomJoinResp, RoomLeaveResp, RoomListResp, RoomLoginReq, RoomLoginResp,
    RoomMember,
};
use grimoire_svcfw::Pusher;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8600")]
    listen: String,
    #[arg(long, default_value = "127.0.0.1:8500")]
    registry: String,
    #[arg(long, default_value = "room-svc-1")]
    node_id: String,
}

#[derive(Clone)]
struct Player {
    conn_id: u32,
    player_id: u32,
    name: String,
    room_id: u32, // 0 = 未入房
}

#[derive(Clone)]
struct Room {
    id: u32,
    name: String,
    capacity: u32,
    members: Vec<Player>,
}

struct App {
    players: DashMap<u32, Player>,
    rooms: DashMap<u32, Room>,
    next_player: AtomicU32,
    next_room: AtomicU32,
    pusher: Pusher,
}

impl App {
    fn new(pusher: Pusher) -> Self {
        Self {
            players: DashMap::new(),
            rooms: DashMap::new(),
            next_player: AtomicU32::new(1),
            next_room: AtomicU32::new(1),
            pusher,
        }
    }

    fn login(&self, conn_id: u32, name: String) -> RoomLoginResp {
        if let Some(p) = self.players.get(&conn_id) {
            return RoomLoginResp { player_id: p.player_id, name: p.name.clone() };
        }
        let player_id = self.next_player.fetch_add(1, Ordering::Relaxed);
        self.players.insert(
            conn_id,
            Player { conn_id, player_id, name: name.clone(), room_id: 0 },
        );
        info!("room player {} (conn {}) logged in as {}", player_id, conn_id, name);
        RoomLoginResp { player_id, name }
    }

    fn create_room(&self, conn_id: u32, name: String, capacity: u32) -> Option<RoomCreateResp> {
        // 注意：必须先 clone 出 owned 数据并释放 DashMap Ref，
        // 否则下面再对同一分片 get_mut 会自死锁
        let p = match self.players.get(&conn_id) {
            Some(r) => r.clone(),
            None => return None,
        };
        if p.room_id != 0 {
            return None; // 已在房间中
        }
        let room_id = self.next_room.fetch_add(1, Ordering::Relaxed);
        let room = Room {
            id: room_id,
            name: if name.is_empty() { format!("房间{}", room_id) } else { name },
            capacity: capacity.clamp(2, 64),
            members: vec![p.clone()],
        };
        self.rooms.insert(room_id, room.clone());
        self.players.get_mut(&conn_id).unwrap().room_id = room_id;
        debug!("room {} created by {}", room_id, p.player_id);
        Some(RoomCreateResp { room_id })
    }

    fn join_room(&self, conn_id: u32, room_id: u32) -> Option<RoomJoinResp> {
        let p = match self.players.get(&conn_id) {
            Some(r) => r.clone(),
            None => return None,
        };
        let mut room = self.rooms.get_mut(&room_id)?;
        if p.room_id == room_id {
            return Some(RoomJoinResp { room: Some(room.info()) });
        }
        if room.members.len() >= room.capacity as usize {
            return None; // 满员
        }
        room.members.push(p.clone());
        self.players.get_mut(&conn_id).unwrap().room_id = room_id;
        Some(RoomJoinResp { room: Some(room.info()) })
    }

    fn leave_room(&self, conn_id: u32) -> bool {
        let p = match self.players.get(&conn_id) {
            Some(r) => r.clone(),
            None => return false,
        };
        let mut room = match self.rooms.get_mut(&p.room_id) {
            Some(r) => r,
            None => return false,
        };
        room.members.retain(|m| m.player_id != p.player_id);
        self.players.get_mut(&conn_id).unwrap().room_id = 0;
        if room.members.is_empty() {
            drop(room);
            self.rooms.remove(&p.room_id);
        }
        true
    }

    fn list_rooms(&self) -> RoomListResp {
        RoomListResp {
            rooms: self.rooms.iter().map(|r| r.info()).collect(),
        }
    }

    /// 向房间内所有成员推送状态。
    async fn broadcast(&self, room: &Room, event: &str, actor_id: u32) {
        let payload = grimoire_pb::pb::RoomStatePush {
            event: event.to_string(),
            actor_id,
            room: Some(room.info()),
        };
        let data = grimoire_pb::pb::encode_message(&payload);
        for m in &room.members {
            let _ = self.pusher.push(m.conn_id, msg::ROOM_STATE_PUSH, data.clone()).await;
        }
    }
}

#[derive(Clone)]
struct Bridge {
    app: Arc<App>,
}

/// 统一请求分发：unary 与流式共用，错误一律走 code=1 返回而非 gRPC Status。
async fn process(app: &Arc<App>, req: ForwardRequest) -> ForwardReply {
    debug!("room handle msg 0x{:X} from conn {}", req.msg_id, req.conn_id);
    let reply = |payload: Vec<u8>| ForwardReply {
        conn_id: req.conn_id,
        seq: req.seq,
        msg_id: req.msg_id,
        code: 0,
        payload,
    };
    let err = |text: String| ForwardReply {
        conn_id: req.conn_id,
        seq: req.seq,
        msg_id: req.msg_id,
        code: 1,
        payload: text.into_bytes(),
    };
    match req.msg_id {
        msg::ROOM_LOGIN => match dec::<RoomLoginReq>(&req.payload) {
            Ok(m) => reply(encode(&app.login(req.conn_id, m.name))),
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::ROOM_CREATE => match dec::<RoomCreateReq>(&req.payload) {
            Ok(m) => match app.create_room(req.conn_id, m.name, m.capacity) {
                Some(r) => reply(encode(&r)),
                None => err("已在房间中".into()),
            },
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::ROOM_JOIN => match dec::<RoomJoinReq>(&req.payload) {
            Ok(m) => match app.join_room(req.conn_id, m.room_id) {
                Some(r) => {
                    if let Some(room) = app.rooms.get(&m.room_id) {
                        let room = room.clone();
                        app.broadcast(&room, "join", req.conn_id).await;
                    }
                    reply(encode(&r))
                }
                None => err("房间不存在或已满".into()),
            },
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::ROOM_LEAVE => {
            app.leave_room(req.conn_id);
            reply(encode(&RoomLeaveResp {}))
        }
        msg::ROOM_LIST => reply(encode(&app.list_rooms())),
        msg::ROOM_CHAT => match dec::<grimoire_pb::pb::RoomChatReq>(&req.payload) {
            Ok(m) => {
                if let Some(p) = app.players.get(&req.conn_id) {
                    if p.room_id != 0 {
                        if let Some(room) = app.rooms.get(&p.room_id) {
                            let push = RoomChatPush {
                                room_id: room.id,
                                player_id: p.player_id,
                                name: p.name.clone(),
                                text: m.text.clone(),
                            };
                            let data = grimoire_pb::pb::encode_message(&push);
                            for mem in &room.members {
                                let _ = app.pusher.push(mem.conn_id, msg::ROOM_CHAT_PUSH, data.clone()).await;
                            }
                        }
                    }
                }
                reply(encode(&grimoire_pb::pb::RoomChatResp {}))
            }
            Err(e) => err(format!("bad payload: {e}")),
        },
        _ => err(format!("unknown msg_id 0x{:X}", req.msg_id)),
    }
}

#[tonic::async_trait]
impl ServiceBridge for Bridge {
    type BridgeStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ForwardReply, tonic::Status>> + Send>,
    >;

    async fn handle_message(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardReply>, Status> {
        Ok(Response::new(process(&self.app, request.into_inner()).await))
    }

    /// 双向流：持久连接上复用一条 h2 流处理所有请求。
    async fn bridge_stream(
        &self,
        request: Request<tonic::Streaming<ForwardRequest>>,
    ) -> Result<Response<Self::BridgeStreamStream>, Status> {
        let mut rx = request.into_inner();
        let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<ForwardReply, Status>>(256);
        let app = self.app.clone();
        tokio::spawn(async move {
            loop {
                match rx.message().await {
                    Ok(Some(req)) => {
                        let reply = process(&app, req).await;
                        if tx.send(Ok(reply)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(out_rx))))
    }

    async fn player_connected(
        &self,
        request: Request<PlayerEvent>,
    ) -> Result<Response<grimoire_pb::pb::EventReply>, Status> {
        let conn_id = request.into_inner().conn_id;
        debug!("room player connected conn {}", conn_id);
        Ok(Response::new(grimoire_pb::pb::EventReply { ok: true }))
    }

    async fn player_disconnected(
        &self,
        request: Request<PlayerEvent>,
    ) -> Result<Response<grimoire_pb::pb::EventReply>, Status> {
        let conn_id = request.into_inner().conn_id;
        let app = &self.app;
        if let Some(p) = app.players.get(&conn_id).map(|r| r.clone()) {
            app.leave_room(conn_id);
            app.players.remove(&conn_id);
            if let Some(room) = app.rooms.get(&p.room_id) {
                let room = room.clone();
                app.broadcast(&room, "leave", p.player_id).await;
            }
            info!("room player {} disconnected", p.player_id);
        }
        Ok(Response::new(grimoire_pb::pb::EventReply { ok: true }))
    }
}

fn encode<T: prost::Message>(m: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    prost::Message::encode(m, &mut buf).unwrap();
    buf
}

fn dec<T: prost::Message + Default>(b: &[u8]) -> Result<T, prost::DecodeError> {
    grimoire_pb::pb::decode_message(b)
}

impl Room {
    fn info(&self) -> RoomInfo {
        RoomInfo {
            room_id: self.id,
            name: self.name.clone(),
            capacity: self.capacity,
            members: self
                .members
                .iter()
                .map(|m| RoomMember { player_id: m.player_id, name: m.name.clone() })
                .collect(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let pusher = Pusher::connect(&args.registry).await?;
    let app = Arc::new(App::new(pusher));

    grimoire_svcfw::register_and_heartbeat(
        &args.registry,
        svc::ROOM,
        &args.node_id,
        &args.listen,
        HashMap::new(),
        10,
    )
    .await?;

    info!("room-svc listening on {}", args.listen);
    Server::builder()
        .add_service(ServiceBridgeServer::new(Bridge { app }))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
