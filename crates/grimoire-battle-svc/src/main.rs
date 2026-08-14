//! 实时对战玩法服务（帧同步）。
//!
//! 范式特征：
//!  - 通信：低延迟输入上行 + 服务端按固定 tick(20Hz) 权威广播世界快照下行
//!  - 确定性：同一帧内所有客户端收到相同的权威状态，客户端只负责表现
//!  - 一致性：不做逐条可靠确认，只同步最新状态（适合高频实时，允许丢中间帧）
//!  - 扩展：每个战斗实例独立无状态，可跨节点负载均衡；扩展性最好

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use dashmap::DashMap;
use grimoire_common::{msg, svc};
use grimoire_pb::pb::{
    service_bridge_server::{ServiceBridge, ServiceBridgeServer},
    BattleInputReq, BattleInputResp, BattleJoinReq, BattleJoinResp, BattleLeaveResp, BattleResyncReq,
    BattleResyncResp,
    BattlePlayer, ForwardReply, ForwardRequest, FrameSyncPush, PlayerEvent,
};
use grimoire_svcfw::Pusher;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};

const FRAME_RATE: u32 = 20; // Hz
const SPEED: f32 = 8.0; // 单位/秒
const FIELD: f32 = 100.0;
const BATTLE_CAPACITY: usize = 2;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8700")]
    listen: String,
    #[arg(long, default_value = "127.0.0.1:8500")]
    registry: String,
    #[arg(long, default_value = "battle-svc-1")]
    node_id: String,
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
}

#[derive(Clone)]
struct Player {
    player_id: u32,
    x: f32,
    y: f32,
    conn_id: u32,
}

impl Player {
    fn proto(&self) -> BattlePlayer {
        BattlePlayer { player_id: self.player_id, x: self.x, y: self.y, score: 0 }
    }
}

/// 输入延迟补偿帧数：服务端把输入延后 INPUT_DELAY_FRAMES 帧应用，
/// 吸收网络抖动，让同一帧内所有玩家的输入公平生效
const INPUT_DELAY_FRAMES: u64 = 2;
/// 权威快照环形缓冲大小（快照回放用）
const SNAPSHOT_RING: usize = 30;

struct Battle {
    id: u32,
    frame: u64,
    players: HashMap<u32, Player>,
    /// 帧号 -> 该帧待应用的输入 (player_id, dir_x, dir_y)
    pending: HashMap<u64, Vec<(u32, f32, f32)>>,
    /// 权威快照环形缓冲：(frame, 该帧玩家状态)
    ring: std::collections::VecDeque<(u64, Vec<BattlePlayer>)>,
}

impl Battle {
    fn new(id: u32) -> Self {
        Self {
            id,
            frame: 0,
            players: HashMap::new(),
            pending: HashMap::new(),
            ring: std::collections::VecDeque::new(),
        }
    }

    fn add_player(&mut self, p: Player) {
        let (x, y) = match self.players.len() {
            0 => (25.0, 50.0),
            _ => (75.0, 50.0),
        };
        let mut p = p;
        p.x = x;
        p.y = y;
        self.players.insert(p.player_id, p);
    }

    fn remove_player(&mut self, player_id: u32) {
        self.players.remove(&player_id);
        for v in self.pending.values_mut() {
            v.retain(|(pid, _, _)| *pid != player_id);
        }
    }

    fn is_full(&self) -> bool {
        self.players.len() >= BATTLE_CAPACITY
    }

    /// 延迟补偿：把输入排入目标帧的槽位。
    /// 目标帧 = 输入帧 + INPUT_DELAY_FRAMES；已过期则排入下一帧（latest-wins）。
    fn input(&mut self, player_id: u32, dx: f32, dy: f32, frame: u64) {
        let target = frame.saturating_add(INPUT_DELAY_FRAMES).max(self.frame + 1);
        self.pending.entry(target).or_default().push((player_id, dx, dy));
    }

    /// 确定性模拟：应用本帧排队的输入，推进一帧，记录权威快照。
    fn tick_frame(&mut self) {
        let dt = 1.0 / FRAME_RATE as f32;
        if let Some(inputs) = self.pending.remove(&self.frame) {
            for (pid, dx, dy) in inputs {
                if let Some(p) = self.players.get_mut(&pid) {
                    let norm = (dx * dx + dy * dy).sqrt();
                    if norm > 1e-6 {
                        p.x += (dx / norm) * SPEED * dt;
                        p.y += (dy / norm) * SPEED * dt;
                        p.x = p.x.clamp(0.0, FIELD);
                        p.y = p.y.clamp(0.0, FIELD);
                    }
                }
            }
        }
        self.frame += 1;
        let snap: Vec<BattlePlayer> = self.players.values().map(|p| p.proto()).collect();
        self.ring.push_back((self.frame, snap));
        if self.ring.len() > SNAPSHOT_RING {
            self.ring.pop_front();
        }
    }

    /// 快照回放：返回 last_frame 之后（若太旧则回退最近 N 帧）的权威快照。
    fn resync(&self, last_frame: u64) -> Vec<FrameSyncPush> {
        self.ring
            .iter()
            .filter(|(f, _)| *f > last_frame)
            .map(|(f, players)| FrameSyncPush {
                frame: *f,
                battle_id: self.id,
                players: players.clone(),
            })
            .collect()
    }

    /// 生成要广播的帧快照（编码一次，发给所有成员）。
    fn snapshot(&self) -> Vec<u8> {
        grimoire_pb::pb::encode_message(&FrameSyncPush {
            frame: self.frame,
            battle_id: self.id,
            players: self.players.values().map(|p| p.proto()).collect(),
        })
    }
}

struct App {
    battles: DashMap<u32, Arc<Mutex<Battle>>>,
    /// conn_id -> (battle_id, player_id)
    conn_battle: DashMap<u32, (u32, u32)>,
    next_player: AtomicU32,
    next_battle: AtomicU32,
    pusher: Pusher,
    session_dir: Option<Arc<grimoire_svcfw::SessionDir>>,
    node_id: String,
}

impl App {
    fn new(pusher: Pusher, session_dir: Option<Arc<grimoire_svcfw::SessionDir>>, node_id: String) -> Self {
        Self {
            battles: DashMap::new(),
            conn_battle: DashMap::new(),
            next_player: AtomicU32::new(1),
            next_battle: AtomicU32::new(1),
            pusher,
            session_dir,
            node_id,
        }
    }

    /// 匹配：优先加入未满的战斗，否则新建（简易撮合）。
    async fn join(&self, conn_id: u32) -> BattleJoinResp {
        if let Some((bid, pid)) = self.conn_battle.get(&conn_id).map(|v| *v) {
            if let Some(b) = self.battles.get(&bid) {
                let battle = b.value().lock().await;
                return self.join_resp(bid, pid, &battle);
            }
        }
        // 找一个有空位的战斗
        let mut target_id = None;
        for e in self.battles.iter() {
            let id = *e.key();
            let b = e.value();
            let guard = b.lock().await;
            if guard.players.len() < BATTLE_CAPACITY {
                target_id = Some(id);
                break;
            }
        }
        let created_new = target_id.is_none();
        let target_id = match target_id {
            Some(id) => id,
            None => {
                let id = self.next_battle.fetch_add(1, Ordering::Relaxed);
                self.battles.insert(id, Arc::new(Mutex::new(Battle::new(id))));
                id
            }
        };

        let player_id = self.next_player.fetch_add(1, Ordering::Relaxed);
        let b = self.battles.get(&target_id).unwrap();
        let mut battle = b.value().lock().await;
        battle.add_player(Player { player_id, x: 0.0, y: 0.0, conn_id });
        let full = battle.is_full();
        let resp = self.join_resp(target_id, player_id, &battle);
        drop(battle);
        self.conn_battle.insert(conn_id, (target_id, player_id));
        // 会话目录 + 撮合大厅
        if let Some(sd) = &self.session_dir {
            let _ = sd.bind(msg::DOMAIN_BATTLE, target_id, &self.node_id).await;
            if created_new {
                // 新局 1 人等待匹配 → 登记大厅；满局时清掉
                let _ = sd.lobby_set(msg::DOMAIN_BATTLE, target_id).await;
            }
            if full {
                sd.lobby_clear(msg::DOMAIN_BATTLE, target_id).await;
            }
        }
        info!("battle {} joined by player {}", target_id, player_id);
        resp
    }

    fn join_resp(&self, battle_id: u32, player_id: u32, battle: &Battle) -> BattleJoinResp {
        BattleJoinResp {
            battle_id,
            player_id,
            frame_rate: FRAME_RATE,
            players: battle.players.values().map(|p| p.proto()).collect(),
        }
    }

    async fn input(&self, conn_id: u32, dx: f32, dy: f32, frame: u64) -> bool {
        if let Some((bid, pid)) = self.conn_battle.get(&conn_id).map(|v| *v) {
            if let Some(b) = self.battles.get(&bid) {
                let mut battle = b.value().lock().await;
                battle.input(pid, dx, dy, frame);
                return true;
            }
        }
        false
    }

    /// 快照回放：返回客户端缺失帧的权威快照。
    async fn resync(&self, conn_id: u32, last_frame: u64) -> Option<Vec<FrameSyncPush>> {
        let (bid, _pid) = *self.conn_battle.get(&conn_id)?;
        let b = self.battles.get(&bid)?;
        let battle = b.value().lock().await;
        Some(battle.resync(last_frame))
    }

    async fn leave(&self, conn_id: u32) -> bool {
        let Some((_, (bid, pid))) = self.conn_battle.remove(&conn_id) else {
            return false;
        };
        if let Some(b) = self.battles.get(&bid) {
            let mut battle = b.value().lock().await;
            battle.remove_player(pid);
            let empty = battle.players.is_empty();
            let single = battle.players.len() == 1;
            // 先释放 Ref（读锁）再 remove（写锁），否则 DashMap 分片自死锁
            drop(battle);
            drop(b);
            if empty {
                self.battles.remove(&bid);
                if let Some(sd) = &self.session_dir {
                    let _ = sd.remove(msg::DOMAIN_BATTLE, bid).await;
                }
            } else if single {
                // 满局退成 1 人 → 重新登记大厅等待匹配
                if let Some(sd) = &self.session_dir {
                    let _ = sd.lobby_set(msg::DOMAIN_BATTLE, bid).await;
                }
            }
        }
        true
    }

    /// 20Hz 全局模拟节拍。
    async fn tick(&self) {
        // 先把 Arc<Mutex> 快照出来，绝不在 DashMap 迭代锁内 await
        let battles: Vec<(u32, Arc<Mutex<Battle>>)> = self
            .battles
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();
        for (_bid, b) in battles {
            let (payload, targets) = {
                let mut battle = b.lock().await;
                battle.tick_frame();
                (battle.snapshot(), battle.players.values().map(|p| p.conn_id).collect::<Vec<_>>())
            };
            for conn in targets {
                // 帧同步走 UDP 低延迟通道（客户端绑定 UDP 后由网关转发）
                let _ = self.pusher.push_udp(conn, msg::BATTLE_FRAME_SYNC, payload.clone()).await;
            }
        }
    }
}

#[derive(Clone)]
struct Bridge {
    app: Arc<App>,
}

/// 统一请求分发：unary 与流式共用，错误走 code=1。
async fn process(app: &Arc<App>, req: ForwardRequest) -> ForwardReply {
    debug!("battle handle msg 0x{:X} from conn {}", req.msg_id, req.conn_id);
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
        msg::BATTLE_JOIN => match dec::<BattleJoinReq>(&req.payload) {
            Ok(_) => reply(encode(&app.join(req.conn_id).await)),
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::BATTLE_INPUT => match dec::<BattleInputReq>(&req.payload) {
            Ok(m) => {
                app.input(req.conn_id, m.dir_x, m.dir_y, m.frame).await;
                reply(encode(&BattleInputResp {}))
            }
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::BATTLE_RESYNC => match dec::<BattleResyncReq>(&req.payload) {
            Ok(m) => {
                match app.resync(req.conn_id, m.last_frame).await {
                    Some(frames) => reply(encode(&BattleResyncResp { frames })),
                    None => err("未在对局中".into()),
                }
            }
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::BATTLE_LEAVE => {
            app.leave(req.conn_id).await;
            reply(encode(&BattleLeaveResp {}))
        }
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
        debug!("battle player connected conn {}", conn_id);
        Ok(Response::new(grimoire_pb::pb::EventReply { ok: true }))
    }

    async fn player_disconnected(
        &self,
        request: Request<PlayerEvent>,
    ) -> Result<Response<grimoire_pb::pb::EventReply>, Status> {
        let conn_id = request.into_inner().conn_id;
        self.app.leave(conn_id).await;
        debug!("battle conn {} disconnected", conn_id);
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let pusher = Pusher::connect(&args.registry).await?;
    let session_dir = match grimoire_svcfw::SessionDir::connect(&args.redis_url).await {
        Ok(sd) => Some(Arc::new(sd)),
        Err(e) => {
            warn!("session dir disabled: {}", e);
            None
        }
    };
    let app = Arc::new(App::new(pusher, session_dir, args.node_id.clone()));

    // 20Hz 模拟节拍
    let ticker = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(1000 / FRAME_RATE as u64));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            ticker.tick().await;
        }
    });

    grimoire_svcfw::register_and_heartbeat(
        &args.registry,
        svc::BATTLE,
        &args.node_id,
        &args.listen,
        HashMap::new(),
        10,
    )
    .await?;

    info!("battle-svc listening on {}", args.listen);
    Server::builder()
        .add_service(ServiceBridgeServer::new(Bridge { app }))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
