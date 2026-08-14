//! 客户端连接会话：读写循环、请求转发、UDP 绑定、清理。

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use futures::StreamExt;
use grimoire_common::{conn, msg};
use grimoire_net::{udp, Frame, FrameCodec, PType};
use grimoire_pb::pb::{service_bridge_client::ServiceBridgeClient, ForwardRequest, PlayerEvent};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, warn};

use crate::discovery::Discovery;

pub struct Ctx {
    pub sessions: Arc<DashMap<u32, Arc<Session>>>,
    pub discovery: Arc<Discovery>,
    pub next_conn_id: AtomicU32,
    /// 本网关 ID（写进 conn_id 高 8 位，全局唯一）
    pub gateway_id: u8,
    /// UDP 下行通道：发给 gateway 的 UDP 写任务 (目标地址, 数据报)
    pub udp_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
}

pub struct Session {
    pub conn_id: u32,
    tx: mpsc::Sender<Frame>,
    active_domains: Mutex<HashSet<u32>>,
    /// 客户端 UDP 源地址（首次收到其 UDP 包时绑定）
    udp_addr: Mutex<Option<SocketAddr>>,
}

impl Session {
    pub fn new(conn_id: u32, tx: mpsc::Sender<Frame>) -> Arc<Self> {
        Arc::new(Self {
            conn_id,
            tx,
            active_domains: Mutex::new(HashSet::new()),
            udp_addr: Mutex::new(None),
        })
    }

    pub async fn send(&self, f: Frame) -> bool {
        self.tx.send(f).await.is_ok()
    }

    /// 首次访问某玩法域时记录，用于断开时定向通知。
    async fn mark_domain(&self, domain: u32) -> bool {
        let mut d = self.active_domains.lock().await;
        d.insert(domain)
    }

    pub async fn domains(&self) -> Vec<u32> {
        self.active_domains.lock().await.iter().copied().collect()
    }

    pub async fn bind_udp(&self, addr: SocketAddr) {
        *self.udp_addr.lock().await = Some(addr);
    }

    pub async fn udp_addr(&self) -> Option<SocketAddr> {
        *self.udp_addr.lock().await
    }

    /// 通过 UDP 下发（未绑定 UDP 则返回 false，调用方回退 TCP）。
    async fn send_udp(&self, ctx: &Ctx, msg_id: u32, seq: u32, payload: Bytes) -> bool {
        let Some(addr) = self.udp_addr().await else {
            return false;
        };
        let dgram = udp::push_datagram(msg_id, seq, payload);
        ctx.udp_tx.send((addr, dgram)).await.is_ok()
    }

    /// 下发一条 push：优先 UDP（请求了才用），否则 TCP。
    pub async fn push_msg(&self, ctx: &Ctx, msg_id: u32, payload: Bytes, prefer_udp: bool) -> bool {
        if prefer_udp && self.send_udp(ctx, msg_id, 0, payload.clone()).await {
            return true;
        }
        self.send(Frame { ptype: PType::Push, msg_id, seq: 0, payload }).await
    }
}

pub async fn accept_and_serve(ctx: Arc<Ctx>, stream: TcpStream, peer: SocketAddr) {
    // 游戏服务器标配：禁用 Nagle，避免小包延迟叠加
    let _ = stream.set_nodelay(true);
    let local = ctx.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let conn_id = conn::make(ctx.gateway_id, local);
    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Frame>(128);

    let session = Session::new(conn_id, tx);
    ctx.sessions.insert(conn_id, session.clone());
    debug!("conn {} (gw {}) from {} connected", conn_id, ctx.gateway_id, peer);

    // 欢迎帧：把全局 conn_id 下发给客户端（UDP 绑定等场景需要）
    let _ = session
        .send(Frame {
            ptype: PType::Push,
            msg_id: msg::SYS_CONN_ID,
            seq: 0,
            payload: Bytes::copy_from_slice(&conn_id.to_be_bytes()),
        })
        .await;

    // 写侧任务：把 push/响应写回 socket
    let mut framed_w = FramedWrite::new(write_half, FrameCodec);
    tokio::spawn(async move {
        let mut rx = rx;
        while let Some(f) = rx.recv().await {
            if let Err(e) = framed_w.send(f).await {
                warn!("conn {} write error: {}", conn_id, e);
                break;
            }
        }
    });

    // 读侧循环
    let mut framed_r = FramedRead::new(read_half, FrameCodec);
    loop {
        let frame = match framed_r.next().await {
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                warn!("conn {} read error: {}", conn_id, e);
                break;
            }
            None => break,
        };
        match frame.ptype {
            PType::Request => {
                let s = session.clone();
                let c = ctx.clone();
                tokio::spawn(forward_request(c, s, frame));
            }
            PType::Heartbeat => {
                if !session
                    .send(Frame { ptype: PType::Heartbeat, msg_id: 0, seq: 0, payload: Bytes::new() })
                    .await
                {
                    break;
                }
            }
            PType::Close => break,
            _ => {}
        }
    }

    // 连接结束，清理
    ctx.sessions.remove(&conn_id);
    let domains = session.domains().await;
    debug!("conn {} closed, notifying domains {:?}", conn_id, domains);
    let c = ctx.clone();
    tokio::spawn(async move {
        for domain in domains {
            notify_disconnected(&c, domain, conn_id).await;
        }
    });
}

/// 把客户端请求转发到对应玩法服务，取回响应并回写。
async fn forward_request(ctx: Arc<Ctx>, session: Arc<Session>, frame: Frame) {
    let domain = msg::domain_of(frame.msg_id);
    debug!("conn {} forward msg 0x{:X} (domain 0x{:X})", session.conn_id, frame.msg_id, domain);
    let result: Result<(), String> = async {
        let svc_name = crate::discovery::service_for_domain(domain)
            .ok_or_else(|| format!("unknown domain 0x{:X}", domain))?;
        let node = ctx.discovery.resolve(domain).await.ok_or_else(|| format!("no {} available", svc_name))?;
        let ch = ctx.discovery.channel_for(&node.addr).await.ok_or_else(|| format!("cannot connect {}", node.addr))?;
        let mut client = ServiceBridgeClient::new(ch);
        // 首次进入该玩法域时通知业务服务
        if session.mark_domain(domain).await {
            let _ = client.player_connected(PlayerEvent { conn_id: session.conn_id }).await;
        }
        let req = tonic::Request::new(ForwardRequest {
            conn_id: session.conn_id,
            seq: frame.seq,
            msg_id: frame.msg_id,
            payload: frame.payload.to_vec(),
        });
        let resp = client.handle_message(req).await.map_err(|e| e.to_string())?;
        debug!("conn {} got reply for msg 0x{:X}", session.conn_id, frame.msg_id);
        let reply_frame = Frame {
            ptype: PType::Response,
            msg_id: frame.msg_id,
            seq: frame.seq,
            payload: Bytes::from(resp.into_inner().payload),
        };
        if !session.send(reply_frame).await {
            return Err("session closed".into());
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        warn!("forward msg 0x{:X} err: {}", frame.msg_id, e);
        let _ = session
            .send(Frame {
                ptype: PType::Response,
                msg_id: frame.msg_id,
                seq: frame.seq,
                payload: Bytes::from(format!("err:{}", e)),
            })
            .await;
    }
}

/// UDP 数据报入口：绑定 conn <-> 源地址，并把输入转发给业务服务。
pub async fn handle_udp_datagram(ctx: Arc<Ctx>, src: SocketAddr, data: Vec<u8>) {
    let Some((conn_id, msg_id, payload)) = udp::parse_peer_packet(&data) else {
        return;
    };
    let Some(session) = ctx.sessions.get(&conn_id).map(|r| r.clone()) else {
        debug!("udp from {} for unknown conn {}", src, conn_id);
        return;
    };
    session.bind_udp(src).await;
    let s = session.clone();
    let c = ctx.clone();
    tokio::spawn(forward_udp(c, s, msg_id, Bytes::copy_from_slice(payload)));
}

/// UDP 输入转发（一次性，不回响应）。
async fn forward_udp(ctx: Arc<Ctx>, session: Arc<Session>, msg_id: u32, payload: Bytes) {
    let domain = msg::domain_of(msg_id);
    let Ok(svc_name) = crate::discovery::service_for_domain(domain).ok_or_else(|| "unknown domain") else {
        return;
    };
    let Some(node) = ctx.discovery.resolve(domain).await else {
        warn!("udp forward: no {} available", svc_name);
        return;
    };
    let Some(ch) = ctx.discovery.channel_for(&node.addr).await else { return };
    let mut client = ServiceBridgeClient::new(ch);
    if session.mark_domain(domain).await {
        let _ = client.player_connected(PlayerEvent { conn_id: session.conn_id }).await;
    }
    let _ = client
        .handle_message(ForwardRequest {
            conn_id: session.conn_id,
            seq: 0,
            msg_id,
            payload: payload.to_vec(),
        })
        .await;
}

async fn notify_disconnected(ctx: &Ctx, domain: u32, conn_id: u32) {
    let Some(node) = ctx.discovery.resolve(domain).await else { return };
    let Some(ch) = ctx.discovery.channel_for(&node.addr).await else { return };
    let mut client = ServiceBridgeClient::new(ch);
    if let Err(e) = client.player_disconnected(PlayerEvent { conn_id }).await {
        warn!("notify disconnect {} to 0x{:X} failed: {}", conn_id, domain, e);
    }
}
