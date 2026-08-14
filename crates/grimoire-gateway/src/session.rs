//! 客户端连接会话：读写循环、请求转发、清理。

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::SinkExt;
use futures::StreamExt;
use grimoire_common::msg;
use grimoire_net::{Frame, FrameCodec, PType};
use grimoire_pb::pb::{
    service_bridge_client::ServiceBridgeClient, ForwardRequest, PlayerEvent,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, warn};

use crate::discovery::Discovery;

pub struct Ctx {
    pub sessions: Arc<DashMap<u32, Arc<Session>>>,
    pub discovery: Arc<Discovery>,
    pub next_conn_id: AtomicU32,
}

pub struct Session {
    pub conn_id: u32,
    tx: mpsc::Sender<Frame>,
    active_domains: Mutex<HashSet<u32>>,
}

impl Session {
    pub fn new(conn_id: u32, tx: mpsc::Sender<Frame>) -> Arc<Self> {
        Arc::new(Self {
            conn_id,
            tx,
            active_domains: Mutex::new(HashSet::new()),
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
}

pub async fn accept_and_serve(
    ctx: Arc<Ctx>,
    stream: TcpStream,
    peer: SocketAddr,
) {
    let conn_id = ctx.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Frame>(128);

    let session = Session::new(conn_id, tx);
    ctx.sessions.insert(conn_id, session.clone());
    debug!("conn {} from {} connected", conn_id, peer);

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
                if !session.send(Frame { ptype: PType::Heartbeat, msg_id: 0, seq: 0, payload: Bytes::new() }).await {
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
        let node = ctx.discovery.resolve(domain)
            .await
            .ok_or_else(|| format!("no {} available", svc_name))?;
        let ch = ctx.discovery.channel_for(&node.addr)
            .await
            .ok_or_else(|| format!("cannot connect {}", node.addr))?;
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
        let _ = session.send(Frame {
            ptype: PType::Response,
            msg_id: frame.msg_id,
            seq: frame.seq,
            payload: Bytes::from(format!("err:{}", e)),
        }).await;
    }
}

async fn notify_disconnected(ctx: &Ctx, domain: u32, conn_id: u32) {
    let Some(node) = ctx.discovery.resolve(domain).await else { return };
    let Some(ch) = ctx.discovery.channel_for(&node.addr).await else { return };
    let mut client = ServiceBridgeClient::new(ch);
    if let Err(e) = client
        .player_disconnected(PlayerEvent { conn_id })
        .await
    {
        warn!("notify disconnect {} to 0x{:X} failed: {}", conn_id, domain, e);
    }
}
