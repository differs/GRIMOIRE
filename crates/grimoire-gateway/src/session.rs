//! 客户端连接会话：读写循环、请求转发、UDP 绑定、清理。

use std::time::Duration;
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
use tracing::{debug, info, warn};

use crate::discovery::Discovery;
use crate::kcp::{spawn_kcp_driver, KcpSession};
use crate::stream::Streams;

/// 网关侧会话宽限：断线后保留 conn_id 映射的时间（供连接迁移）
const SESSION_GRACE: u64 = 8;

pub struct Ctx {
    pub sessions: Arc<DashMap<u32, Arc<Session>>>,
    pub discovery: Arc<Discovery>,
    pub streams: Arc<Streams>,
    pub next_conn_id: AtomicU32,
    /// 本网关 ID（写进 conn_id 高 8 位，全局唯一）
    pub gateway_id: u8,
    /// UDP 下行通道：发给 gateway 的 UDP 写任务 (目标地址, 数据报)
    pub udp_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
    /// KCP 会话表：conn_id -> KCP 会话（可靠 UDP）
    pub kcp_sessions: Arc<DashMap<u32, Arc<KcpSession>>>,
    /// UDP socket（KCP 新建会话/输出用）
    pub udp_sock: Arc<tokio::net::UdpSocket>,
    /// KCP 参数
    pub kcp_interval: i32,
    pub kcp_resend: i32,
    pub kcp_nc: bool,
}

pub struct Session {
    conn_id: AtomicU32,
    tx: mpsc::Sender<Frame>,
    /// 已激活玩法域位掩码（bit = domain>>24），原子操作，热路径无锁
    active_domains: AtomicU32,
    /// 客户端 UDP 源地址（首次收到其 UDP 包时绑定）
    udp_addr: Mutex<Option<SocketAddr>>,
}

impl Session {
    pub fn new(conn_id: u32, tx: mpsc::Sender<Frame>) -> Arc<Self> {
        Arc::new(Self {
            conn_id: AtomicU32::new(conn_id),
            tx,
            active_domains: AtomicU32::new(0),
            udp_addr: Mutex::new(None),
        })
    }

    pub fn conn_id(&self) -> u32 {
        self.conn_id.load(Ordering::Relaxed)
    }

    /// 连接迁移：把会话重绑到旧 conn_id（服务端视角无感）。
    /// 同时清空玩法域记录，令首个请求重新触发 PlayerConnected 通知。
    pub async fn rebind(&self, new_id: u32) {
        self.conn_id.store(new_id, Ordering::Relaxed);
        self.active_domains.store(0, Ordering::Relaxed);
    }

    pub async fn send(&self, f: Frame) -> bool {
        self.tx.send(f).await.is_ok()
    }

    /// 首次访问某玩法域时记录（原子位掩码，无锁），返回是否为新激活。
    fn mark_domain(&self, domain: u32) -> bool {
        let bit = 1u32 << ((domain >> 24) & 0x1F);
        let prev = self.active_domains.fetch_or(bit, Ordering::Relaxed);
        (prev & bit) == 0
    }

    pub fn domains(&self) -> Vec<u32> {
        let bits = self.active_domains.load(Ordering::Relaxed);
        (1..=3).filter(|d| bits & (1 << d) != 0).map(|d| d << 24).collect()
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

    /// 下发一条 push：优先 KCP（若已建可靠 UDP 会话）→ 裸 UDP → TCP。
    pub async fn push_msg(&self, ctx: &Ctx, msg_id: u32, payload: Bytes, prefer_udp: bool) -> bool {
        if prefer_udp {
            if self.send_kcp(ctx, msg_id, payload.clone()).await {
                return true;
            }
            if self.send_udp(ctx, msg_id, 0, payload.clone()).await {
                return true;
            }
            return self.send(Frame { ptype: PType::Push, msg_id, seq: 0, payload }).await;
        }
        self.send(Frame { ptype: PType::Push, msg_id, seq: 0, payload }).await
    }

    /// 通过 KCP 会话发送（可靠 UDP）。
    async fn send_kcp(&self, ctx: &Ctx, msg_id: u32, payload: Bytes) -> bool {
        let Some(kcps) = ctx.kcp_sessions.get(&self.conn_id()).map(|r| r.clone()) else {
            return false;
        };
        let dgram = udp::peer_packet(self.conn_id(), msg_id, &payload);
        kcps.send_packet(&dgram)
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
            PType::Request if frame.msg_id == msg::SYS_RESUME => {
                handle_resume(&ctx, &session, conn_id, frame).await;
            }
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

    // 连接结束：立即通知业务服务断开（服务端有宽限恢复），
    // 但会话表保留 SESSION_GRACE 秒供连接迁移重绑。
    let domains = session.domains();
    debug!("conn {} closed, notifying domains {:?}", conn_id, domains);
    let c = ctx.clone();
    tokio::spawn(async move {
        for domain in domains {
            notify_disconnected(&c, domain, conn_id).await;
        }
    });
    let old_arc = session.clone();
    let c = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(SESSION_GRACE)).await;
        // 先取判定值并释放 Ref，再 remove（Ref 存活时 remove 会分片自死锁）
        let is_current = c
            .sessions
            .get(&conn_id)
            .map(|cur| Arc::ptr_eq(&cur, &old_arc))
            .unwrap_or(false);
        if is_current {
            c.sessions.remove(&conn_id);
            c.kcp_sessions.remove(&conn_id);
        }
    });
}

/// 连接迁移：把当前连接重绑到旧 conn_id，使业务服务视角无缝衔接。
async fn handle_resume(ctx: &Ctx, session: &Arc<Session>, new_conn_id: u32, frame: Frame) {
    let old = if frame.payload.len() == 4 {
        u32::from_be_bytes([frame.payload[0], frame.payload[1], frame.payload[2], frame.payload[3]])
    } else {
        let _ = session
            .send(Frame {
                ptype: PType::Response,
                msg_id: 0,
                seq: frame.seq,
                payload: Bytes::from_static(b"err:bad resume payload"),
            })
            .await;
        return;
    };
    if old == new_conn_id || !ctx.sessions.contains_key(&old) {
        let _ = session
            .send(Frame {
                ptype: PType::Response,
                msg_id: 0,
                seq: frame.seq,
                payload: Bytes::from_static(b"err:no session to resume"),
            })
            .await;
        return;
    }
    // 踢掉旧连接（若仍存活）
    if let Some((_, old_s)) = ctx.sessions.remove(&old) {
        let _ = old_s
            .send(Frame { ptype: PType::Close, msg_id: 0, seq: 0, payload: Bytes::from_static(b"migrated") })
            .await;
    }
    // 把当前会话从新 conn_id 重绑到旧 conn_id
    ctx.sessions.remove(&new_conn_id);
    session.rebind(old).await;
    ctx.sessions.insert(old, session.clone());
    info!("conn {} resumed as {}", new_conn_id, old);
    let _ = session
        .send(Frame {
            ptype: PType::Response,
            msg_id: msg::SYS_RESUME,
            seq: frame.seq,
            payload: Bytes::copy_from_slice(&old.to_be_bytes()),
        })
        .await;
}

/// 把客户端请求经流式复用转发到对应玩法服务，取回响应并回写。
async fn forward_request(ctx: Arc<Ctx>, session: Arc<Session>, frame: Frame) {
    let domain = msg::domain_of(frame.msg_id);
    debug!("conn {} forward msg 0x{:X} (domain 0x{:X})", session.conn_id(), frame.msg_id, domain);
    let result: Result<(), String> = async {
        let svc_name = crate::discovery::service_for_domain(domain)
            .ok_or_else(|| format!("unknown domain 0x{:X}", domain))?;
        let node = ctx.discovery.resolve(domain, Some(session.conn_id())).await.ok_or_else(|| format!("no {} available", svc_name))?;
        // 首次进入该玩法域时通知业务服务（低频，unary 即可）
        if session.mark_domain(domain) {
            if let Some(ch) = ctx.discovery.channel_for(&node.addr).await {
                let mut client = ServiceBridgeClient::new(ch);
                let _ = client.player_connected(PlayerEvent { conn_id: session.conn_id() }).await;
            }
        }
        let conn = ctx.streams.get_or_open(domain, &node).await?;
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        ctx.streams.pending.insert((session.conn_id(), frame.seq), resp_tx);
        ctx.streams
            .send(&conn, ForwardRequest {
                conn_id: session.conn_id(),
                seq: frame.seq,
                msg_id: frame.msg_id,
                payload: frame.payload.to_vec(),
            })
            .await?;
        let reply = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await
            .map_err(|_| "timeout waiting reply".to_string())?
            .map_err(|_| "bridge stream closed".to_string())?;
        debug!("conn {} got reply for msg 0x{:X}", session.conn_id(), frame.msg_id);
        let reply_frame = if reply.code != 0 {
            // 业务错误：msg_id=0 + 错误文本（客户端约定）
            Frame { ptype: PType::Response, msg_id: 0, seq: frame.seq, payload: Bytes::from(reply.payload) }
        } else {
            Frame { ptype: PType::Response, msg_id: reply.msg_id, seq: reply.seq, payload: Bytes::from(reply.payload) }
        };
        if !session.send(reply_frame).await {
            return Err("session closed".into());
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        warn!("forward msg 0x{:X} err: {}", frame.msg_id, e);
        ctx.streams.pending.remove(&(session.conn_id(), frame.seq));
        let _ = session
            .send(Frame {
                ptype: PType::Response,
                msg_id: 0,
                seq: frame.seq,
                payload: Bytes::from(format!("err:{}", e)),
            })
            .await;
    }
}

/// UDP 数据报入口：裸 UDP（'MU' 开头）绑定 conn <-> 源地址并转发输入。
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

/// KCP 数据报入口：按 conv(前4字节) 定位/创建会话，解出应用载荷后转发。
pub async fn handle_kcp_datagram(ctx: Arc<Ctx>, conv: u32, src: SocketAddr, data: Vec<u8>) {
    let session = match ctx.kcp_sessions.get(&conv) {
        Some(s) => s.clone(),
        None => {
            let s = KcpSession::new(conv, ctx.udp_sock.clone(), src, ctx.kcp_interval, ctx.kcp_resend, ctx.kcp_nc);
            ctx.kcp_sessions.insert(conv, s.clone());
            // driver 收到会话内数据后回调处理
            let c = ctx.clone();
            let on_data = move |pkt: Vec<u8>| {
                if let Some((conn_id, msg_id, payload)) = udp::parse_peer_packet(&pkt) {
                    let payload = payload.to_vec();
                    let c = c.clone();
                    tokio::spawn(async move {
                        if let Some(session) = c.sessions.get(&conn_id).map(|r| r.clone()) {
                            session.bind_udp(src).await;
                            forward_udp(c.clone(), session, msg_id, Bytes::from(payload)).await;
                        }
                    });
                }
            };
            spawn_kcp_driver(ctx.kcp_sessions.clone(), conv, s.clone(), on_data);
            debug!("new kcp session conv={} from {}", conv, src);
            s
        }
    };
    let _ = session.inner.lock().unwrap().input(&data);
}

/// UDP 输入转发（一次性，不回响应；seq=0 的响应会被 reader 丢弃）。
async fn forward_udp(ctx: Arc<Ctx>, session: Arc<Session>, msg_id: u32, payload: Bytes) {
    let domain = msg::domain_of(msg_id);
    let Ok(svc_name) = crate::discovery::service_for_domain(domain).ok_or_else(|| "unknown domain") else {
        return;
    };
    let Some(node) = ctx.discovery.resolve(domain, Some(session.conn_id())).await else {
        warn!("udp forward: no {} available", svc_name);
        return;
    };
    if session.mark_domain(domain) {
        if let Some(ch) = ctx.discovery.channel_for(&node.addr).await {
            let mut client = ServiceBridgeClient::new(ch);
            let _ = client.player_connected(PlayerEvent { conn_id: session.conn_id() }).await;
        }
    }
    let Ok(conn) = ctx.streams.get_or_open(domain, &node).await else {
        return;
    };
    let _ = ctx
        .streams
        .send(&conn, ForwardRequest {
            conn_id: session.conn_id(),
            seq: 0,
            msg_id,
            payload: payload.to_vec(),
        })
        .await;
}

async fn notify_disconnected(ctx: &Ctx, domain: u32, conn_id: u32) {
    let Some(node) = ctx.discovery.resolve(domain, Some(conn_id)).await else { return };
    let Some(ch) = ctx.discovery.channel_for(&node.addr).await else { return };
    let mut client = ServiceBridgeClient::new(ch);
    if let Err(e) = client.player_disconnected(PlayerEvent { conn_id }).await {
        warn!("notify disconnect {} to 0x{:X} failed: {}", conn_id, domain, e);
    }
}
