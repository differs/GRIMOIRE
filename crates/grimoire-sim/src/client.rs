//! 客户端 SDK：连接网关、收发帧、并发请求、push 订阅、心跳。

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use grimoire_common::msg;
use grimoire_net::{udp, Frame, FrameCodec, PType};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Client {
    tx: mpsc::Sender<Frame>,
    responses: Arc<DashMap<u32, oneshot::Sender<Frame>>>,
    seq: Arc<AtomicU32>,
    pub pushes: broadcast::Receiver<Frame>,
    conn_id_label: u32,
    /// 网关欢迎帧下发的全局连接号
    conn_id: Arc<AtomicU32>,
}

impl Client {
    pub async fn connect(addr: &str, conn_id_label: u32) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await.context("connect gateway")?;
        let _ = stream.set_nodelay(true);
        let (read_half, write_half) = stream.into_split();

        let (tx, mut tx_rx) = mpsc::channel::<Frame>(128);
        tokio::spawn(async move {
            let mut w = FramedWrite::new(write_half, FrameCodec);
            while let Some(f) = tx_rx.recv().await {
                if w.send(f).await.is_err() {
                    break;
                }
            }
        });

        let responses: Arc<DashMap<u32, oneshot::Sender<Frame>>> = Arc::new(DashMap::new());
        let (push_tx, pushes) = broadcast::channel(128);
        let resp_map = responses.clone();
        let conn_id = Arc::new(AtomicU32::new(0));
        let conn_id_w = conn_id.clone();
        tokio::spawn(async move {
            let mut r = FramedRead::new(read_half, FrameCodec);
            while let Some(item) = r.next().await {
                match item {
                    Ok(f) => match f.ptype {
                        PType::Response => {
                            if let Some((_, tx)) = resp_map.remove(&f.seq) {
                                let _ = tx.send(f);
                            }
                        }
                        PType::Push => {
                            if f.msg_id == msg::SYS_CONN_ID && f.payload.len() == 4 {
                                let cid = u32::from_be_bytes([f.payload[0], f.payload[1], f.payload[2], f.payload[3]]);
                                conn_id_w.store(cid, Ordering::Relaxed);
                            } else {
                                let _ = push_tx.send(f);
                            }
                        }
                        PType::Heartbeat => {}
                        PType::Close => break,
                        _ => {}
                    },
                    Err(e) => {
                        eprintln!("[client {}] read error: {}", conn_id_label, e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            tx,
            responses,
            seq: Arc::new(AtomicU32::new(1)),
            pushes,
            conn_id_label,
            conn_id,
        })
    }

    /// 全局连接号（网关下发）。连接建立后很快可用。
    pub fn conn_id(&self) -> u32 {
        self.conn_id.load(Ordering::Relaxed)
    }

    /// 等待欢迎帧下发 conn_id（最多 ~100ms）。
    pub async fn wait_conn_id(&self) -> u32 {
        for _ in 0..20 {
            let cid = self.conn_id();
            if cid != 0 {
                return cid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        0
    }

    /// 连接迁移后覆盖为本机已知的旧连接号。
    pub fn set_conn_id(&self, cid: u32) {
        self.conn_id.store(cid, Ordering::Relaxed);
    }

    /// 连接迁移：请求网关把当前连接重绑到旧 conn_id（断线重连恢复会话）。
    pub async fn resume(&self, old_conn_id: u32) -> anyhow::Result<()> {
        let r = self.request(msg::SYS_RESUME, old_conn_id.to_be_bytes().to_vec()).await?;
        if r.msg_id == msg::SYS_RESUME && r.payload.len() == 4 {
            let cid = u32::from_be_bytes([r.payload[0], r.payload[1], r.payload[2], r.payload[3]]);
            self.set_conn_id(cid);
            Ok(())
        } else {
            Err(anyhow::anyhow!("resume failed: {}", String::from_utf8_lossy(&r.payload)))
        }
    }

    /// 发送请求并等待对应响应（5s 超时）。
    pub async fn request(&self, msg_id: u32, payload: Vec<u8>) -> anyhow::Result<Frame> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();
        self.responses.insert(seq, resp_tx);
        let frame = Frame { ptype: PType::Request, msg_id, seq, payload: Bytes::from(payload) };
        self.tx.send(frame).await.context("send")?;
        let f = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await
            .context("timeout waiting response")?
            .map_err(|_| anyhow::anyhow!("channel closed"))?;
        // 服务端错误以 err: 前缀返回
        if f.msg_id == 0 {
            return Err(anyhow::anyhow!("server error: {}", String::from_utf8_lossy(&f.payload)));
        }
        Ok(f)
    }

    /// 后台心跳（每 5s）。
    pub fn start_heartbeat(&self) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let f = Frame { ptype: PType::Heartbeat, msg_id: 0, seq: 0, payload: Bytes::new() };
                if tx.send(f).await.is_err() {
                    break;
                }
            }
        });
    }

}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            responses: self.responses.clone(),
            seq: self.seq.clone(),
            pushes: self.pushes.resubscribe(),
            conn_id_label: self.conn_id_label,
            conn_id: self.conn_id.clone(),
        }
    }
}

/// UDP 实时对战通道：绑定 conn_id、发送输入、接收帧同步。
#[derive(Clone)]
pub struct UdpBattle {
    sock: Arc<UdpSocket>,
    conn_id: u32,
}

impl UdpBattle {
    /// 绑定到网关 UDP 端口，并发送首包建立会话绑定。
    pub async fn bind(gateway_udp: &str, conn_id: u32) -> anyhow::Result<Self> {
        let sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        sock.connect(gateway_udp).await.context("udp connect")?;
        let pkt = udp::peer_packet(conn_id, msg::BATTLE_INPUT, &[]);
        sock.send(&pkt).await.context("udp bind packet")?;
        Ok(Self { sock, conn_id })
    }

    pub async fn send_input(&self, dir_x: f32, dir_y: f32) -> anyhow::Result<()> {
        let payload = grimoire_pb::pb::encode_message(&grimoire_pb::pb::BattleInputReq { dir_x, dir_y });
        let pkt = udp::peer_packet(self.conn_id, msg::BATTLE_INPUT, &payload);
        self.sock.send(&pkt).await?;
        Ok(())
    }

    /// 接收一条网关推送数据报。
    pub async fn recv_push(&self) -> anyhow::Result<Option<(u32, u32, Bytes)>> {
        let mut buf = vec![0u8; udp::MAX_DATAGRAM];
        let n = self.sock.recv(&mut buf).await?;
        Ok(udp::parse_push_datagram(&buf[..n]))
    }
}

/// KCP 可靠 UDP 对战通道：可靠有序、快重传（kcp 0.3 实现）。
#[derive(Clone)]
pub struct UdpKcp {
    kcp: Arc<std::sync::Mutex<kcp::Kcp<UdpKcpOutput>>>,
    conn_id: u32,
    frames: Arc<tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>>,
}

pub struct UdpKcpOutput {
    sock: Arc<UdpSocket>,
    addr: std::net::SocketAddr,
}

impl std::io::Write for UdpKcpOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sock
            .try_send_to(buf, self.addr)
            .map_err(std::io::Error::other)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn now_ms() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u32
}

impl UdpKcp {
    /// 绑定 KCP 会话：首包即建立网关侧会话。
    pub async fn bind(gateway_udp: &str, conn_id: u32) -> anyhow::Result<Self> {
        Self::bind_with(gateway_udp, conn_id, 10, 2, true).await
    }

    /// 绑定 KCP 会话（自定义参数：间隔 ms / 快重传 / 流控）。
    pub async fn bind_with(gateway_udp: &str, conn_id: u32, interval: i32, resend: i32, nc: bool) -> anyhow::Result<Self> {
        let sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        let addr: std::net::SocketAddr = gateway_udp.parse()?;
        let output = UdpKcpOutput { sock: sock.clone(), addr };
        let mut kcp = kcp::Kcp::new(conn_id, output);
        kcp.set_nodelay(true, interval, resend, nc);
        kcp.set_wndsize(128, 128);
        let kcp = Arc::new(std::sync::Mutex::new(kcp));

        let (frames_tx, frames_rx) = mpsc::channel::<Vec<u8>>(128);
        // 10ms 驱动：读 UDP → input；update（触发发送/重传）；排空 recv
        let k2 = kcp.clone();
        let tx2 = frames_tx.clone();
        let sock2 = sock.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            let mut buf = vec![0u8; 8192];
            loop {
                tick.tick().await;
                // 1) UDP 数据包喂给 KCP
                loop {
                    match sock2.try_recv_from(&mut buf) {
                        Ok((n, _)) => {
                            let _ = k2.lock().unwrap().input(&buf[..n]);
                        }
                        Err(_) => break,
                    }
                }
                // 2) 时钟驱动 + 排空应用数据
                let mut received = Vec::new();
                {
                    let mut k = k2.lock().unwrap();
                    let _ = k.update(now_ms());
                    loop {
                        let mut b = vec![0u8; 4096];
                        match k.recv(&mut b) {
                            Ok(n) => received.push(b[..n].to_vec()),
                            Err(_) => break,
                        }
                    }
                }
                for pkt in received {
                    if tx2.send(pkt).await.is_err() {
                        return;
                    }
                }
            }
        });

        // 首包：建立网关侧 KCP 会话（空输入）
        {
            let mut k = kcp.lock().unwrap();
            let pkt = udp::peer_packet(conn_id, msg::BATTLE_INPUT, &[]);
            let _ = k.send(&pkt);
            let _ = k.update(now_ms());
        }
        let _ = frames_tx;
        Ok(Self { kcp, conn_id, frames: Arc::new(tokio::sync::Mutex::new(frames_rx)) })
    }

    pub async fn send_input(&self, dir_x: f32, dir_y: f32) -> anyhow::Result<()> {
        let payload = grimoire_pb::pb::encode_message(&grimoire_pb::pb::BattleInputReq { dir_x, dir_y });
        let pkt = udp::peer_packet(self.conn_id, msg::BATTLE_INPUT, &payload);
        self.kcp.lock().unwrap().send(&pkt)?;
        Ok(())
    }

    /// 接收一条应用消息（KCP 解包后的 peer_packet）。
    pub async fn recv_push(&self) -> anyhow::Result<Option<(u32, u32, Bytes)>> {
        let mut rx = self.frames.lock().await;
        match rx.recv().await {
            Some(pkt) => Ok(udp::parse_peer_packet(&pkt).map(|(_, msg_id, p)| (msg_id, 0, Bytes::copy_from_slice(p)))),
            None => Ok(None),
        }
    }
}

/// 编码 protobuf 消息
pub fn enc<T: prost::Message>(m: &T) -> Vec<u8> {
    grimoire_pb::pb::encode_message(m)
}

/// 解码 protobuf 消息
pub fn dec<T: prost::Message + Default>(payload: &[u8]) -> anyhow::Result<T> {
    Ok(grimoire_pb::pb::decode_message(payload)?)
}
