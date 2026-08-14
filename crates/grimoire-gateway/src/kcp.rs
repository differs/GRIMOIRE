//! KCP 可靠 UDP 层：为实时对战提供可靠有序传输（快重传 + 拥塞控制）。
//!
//! 与裸 UDP 通道并存，按数据报首字节自动识别：
//!   - 以 'MU' 开头 → 裸 UDP（既有 battle-udp 通道）
//!   - 其他 → KCP 包（前 4 字节 = conv = conn_id）
//! KCP 会话以 conn_id 为键；会话内部载荷仍是 peer_packet 格式。

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use kcp::Kcp;
use tokio::net::UdpSocket;
use tracing::{debug, warn};

/// KCP 输出回调：同步写入 UDP（try_send_to 非阻塞）
pub struct UdpOutput {
    pub sock: Arc<UdpSocket>,
    pub addr: SocketAddr,
}

impl Write for UdpOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.sock.try_send_to(buf, self.addr) {
            Ok(n) => Ok(n),
            Err(e) => Err(std::io::Error::other(e)),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct KcpSession {
    pub inner: std::sync::Mutex<Kcp<UdpOutput>>,
}

impl KcpSession {
    pub fn new(conv: u32, sock: Arc<UdpSocket>, addr: SocketAddr, interval: i32, resend: i32, nc: bool) -> Arc<Self> {
        let output = UdpOutput { sock, addr };
        let mut kcp = Kcp::new(conv, output);
        // nodelay: 低延迟模式（interval ms 间隔、resend 次快重传、可选流控）
        kcp.set_nodelay(true, interval, resend, nc);
        kcp.set_wndsize(128, 128);
        Arc::new(Self { inner: std::sync::Mutex::new(kcp) })
    }

    /// 向 KCP 会话写入一条应用数据（由 driver 的 update 负责发出）。
    pub fn send_packet(&self, data: &[u8]) -> bool {
        self.inner.lock().unwrap().send(data).is_ok()
    }
}

fn now_ms() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u32
}

/// 驱动一个 KCP 会话：10ms 周期 update（触发重传/发送）+ 排空接收缓冲。
pub fn spawn_kcp_driver(
    sessions: Arc<DashMap<u32, Arc<KcpSession>>>,
    conv: u32,
    session: Arc<KcpSession>,
    on_data: impl Fn(Vec<u8>) + Send + Sync + 'static,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            tick.tick().await;
            let mut received = Vec::new();
            {
                let mut k = session.inner.lock().unwrap();
                if let Err(e) = k.update(now_ms()) {
                    debug!("kcp {} update err: {}", conv, e);
                }
                loop {
                    let mut buf = vec![0u8; 4096];
                    match k.recv(&mut buf) {
                        Ok(n) => received.push(buf[..n].to_vec()),
                        Err(_) => break,
                    }
                }
            }
            for pkt in received {
                on_data(pkt);
            }
            // 会话被移除或已被替换时退出
            let gone = sessions
                .get(&conv)
                .map(|s| !Arc::ptr_eq(&s, &session))
                .unwrap_or(true);
            if gone {
                break;
            }
        }
        warn!("kcp session {} driver exited", conv);
    });
}
