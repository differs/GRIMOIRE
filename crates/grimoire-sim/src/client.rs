//! 客户端 SDK：连接网关、收发帧、并发请求、push 订阅、心跳。

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use grimoire_net::{Frame, FrameCodec, PType};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct Client {
    tx: mpsc::Sender<Frame>,
    responses: Arc<DashMap<u32, oneshot::Sender<Frame>>>,
    seq: AtomicU32,
    pub pushes: broadcast::Receiver<Frame>,
    conn_id_label: u32,
}

impl Client {
    pub async fn connect(addr: &str, conn_id_label: u32) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await.context("connect gateway")?;
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
                            let _ = push_tx.send(f);
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

        Ok(Self { tx, responses, seq: AtomicU32::new(1), pushes, conn_id_label })
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
            seq: AtomicU32::new(self.seq.load(Ordering::Relaxed)),
            pushes: self.pushes.resubscribe(),
            conn_id_label: self.conn_id_label,
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
