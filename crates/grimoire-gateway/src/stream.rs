//! 网关 -> 业务服务 的双向流多路复用（高性能路径）。
//!
//! 每个 (玩法域, 节点) 建立一条持久 h2 双向流，所有客户端请求/响应复用同一条流，
//! 消除"每条消息一次 unary RPC"的 h2 流创建/销毁开销。
//! 响应按 (conn_id, seq) 关联回对应客户端连接。

use std::sync::Arc;

use dashmap::DashMap;
use grimoire_pb::pb::{service_bridge_client::ServiceBridgeClient, ForwardReply, ForwardRequest};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

use crate::discovery::{Discovery, NodeInfo};

pub struct StreamConn {
    /// 请求下发通道（forward 任务发送，tonic 自动驱动上行流）
    req_tx: mpsc::Sender<ForwardRequest>,
}

pub struct Streams {
    pub discovery: Arc<Discovery>,
    /// (玩法域, 节点地址) -> 持久流
    streams: DashMap<(u32, String), Arc<StreamConn>>,
    /// (conn_id, seq) -> 等待中的响应
    pub pending: DashMap<(u32, u32), oneshot::Sender<ForwardReply>>,
    /// 串行化流建立（建流时有网络调用，避免并发重复建流）
    open_lock: Mutex<()>,
}

impl Streams {
    pub fn new(discovery: Arc<Discovery>) -> Arc<Self> {
        Arc::new(Self {
            discovery,
            streams: DashMap::new(),
            pending: DashMap::new(),
            open_lock: Mutex::new(()),
        })
    }

    /// 获取（必要时建立）到某节点的持久流。
    pub async fn get_or_open(
        self: &Arc<Self>,
        domain: u32,
        node: &Arc<NodeInfo>,
    ) -> Result<Arc<StreamConn>, String> {
        let key = (domain, node.addr.clone());
        if let Some(c) = self.streams.get(&key) {
            return Ok(c.clone());
        }
        // 串行化建流窗口
        let _guard = self.open_lock.lock().await;
        if let Some(c) = self.streams.get(&key) {
            return Ok(c.clone());
        }

        let ch = self
            .discovery
            .channel_for(&node.addr)
            .await
            .ok_or_else(|| format!("connect {}", node.addr))?;
        let mut client = ServiceBridgeClient::new(ch);
        let (req_tx, req_rx) = mpsc::channel::<ForwardRequest>(256);
        // 上行流直接产出 ForwardRequest（tonic 内部按成功消息编码）
        let outbound = ReceiverStream::new(req_rx);
        let resp = client
            .bridge_stream(outbound)
            .await
            .map_err(|e| e.to_string())?;
        let mut replies = resp.into_inner();

        let conn = Arc::new(StreamConn { req_tx });
        // 读侧：把响应按 (conn_id, seq) 路由给等待的请求
        let key2 = key.clone();
        let conn2 = conn.clone();
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                match replies.message().await {
                    Ok(Some(reply)) => {
                        if let Some((_, tx)) = s.pending.remove(&(reply.conn_id, reply.seq)) {
                            let _ = tx.send(reply);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("bridge stream to {} closed: {}", key2.1, e);
                        break;
                    }
                }
            }
            // 流失效：仅当仍指向本连接时移除，后续请求自动重连
            s.streams.remove_if(&key2, |_, v| Arc::ptr_eq(v, &conn2));
        });

        self.streams.insert(key, conn.clone());
        debug!("opened bridge stream to {} (domain 0x{:X})", node.addr, domain);
        Ok(conn)
    }

    /// 向流内发送一个请求。
    pub async fn send(&self, conn: &Arc<StreamConn>, req: ForwardRequest) -> Result<(), String> {
        conn.req_tx
            .send(req)
            .await
            .map_err(|_| "bridge stream closed".to_string())
    }
}
