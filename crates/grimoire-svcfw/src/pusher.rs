//! 网关 push/kick 客户端：按 conn_id 中编码的网关 ID 自动发现目标网关。
//!
//! 多活网关下，每个网关把自己的 gRPC 地址注册到注册中心（service="gateway"，meta.id=网关ID）。
//! 本类根据 conn_id 高 8 位定位网关，懒建立并缓存 gRPC 通道。
//!
//! 高性能路径：对每个网关建立一条持久双向流（PushStream），所有推送（帧同步/状态广播）
//! 复用同一条流，避免每帧一次 unary RPC 的 h2 流开销。推送为即发即忘（try_send），
//! 通道满时丢弃（帧同步"最新状态优先"语义天然容忍丢帧）。

use anyhow::Context;
use dashmap::DashMap;
use grimoire_common::conn;
use grimoire_pb::pb::{
    gateway_service_client::GatewayServiceClient, DiscoverRequest, KickRequest,
    registry_service_client::RegistryServiceClient, PushRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::warn;

/// 到某网关的持久推送流
struct PushStream {
    req_tx: mpsc::Sender<PushRequest>,
}

#[derive(Clone)]
pub struct Pusher {
    reg: RegistryServiceClient<Channel>,
    /// 网关 ID -> 已建立的 gRPC 通道
    channels: DashMap<u8, Channel>,
    /// 网关 ID -> 持久推送流
    streams: DashMap<u8, std::sync::Arc<PushStream>>,
    /// 串行化建流
    open_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl Pusher {
    pub async fn connect(registry_addr: &str) -> anyhow::Result<Self> {
        let reg = RegistryServiceClient::connect(format!("http://{registry_addr}"))
            .await
            .context("connect registry")?;
        Ok(Self {
            reg,
            channels: DashMap::new(),
            streams: DashMap::new(),
            open_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// 依据 conn_id 定位并连接其所在网关。
    async fn channel_for(&self, conn_id: u32) -> Option<Channel> {
        let gw = conn::gateway_id_of(conn_id);
        if let Some(ch) = self.channels.get(&gw) {
            return Some(ch.clone());
        }
        let nodes = match self
            .reg
            .clone()
            .discover(DiscoverRequest { service: grimoire_common::svc::GATEWAY.to_string() })
            .await
        {
            Ok(r) => r.into_inner().nodes,
            Err(e) => {
                warn!("discover gateways failed: {}", e);
                return None;
            }
        };
        let node = match nodes.iter().find(|n| {
            n.meta.get("id").and_then(|v| v.parse::<u8>().ok()) == Some(gw)
        }) {
            Some(n) => n,
            None => {
                warn!("gateway id {} not found among {} nodes", gw, nodes.len());
                return None;
            }
        };
        let ch = match Channel::from_shared(format!("http://{}", node.addr)) {
            Ok(c) => match c.connect().await {
                Ok(ch) => ch,
                Err(e) => {
                    warn!("cannot connect gateway {} at {}: {}", node.node_id, node.addr, e);
                    return None;
                }
            },
            Err(_) => return None,
        };
        self.channels.insert(gw, ch.clone());
        Some(ch)
    }

    /// 获取（必要时建立）到 conn 所在网关的持久推送流。
    async fn stream_for(&self, conn_id: u32) -> Option<std::sync::Arc<PushStream>> {
        let gw = conn::gateway_id_of(conn_id);
        if let Some(s) = self.streams.get(&gw) {
            return Some(s.clone());
        }
        let _guard = self.open_lock.lock().await;
        if let Some(s) = self.streams.get(&gw) {
            return Some(s.clone());
        }
        let ch = self.channel_for(conn_id).await?;
        let mut client = GatewayServiceClient::new(ch);
        let (req_tx, req_rx) = mpsc::channel::<PushRequest>(1024);
        let outbound = ReceiverStream::new(req_rx);
        let resp = match client.push_stream(outbound).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                warn!("open push stream to gw {} failed: {}", gw, e);
                return None;
            }
        };
        let mut replies = resp;
        let s = std::sync::Arc::new(PushStream { req_tx });
        // 排空回包；流失效时移除条目，下次推送自动重建
        let streams = self.streams.clone();
        let s2 = s.clone();
        tokio::spawn(async move {
            while let Ok(Some(_)) = replies.message().await {}
            streams.remove_if(&gw, |_, v| std::sync::Arc::ptr_eq(v, &s2));
        });
        self.streams.insert(gw, s.clone());
        Some(s)
    }

    /// 即发即忘推送：复用持久流，通道满则丢弃（帧同步容忍）。
    async fn push_via(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>, udp: bool) -> bool {
        match self.stream_for(conn_id).await {
            Some(s) => s.req_tx.try_send(PushRequest { conn_id, msg_id, payload, udp }).is_ok(),
            None => false,
        }
    }

    /// 向指定连接推送一条服务端消息（TCP）。
    pub async fn push(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>) -> bool {
        self.push_via(conn_id, msg_id, payload, false).await
    }

    /// 向指定连接推送（要求走 UDP 低延迟通道；未绑定 UDP 时由网关回退 TCP）。
    pub async fn push_udp(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>) -> bool {
        self.push_via(conn_id, msg_id, payload, true).await
    }

    pub async fn kick(&self, conn_id: u32, reason: u32, detail: &str) -> bool {
        let Some(ch) = self.channel_for(conn_id).await else { return false };
        let mut c = GatewayServiceClient::new(ch);
        c.kick(KickRequest {
            conn_id,
            reason,
            detail: detail.to_string(),
        })
        .await
        .map(|r| r.into_inner().ok)
        .unwrap_or(false)
    }
}
