//! 网关 push/kick 客户端：按 conn_id 中编码的网关 ID 自动发现目标网关。
//!
//! 多活网关下，每个网关把自己的 gRPC 地址注册到注册中心（service="gateway"，meta.id=网关ID）。
//! 本类根据 conn_id 高 8 位定位网关，懒建立并缓存 gRPC 通道。

use anyhow::Context;
use dashmap::DashMap;
use grimoire_common::conn;
use grimoire_pb::pb::{
    gateway_service_client::GatewayServiceClient, DiscoverRequest, KickRequest,
    registry_service_client::RegistryServiceClient, PushRequest,
};
use tonic::transport::Channel;
use tracing::warn;

#[derive(Clone)]
pub struct Pusher {
    reg: RegistryServiceClient<Channel>,
    /// 网关 ID -> 已建立的 gRPC 通道
    channels: DashMap<u8, Channel>,
}

impl Pusher {
    pub async fn connect(registry_addr: &str) -> anyhow::Result<Self> {
        let reg = RegistryServiceClient::connect(format!("http://{registry_addr}"))
            .await
            .context("connect registry")?;
        Ok(Self { reg, channels: DashMap::new() })
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
                warn!("gateway id {} not found among {} nodes: {:?}", gw, nodes.len(),
                    nodes.iter().map(|n| format!("{}@{}", n.node_id, n.addr)).collect::<Vec<_>>());
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
            Err(_) => {
                warn!("bad gateway addr {}", node.addr);
                return None;
            }
        };
        self.channels.insert(gw, ch.clone());
        Some(ch)
    }

    /// 向指定连接推送一条服务端消息（TCP）。
    pub async fn push(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>) -> bool {
        let Some(ch) = self.channel_for(conn_id).await else { return false };
        let mut c = GatewayServiceClient::new(ch);
        c.push(PushRequest { conn_id, msg_id, payload, udp: false })
            .await
            .map(|r| r.into_inner().ok)
            .unwrap_or(false)
    }

    /// 向指定连接推送（要求走 UDP 低延迟通道；未绑定 UDP 时由网关回退 TCP）。
    pub async fn push_udp(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>) -> bool {
        let Some(ch) = self.channel_for(conn_id).await else { return false };
        let mut c = GatewayServiceClient::new(ch);
        c.push(PushRequest { conn_id, msg_id, payload, udp: true })
            .await
            .map(|r| r.into_inner().ok)
            .unwrap_or(false)
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
