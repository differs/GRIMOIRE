//! 网关 push/kick 客户端封装。

use anyhow::Context;
use grimoire_pb::pb::{
    gateway_service_client::GatewayServiceClient, KickRequest, PushRequest,
};
use tonic::transport::Channel;

#[derive(Clone)]
pub struct Pusher {
    inner: GatewayServiceClient<Channel>,
}

impl Pusher {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let inner = GatewayServiceClient::connect(format!("http://{addr}"))
            .await
            .context("connect gateway")?;
        Ok(Self { inner })
    }

    /// 向指定连接推送一条服务端消息。
    pub async fn push(&self, conn_id: u32, msg_id: u32, payload: Vec<u8>) -> bool {
        let mut c = self.inner.clone();
        c.push(PushRequest {
            conn_id,
            msg_id,
            payload,
        })
        .await
        .map(|r| r.into_inner().ok)
        .unwrap_or(false)
    }

    pub async fn kick(&self, conn_id: u32, reason: u32, detail: &str) -> bool {
        let mut c = self.inner.clone();
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
