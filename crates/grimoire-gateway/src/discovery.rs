//! 服务发现：按玩法域查询注册中心，缓存节点列表与 gRPC 通道。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use grimoire_common::msg;
use grimoire_pb::pb::{registry_service_client::RegistryServiceClient, NodeInfo};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// 玩法域 -> 服务名
pub fn service_for_domain(domain: u32) -> Option<&'static str> {
    Some(match domain {
        msg::DOMAIN_ROOM => grimoire_common::svc::ROOM,
        msg::DOMAIN_BATTLE => grimoire_common::svc::BATTLE,
        msg::DOMAIN_CARD => grimoire_common::svc::CARD,
        _ => return None,
    })
}

pub struct Discovery {
    reg: RegistryServiceClient<Channel>,
    /// 玩法域 -> 节点列表
    cache: DashMap<u32, Vec<NodeInfo>>,
    last_fetch: RwLock<HashMap<u32, Instant>>,
    cache_ttl: Duration,
    /// 轮询游标
    rr: AtomicU64,
    /// 服务地址 -> 已建立的 gRPC 通道
    channels: DashMap<String, Channel>,
}

impl Discovery {
    pub async fn new(registry_addr: String) -> anyhow::Result<Arc<Self>> {
        let reg = RegistryServiceClient::connect(format!("http://{registry_addr}")).await?;
        Ok(Arc::new(Self {
            reg,
            cache: DashMap::new(),
            last_fetch: RwLock::new(HashMap::new()),
            cache_ttl: Duration::from_secs(3),
            rr: AtomicU64::new(0),
            channels: DashMap::new(),
        }))
    }

    /// 获取某玩法域的一个可用节点（轮询）。
    pub async fn resolve(&self, domain: u32) -> Option<NodeInfo> {
        let svc_name = service_for_domain(domain)?;
        let cached = self.cache.get(&domain).map(|v| v.clone());
        let fresh = match cached {
            Some(nodes) if !nodes.is_empty() => {
                let last = self.last_fetch.read().await.get(&domain).copied().unwrap_or(Instant::now());
                if last.elapsed() < self.cache_ttl {
                    nodes
                } else {
                    self.fetch(domain, svc_name).await?
                }
            }
            _ => self.fetch(domain, svc_name).await?,
        };
        if fresh.is_empty() {
            warn!("no {} instance registered", svc_name);
            return None;
        }
        let idx = (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % fresh.len();
        Some(fresh[idx].clone())
    }

    async fn fetch(&self, domain: u32, svc_name: &str) -> Option<Vec<NodeInfo>> {
        let mut last = self.last_fetch.write().await;
        let reply = self.reg.clone().discover(tonic::Request::new(
            grimoire_pb::pb::DiscoverRequest { service: svc_name.to_string() },
        )).await;
        match reply {
            Ok(resp) => {
                let nodes = resp.into_inner().nodes;
                debug!("discovered {} nodes for {}", nodes.len(), svc_name);
                self.cache.insert(domain, nodes.clone());
                last.insert(domain, Instant::now());
                Some(nodes)
            }
            Err(e) => {
                warn!("discover {} failed: {}", svc_name, e);
                None
            }
        }
    }

    /// 获取到某节点地址的 gRPC 通道（懒连接 + 缓存）。
    pub async fn channel_for(&self, addr: &str) -> Option<Channel> {
        if let Some(ch) = self.channels.get(addr) {
            return Some(ch.clone());
        }
        match Channel::from_shared(format!("http://{}", addr))
            .ok()?
            .connect()
            .await
        {
            Ok(ch) => {
                self.channels.insert(addr.to_string(), ch.clone());
                Some(ch)
            }
            Err(e) => {
                warn!("connect {} failed: {}", addr, e);
                None
            }
        }
    }
}
