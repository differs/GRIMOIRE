//! 服务发现：按玩法域查询注册中心，缓存节点列表与 gRPC 通道。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use grimoire_common::msg;
pub use grimoire_pb::pb::NodeInfo;
use grimoire_pb::pb::registry_service_client::RegistryServiceClient;
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
    /// 玩法域 -> 节点列表（Arc 缓存，避免每次请求克隆 String）
    cache: DashMap<u32, Vec<Arc<NodeInfo>>>,
    last_fetch: RwLock<HashMap<u32, Instant>>,
    cache_ttl: Duration,
    /// 轮询游标
    rr: AtomicU64,
    /// 服务地址 -> 已建立的 gRPC 通道
    /// 节点地址 -> h2 连接池（多连接分摊流锁竞争）
    channels: DashMap<String, Vec<Channel>>,
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

    /// 获取某玩法域的一个可用节点（轮询，无克隆开销）。
    /// 注意：任何 await 期间都不得持有 DashMap Ref / 锁，否则与 fetch 相互等待死锁。
    pub async fn resolve(&self, domain: u32) -> Option<Arc<NodeInfo>> {
        let svc_name = service_for_domain(domain)?;
        // 先取缓存时间（临时锁，语句结束即释放），再查缓存（临时 Ref，无 await）
        let last = self.last_fetch.read().await.get(&domain).copied().unwrap_or(Instant::now());
        let need_fetch = match self.cache.get(&domain) {
            Some(nodes) if !nodes.is_empty() => last.elapsed() >= self.cache_ttl,
            _ => true,
        };
        if need_fetch {
            self.fetch(domain, svc_name).await?;
        }
        let nodes = self.cache.get(&domain)?;
        if nodes.is_empty() {
            warn!("no {} instance registered", svc_name);
            return None;
        }
        let idx = (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % nodes.len();
        nodes.get(idx).cloned()
    }

    async fn fetch(&self, domain: u32, svc_name: &str) -> Option<()> {
        // 不在锁内做网络调用；两并发 fetch 幂等可重入
        let reply = self.reg.clone().discover(tonic::Request::new(
            grimoire_pb::pb::DiscoverRequest { service: svc_name.to_string() },
        )).await;
        match reply {
            Ok(resp) => {
                let nodes: Vec<Arc<NodeInfo>> = resp.into_inner().nodes.into_iter().map(Arc::new).collect();
                debug!("discovered {} nodes for {}", nodes.len(), svc_name);
                self.cache.insert(domain, nodes);
                self.last_fetch.write().await.insert(domain, Instant::now());
                Some(())
            }
            Err(e) => {
                warn!("discover {} failed: {}", svc_name, e);
                None
            }
        }
    }

    /// 获取到某节点地址的 gRPC 通道（懒连接 + 连接池轮询）。
    /// 单条 h2 连接在高并发 unary RPC 下流锁竞争明显，池化后可线性扩展。
    pub async fn channel_for(&self, addr: &str) -> Option<Channel> {
        const POOL_SIZE: usize = 4;
        if let Some(pool) = self.channels.get(addr) {
            let idx = (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % pool.len();
            return pool.get(idx).cloned();
        }
        let mut pool = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            match Channel::from_shared(format!("http://{}", addr))
                .ok()?
                .connect()
                .await
            {
                Ok(ch) => pool.push(ch),
                Err(e) => {
                    warn!("connect {} failed: {}", addr, e);
                    return None;
                }
            }
        }
        let addr_owned = addr.to_string();
        let idx = (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % pool.len();
        let ch = pool[idx].clone();
        self.channels.insert(addr_owned, pool);
        Some(ch)
    }
}
