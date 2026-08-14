//! 服务发现：按玩法域查询注册中心，缓存节点列表与 gRPC 通道。
//!
//! 负载均衡：默认轮询；传入 key（conn_id）时用一致性哈希环选节点 → 会话亲和，
//! 同一连接的请求始终落在同一节点（有内存态的服务必需，否则状态碎片化）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use grimoire_common::msg;
pub use grimoire_pb::pb::NodeInfo;
use grimoire_pb::pb::registry_service_client::RegistryServiceClient;
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

/// 一致性哈希环：每节点 100 个虚拟点，key 就近路由；节点增减只重映射少量连接。
struct ConsistentHash {
    ring: Vec<(u32, usize)>, // (哈希点, 节点下标)
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

impl ConsistentHash {
    fn build(nodes: &[Arc<NodeInfo>]) -> Self {
        let mut ring = Vec::with_capacity(nodes.len() * 100);
        for (idx, n) in nodes.iter().enumerate() {
            for v in 0..100u32 {
                let key = format!("{}#{}", n.node_id, v);
                ring.push((fnv1a(key.as_bytes()), idx));
            }
        }
        ring.sort_unstable_by_key(|(p, _)| *p);
        Self { ring }
    }

    fn pick(&self, key: u32) -> usize {
        if self.ring.is_empty() {
            return 0;
        }
        let h = fnv1a(&key.to_le_bytes());
        match self.ring.binary_search_by_key(&h, |(p, _)| *p) {
            Ok(i) => self.ring[i].1,
            Err(i) => self.ring[i % self.ring.len()].1,
        }
    }
}

struct CacheEntry {
    nodes: Vec<Arc<NodeInfo>>,
    ring: ConsistentHash,
    /// 抓取时刻（纳秒），用于缓存新鲜度判断（原子读，热路径无锁）
    fetched_at: u64,
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

pub struct Discovery {
    reg: RegistryServiceClient<Channel>,
    /// 玩法域 -> 缓存项
    cache: DashMap<u32, CacheEntry>,
    cache_ttl_ns: u64,
    /// 轮询游标
    rr: AtomicU64,
    /// 节点地址 -> h2 连接池（多连接分摊流锁竞争）
    channels: DashMap<String, Vec<Channel>>,
}

impl Discovery {
    pub async fn new(registry_addr: String) -> anyhow::Result<Arc<Self>> {
        let reg = RegistryServiceClient::connect(format!("http://{registry_addr}")).await?;
        Ok(Arc::new(Self {
            reg,
            cache: DashMap::new(),
            cache_ttl_ns: Duration::from_secs(3).as_nanos() as u64,
            rr: AtomicU64::new(0),
            channels: DashMap::new(),
        }))
    }

    /// 获取某玩法域的一个可用节点。
    /// `key = Some(conn_id)` 时按一致性哈希选节点（会话亲和），None 时轮询。
    /// 热路径仅一次 DashMap 读 + 一次哈希/二分，无锁无 await。
    pub async fn resolve(&self, domain: u32, key: Option<u32>) -> Option<Arc<NodeInfo>> {
        let svc_name = service_for_domain(domain)?;
        let now = now_nanos();
        let need_fetch = match self.cache.get(&domain) {
            Some(e) => e.nodes.is_empty() || now.saturating_sub(e.fetched_at) > self.cache_ttl_ns,
            None => true,
        };
        if need_fetch {
            self.fetch(domain, svc_name).await?;
        }
        let e = self.cache.get(&domain)?;
        if e.nodes.is_empty() {
            warn!("no {} instance registered", svc_name);
            return None;
        }
        let idx = match key {
            Some(k) => e.ring.pick(k),
            None => (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % e.nodes.len(),
        };
        e.nodes.get(idx).cloned()
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
                let ring = ConsistentHash::build(&nodes);
                self.cache.insert(domain, CacheEntry { nodes, ring, fetched_at: now_nanos() });
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
