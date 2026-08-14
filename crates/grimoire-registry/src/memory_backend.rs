//! 内存后端（无外部依赖时的兜底）：服务名 -> (node_id -> NodeRecord)。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use grimoire_pb::pb::NodeInfo;
use tokio::sync::Mutex;
use tracing::warn;

pub type SharedMemory = Arc<Mutex<RegistryState>>;

#[derive(Default)]
pub struct RegistryState {
    pub services: HashMap<String, HashMap<String, NodeRecord>>,
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[derive(Clone)]
pub struct NodeRecord {
    pub addr: String,
    pub meta: HashMap<String, String>,
    pub expires_at: u128,
}

impl NodeRecord {
    fn new(addr: String, meta: HashMap<String, String>, ttl_secs: i32) -> Self {
        Self {
            addr,
            meta,
            expires_at: now_ns() + (ttl_secs.max(1) as u128) * 1_000_000_000,
        }
    }
    fn refresh(&mut self, ttl_secs: i32) {
        self.expires_at = now_ns() + (ttl_secs.max(1) as u128) * 1_000_000_000;
    }
}

impl RegistryState {
    pub fn insert(&mut self, service: &str, node_id: &str, addr: String, meta: HashMap<String, String>, ttl: i32) {
        self.services
            .entry(service.to_string())
            .or_default()
            .insert(node_id.to_string(), NodeRecord::new(addr, meta, ttl));
    }

    pub fn heartbeat(&mut self, service: &str, node_id: &str, ttl: i32) -> bool {
        match self.services.get_mut(service).and_then(|m| m.get_mut(node_id)) {
            Some(n) => {
                n.refresh(ttl);
                true
            }
            None => false,
        }
    }

    pub fn discover(&self, service: &str) -> Vec<NodeInfo> {
        self.services
            .get(service)
            .map(|m| {
                m.iter()
                    .map(|(id, n)| NodeInfo {
                        node_id: id.clone(),
                        addr: n.addr.clone(),
                        meta: n.meta.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn remove(&mut self, service: &str, node_id: &str) {
        if let Some(m) = self.services.get_mut(service) {
            m.remove(node_id);
        }
    }
}

/// 后台过期清理：周期扫描移除过期节点。
pub async fn sweeper(state: SharedMemory, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        let now = now_ns();
        let mut st = state.lock().await;
        for (svc, nodes) in st.services.iter_mut() {
            let before = nodes.len();
            nodes.retain(|_, n| n.expires_at > now);
            if nodes.len() != before {
                warn!("{} nodes expired, remaining {}", svc, nodes.len());
            }
        }
    }
}
