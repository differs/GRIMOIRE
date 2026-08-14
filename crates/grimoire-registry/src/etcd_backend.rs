//! etcd 后端：用 etcd 租约/键值实现注册、心跳续约、前缀发现。
//!
//! 数据模型（key 前缀 `/grimoire/{service}/{node_id}`）：
//!   - Register:  申请租约 + 写入节点 JSON {addr, meta}
//!   - Heartbeat: 触发 etcd 租约续约 + 校验 key 仍存在（租约过期自动删 key）
//!   - Discover:  Range 前缀读取
//!   - Unregister: 删 key + 撤销租约
//!   - TTL 兜底:  服务若失联，etcd 租约自动过期删除 key（无需本地扫描）

use std::collections::HashMap;

use anyhow::Context;
use dashmap::DashMap;
use etcd_client::{Client as EtcdClient, ConnectOptions, DeleteOptions, GetOptions, PutOptions};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

const KEY_PREFIX: &str = "/grimoire/";

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeValue {
    pub addr: String,
    pub meta: HashMap<String, String>,
}

struct LeaseState {
    lease_id: i64,
    renew_tx: tokio::sync::mpsc::Sender<()>,
}

pub struct EtcdBackend {
    client: EtcdClient,
    /// (service, node_id) -> 租约状态
    leases: DashMap<(String, String), LeaseState>,
}

impl EtcdBackend {
    pub async fn connect(endpoints: Vec<String>) -> anyhow::Result<Self> {
        let client = EtcdClient::connect(endpoints, Some(ConnectOptions::default().with_timeout(std::time::Duration::from_secs(5))))
            .await
            .context("connect etcd")?;
        Ok(Self { client, leases: DashMap::new() })
    }

    fn key(service: &str, node_id: &str) -> String {
        format!("{}{}/{}", KEY_PREFIX, service, node_id)
    }
    fn prefix(service: &str) -> String {
        format!("{}{}/", KEY_PREFIX, service)
    }

    pub async fn register(
        &self,
        service: &str,
        node_id: &str,
        addr: &str,
        meta: HashMap<String, String>,
        ttl_secs: i32,
    ) -> anyhow::Result<()> {
        // 撤销旧租约（覆盖注册场景）
        let key0 = (service.to_string(), node_id.to_string());
        let old_lease = self.leases.get(&key0).map(|old| old.lease_id);
        if let Some(lease_id) = old_lease {
            let mut c = self.client.clone();
            let _ = c.lease_revoke(lease_id).await;
            self.leases.remove(&key0);
        }
        let ttl = ttl_secs.max(1) as i64;
        let mut c = self.client.clone();
        let lease = c.lease_grant(ttl, None).await.context("lease_grant")?;
        let lease_id = lease.id();

        let value = serde_json::to_string(&NodeValue { addr: addr.to_string(), meta })?;
        c.put(
                Self::key(service, node_id),
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
            .context("put")?;

        // 每节点一个租约续约协程：由 Heartbeat RPC 驱动
        let (renew_tx, renew_rx) = tokio::sync::mpsc::channel::<()>(8);
        spawn_keeper(self.client.clone(), lease_id, renew_rx);
        self.leases.insert((service.to_string(), node_id.to_string()), LeaseState { lease_id, renew_tx });
        info!("registered {} {} at {}", service, node_id, addr);
        Ok(())
    }

    /// 触发续约，并校验 etcd 中 key 仍存在（租约过期则 key 已被自动删除）。
    pub async fn heartbeat(&self, service: &str, node_id: &str) -> bool {
        let Some(state) = self.leases.get(&(service.to_string(), node_id.to_string())) else {
            return false;
        };
        let _ = state.renew_tx.try_send(());
        drop(state);
        // 权威校验：key 是否仍在 etcd 中
        let mut c = self.client.clone();
        match c.get(Self::key(service, node_id), None).await {
            Ok(resp) => {
                let alive = !resp.kvs().is_empty();
                debug!("heartbeat {} {} -> {}", service, node_id, if alive { "ok" } else { "lease-lost" });
                alive
            }
            Err(e) => {
                warn!("heartbeat {} {} etcd get error: {}", service, node_id, e);
                false
            }
        }
    }

    pub async fn discover(&self, service: &str) -> Vec<(String, NodeValue)> {
        let mut c = self.client.clone();
        match c.get(Self::prefix(service), Some(GetOptions::new().with_prefix())).await {
            Ok(resp) => resp
                .kvs()
                .iter()
                .filter_map(|kv| {
                    let node_id = kv.key_str().ok()?.strip_prefix(&Self::prefix(service))?.to_string();
                    let value: NodeValue = serde_json::from_str(kv.value_str().ok()?).ok()?;
                    Some((node_id, value))
                })
                .collect(),
            Err(e) => {
                warn!("discover {} error: {}", service, e);
                Vec::new()
            }
        }
    }

    pub async fn unregister(&self, service: &str, node_id: &str) {
        let k = (service.to_string(), node_id.to_string());
        let mut c = self.client.clone();
        if let Some(state) = self.leases.remove(&k) {
            let _ = c.lease_revoke(state.1.lease_id).await;
        }
        let _ = c.delete(Self::key(service, node_id), Some(DeleteOptions::default())).await;
    }
}

/// 租约续约协程：等待 Heartbeat RPC 信号，向 etcd 发送 keep_alive。
fn spawn_keeper(mut client: EtcdClient, lease_id: i64, mut renew_rx: tokio::sync::mpsc::Receiver<()>) {
    tokio::spawn(async move {
        let (mut sender, mut stream) = match client.lease_keep_alive(lease_id).await {
            Ok(v) => v,
            Err(e) => {
                warn!("lease_keep_alive {} failed: {}", lease_id, e);
                return;
            }
        };
        while renew_rx.recv().await.is_some() {
            if sender.keep_alive().await.is_err() {
                warn!("keep_alive {} send failed", lease_id);
                break;
            }
            // 消费响应，保持流活跃；流关闭说明租约已失效
            match stream.message().await {
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        debug!("keeper for lease {} exited", lease_id);
    });
}
