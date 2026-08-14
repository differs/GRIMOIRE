//! 服务注册 + 后台心跳续约。
//!
//! 约定：注册后由框架在 ttl/3 周期续约；进程退出由注册中心 TTL 过期兜底清理。
//! （正式项目应配合信号量优雅反注册，这里用 etcd 同款"租约过期"模型。）

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use grimoire_pb::pb::{registry_service_client::RegistryServiceClient, RegisterRequest};
use tracing::{debug, info, warn};

/// 注册到注册中心并启动心跳协程。返回后即完成注册，心跳持续到进程退出。
pub async fn register_and_heartbeat(
    registry_addr: &str,
    service: &str,
    node_id: &str,
    addr: &str,
    meta: HashMap<String, String>,
    ttl_secs: i32,
) -> anyhow::Result<()> {
    let mut client = RegistryServiceClient::connect(format!("http://{registry_addr}"))
        .await
        .context("connect registry")?;

    client
        .register(RegisterRequest {
            service: service.to_string(),
            node_id: node_id.to_string(),
            addr: addr.to_string(),
            meta,
            ttl_secs,
        })
        .await
        .context("register")?;
    info!("registered {} {} at {}", service, node_id, addr);

    let period = Duration::from_secs((ttl_secs.max(3) / 3).max(1) as u64);
    let addr_owned = addr.to_string();
    let svc_owned = service.to_string();
    let node_owned = node_id.to_string();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            match tokio::time::timeout(
                Duration::from_secs(period.as_secs()),
                client.heartbeat(grimoire_pb::pb::HeartbeatRequest {
                    service: svc_owned.clone(),
                    node_id: node_owned.clone(),
                }),
            )
            .await
            {
                Ok(Ok(r)) => {
                    let ok = r.into_inner().ok;
                    debug!("heartbeat {} {} sent -> ok={}", svc_owned, node_owned, ok);
                    if !ok {
                        warn!("{} {} heartbeat rejected, re-registering", svc_owned, node_owned);
                        let _ = client
                            .register(RegisterRequest {
                                service: svc_owned.clone(),
                                node_id: node_owned.clone(),
                                addr: addr_owned.clone(),
                                meta: Default::default(),
                                ttl_secs,
                            })
                            .await;
                    }
                }
                Ok(Err(e)) => warn!("heartbeat {} {} failed: {}", svc_owned, node_owned, e),
                Err(_) => warn!("heartbeat {} {} TIMEOUT (stalled)", svc_owned, node_owned),
            }
        }
    });
    Ok(())
}
