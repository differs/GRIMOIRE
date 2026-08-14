use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use grimoire_pb::pb::{
    registry_service_server::{RegistryService, RegistryServiceServer},
    DiscoverReply, DiscoverRequest, HeartbeatReply, HeartbeatRequest, NodeInfo, RegisterReply,
    RegisterRequest, UnregisterReply, UnregisterRequest,
};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8500")]
    listen: String,
}

#[derive(Clone)]
struct NodeRecord {
    addr: String,
    meta: HashMap<String, String>,
    /// 绝对过期时刻（纳秒），续约即重置
    expires_at: u128,
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

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// 注册中心：内存实现，服务名 -> (node_id -> NodeRecord)
struct RegistryState {
    services: HashMap<String, HashMap<String, NodeRecord>>,
}

type Shared = Arc<Mutex<RegistryState>>;

#[derive(Clone)]
struct RegistrySvc {
    state: Shared,
    default_ttl: i32,
}

#[tonic::async_trait]
impl RegistryService for RegistrySvc {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let req = request.into_inner();
        let mut st = self.state.lock().await;
        let ttl = if req.ttl_secs > 0 { req.ttl_secs } else { self.default_ttl };
        st.services
            .entry(req.service.clone())
            .or_default()
            .insert(req.node_id.clone(), NodeRecord::new(req.addr.clone(), req.meta.clone(), ttl));
        info!("registered {} {}", req.service, req.node_id);
        Ok(Response::new(RegisterReply { ok: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let req = request.into_inner();
        let mut st = self.state.lock().await;
        if let Some(node) = st.services.get_mut(&req.service).and_then(|m| m.get_mut(&req.node_id)) {
            node.refresh(self.default_ttl);
            Ok(Response::new(HeartbeatReply { ok: true }))
        } else {
            Ok(Response::new(HeartbeatReply { ok: false }))
        }
    }

    async fn discover(
        &self,
        request: Request<DiscoverRequest>,
    ) -> Result<Response<DiscoverReply>, Status> {
        let req = request.into_inner();
        let st = self.state.lock().await;
        let nodes = st
            .services
            .get(&req.service)
            .map(|m| {
                m.iter()
                    .map(|(id, n)| NodeInfo {
                        node_id: id.clone(),
                        addr: n.addr.clone(),
                        meta: n.meta.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Response::new(DiscoverReply { nodes }))
    }

    async fn unregister(
        &self,
        request: Request<UnregisterRequest>,
    ) -> Result<Response<UnregisterReply>, Status> {
        let req = request.into_inner();
        let mut st = self.state.lock().await;
        if let Some(m) = st.services.get_mut(&req.service) {
            m.remove(&req.node_id);
        }
        Ok(Response::new(UnregisterReply { ok: true }))
    }
}

/// 后台过期清理：周期扫描移除过期节点
async fn sweeper(state: Shared, interval: Duration) {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let state: Shared = Arc::new(Mutex::new(RegistryState { services: HashMap::new() }));
    tokio::spawn(sweeper(state.clone(), Duration::from_secs(1)));

    let svc = RegistrySvc { state, default_ttl: 10 };
    info!("registry listening on {}", args.listen);
    Server::builder()
        .add_service(RegistryServiceServer::new(svc))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
