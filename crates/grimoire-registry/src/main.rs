use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use grimoire_pb::pb::{
    registry_service_server::{RegistryService, RegistryServiceServer},
    DiscoverReply, DiscoverRequest, HeartbeatReply, HeartbeatRequest, NodeInfo, RegisterReply,
    RegisterRequest, UnregisterReply, UnregisterRequest,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info};

mod etcd_backend;
mod memory_backend;

use etcd_backend::EtcdBackend;
use memory_backend::SharedMemory;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8500")]
    listen: String,
    /// etcd 地址列表，逗号分隔。缺省则使用内存后端。
    #[arg(long, default_value = "")]
    registry: String,
}

#[derive(Clone)]
enum Backend {
    Memory(SharedMemory),
    Etcd(Arc<EtcdBackend>),
}

#[derive(Clone)]
struct RegistrySvc {
    backend: Backend,
    default_ttl: i32,
}

#[tonic::async_trait]
impl RegistryService for RegistrySvc {
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<RegisterReply>, Status> {
        let req = request.into_inner();
        let ttl = if req.ttl_secs > 0 { req.ttl_secs } else { self.default_ttl };
        match &self.backend {
            Backend::Memory(st) => {
                st.lock().await.insert(&req.service, &req.node_id, req.addr, req.meta, ttl);
                info!("registered {} {}", req.service, req.node_id);
            }
            Backend::Etcd(e) => {
                e.register(&req.service, &req.node_id, &req.addr, req.meta, ttl)
                    .await
                    .map_err(|err| Status::internal(format!("etcd register: {err}")))?;
            }
        }
        Ok(Response::new(RegisterReply { ok: true }))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatReply>, Status> {
        let req = request.into_inner();
        let ok = match &self.backend {
            Backend::Memory(st) => {
                let mut st = st.lock().await;
                st.heartbeat(&req.service, &req.node_id, self.default_ttl)
            }
            Backend::Etcd(e) => e.heartbeat(&req.service, &req.node_id).await,
        };
        if !ok {
            debug!("heartbeat {} {} -> MISS", req.service, req.node_id);
        }
        Ok(Response::new(HeartbeatReply { ok }))
    }

    async fn discover(&self, request: Request<DiscoverRequest>) -> Result<Response<DiscoverReply>, Status> {
        let req = request.into_inner();
        let nodes = match &self.backend {
            Backend::Memory(st) => {
                let st = st.lock().await;
                st.discover(&req.service)
            }
            Backend::Etcd(e) => {
                e.discover(&req.service)
                    .await
                    .into_iter()
                    .map(|(node_id, v)| NodeInfo {
                        node_id,
                        addr: v.addr,
                        meta: v.meta,
                    })
                    .collect()
            }
        };
        Ok(Response::new(DiscoverReply { nodes }))
    }

    async fn unregister(&self, request: Request<UnregisterRequest>) -> Result<Response<UnregisterReply>, Status> {
        let req = request.into_inner();
        match &self.backend {
            Backend::Memory(st) => {
                st.lock().await.remove(&req.service, &req.node_id);
            }
            Backend::Etcd(e) => e.unregister(&req.service, &req.node_id).await,
        }
        Ok(Response::new(UnregisterReply { ok: true }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let backend = if args.registry.is_empty() {
        info!("using in-memory registry backend");
        let mem = SharedMemory::default();
        {
            let m = mem.clone();
            tokio::spawn(async move { memory_backend::sweeper(m, Duration::from_secs(1)).await });
        }
        Backend::Memory(mem)
    } else {
        let endpoints: Vec<String> = args.registry.split(',').map(|s| s.trim().to_string()).collect();
        info!("using etcd registry backend at {:?}", endpoints);
        let etcd = EtcdBackend::connect(endpoints).await?;
        Backend::Etcd(Arc::new(etcd))
    };

    let svc = RegistrySvc { backend, default_ttl: 10 };
    info!("registry listening on {}", args.listen);
    Server::builder()
        .add_service(RegistryServiceServer::new(svc))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
