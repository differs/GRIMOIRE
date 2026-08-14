use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use grimoire_net::{Frame, PType};
use grimoire_pb::pb::{
    gateway_service_server::{GatewayService, GatewayServiceServer},
    KickReply, KickRequest, PushReply, PushRequest,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod discovery;
mod session;

use discovery::Discovery;
use session::{Ctx, Session};

#[derive(Parser)]
struct Args {
    /// 客户端 TCP 接入端口
    #[arg(long, default_value = "127.0.0.1:9000")]
    client_listen: String,
    /// 业务服务 push/kick 的 gRPC 端口
    #[arg(long, default_value = "127.0.0.1:9100")]
    grpc_listen: String,
    /// 注册中心地址
    #[arg(long, default_value = "127.0.0.1:8500")]
    registry: String,
}

/// gRPC 侧：业务服务主动 push/kick 到客户端连接
struct GatewayGrpc {
    sessions: Arc<DashMap<u32, Arc<Session>>>,
}

#[tonic::async_trait]
impl GatewayService for GatewayGrpc {
    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushReply>, Status> {
        let req = request.into_inner();
        let ok = match self.sessions.get(&req.conn_id) {
            Some(s) => {
                debug!("push conn {} msg 0x{:X}", req.conn_id, req.msg_id);
                s.send(Frame {
                    ptype: PType::Push,
                    msg_id: req.msg_id,
                    seq: 0,
                    payload: req.payload.into(),
                })
                .await
            }
            None => {
                warn!("push conn {} not found", req.conn_id);
                false
            }
        };
        if !ok {
            warn!("push to conn {} failed", req.conn_id);
        }
        Ok(Response::new(PushReply { ok }))
    }

    async fn kick(&self, request: Request<KickRequest>) -> Result<Response<KickReply>, Status> {
        let req = request.into_inner();
        let ok = match self.sessions.remove(&req.conn_id) {
            Some((_, s)) => {
                let _ = s
                    .send(Frame {
                        ptype: PType::Close,
                        msg_id: 0,
                        seq: 0,
                        payload: format!("kick:{}:{}", req.reason, req.detail).into(),
                    })
                    .await;
                true
            }
            None => false,
        };
        Ok(Response::new(KickReply { ok }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let discovery = Discovery::new(args.registry.clone()).await?;
    let ctx = Arc::new(Ctx {
        sessions: Arc::new(DashMap::new()),
        discovery,
        next_conn_id: Default::default(),
    });

    // TCP 客户端接入
    let listener = tokio::net::TcpListener::bind(&args.client_listen).await?;
    info!("gateway client tcp listening on {}", args.client_listen);

    let ctx_tcp = ctx.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let c = ctx_tcp.clone();
                    tokio::spawn(session::accept_and_serve(c, stream, peer));
                }
                Err(e) => warn!("accept error: {}", e),
            }
        }
    });

    // 业务服务桥接 gRPC（与 TCP 接入共享同一 sessions 表）
    let grpc = GatewayGrpc { sessions: ctx.sessions.clone() };
    info!("gateway grpc listening on {}", args.grpc_listen);
    tonic::transport::Server::builder()
        .add_service(GatewayServiceServer::new(grpc))
        .serve(args.grpc_listen.parse()?)
        .await?;
    Ok(())
}
