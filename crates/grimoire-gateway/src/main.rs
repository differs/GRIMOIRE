use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use grimoire_net::{udp, Frame, PType};
use grimoire_pb::pb::{
    gateway_service_server::{GatewayService, GatewayServiceServer},
    KickReply, KickRequest, PushReply, PushRequest,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod discovery;
mod kcp;
mod session;
mod stream;

use session::Ctx;

#[derive(Parser)]
struct Args {
    /// 网关 ID（高 8 位编码进 conn_id，多活实例须唯一）
    #[arg(long, default_value = "1")]
    id: u8,
    /// 客户端 TCP 接入端口
    #[arg(long, default_value = "127.0.0.1:9000")]
    client_listen: String,
    /// 业务服务 push/kick 的 gRPC 端口
    #[arg(long, default_value = "127.0.0.1:9100")]
    grpc_listen: String,
    /// 实时对战 UDP 端口
    #[arg(long, default_value = "127.0.0.1:9020")]
    udp_listen: String,
    /// 注册中心地址
    #[arg(long, default_value = "127.0.0.1:8500")]
    registry: String,
    /// KCP 刷新间隔(ms)
    #[arg(long, default_value = "10")]
    kcp_interval: i32,
    /// KCP 快重传次数
    #[arg(long, default_value = "2")]
    kcp_resend: i32,
    /// KCP 关闭流控(纯快传模式)
    #[arg(long, default_value = "true")]
    kcp_nc: bool,
    /// Redis 地址（会话目录）
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis: String,
    /// 鉴权密钥（空 = 关闭鉴权）
    #[arg(long, default_value = "")]
    auth_secret: String,
    /// TLS 证书（PEM）；与 --tls-key 同时提供则启用 TLS
    #[arg(long, default_value = "")]
    tls_cert: String,
    /// TLS 私钥（PEM）
    #[arg(long, default_value = "")]
    tls_key: String,
}

/// gRPC 侧：业务服务主动 push/kick 到客户端连接
struct GatewayGrpc {
    ctx: Arc<Ctx>,
}

#[tonic::async_trait]
impl GatewayService for GatewayGrpc {
    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushReply>, Status> {
        let req = request.into_inner();
        let ok = route_push(&self.ctx, req.clone()).await;
        if !ok {
            warn!("push to conn {} failed", req.conn_id);
        }
        Ok(Response::new(PushReply { ok }))
    }

    type PushStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<PushReply, Status>> + Send>,
    >;

    /// 双向流式推送：一条持久流承载所有帧同步/状态广播。
    async fn push_stream(
        &self,
        request: Request<tonic::Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStreamStream>, Status> {
        let mut rx = request.into_inner();
        let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<PushReply, Status>>(256);
        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            loop {
                match rx.message().await {
                    Ok(Some(req)) => {
                        let ok = route_push(&ctx, req).await;
                        if tx.send(Ok(PushReply { ok })).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(out_rx))))
    }

    async fn kick(&self, request: Request<KickRequest>) -> Result<Response<KickReply>, Status> {
        let req = request.into_inner();
        let ok = match self.ctx.sessions.remove(&req.conn_id) {
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

/// 路由一条推送：KCP 会话 → 裸 UDP → TCP 回退。
async fn route_push(ctx: &Ctx, req: PushRequest) -> bool {
    // 先把 Arc 克隆出 Ref 再 await，避免 Ref 跨 await 持有分片读锁
    match ctx.sessions.get(&req.conn_id).map(|r| r.clone()) {
        Some(s) => {
            debug!("push conn {} msg 0x{:X} udp={}", req.conn_id, req.msg_id, req.udp);
            s.push_msg(ctx, req.msg_id, req.payload.into(), req.udp).await
        }
        None => {
            debug!("push conn {} not found", req.conn_id);
            false
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let discovery = discovery::Discovery::new(args.registry.clone()).await?;
    let streams = stream::Streams::new(discovery.clone());
    let (udp_tx, mut udp_rx) = tokio::sync::mpsc::channel::<(SocketAddr, Vec<u8>)>(512);
    let udp_sock = Arc::new(tokio::net::UdpSocket::bind(&args.udp_listen).await?);
    let session_dir = match grimoire_svcfw::SessionDir::connect(&args.redis).await {
        Ok(sd) => {
            info!("session directory enabled (redis)");
            Some(Arc::new(sd))
        }
        Err(e) => {
            warn!("session directory disabled: {}", e);
            None
        }
    };
    let ctx = Arc::new(Ctx {
        sessions: Arc::new(DashMap::new()),
        discovery,
        streams,
        next_conn_id: Default::default(),
        gateway_id: args.id,
        udp_tx,
        kcp_sessions: Arc::new(DashMap::new()),
        udp_sock: udp_sock.clone(),
        kcp_interval: args.kcp_interval,
        kcp_resend: args.kcp_resend,
        kcp_nc: args.kcp_nc,
        session_dir,
        conn_session: Arc::new(DashMap::new()),
        session_cache: Arc::new(DashMap::new()),
        auth_secret: args.auth_secret.clone(),
    });
    if !args.auth_secret.is_empty() {
        info!("auth enabled");
    }

    // 注册到注册中心（多活网关按 id 区分）
    grimoire_svcfw::register_and_heartbeat(
        &args.registry,
        grimoire_common::svc::GATEWAY,
        &format!("gw-{}", args.id),
        &args.grpc_listen,
        HashMap::from([("id".to_string(), args.id.to_string())]),
        10,
    )
    .await?;

    // TCP 客户端接入（可选 TLS 包裹）
    let listener = tokio::net::TcpListener::bind(&args.client_listen).await?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls_acceptor = if !args.tls_cert.is_empty() && !args.tls_key.is_empty() {
        let certs: Vec<rustls::pki_types::CertificateDer> = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(&args.tls_cert)?))
            .collect::<Result<_, _>>()?;
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(&args.tls_key)?))?
            .ok_or_else(|| anyhow::anyhow!("no private key in {}", args.tls_key))?;
        let cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        info!("gateway {} TLS enabled on {}", args.id, args.client_listen);
        Some(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(cfg)))
    } else {
        info!("gateway {} tcp listening on {}", args.id, args.client_listen);
        None
    };

    let ctx_tcp = ctx.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    let c = ctx_tcp.clone();
                    if let Some(acceptor) = &tls_acceptor {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(tls) => {
                                    let (r, w) = tokio::io::split(tls);
                                    session::accept_and_serve(c, r, w, peer).await;
                                }
                                Err(e) => warn!("tls accept from {} error: {}", peer, e),
                            }
                        });
                    } else {
                        let (r, w) = stream.into_split();
                        tokio::spawn(session::accept_and_serve(c, r, w, peer));
                    }
                }
                Err(e) => warn!("accept error: {}", e),
            }
        }
    });

    // UDP 实时对战通道（裸 UDP 与 KCP 并存，按首字节自动识别）
    info!("gateway {} udp listening on {}", args.id, args.udp_listen);
    {
        let sock = udp_sock.clone();
        let c = ctx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; udp::MAX_DATAGRAM * 2];
            loop {
                match sock.recv_from(&mut buf).await {
                    Ok((n, src)) => {
                        let c = c.clone();
                        let data = buf[..n].to_vec();
                        if n >= 2 && &data[0..2] == grimoire_net::udp::MAGIC {
                            tokio::spawn(session::handle_udp_datagram(c, src, data));
                        } else if n >= 4 {
                            let conv = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                            tokio::spawn(session::handle_kcp_datagram(c, conv, src, data));
                        }
                    }
                    Err(e) => warn!("udp recv error: {}", e),
                }
            }
        });
    }
    {
        let sock = udp_sock.clone();
        tokio::spawn(async move {
            while let Some((addr, dgram)) = udp_rx.recv().await {
                if let Err(e) = sock.send_to(&dgram, addr).await {
                    warn!("udp send to {} error: {}", addr, e);
                }
            }
        });
    }

    // 业务服务桥接 gRPC
    let grpc = GatewayGrpc { ctx };
    info!("gateway {} grpc listening on {}", args.id, args.grpc_listen);
    tonic::transport::Server::builder()
        .add_service(GatewayServiceServer::new(grpc))
        .serve(args.grpc_listen.parse()?)
        .await?;
    Ok(())
}
