//! 测试/压测客户端。
//!
//! 模式：
//!   room       —— MMO 大厅/房间制全流程 demo（两客户端互动作业）
//!   room-migrate —— 连接迁移 demo（断线重连恢复会话）
//!   battle     —— 实时对战帧同步 demo（两客户端入局，观察 20Hz 快照广播）
//!   battle-udp —— 实时对战 UDP 通道 demo（输入与帧同步都走 UDP）
//!   card       —— 卡牌回合制 demo（两客户端对局，演示权威校验与视角裁剪）
//!   bench      —— 压测：N 连接并发发请求，统计 QPS 与延迟分位

mod client;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use client::{dec, enc, Client, UdpBattle, UdpKcp};
use grimoire_common::msg;
use grimoire_pb::pb::*;
use tracing::info;

#[derive(Parser)]
struct Args {
    /// 客户端 A 连接的网关
    #[arg(long, default_value = "127.0.0.1:9000")]
    gateway: String,
    /// 客户端 B 连接的网关（多活网关演示时指向另一实例）
    #[arg(long, default_value = "127.0.0.1:9000")]
    gateway_b: String,
    /// 客户端 A 所在网关的 UDP 端口（battle-udp 用）
    #[arg(long, default_value = "127.0.0.1:9020")]
    udp_gateway: String,
    /// 客户端 B 所在网关的 UDP 端口（多网关时指向另一实例）
    #[arg(long, default_value = "127.0.0.1:9020")]
    udp_gateway_b: String,
    #[arg(long, default_value = "room")]
    mode: String,
    /// bench 模式：并发连接数
    #[arg(long, default_value = "100")]
    clients: usize,
    /// bench 模式：压测时长（秒）
    #[arg(long, default_value = "5")]
    duration: u64,
    #[arg(long, default_value = "0")]
    rps: u64,
    /// 每连接并发的在途请求数（管线）；>1 测吞吐天花板
    #[arg(long, default_value = "1")]
    pipeline: u32,
    /// KCP 刷新间隔(ms)（battle-stress 用）
    #[arg(long, default_value = "10")]
    kcp_interval: i32,
    /// KCP 快重传次数（battle-stress 用）
    #[arg(long, default_value = "2")]
    kcp_resend: i32,
    /// 用 TLS 连接网关（dev 接受自签证书）
    #[arg(long, default_value = "false")]
    tls: bool,
    /// 鉴权 token（空 = 不鉴权）
    #[arg(long, default_value = "")]
    auth_token: String,
}

/// 连接网关（可选 TLS + 鉴权）
async fn connect(args: &Args, gw: &str, label: u32) -> Result<Client> {
    let c = Client::connect_with(gw, label, args.tls).await?;
    if !args.auth_token.is_empty() {
        c.auth(&args.auth_token).await?;
    }
    Ok(c)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .init();
    let args = Args::parse();
    match args.mode.as_str() {
        "room" => room_demo(&args).await?,
        "room-migrate" => room_migrate_demo(&args).await?,
        "battle" => battle_demo(&args).await?,
        "battle-udp" => battle_udp_demo(&args).await?,
        "battle-kcp" => battle_kcp_demo(&args).await?,
        "battle-stress" => battle_stress_demo(&args).await?,
        "card" => card_demo(&args).await?,
        "bench" => bench(&args).await?,
        other => anyhow::bail!("unknown mode: {other}"),
    }
    Ok(())
}

async fn wait_pushes(c: &Client, kind: &str, secs: u64) {
    let mut rx = c.pushes.resubscribe();
    let deadline = tokio::time::sleep(Duration::from_secs(secs));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            f = rx.recv() => {
                match f {
                    Ok(frame) => {
                        match frame.msg_id {
                            msg::ROOM_STATE_PUSH => {
                                if let Ok(p) = dec::<RoomStatePush>(&frame.payload) {
                                    info!("[{}] push event={} actor={} room#{} members={}",
                                        kind, p.event, p.actor_id,
                                        p.room.as_ref().map(|r| r.room_id).unwrap_or(0),
                                        p.room.as_ref().map(|r| r.members.len()).unwrap_or(0));
                                }
                            }
                            msg::ROOM_CHAT_PUSH => {
                                if let Ok(p) = dec::<RoomChatPush>(&frame.payload) {
                                    info!("[{}] chat {}: {}", kind, p.name, p.text);
                                }
                            }
                            msg::BATTLE_FRAME_SYNC => {
                                if let Ok(p) = dec::<FrameSyncPush>(&frame.payload) {
                                    let ps: Vec<String> = p.players.iter().map(|x| format!("P{}@({:.0},{:.0})", x.player_id, x.x, x.y)).collect();
                                    info!("[{}] frame#{}: {}", kind, p.frame, ps.join(" "));
                                }
                            }
                            msg::CARD_SNAPSHOT_PUSH => {
                                if let Ok(p) = dec::<CardSnapshotPush>(&frame.payload) {
                                    info!("[{}] card snapshot: {}", kind, fmt_card_state(p.state.as_ref()));
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn fmt_card_state(s: Option<&CardGameState>) -> String {
    let Some(s) = s else { return "none".into() };
    let mut out = format!(
        "game#{} phase={} turn=P{} winner={}",
        s.game_id, s.phase, s.turn_player, s.winner
    );
    for p in &s.players {
        out.push_str(&format!(" [P{} hp={} score={} hand{}]", p.player_id, p.hp, p.score, p.hand_count));
    }
    out
}

async fn room_demo(args: &Args) -> Result<()> {
    info!("=== room demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    a.start_heartbeat();
    let b = connect(args, &args.gateway_b, 2).await?;
    b.start_heartbeat();
    // 提前订阅 push，与交互流程并行打印
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A", 6), wait_pushes(&pb, "B", 6));
    });

    let r = a.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小明".into() })).await?;
    let login = dec::<RoomLoginResp>(&r.payload)?;
    info!("A logged in player={} name={} games={} wins={}", login.player_id, login.name, login.games, login.wins);

    let r = a.request(msg::ROOM_CREATE, enc(&RoomCreateReq { name: "开黑房".into(), capacity: 8 })).await?;
    let room = dec::<RoomCreateResp>(&r.payload)?;
    info!("A created room #{}", room.room_id);
    a.bind_session(msg::DOMAIN_ROOM, room.room_id).await?;

    // B：先绑定会话再登录 —— 登录/入房都按会话路由到房间所在节点，
    // 玩家状态与房间同节点（跨实例关键）
    b.bind_session(msg::DOMAIN_ROOM, room.room_id).await?;
    let r = b.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小红".into() })).await?;
    let login2 = dec::<RoomLoginResp>(&r.payload)?;
    info!("B logged in player={}", login2.player_id);
    let r = b.request(msg::ROOM_JOIN, enc(&RoomJoinReq { room_id: room.room_id })).await?;
    let joined = dec::<RoomJoinResp>(&r.payload)?;
    info!("B joined room#{} members={}", joined.room.as_ref().map(|x| x.room_id).unwrap_or(0), joined.room.as_ref().map(|x| x.members.len()).unwrap_or(0));

    let _ = a.request(msg::ROOM_CHAT, enc(&RoomChatReq { text: "大家好啊".into() })).await?;

    let r = a.request(msg::ROOM_LIST, vec![]).await?;
    let list = dec::<RoomListResp>(&r.payload)?;
    info!("A lists rooms: {}", list.rooms.len());

    printer.await?;
    info!("=== room demo done ===");
    Ok(())
}

async fn battle_demo(args: &Args) -> Result<()> {
    info!("=== battle demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    a.start_heartbeat();
    let b = connect(args, &args.gateway_b, 2).await?;
    b.start_heartbeat();

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
    info!("A joined battle#{} as player {} ({}Hz)", ja.battle_id, ja.player_id, ja.frame_rate);
    a.bind_session(msg::DOMAIN_BATTLE, ja.battle_id).await?;
    // B 不做显式绑定 → 网关从 Redis 撮合大厅自动匹配到 A 的战斗（跨节点撮合）
    let r = b.request(msg::BATTLE_JOIN, vec![]).await?;
    let jb = dec::<BattleJoinResp>(&r.payload)?;
    info!("B joined battle#{} as player {}", jb.battle_id, jb.player_id);

    // A 持续移动（带客户端帧号，用于服务端延迟补偿）
    let a2 = a.clone();
    let push_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut t = 0.0_f32;
        let mut f = 0u64;
        loop {
            tick.tick().await;
            t += 0.5;
            f += 1;
            let (dx, dy) = (t.cos(), t.sin());
            let _ = a2.request(msg::BATTLE_INPUT, enc(&BattleInputReq { dir_x: dx, dir_y: dy, frame: f })).await;
        }
    });

    tokio::join!(wait_pushes(&a, "A", 4), wait_pushes(&b, "B", 4));
    push_task.abort();
    info!("=== battle demo done ===");
    Ok(())
}

/// 实时对战 UDP 通道：TCP 入局拿身份，UDP 发输入 + 收帧同步。
async fn battle_udp_demo(args: &Args) -> Result<()> {
    info!("=== battle-udp demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    a.start_heartbeat();
    let b = connect(args, &args.gateway_b, 2).await?;
    b.start_heartbeat();
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A(tcp-fallback)", 5), wait_pushes(&pb, "B(tcp-fallback)", 5));
    });

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
    a.bind_session(msg::DOMAIN_BATTLE, ja.battle_id).await?;
    b.bind_session(msg::DOMAIN_BATTLE, ja.battle_id).await?;
    let r = b.request(msg::BATTLE_JOIN, vec![]).await?;
    let jb = dec::<BattleJoinResp>(&r.payload)?;
    info!("A player {} joined battle#{}, B player {} joined ({}Hz)",
        ja.player_id, ja.battle_id, jb.player_id, ja.frame_rate);

    // 等欢迎帧拿到 conn_id，建立 UDP 通道
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a_cid = a.conn_id();
    let b_cid = b.conn_id();
    info!("conn_id A={} B={} (网关ID A={} B={})", a_cid, b_cid,
        grimoire_common::conn::gateway_id_of(a_cid), grimoire_common::conn::gateway_id_of(b_cid));
    let ua = UdpBattle::bind(&args.udp_gateway, a_cid).await?;
    let ub = UdpBattle::bind(&args.udp_gateway, b_cid).await?;
    info!("UDP 通道绑定完成");

    // A 持续通过 UDP 发输入
    let ua_send = ua.clone();
    let ua2 = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        let mut t = 0.0_f32;
        let mut f = 0u64;
        loop {
            tick.tick().await;
            t += 0.3;
            f += 1;
            let _ = ua_send.send_input(t.cos(), t.sin(), f).await;
        }
    });

    // 两端收 UDP 帧同步
    let deadline = tokio::time::sleep(Duration::from_secs(4));
    tokio::pin!(deadline);
    let mut fa = 0u64;
    let mut fb = 0u64;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            r = ua.recv_push() => {
                if let Ok(Some((_, _, payload))) = r {
                    if let Ok(p) = dec::<FrameSyncPush>(&payload) {
                        if p.frame != fa { fa = p.frame; info!("[UDP A] frame#{}: {}", p.frame, fmt_battle(&p.players)); }
                    }
                }
            }
            r = ub.recv_push() => {
                if let Ok(Some((_, _, payload))) = r {
                    if let Ok(p) = dec::<FrameSyncPush>(&payload) {
                        if p.frame != fb { fb = p.frame; info!("[UDP B] frame#{}: {}", p.frame, fmt_battle(&p.players)); }
                    }
                }
            }
        }
    }
    ua2.abort();
    printer.abort();
    info!("=== battle-udp demo done ===");
    Ok(())
}

/// 实时对战 KCP 通道：与 battle-udp 相同流程，但走 KCP 可靠 UDP。
async fn battle_kcp_demo(args: &Args) -> Result<()> {
    info!("=== battle-kcp demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    a.start_heartbeat();
    let b = connect(args, &args.gateway_b, 2).await?;
    b.start_heartbeat();

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
    a.bind_session(msg::DOMAIN_BATTLE, ja.battle_id).await?;
    b.bind_session(msg::DOMAIN_BATTLE, ja.battle_id).await?;
    let r = b.request(msg::BATTLE_JOIN, vec![]).await?;
    let jb = dec::<BattleJoinResp>(&r.payload)?;
    info!("A player {} joined battle#{}, B player {} joined ({}Hz)",
        ja.player_id, ja.battle_id, jb.player_id, ja.frame_rate);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let a_cid = a.wait_conn_id().await;
    let b_cid = b.wait_conn_id().await;
    let ua = UdpKcp::bind(&args.udp_gateway, a_cid).await?;
    let ub = UdpKcp::bind(&args.udp_gateway_b, b_cid).await?;
    info!("KCP 会话绑定完成 (conn {} / {})", a_cid, b_cid);

    // A 持续发输入（客户端本地预测同步推进；服务端延迟 2 帧补偿）
    let a_pid = ja.player_id;
    let predicted = Arc::new(tokio::sync::Mutex::new((25.0f32, 50.0f32)));
    let ua_send_c = ua.clone();
    let ua_send = tokio::spawn({
        let pred = predicted.clone();
        async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            let mut t = 0.0_f32;
            let mut f = 0u64;
            loop {
                tick.tick().await;
                t += 0.3;
                f += 1;
                let (dx, dy) = (t.cos(), t.sin());
                let _ = ua_send_c.send_input(dx, dy, f).await;
                // 本地预测：与服务端相同的确定性移动
                let norm = (dx * dx + dy * dy).sqrt();
                let mut p = pred.lock().await;
                if norm > 1e-6 {
                    p.0 += (dx / norm) * 8.0 * 0.05;
                    p.1 += (dy / norm) * 8.0 * 0.05;
                }
            }
        }
    });

    // 两端收 KCP 帧同步，展示预测 vs 权威（延迟补偿）
    let deadline = tokio::time::sleep(Duration::from_secs(4));
    tokio::pin!(deadline);
    let mut fa = 0u64;
    let mut fb = 0u64;
    let mut resynced = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            r = ua.recv_push() => {
                if let Ok(Some((_, _, payload))) = r {
                    if let Ok(p) = dec::<FrameSyncPush>(&payload) {
                        if p.frame != fa {
                            fa = p.frame;
                            let pred = predicted.lock().await;
                            let auth = p.players.iter().find(|x| x.player_id == a_pid).cloned();
                            match auth {
                                Some(ap) => info!("[KCP A] frame#{} pred=({:.0},{:.0}) auth=({:.0},{:.0})", p.frame, pred.0, pred.1, ap.x, ap.y),
                                None => info!("[KCP A] frame#{}: {}", p.frame, fmt_battle(&p.players)),
                            }
                        }
                        // 快照回放：模拟断帧（每 15 帧请求一次重同步）
                        if !resynced && p.frame >= 15 {
                            resynced = true;
                            let last = p.frame.saturating_sub(3);
                            if let Ok(r) = a.request(msg::BATTLE_RESYNC, enc(&BattleResyncReq { last_frame: last })).await {
                                if let Ok(rs) = dec::<BattleResyncResp>(&r.payload) {
                                    info!("[KCP A] 快照回放: 请求从帧{} 之后, 收到 {} 帧权威快照", last, rs.frames.len());
                                }
                            }
                        }
                    }
                }
            }
            r = ub.recv_push() => {
                if let Ok(Some((_, _, payload))) = r {
                    if let Ok(p) = dec::<FrameSyncPush>(&payload) {
                        if p.frame != fb { fb = p.frame; info!("[KCP B] frame#{}: {}", p.frame, fmt_battle(&p.players)); }
                    }
                }
            }
        }
    }
    ua_send.abort();
    info!("=== battle-kcp demo done ===");
    Ok(())
}

fn fmt_battle(players: &[BattlePlayer]) -> String {
    players.iter().map(|p| format!("P{}@({:.0},{:.0})", p.player_id, p.x, p.y)).collect::<Vec<_>>().join(" ")
}

/// KCP 对战压测：N 个客户端（2 人/局），KCP 通道收发帧同步。
/// 统计帧同步到达率、乱序/丢帧情况与单帧延迟。
async fn battle_stress_demo(args: &Args) -> Result<()> {
    let n = args.clients.max(2);
    let n = n - n % 2;
    info!("=== battle-stress: {} 客户端, {} 局, {}s, KCP ===", n, n / 2, args.duration);

    let clients: Vec<Client> = {
        let mut v = Vec::new();
        for i in 0..n {
            let gw = if i % 2 == 0 { args.gateway.clone() } else { args.gateway_b.clone() };
            match connect(args, &gw, i as u32).await {
                Ok(c) => v.push(c),
                Err(e) => {
                    info!("client {} connect fail: {}", i, e);
                    return Ok(());
                }
            }
        }
        v
    };
    // 全部入局 + 绑定会话（按 battle 路由后续输入）
    for c in &clients {
        if let Ok(r) = c.request(msg::BATTLE_JOIN, vec![]).await {
            if let Ok(j) = dec::<BattleJoinResp>(&r.payload) {
                let _ = c.bind_session(msg::DOMAIN_BATTLE, j.battle_id).await;
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    info!("joined {} clients", clients.len());

    // 各自建立 KCP 会话并发输入（按奇偶选对应网关的 UDP 端口）
    let dur = args.duration;
    let udp_a = args.udp_gateway.clone();
    let udp_b = args.udp_gateway_b.clone();
    let kcp_interval = args.kcp_interval;
    let kcp_resend = args.kcp_resend;
    let mut handles = Vec::new();
    for (i, c) in clients.into_iter().enumerate() {
        let cid = c.wait_conn_id().await;
        let udp = if i % 2 == 0 { udp_a.clone() } else { udp_b.clone() };
        handles.push(tokio::spawn(async move {
            // 持有客户端以保持 TCP 连接存活（Drop 会关闭连接导致被踢出战斗）
            let _keep_client = c;
            let Ok(kcp) = UdpKcp::bind_with(&udp, cid, kcp_interval, kcp_resend, true).await else {
                info!("client {} kcp bind fail", cid);
                return (0u64, 0u64, Duration::ZERO);
            };
            let mut frames = 0u64;
            let mut prev = 0u64;
            let mut gaps = 0u64;
            let mut sum_lat = Duration::ZERO;
            let mut min_int = Duration::MAX;
            let mut max_int = Duration::ZERO;
            let mut last_recv = Instant::now();
            let deadline = tokio::time::sleep(Duration::from_secs(dur));
            tokio::pin!(deadline);
            // 输入循环
            let input = tokio::spawn({
                let k = kcp.clone();
                async move {
                    let mut tick = tokio::time::interval(Duration::from_millis(50));
                    let mut t = 0.0_f32;
                    let mut f = 0u64;
                    loop {
                        tick.tick().await;
                        t += 0.3;
                        f += 1;
                        let _ = k.send_input(t.cos(), t.sin(), f).await;
                    }
                }
            });
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    r = kcp.recv_push() => {
                        if let Ok(Some((_, _, payload))) = r {
                            if let Ok(p) = dec::<FrameSyncPush>(&payload) {
                                let t0 = Instant::now();
                                frames += 1;
                                if prev != 0 && p.frame != prev + 1 { gaps += 1; }
                                prev = p.frame;
                                if frames > 1 {
                                    let iv = t0.duration_since(last_recv);
                                    sum_lat += iv;
                                    if iv < min_int { min_int = iv; }
                                    if iv > max_int { max_int = iv; }
                                }
                                last_recv = t0;
                            }
                        }
                    }
                }
            }
            input.abort();
            info!("client {} kcp frames={} gaps={} min_int={:?} max_int={:?}", cid, frames, gaps, min_int, max_int);
            (frames, gaps, sum_lat)
        }));
    }
    let mut total_frames = 0u64;
    let mut total_gaps = 0u64;
    let mut total_lat = Duration::ZERO;
    let mut cnt = 0u64;
    for h in handles {
        if let Ok((f, g, l)) = h.await {
            total_frames += f;
            total_gaps += g;
            total_lat += l;
            cnt += 1;
        }
    }
    let per = if cnt > 0 { total_frames as f64 / cnt as f64 } else { 0.0 };
    info!(
        "KCP 结果: {} 客户端, 总帧数={}, 每客户端平均帧={:.0} ({:.1}Hz), 乱序/丢帧={}, 平均帧延迟={:?}",
        cnt, total_frames, per, per / args.duration as f64, total_gaps,
        total_lat.checked_div(total_frames.max(1) as u32)
    );
    info!("=== battle-stress done ===");
    Ok(())
}

async fn card_demo(args: &Args) -> Result<()> {
    info!("=== card demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    a.start_heartbeat();
    let b = connect(args, &args.gateway_b, 2).await?;
    b.start_heartbeat();
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A", 5), wait_pushes(&pb, "B", 5));
    });

    let r = a.request(msg::CARD_START, vec![]).await?;
    let sa = dec::<CardStartResp>(&r.payload)?;
    info!("A started, hand: {}", fmt_hand(sa.state.as_ref()));
    let gid = sa.state.as_ref().map(|st| st.game_id).unwrap_or(0);
    a.bind_session(msg::DOMAIN_CARD, gid).await?;
    // B 加入前绑定到同一对局 → 按会话路由到 A 所在节点
    b.bind_session(msg::DOMAIN_CARD, gid).await?;
    let r = b.request(msg::CARD_START, vec![]).await?;
    let sb = dec::<CardStartResp>(&r.payload)?;
    info!("B started, hand: {}", fmt_hand(sb.state.as_ref()));

    // 双方轮流打到对局结束（触发持久化落库）
    // 出牌成功即自动切换回合；无牌可出时由行动者结束回合
    let p1_id = sa.state.as_ref().map(|st| st.players[0].player_id).unwrap_or(0);
    let mut state = sa.state.clone();
    let mut guard = 0u32;
    loop {
        let turn_pid = state.as_ref().map(|st| st.turn_player).unwrap_or(0);
        let c = if turn_pid == p1_id { &a } else { &b };
        let r = c.request(msg::CARD_PLAY, enc(&CardPlayReq { hand_index: 0, target_player: 0 })).await?;
        let p = dec::<CardPlayResp>(&r.payload)?;
        let finished = p.state.as_ref().map(|st| st.phase == 1).unwrap_or(false);
        info!("P{} plays: ok={} {} (finished={})", turn_pid, p.ok, p.detail, finished);
        if finished {
            info!("对局结束 winner=P{}", p.state.as_ref().map(|st| st.winner).unwrap_or(0));
            break;
        }
        if !p.ok && p.detail.contains("手牌序号无效") {
            // 无牌可出 → 行动者结束回合，采纳其返回的新状态
            let r = c.request(msg::CARD_END_TURN, vec![]).await?;
            if let Ok(e) = dec::<CardEndTurnResp>(&r.payload) {
                if let Some(s) = e.state {
                    state = Some(s);
                }
            }
        }
        // 失败响应的 state 为 None，保留上一状态
        if let Some(s) = p.state {
            state = Some(s);
        }
        guard += 1;
        if guard > 40 {
            info!("对局超过 40 步仍未结束，中止");
            break;
        }
    }

    // 非法操作演示：轮到 B，A 再出牌应被拒绝
    let r = a.request(msg::CARD_PLAY, enc(&CardPlayReq { hand_index: 0, target_player: 0 })).await?;
    let p = dec::<CardPlayResp>(&r.payload)?;
    info!("A illegal play (not turn): ok={} detail={}", p.ok, p.detail);

    // B 结束回合
    let r = b.request(msg::CARD_END_TURN, vec![]).await?;
    let e = dec::<CardEndTurnResp>(&r.payload)?;
    info!("B ends turn: ok={} {}", e.ok, fmt_card_state(e.state.as_ref()));

    printer.await?;
    info!("=== card demo done ===");
    Ok(())
}

fn fmt_hand(s: Option<&CardGameState>) -> String {
    match s {
        Some(st) => st
            .players
            .iter()
            .find(|p| !p.hand.is_empty())
            .map(|p| p.hand.iter().map(|c| format!("{}[{}]", c.name, c.power)).collect::<Vec<_>>().join(", "))
            .unwrap_or_default(),
        None => "none".into(),
    }
}

/// 连接迁移 demo：A 建房入局 → 掉线 → 重连并恢复同一会话。
async fn room_migrate_demo(args: &Args) -> Result<()> {
    info!("=== room-migrate demo ===");
    let a = connect(args, &args.gateway, 1).await?;
    let a_cid = a.wait_conn_id().await;
    info!("A connected, conn_id={}", a_cid);

    let r = a.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小明".into() })).await?;
    let login = dec::<RoomLoginResp>(&r.payload)?;
    info!("A logged in player={} name={} games={} wins={}", login.player_id, login.name, login.games, login.wins);
    let r = a.request(msg::ROOM_CREATE, enc(&RoomCreateReq { name: "开黑房".into(), capacity: 8 })).await?;
    let room = dec::<RoomCreateResp>(&r.payload)?;
    info!("A created room #{}", room.room_id);
    a.bind_session(msg::DOMAIN_ROOM, room.room_id).await?;

    let b = connect(args, &args.gateway_b, 2).await?;
    // B 先绑定再登录，保证玩家与房间同节点
    b.bind_session(msg::DOMAIN_ROOM, room.room_id).await?;
    let r = b.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小红".into() })).await?;
    let login_b = dec::<RoomLoginResp>(&r.payload)?;
    let _ = b.request(msg::ROOM_JOIN, enc(&RoomJoinReq { room_id: room.room_id })).await?;
    info!("B(player {}) joined room#{}", login_b.player_id, room.room_id);

    // A 硬掉线（直接断开 TCP，不清理会话）
    drop(a);
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("A dropped connection, reconnecting...");

    // A 重连并迁移会话
    let a2 = connect(args, &args.gateway, 3).await?;
    let new_cid = a2.wait_conn_id().await;
    a2.resume(a_cid).await?;
    info!("A resumed: conn {} -> {}", new_cid, a2.conn_id());
    a2.start_heartbeat();
    // 新连接需重新绑定会话
    a2.bind_session(msg::DOMAIN_ROOM, room.room_id).await?;

    // 重新登录：应返回相同 player_id（会话未丢失）
    let r = a2.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小明".into() })).await?;
    let login2 = dec::<RoomLoginResp>(&r.payload)?;
    info!("A re-login player={} (原 player={})", login2.player_id, login.player_id);
    assert_eq!(login2.player_id, login.player_id, "会话迁移后 player_id 应保持一致");

    // 房间状态：A 仍应在房间内，成员数=2
    let r = a2.request(msg::ROOM_LIST, vec![]).await?;
    let list = dec::<RoomListResp>(&r.payload)?;
    let room_state = list.rooms.iter().find(|x| x.room_id == room.room_id).cloned();
    match room_state {
        Some(rs) => {
            info!("房间仍存在: members={}", rs.members.len());
            assert_eq!(rs.members.len(), 2, "迁移后房间成员应保持 2 人");
            for m in &rs.members {
                info!("  member P{} name={}", m.player_id, m.name);
            }
        }
        None => anyhow::bail!("迁移后房间丢失了！"),
    }
    info!("=== room-migrate demo done ===");
    Ok(())
}

/// 压测：N 连接 × 每连接 pipeline 个在途请求并发发 RoomList。
/// pipeline=1 时 QPS ≈ 连接数/RTT（测延迟）；pipeline>1 时绕开 RTT 限制，
/// 直接压出服务器真实吞吐上限。
async fn bench(args: &Args) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering as AtOrder};
    use std::sync::Arc;
    let pipeline = args.pipeline.max(1);
    info!(
        "=== bench {} clients x {}s, pipeline={} on {} (msg=RoomList) ===",
        args.clients, args.duration, pipeline, args.gateway
    );
    // 每个 worker 本地计数（无共享争用）；延迟按 1/100 采样
    let lats = Arc::new(tokio::sync::Mutex::new(Vec::<Duration>::new()));
    let mut handles = Vec::new();
    let mut counters = Vec::new();

    for i in 0..args.clients {
        let gw = args.gateway.clone();
        let Ok(c) = Client::connect(&gw, i as u32).await else { continue };
        for _ in 0..pipeline {
            let c = c.clone();
            let lats = lats.clone();
            let counter = Arc::new(AtomicU64::new(0));
            counters.push(counter.clone());
            // 限速仅用于 pipeline=1 的向后兼容场景
            let interval = if pipeline == 1 && args.rps > 0 {
                Some(tokio::time::interval(Duration::from_micros(1_000_000 / args.rps.max(1))))
            } else {
                None
            };
            handles.push(tokio::spawn(async move {
                let mut interval = interval;
                let mut n = 0u64;
                loop {
                    if let Some(iv) = &mut interval {
                        iv.tick().await;
                    }
                    let start = Instant::now();
                    let r = c.request(msg::ROOM_LIST, vec![]).await;
                    let lat = start.elapsed();
                    if r.is_ok() {
                        n += 1;
                        counter.fetch_add(1, AtOrder::Relaxed);
                        // 采样：每 100 个锁一次共享 lats，降低聚合开销
                        if n % 100 == 0 {
                            lats.lock().await.push(lat);
                        }
                    }
                }
            }));
        }
    }

    tokio::time::sleep(Duration::from_secs(args.duration)).await;
    for h in &handles {
        h.abort();
    }
    drop(handles);

    // 聚合：worker 本地计数直接读取
    let total: u64 = counters.iter().map(|c| c.load(AtOrder::Relaxed)).sum();
    let mut lats = {
        let mut l = lats.lock().await;
        std::mem::take(&mut *l)
    };

    let elapsed = args.duration as f64;
    let qps = total as f64 / elapsed;
    lats.sort();
    let p = |q: f64| -> Duration {
        if lats.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((lats.len() as f64) * q).ceil() as usize;
        lats[idx.clamp(1, lats.len()) - 1]
    };
    info!(
        "total={} qps={:.0} avg={:?} p50={:?} p99={:?} (采样数 {})",
        total, qps, lats.iter().sum::<Duration>() / lats.len().max(1) as u32, p(0.50), p(0.99), lats.len()
    );
    Ok(())
}
