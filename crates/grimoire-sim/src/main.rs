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

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use client::{dec, enc, Client, UdpBattle, UdpKcp};
use grimoire_common::msg;
use grimoire_pb::pb::*;
use tokio::sync::mpsc;
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
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway_b, 2).await?;
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
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway_b, 2).await?;
    b.start_heartbeat();

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
    info!("A joined battle#{} as player {} ({}Hz)", ja.battle_id, ja.player_id, ja.frame_rate);
    let r = b.request(msg::BATTLE_JOIN, vec![]).await?;
    let jb = dec::<BattleJoinResp>(&r.payload)?;
    info!("B joined battle#{} as player {}", jb.battle_id, jb.player_id);

    // A 持续移动
    let a2 = a.clone();
    let push_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut t = 0.0_f32;
        loop {
            tick.tick().await;
            t += 0.5;
            let (dx, dy) = (t.cos(), t.sin());
            let _ = a2.request(msg::BATTLE_INPUT, enc(&BattleInputReq { dir_x: dx, dir_y: dy })).await;
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
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway_b, 2).await?;
    b.start_heartbeat();
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A(tcp-fallback)", 5), wait_pushes(&pb, "B(tcp-fallback)", 5));
    });

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
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
        loop {
            tick.tick().await;
            t += 0.3;
            let _ = ua_send.send_input(t.cos(), t.sin()).await;
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
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway_b, 2).await?;
    b.start_heartbeat();

    let r = a.request(msg::BATTLE_JOIN, vec![]).await?;
    let ja = dec::<BattleJoinResp>(&r.payload)?;
    let r = b.request(msg::BATTLE_JOIN, vec![]).await?;
    let jb = dec::<BattleJoinResp>(&r.payload)?;
    info!("A player {} joined battle#{}, B player {} joined ({}Hz)",
        ja.player_id, ja.battle_id, jb.player_id, ja.frame_rate);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let a_cid = a.wait_conn_id().await;
    let b_cid = b.wait_conn_id().await;
    let ua = UdpKcp::bind(&args.udp_gateway, a_cid).await?;
    let ub = UdpKcp::bind(&args.udp_gateway, b_cid).await?;
    info!("KCP 会话绑定完成 (conn {} / {})", a_cid, b_cid);

    // A 持续发输入
    let ua_send_c = ua.clone();
    let ua_send = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        let mut t = 0.0_f32;
        loop {
            tick.tick().await;
            t += 0.3;
            let _ = ua_send_c.send_input(t.cos(), t.sin()).await;
        }
    });

    // 两端收 KCP 帧同步
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
                        if p.frame != fa { fa = p.frame; info!("[KCP A] frame#{}: {}", p.frame, fmt_battle(&p.players)); }
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

async fn card_demo(args: &Args) -> Result<()> {
    info!("=== card demo ===");
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway_b, 2).await?;
    b.start_heartbeat();
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A", 5), wait_pushes(&pb, "B", 5));
    });

    let r = a.request(msg::CARD_START, vec![]).await?;
    let sa = dec::<CardStartResp>(&r.payload)?;
    info!("A started, hand: {}", fmt_hand(sa.state.as_ref()));
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
    let a = Client::connect(&args.gateway, 1).await?;
    let a_cid = a.wait_conn_id().await;
    info!("A connected, conn_id={}", a_cid);

    let r = a.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小明".into() })).await?;
    let login = dec::<RoomLoginResp>(&r.payload)?;
    info!("A logged in player={} name={} games={} wins={}", login.player_id, login.name, login.games, login.wins);
    let r = a.request(msg::ROOM_CREATE, enc(&RoomCreateReq { name: "开黑房".into(), capacity: 8 })).await?;
    let room = dec::<RoomCreateResp>(&r.payload)?;
    info!("A created room #{}", room.room_id);

    let b = Client::connect(&args.gateway_b, 2).await?;
    let r = b.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小红".into() })).await?;
    let login_b = dec::<RoomLoginResp>(&r.payload)?;
    let _ = b.request(msg::ROOM_JOIN, enc(&RoomJoinReq { room_id: room.room_id })).await?;
    info!("B(player {}) joined room#{}", login_b.player_id, room.room_id);

    // A 硬掉线（直接断开 TCP，不清理会话）
    drop(a);
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("A dropped connection, reconnecting...");

    // A 重连并迁移会话
    let a2 = Client::connect(&args.gateway, 3).await?;
    let new_cid = a2.wait_conn_id().await;
    a2.resume(a_cid).await?;
    info!("A resumed: conn {} -> {}", new_cid, a2.conn_id());
    a2.start_heartbeat();

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
    let pipeline = args.pipeline.max(1);
    info!(
        "=== bench {} clients x {}s, pipeline={} on {} (msg=RoomList) ===",
        args.clients, args.duration, pipeline, args.gateway
    );
    let (tx, mut rx) = mpsc::channel::<(u64, Duration)>(args.clients * pipeline as usize * 100);

    for i in 0..args.clients {
        let gw = args.gateway.clone();
        let Ok(c) = Client::connect(&gw, i as u32).await else { continue };
        for _ in 0..pipeline {
            let tx = tx.clone();
            let c = c.clone();
            // 限速仅用于 pipeline=1 的向后兼容场景
            let interval = if pipeline == 1 && args.rps > 0 {
                Some(tokio::time::interval(Duration::from_micros(1_000_000 / args.rps.max(1))))
            } else {
                None
            };
            tokio::spawn(async move {
                let mut interval = interval;
                loop {
                    if let Some(iv) = &mut interval {
                        iv.tick().await;
                    }
                    let start = Instant::now();
                    let r = c.request(msg::ROOM_LIST, vec![]).await;
                    let lat = start.elapsed();
                    if r.is_ok() {
                        if tx.send((1, lat)).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    }
    drop(tx);

    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let mut total = 0u64;
    let mut lats: Vec<Duration> = Vec::new();
    while let Some((_, lat)) = rx.recv().await {
        if Instant::now() > deadline {
            break;
        }
        total += 1;
        lats.push(lat);
    }

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
        "total={} qps={:.0} avg={:?} p50={:?} p99={:?}",
        total, qps, lats.iter().sum::<Duration>() / lats.len().max(1) as u32, p(0.50), p(0.99)
    );
    Ok(())
}
