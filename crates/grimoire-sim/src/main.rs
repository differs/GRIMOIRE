//! 测试/压测客户端。
//!
//! 模式：
//!   room    —— MMO 大厅/房间制全流程 demo（两客户端互动作业）
//!   battle  —— 实时对战帧同步 demo（两客户端入局，观察 20Hz 快照广播）
//!   card    —— 卡牌回合制 demo（两客户端对局，演示权威校验与视角裁剪）
//!   bench   —— 压测：N 连接并发发请求，统计 QPS 与延迟分位

mod client;

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use client::{dec, enc, Client};
use grimoire_common::msg;
use grimoire_pb::pb::*;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9000")]
    gateway: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .init();
    let args = Args::parse();
    match args.mode.as_str() {
        "room" => room_demo(&args).await?,
        "battle" => battle_demo(&args).await?,
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
    let b = Client::connect(&args.gateway, 2).await?;
    b.start_heartbeat();
    // 提前订阅 push，与交互流程并行打印
    let (pa, pb) = (a.clone(), b.clone());
    let printer = tokio::spawn(async move {
        tokio::join!(wait_pushes(&pa, "A", 6), wait_pushes(&pb, "B", 6));
    });

    let r = a.request(msg::ROOM_LOGIN, enc(&RoomLoginReq { name: "小明".into() })).await?;
    let login = dec::<RoomLoginResp>(&r.payload)?;
    info!("A logged in player={} name={}", login.player_id, login.name);

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
    let b = Client::connect(&args.gateway, 2).await?;
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

async fn card_demo(args: &Args) -> Result<()> {
    info!("=== card demo ===");
    let a = Client::connect(&args.gateway, 1).await?;
    a.start_heartbeat();
    let b = Client::connect(&args.gateway, 2).await?;
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

    // A 出一张牌（打对面玩家）
    let r = a.request(msg::CARD_PLAY, enc(&CardPlayReq { hand_index: 0, target_player: 0 })).await?;
    let p = dec::<CardPlayResp>(&r.payload)?;
    info!("A plays: ok={} detail={} {}", p.ok, p.detail, fmt_card_state(p.state.as_ref()));

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

/// 压测：N 连接并发发 RoomList，统计 QPS 与延迟分位。
async fn bench(args: &Args) -> Result<()> {
    info!(
        "=== bench {} clients x {}s on {} (msg=RoomList) ===",
        args.clients, args.duration, args.gateway
    );
    let (tx, mut rx) = mpsc::channel::<(u64, Duration)>(args.clients * 100);

    for i in 0..args.clients {
        let tx = tx.clone();
        let gw = args.gateway.clone();
        let rps = args.rps;
        tokio::spawn(async move {
            let Ok(c) = Client::connect(&gw, i as u32).await else { return };
            let mut interval = if rps > 0 {
                Some(tokio::time::interval(Duration::from_micros(1_000_000 / rps.max(1))))
            } else {
                None
            };
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
                    if tx.send((n, lat)).await.is_err() {
                        return;
                    }
                }
            }
        });
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
