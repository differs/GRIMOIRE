//! 卡牌回合制玩法服务。
//!
//! 范式特征：
//!  - 通信：低频事件上行（出牌/结束回合）+ 全量状态快照下行（每次变更都推送）
//!  - 状态机：game.phase(playing/finished) + turn 严格驱动；非法操作一律服务端拒绝
//!  - 权威校验：所有规则判定（谁出牌/手牌是否有效/目标是否合法）都在服务端
//!  - 视角裁剪：每名玩家只看到自己的手牌，他人只看到手牌数量
//!  - 扩展：单局游戏数据量小且低频，适合单点承载海量对局（垂直扩展友好）

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use grimoire_common::{msg, svc};
use grimoire_pb::pb::{
    service_bridge_server::{ServiceBridge, ServiceBridgeServer},
    CardDef, CardEndTurnReq, CardEndTurnResp, CardGameState, CardPlayReq, CardPlayResp,
    CardPlayerView, CardSnapshotPush, CardStartReq, CardStartResp, CardStateReq, CardStateResp,
    ForwardReply, ForwardRequest, PlayerEvent,
};
use grimoire_svcfw::Pusher;
use rand::seq::SliceRandom;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};

const GAME_CAPACITY: usize = 2;
const INIT_HP: u32 = 20;
const HAND_SIZE: usize = 3;
const PHASE_PLAYING: u32 = 0;
const PHASE_FINISHED: u32 = 1;

/// 静态牌库（练手用固定数据，正式项目从配置/DB 加载）
fn deck() -> Vec<CardDef> {
    vec![
        CardDef { id: 1, name: "火球".to_string(), power: 8 },
        CardDef { id: 2, name: "冰锥".to_string(), power: 6 },
        CardDef { id: 3, name: "闪电".to_string(), power: 9 },
        CardDef { id: 4, name: "暗刃".to_string(), power: 5 },
        CardDef { id: 5, name: "盾击".to_string(), power: 4 },
        CardDef { id: 6, name: "毒液".to_string(), power: 7 },
    ]
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8800")]
    listen: String,
    #[arg(long, default_value = "127.0.0.1:8500")]
    registry: String,
    #[arg(long, default_value = "card-svc-1")]
    node_id: String,
    #[arg(long, default_value = "postgres://grimoire:grimoire@127.0.0.1:5432/grimoire")]
    pg_url: String,
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
}

struct GamePlayer {
    player_id: u32,
    name: String,
    hp: u32,
    score: u32,
    hand: Vec<CardDef>,
    conn_id: u32,
}

struct CardGame {
    id: u32,
    phase: u32,
    /// 当前行动玩家下标
    turn: usize,
    winner: u32,
    players: Vec<GamePlayer>,
}

impl CardGame {
    fn new(id: u32, conn_id: u32, player_id: u32) -> Self {
        Self {
            id,
            phase: PHASE_PLAYING,
            turn: 0,
            winner: 0,
            players: vec![GamePlayer {
                player_id,
                name: format!("玩家{}", player_id),
                hp: INIT_HP,
                score: 0,
                hand: deal_hand(),
                conn_id,
            }],
        }
    }

    fn add_player(&mut self, conn_id: u32, player_id: u32) {
        self.players.push(GamePlayer {
            player_id,
            name: format!("玩家{}", player_id),
            hp: INIT_HP,
            score: 0,
            hand: deal_hand(),
            conn_id,
        });
    }

    fn is_full(&self) -> bool {
        self.players.len() >= GAME_CAPACITY
    }

    fn player_idx_of(&self, player_id: u32) -> Option<usize> {
        self.players.iter().position(|p| p.player_id == player_id)
    }

    /// 对局是否结束
    fn check_finish(&mut self) {
        for p in &self.players {
            if p.hp <= 0 {
                self.phase = PHASE_FINISHED;
                self.winner = self.players.iter().find(|o| o.player_id != p.player_id).map(|o| o.player_id).unwrap_or(0);
                return;
            }
        }
    }

    /// 某个玩家视角的状态快照
    fn view_for(&self, recipient: usize) -> CardGameState {
        CardGameState {
            game_id: self.id,
            turn_player: self.players[self.turn].player_id,
            phase: self.phase,
            winner: self.winner,
            players: self
                .players
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let mine = i == recipient;
                    CardPlayerView {
                        player_id: p.player_id,
                        name: p.name.clone(),
                        hp: p.hp,
                        score: p.score,
                        hand: if mine { p.hand.clone() } else { vec![] },
                        hand_count: p.hand.len() as u32,
                    }
                })
                .collect(),
        }
    }
}

fn deal_hand() -> Vec<CardDef> {
    let mut rng = rand::thread_rng();
    let mut d = deck();
    d.shuffle(&mut rng);
    d.truncate(HAND_SIZE);
    d
}

struct App {
    games: DashMap<u32, Mutex<CardGame>>,
    /// conn_id -> (game_id, player_idx)
    conn_game: DashMap<u32, (u32, usize)>,
    next_player: AtomicU32,
    next_game: AtomicU32,
    pusher: Pusher,
    store: Option<Arc<grimoire_svcfw::ProfileStore>>,
}

impl App {
    fn new(pusher: Pusher, store: Option<Arc<grimoire_svcfw::ProfileStore>>) -> Self {
        Self {
            games: DashMap::new(),
            conn_game: DashMap::new(),
            next_player: AtomicU32::new(1),
            next_game: AtomicU32::new(1),
            pusher,
            store,
        }
    }

    /// 匹配：加入等待中的半局，否则开新局。
    async fn start(&self, conn_id: u32) -> CardStartResp {
        // 已在局中：直接返回当前视角
        if let Some((gid, idx)) = self.conn_game.get(&conn_id).map(|v| *v) {
            if let Some(g) = self.games.get(&gid) {
                let game = g.value().lock().await;
                return CardStartResp { state: Some(game.view_for(idx)) };
            }
        }

        // 找等待中的局（进行中且 1 人未满，跳过已结束的残局）
        let mut target = None;
        for e in self.games.iter() {
            let gid = *e.key();
            let g = e.value();
            let guard = g.lock().await;
            if guard.phase == PHASE_PLAYING && !guard.is_full() {
                target = Some(gid);
                break;
            }
        }
        let player_id = self.next_player.fetch_add(1, Ordering::Relaxed);
        let game_id = match target {
            Some(gid) => gid,
            None => {
                let id = self.next_game.fetch_add(1, Ordering::Relaxed);
                self.games.insert(id, Mutex::new(CardGame::new(id, conn_id, player_id)));
                id
            }
        };

        // 块作用域：Ref 出块即释放，不跨 await
        let (idx, state, started) = {
            let g = self.games.get(&game_id).unwrap();
            let mut game = g.value().lock().await;
            let idx;
            if game.is_full() {
                // 理论上不会到这里（新局）
                idx = 0;
            } else if game.players.iter().any(|p| p.conn_id == conn_id) {
                idx = game.player_idx_of(player_id).unwrap_or(0);
            } else {
                game.add_player(conn_id, player_id);
                idx = game.players.len() - 1;
            }
            self.conn_game.insert(conn_id, (game_id, idx));
            let state = game.view_for(idx);
            let started = game.is_full();
            (idx, state, started)
        };

        // 第二人加入后，给先手玩家也推送开局状态
        if started {
            self.push_snapshot(game_id).await;
        }
        info!("card game {} started/joined, player {} idx {}", game_id, player_id, idx);
        CardStartResp { state: Some(state) }
    }

    async fn play(&self, conn_id: u32, hand_index: u32, target_player: u32) -> CardPlayResp {
        let Some((gid, idx)) = self.conn_game.get(&conn_id).map(|v| *v) else {
            return CardPlayResp { ok: false, detail: "未在对局中".into(), state: None };
        };
        let fail = |detail: &str| CardPlayResp { ok: false, detail: detail.into(), state: None };

        // 全部判定与变更在块内完成，Ref 出块即释放，绝不跨 await
        let outcome = {
            let g = self.games.get(&gid).unwrap();
            let mut game = g.value().lock().await;
            if game.phase != PHASE_PLAYING {
                return fail("对局已结束");
            }
            if game.turn != idx {
                return fail("还没轮到你");
            }
            if hand_index as usize >= game.players[idx].hand.len() {
                return fail("手牌序号无效");
            }
            // 目标：必须指定另一个玩家（默认自动取对手）
            let victim = if game.players.len() > 1 {
                let other = game.players.iter().position(|p| p.player_id != game.players[idx].player_id).unwrap();
                if target_player != 0 {
                    game.player_idx_of(target_player).filter(|t| *t != idx).unwrap_or(other)
                } else {
                    other
                }
            } else {
                return fail("对手还没加入");
            };
            let card = game.players[idx].hand.remove(hand_index as usize);
            game.players[victim].hp = game.players[victim].hp.saturating_sub(card.power);
            game.players[idx].score += card.power;
            game.check_finish();
            let finished = game.phase != PHASE_PLAYING;
            if !finished {
                game.turn = victim; // 回合切换到对方
            }
            (game.view_for(idx), game.winner, finished, format!("{} 造成 {} 点伤害", card.name, card.power))
        };

        let (state, winner, finished, detail) = outcome;

        // 对局结束 → 持久化战绩
        if finished {
            self.persist_result(gid, winner).await;
        }

        let resp = CardPlayResp { ok: true, detail, state: Some(state) };
        self.push_snapshot(gid).await;
        info!(game_id = gid, "card played, finished={}", finished);
        resp
    }

    /// 对局结算落库（Postgres + Redis 失效）。
    async fn persist_result(&self, game_id: u32, winner: u32) {
        let Some(store) = &self.store else { return };
        let Some(g) = self.games.get(&game_id) else { return };
        let players: Vec<i64> = {
            let game = g.value().lock().await;
            game.players.iter().map(|p| p.player_id as i64).collect()
        };
        drop(g);
        if let Err(e) = store.record_game(&players, winner as i64).await {
            warn!("record_game {} failed: {}", game_id, e);
        } else {
            info!("game {} result persisted, winner={}", game_id, winner);
        }
    }

    async fn end_turn(&self, conn_id: u32) -> CardEndTurnResp {
        let Some((gid, idx)) = self.conn_game.get(&conn_id).map(|v| *v) else {
            return CardEndTurnResp { ok: false, state: None };
        };
        // 块作用域：Ref 出块即释放，不跨 await
        let state = {
            let g = self.games.get(&gid).unwrap();
            let mut game = g.value().lock().await;
            if game.phase != PHASE_PLAYING || game.turn != idx {
                return CardEndTurnResp { ok: false, state: None };
            }
            game.turn = (idx + 1) % game.players.len();
            game.view_for(idx)
        };
        self.push_snapshot(gid).await;
        CardEndTurnResp { ok: true, state: Some(state) }
    }

    async fn state(&self, conn_id: u32) -> CardStateResp {
        let Some((gid, idx)) = self.conn_game.get(&conn_id).map(|v| *v) else {
            return CardStateResp { state: None };
        };
        if let Some(g) = self.games.get(&gid) {
            let game = g.value().lock().await;
            return CardStateResp { state: Some(game.view_for(idx)) };
        }
        CardStateResp { state: None }
    }

    /// 给对局内每名玩家推送各自的视角快照。
    async fn push_snapshot(&self, game_id: u32) {
        let Some(g) = self.games.get(&game_id) else { return };
        let targets: Vec<(u32, Vec<u8>)> = {
            let game = g.value().lock().await;
            game.players
                .iter()
                .enumerate()
                .map(|(i, p)| (p.conn_id, grimoire_pb::pb::encode_message(&CardSnapshotPush {
                    state: Some(game.view_for(i)),
                })))
                .collect()
        };
        drop(g); // 释放分片读锁后再推送，避免锁跨 await
        for (conn, payload) in targets {
            let _ = self.pusher.push(conn, msg::CARD_SNAPSHOT_PUSH, payload).await;
        }
    }

    async fn leave(&self, conn_id: u32) {
        let Some((_, (gid, idx))) = self.conn_game.remove(&conn_id) else { return };
        if let Some(g) = self.games.get(&gid) {
            let mut game = g.value().lock().await;
            game.players.remove(idx);
            let empty = game.players.is_empty();
            if empty {
                // 先释放 Ref（读锁）再 remove（写锁），避免分片自死锁
                drop(game);
                drop(g);
                self.games.remove(&gid);
            } else if game.phase == PHASE_PLAYING {
                // 对手退场 → 直接判胜
                game.phase = PHASE_FINISHED;
                if let Some(p) = game.players.first() {
                    game.winner = p.player_id;
                }
                drop(game);
                self.push_snapshot(gid).await;
            }
        }
    }
}

#[derive(Clone)]
struct Bridge {
    app: Arc<App>,
}

/// 统一请求分发：unary 与流式共用，错误走 code=1。
async fn process(app: &Arc<App>, req: ForwardRequest) -> ForwardReply {
    debug!("card handle msg 0x{:X} from conn {}", req.msg_id, req.conn_id);
    let reply = |payload: Vec<u8>| ForwardReply {
        conn_id: req.conn_id,
        seq: req.seq,
        msg_id: req.msg_id,
        code: 0,
        payload,
    };
    let err = |text: String| ForwardReply {
        conn_id: req.conn_id,
        seq: req.seq,
        msg_id: req.msg_id,
        code: 1,
        payload: text.into_bytes(),
    };
    match req.msg_id {
        msg::CARD_START => match dec::<CardStartReq>(&req.payload) {
            Ok(_) => reply(encode(&app.start(req.conn_id).await)),
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::CARD_PLAY => match dec::<CardPlayReq>(&req.payload) {
            Ok(m) => reply(encode(&app.play(req.conn_id, m.hand_index, m.target_player).await)),
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::CARD_END_TURN => match dec::<CardEndTurnReq>(&req.payload) {
            Ok(_) => reply(encode(&app.end_turn(req.conn_id).await)),
            Err(e) => err(format!("bad payload: {e}")),
        },
        msg::CARD_STATE => match dec::<CardStateReq>(&req.payload) {
            Ok(_) => reply(encode(&app.state(req.conn_id).await)),
            Err(e) => err(format!("bad payload: {e}")),
        },
        _ => err(format!("unknown msg_id 0x{:X}", req.msg_id)),
    }
}

#[tonic::async_trait]
impl ServiceBridge for Bridge {
    type BridgeStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ForwardReply, tonic::Status>> + Send>,
    >;

    async fn handle_message(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardReply>, Status> {
        Ok(Response::new(process(&self.app, request.into_inner()).await))
    }

    /// 双向流：持久连接上复用一条 h2 流处理所有请求。
    async fn bridge_stream(
        &self,
        request: Request<tonic::Streaming<ForwardRequest>>,
    ) -> Result<Response<Self::BridgeStreamStream>, Status> {
        let mut rx = request.into_inner();
        let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<ForwardReply, Status>>(256);
        let app = self.app.clone();
        tokio::spawn(async move {
            loop {
                match rx.message().await {
                    Ok(Some(req)) => {
                        let reply = process(&app, req).await;
                        if tx.send(Ok(reply)).await.is_err() {
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

    async fn player_connected(
        &self,
        request: Request<PlayerEvent>,
    ) -> Result<Response<grimoire_pb::pb::EventReply>, Status> {
        let conn_id = request.into_inner().conn_id;
        debug!("card player connected conn {}", conn_id);
        Ok(Response::new(grimoire_pb::pb::EventReply { ok: true }))
    }

    async fn player_disconnected(
        &self,
        request: Request<PlayerEvent>,
    ) -> Result<Response<grimoire_pb::pb::EventReply>, Status> {
        let conn_id = request.into_inner().conn_id;
        self.app.leave(conn_id).await;
        debug!("card conn {} disconnected", conn_id);
        Ok(Response::new(grimoire_pb::pb::EventReply { ok: true }))
    }
}

fn encode<T: prost::Message>(m: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    prost::Message::encode(m, &mut buf).unwrap();
    buf
}

fn dec<T: prost::Message + Default>(b: &[u8]) -> Result<T, prost::DecodeError> {
    grimoire_pb::pb::decode_message(b)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let pusher = Pusher::connect(&args.registry).await?;
    let store = match grimoire_svcfw::ProfileStore::connect(&args.pg_url, &args.redis_url).await {
        Ok(s) => {
            info!("persistence enabled (postgres + redis)");
            Some(Arc::new(s))
        }
        Err(e) => {
            warn!("persistence disabled: {}", e);
            None
        }
    };
    let app = Arc::new(App::new(pusher, store));

    grimoire_svcfw::register_and_heartbeat(
        &args.registry,
        svc::CARD,
        &args.node_id,
        &args.listen,
        Default::default(),
        10,
    )
    .await?;

    info!("card-svc listening on {}", args.listen);
    Server::builder()
        .add_service(ServiceBridgeServer::new(Bridge { app }))
        .serve(args.listen.parse()?)
        .await?;
    Ok(())
}
