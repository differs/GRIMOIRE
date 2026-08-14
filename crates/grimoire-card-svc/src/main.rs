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
use tracing::{debug, info};

const GAME_CAPACITY: usize = 2;
const INIT_HP: u32 = 30;
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
}

impl App {
    fn new(pusher: Pusher) -> Self {
        Self {
            games: DashMap::new(),
            conn_game: DashMap::new(),
            next_player: AtomicU32::new(1),
            next_game: AtomicU32::new(1),
            pusher,
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

        // 找等待中的局（1 人未满）
        let mut target = None;
        for e in self.games.iter() {
            let gid = *e.key();
            let g = e.value();
            let guard = g.lock().await;
            if !guard.is_full() {
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
        drop(game);

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
        let g = self.games.get(&gid).unwrap();
        let mut game = g.value().lock().await;

        let fail = |detail: &str| CardPlayResp { ok: false, detail: detail.into(), state: None };

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
        let mut finished = false;
        game.check_finish();
        if game.phase != PHASE_PLAYING {
            finished = true;
        } else {
            game.turn = victim; // 回合切换到对方
        }
        let state = game.view_for(idx);
        drop(game);

        let resp = CardPlayResp {
            ok: true,
            detail: format!("{} 造成 {} 点伤害", card.name, card.power),
            state: Some(state),
        };
        self.push_snapshot(gid).await;
        info!(game_id = gid, "card played, finished={}", finished);
        resp
    }

    async fn end_turn(&self, conn_id: u32) -> CardEndTurnResp {
        let Some((gid, idx)) = self.conn_game.get(&conn_id).map(|v| *v) else {
            return CardEndTurnResp { ok: false, state: None };
        };
        let g = self.games.get(&gid).unwrap();
        let mut game = g.value().lock().await;
        if game.phase != PHASE_PLAYING || game.turn != idx {
            return CardEndTurnResp { ok: false, state: None };
        }
        game.turn = (idx + 1) % game.players.len();
        let state = game.view_for(idx);
        drop(game);
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

#[tonic::async_trait]
impl ServiceBridge for Bridge {
    async fn handle_message(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardReply>, Status> {
        let req = request.into_inner();
        let app = &self.app;
        let payload = match req.msg_id {
            msg::CARD_START => {
                let _m: CardStartReq = dec(&req.payload)?;
                encode(&app.start(req.conn_id).await)
            }
            msg::CARD_PLAY => {
                let m: CardPlayReq = dec(&req.payload)?;
                encode(&app.play(req.conn_id, m.hand_index, m.target_player).await)
            }
            msg::CARD_END_TURN => {
                let _m: CardEndTurnReq = dec(&req.payload)?;
                encode(&app.end_turn(req.conn_id).await)
            }
            msg::CARD_STATE => {
                let _m: CardStateReq = dec(&req.payload)?;
                encode(&app.state(req.conn_id).await)
            }
            _ => return Err(Status::not_found(format!("unknown msg_id 0x{:X}", req.msg_id))),
        };
        Ok(Response::new(ForwardReply { payload }))
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

fn dec<T: prost::Message + Default>(b: &[u8]) -> Result<T, Status> {
    grimoire_pb::pb::decode_message(b).map_err(|e| Status::invalid_argument(e.to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let pusher = Pusher::connect(&args.registry).await?;
    let app = Arc::new(App::new(pusher));

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
