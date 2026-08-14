//! 玩家资料持久化：Postgres 权威存储 + Redis 缓存。
//!
//! 读写路径：
//!   load: Redis 命中 → 返回；未命中 → Postgres 查询 → 回填 Redis(TTL)
//!   save: Postgres UPSERT → Redis 更新/失效
//! 表结构（首次启动自动建表）：
//!   players(player_id BIGINT PK, name TEXT, games BIGINT, wins BIGINT)

use anyhow::Context;
use redis::aio::MultiplexedConnection;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::{debug, warn};

const REDIS_TTL_SECS: usize = 60;
const KEY_PREFIX: &str = "grimoire:profile:";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct Profile {
    pub player_id: i64,
    pub name: String,
    pub games: i64,
    pub wins: i64,
}

pub struct ProfileStore {
    pg: PgPool,
    redis: MultiplexedConnection,
}

impl ProfileStore {
    pub async fn connect(pg_url: &str, redis_url: &str) -> anyhow::Result<Self> {
        let pg = PgPoolOptions::new()
            .max_connections(8)
            .connect(pg_url)
            .await
            .context("connect postgres")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS players (
                player_id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                games BIGINT NOT NULL DEFAULT 0,
                wins BIGINT NOT NULL DEFAULT 0
            )",
        )
        .execute(&pg)
        .await
        .context("init schema")?;
        let client = redis::Client::open(redis_url).context("open redis")?;
        let redis = client.get_multiplexed_async_connection().await.context("connect redis")?;
        Ok(Self { pg, redis })
    }

    pub fn key(player_id: i64) -> String {
        format!("{}{}", KEY_PREFIX, player_id)
    }

    /// 加载资料（Redis 缓存优先）。
    pub async fn load(&self, player_id: i64) -> Option<Profile> {
        // 1) Redis 缓存
        let cached: Option<String> = redis::cmd("GET")
            .arg(Self::key(player_id))
            .query_async(&mut self.redis.clone())
            .await
            .ok();
        if let Some(json) = cached {
            if let Ok(p) = serde_json::from_str(&json) {
                return Some(p);
            }
        }
        // 2) Postgres
        let row = sqlx::query_as::<_, Profile>(
            "SELECT player_id, name, games, wins FROM players WHERE player_id = $1",
        )
        .bind(player_id)
        .fetch_optional(&self.pg)
        .await
        .ok()
        .flatten();
        if let Some(p) = &row {
            self.cache_set(p).await;
        }
        row
    }

    /// 创建资料（已存在则返回现有记录）。
    pub async fn create(&self, player_id: i64, name: &str) -> anyhow::Result<Profile> {
        let p = sqlx::query_as::<_, Profile>(
            "INSERT INTO players (player_id, name) VALUES ($1, $2)
             ON CONFLICT (player_id) DO UPDATE SET name = EXCLUDED.name
             RETURNING player_id, name, games, wins",
        )
        .bind(player_id)
        .bind(name)
        .fetch_one(&self.pg)
        .await
        .context("insert player")?;
        self.cache_set(&p).await;
        Ok(p)
    }

    /// 记录一局对战结果（无档案行则自动创建，UPSERT）。
    pub async fn record_game(&self, players: &[i64], winner: i64) -> anyhow::Result<()> {
        for &pid in players {
            let win = if pid == winner { 1 } else { 0 };
            sqlx::query(
                "INSERT INTO players (player_id, name) VALUES ($1, 'grimoire')
                 ON CONFLICT (player_id) DO UPDATE
                 SET games = players.games + 1, wins = players.wins + $2",
            )
            .bind(pid)
            .bind(win)
            .execute(&self.pg)
            .await
            .context("upsert stats")?;
            // 失效缓存，下次 load 走 DB
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(Self::key(pid))
                .query_async(&mut self.redis.clone())
                .await;
        }
        debug!("record_game players={:?} winner={}", players, winner);
        Ok(())
    }

    async fn cache_set(&self, p: &Profile) {
        let json = match serde_json::to_string(p) {
            Ok(j) => j,
            Err(_) => return,
        };
        let r: Result<(), redis::RedisError> = redis::cmd("SETEX")
            .arg(Self::key(p.player_id))
            .arg(REDIS_TTL_SECS)
            .arg(json)
            .query_async(&mut self.redis.clone())
            .await;
        if let Err(e) = r {
            warn!("redis set profile {} failed: {}", p.player_id, e);
        }
    }
}
