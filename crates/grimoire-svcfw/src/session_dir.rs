//! Redis 会话目录：玩法会话(房间/对局/战斗) → 托管节点 的映射。
//!
//! 多实例下按"会话"路由的关键：网关收到请求后查目录定位托管该会话的节点，
//! 同一会话内所有玩家的请求都路由到同一实例（跨玩家状态不碎片化）。
//! 服务在建/加入会话时调用 bind 登记；网关负责 lookup。

use anyhow::Context;
use redis::aio::MultiplexedConnection;
use tracing::{debug, warn};

const KEY_PREFIX: &str = "grimoire:sess:";
const TTL_SECS: usize = 3600;

pub struct SessionDir {
    redis: MultiplexedConnection,
}

fn key(domain: u32, session_id: u32) -> String {
    format!("{}{}:{}", KEY_PREFIX, domain, session_id)
}

impl SessionDir {
    pub async fn connect(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url).context("open redis")?;
        let redis = client.get_multiplexed_async_connection().await.context("connect redis")?;
        Ok(Self { redis })
    }

    /// 登记：本节点托管某会话（1h TTL，访问可续）。
    pub async fn bind(&self, domain: u32, session_id: u32, node_id: &str) -> bool {
        let r: Result<(), redis::RedisError> = redis::cmd("SETEX")
            .arg(key(domain, session_id))
            .arg(TTL_SECS)
            .arg(node_id)
            .query_async(&mut self.redis.clone())
            .await;
        if let Err(e) = r {
            warn!("session bind {}:{} -> {} failed: {}", domain, session_id, node_id, e);
            return false;
        }
        debug!("session bound {}:{} -> {}", domain, session_id, node_id);
        true
    }

    /// 查询某会话的托管节点。
    pub async fn lookup(&self, domain: u32, session_id: u32) -> Option<String> {
        let r: Result<Option<String>, redis::RedisError> = redis::cmd("GET")
            .arg(key(domain, session_id))
            .query_async(&mut self.redis.clone())
            .await;
        match r {
            Ok(v) => v,
            Err(e) => {
                warn!("session lookup {}:{} failed: {}", domain, session_id, e);
                None
            }
        }
    }

    /// 删除会话目录条目（会话结束 / 节点失效时清理）。
    pub async fn remove(&self, domain: u32, session_id: u32) {
        let r: Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(key(domain, session_id))
            .query_async(&mut self.redis.clone())
            .await;
        if let Err(e) = r {
            warn!("session remove {}:{} failed: {}", domain, session_id, e);
        }
    }

    fn lobby_key(domain: u32) -> String {
        format!("{}lobby:{}", KEY_PREFIX, domain)
    }

    /// 撮合大厅：登记一个等待匹配的会话（SETNX，已有等待局则不覆盖）。
    pub async fn lobby_set(&self, domain: u32, session_id: u32) -> bool {
        let r: Result<bool, redis::RedisError> = redis::cmd("SET")
            .arg(Self::lobby_key(domain))
            .arg(session_id)
            .arg("NX")
            .query_async(&mut self.redis.clone())
            .await;
        r.unwrap_or(false)
    }

    /// 撮合大厅：原子取走一个等待会话（GETDEL），返回 None 表示无等待局。
    pub async fn lobby_take(&self, domain: u32) -> Option<u32> {
        let r: Result<Option<u32>, redis::RedisError> = redis::cmd("GETDEL")
            .arg(Self::lobby_key(domain))
            .query_async(&mut self.redis.clone())
            .await;
        match r {
            Ok(v) => v,
            Err(e) => {
                warn!("lobby take {} failed: {}", domain, e);
                None
            }
        }
    }

    /// 撮合大厅：仅当值匹配时清除（避免清掉新登记的等待局）。
    pub async fn lobby_clear(&self, domain: u32, session_id: u32) {
        let script = r#"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
        "#;
        let r: Result<i64, redis::RedisError> = redis::Script::new(script)
            .key(Self::lobby_key(domain))
            .arg(session_id)
            .invoke_async(&mut self.redis.clone())
            .await;
        if let Err(e) = r {
            warn!("lobby clear {}:{} failed: {}", domain, session_id, e);
        }
    }
}
