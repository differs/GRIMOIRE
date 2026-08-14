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
}
