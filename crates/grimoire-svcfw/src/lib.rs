//! 服务框架：注册/心跳、网关 push 客户端、Bridge 服务端骨架。
//!
//! 三个玩法服务共享本 crate，避免重复网络/注册逻辑。

pub mod profile;
pub mod reg;
pub mod pusher;

pub use profile::ProfileStore;
pub use reg::register_and_heartbeat;
pub use pusher::Pusher;
