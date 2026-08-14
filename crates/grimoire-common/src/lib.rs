//! msg_id 分配与玩法域工具。
//!
//! 帧头 msg_id 布局：
//!   高 8 位 = 玩法域 (1=room 2=battle 3=card)，低 24 位 = 域内消息号。
//! gateway 仅按玩法域路由，域内消息号由各业务服务自行分发。

pub mod msg {
    pub const DOMAIN_MASK: u32 = 0xFF00_0000;
    pub const DOMAIN_ROOM: u32 = 0x0100_0000;
    pub const DOMAIN_BATTLE: u32 = 0x0200_0000;
    pub const DOMAIN_CARD: u32 = 0x0300_0000;

    /// 系统消息：连接建立后网关下发的"欢迎帧"，payload = conn_id(4B 大端)。
    /// 客户端据此拿到全局连接号，用于 UDP 绑定等场景。
    pub const SYS_CONN_ID: u32 = 0x0000_FFFF;
    /// 系统消息：客户端->网关 连接迁移（断线重连恢复会话）。
    /// payload = 旧 conn_id(4B 大端)；网关把当前连接重绑到旧 conn_id，原连接被踢。
    pub const SYS_RESUME: u32 = 0x0000_FFFE;

    pub fn domain_of(msg_id: u32) -> u32 {
        msg_id & DOMAIN_MASK
    }

    /// ---- room 玩法 ----
    pub const ROOM_LOGIN: u32 = DOMAIN_ROOM | 0x0001;
    pub const ROOM_CREATE: u32 = DOMAIN_ROOM | 0x0002;
    pub const ROOM_JOIN: u32 = DOMAIN_ROOM | 0x0003;
    pub const ROOM_LEAVE: u32 = DOMAIN_ROOM | 0x0004;
    pub const ROOM_LIST: u32 = DOMAIN_ROOM | 0x0005;
    pub const ROOM_CHAT: u32 = DOMAIN_ROOM | 0x0006;
    /// push（服务端->客户端）
    pub const ROOM_STATE_PUSH: u32 = DOMAIN_ROOM | 0x0100;
    pub const ROOM_CHAT_PUSH: u32 = DOMAIN_ROOM | 0x0101;

    /// ---- battle 玩法 ----
    pub const BATTLE_JOIN: u32 = DOMAIN_BATTLE | 0x0001;
    pub const BATTLE_INPUT: u32 = DOMAIN_BATTLE | 0x0002;
    pub const BATTLE_LEAVE: u32 = DOMAIN_BATTLE | 0x0003;
    pub const BATTLE_FRAME_SYNC: u32 = DOMAIN_BATTLE | 0x0100;

    /// ---- card 玩法 ----
    pub const CARD_START: u32 = DOMAIN_CARD | 0x0001;
    pub const CARD_PLAY: u32 = DOMAIN_CARD | 0x0002;
    pub const CARD_END_TURN: u32 = DOMAIN_CARD | 0x0003;
    pub const CARD_STATE: u32 = DOMAIN_CARD | 0x0004;
    pub const CARD_SNAPSHOT_PUSH: u32 = DOMAIN_CARD | 0x0100;
}

/// 服务名常量
pub mod svc {
    pub const GATEWAY: &str = "gateway";
    pub const ROOM: &str = "grimoire-room-svc";
    pub const BATTLE: &str = "grimoire-battle-svc";
    pub const CARD: &str = "grimoire-card-svc";
}

/// 全局连接号：高 8 位 = 网关 ID，低 24 位 = 网关内局部序号。
/// 多活网关场景下，业务服务凭 conn_id 即可定位应由哪个网关下发推送。
pub mod conn {
    pub const GATEWAY_ID_SHIFT: u32 = 24;
    pub const LOCAL_MASK: u32 = 0x00FF_FFFF;

    #[inline]
    pub fn make(gateway_id: u8, local: u32) -> u32 {
        ((gateway_id as u32) << GATEWAY_ID_SHIFT) | (local & LOCAL_MASK)
    }

    #[inline]
    pub fn gateway_id_of(conn_id: u32) -> u8 {
        (conn_id >> GATEWAY_ID_SHIFT) as u8
    }

    #[inline]
    pub fn local_of(conn_id: u32) -> u32 {
        conn_id & LOCAL_MASK
    }
}
