use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub const MAGIC: &[u8; 2] = b"MT";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 16;
pub const MAX_PAYLOAD: u32 = 4 * 1024 * 1024;

/// 包类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PType {
    Request = 0,
    Response = 1,
    Push = 2,
    Heartbeat = 3,
    Close = 4,
}

impl PType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Request,
            1 => Self::Response,
            2 => Self::Push,
            3 => Self::Heartbeat,
            4 => Self::Close,
            _ => return None,
        })
    }
}

/// 一帧消息
#[derive(Debug, Clone)]
pub struct Frame {
    pub ptype: PType,
    /// 高8位玩法域 + 低24位消息号
    pub msg_id: u32,
    pub seq: u32,
    pub payload: Bytes,
}

pub fn encode_into(f: &Frame, buf: &mut BytesMut) {
    buf.reserve(HEADER_LEN + f.payload.len());
    buf.put_slice(MAGIC);
    buf.put_u8(VERSION);
    buf.put_u8(f.ptype as u8);
    buf.put_u32(f.msg_id);
    buf.put_u32(f.seq);
    buf.put_u32(f.payload.len() as u32);
    buf.extend_from_slice(&f.payload);
}

pub fn encode(f: &Frame) -> BytesMut {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + f.payload.len());
    encode_into(f, &mut buf);
    buf
}

/// 粘包/拆包编解码器（TCP 流式）
#[derive(Debug, Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }
        if &src[0..2] != MAGIC {
            return Err(FrameError::BadMagic);
        }
        let ver = src[2];
        if ver != VERSION {
            return Err(FrameError::BadVersion(ver));
        }
        let ptype = PType::from_u8(src[3]).ok_or(FrameError::BadPType(src[3]))?;
        let msg_id = u32::from_be_bytes([src[4], src[5], src[6], src[7]]);
        let seq = u32::from_be_bytes([src[8], src[9], src[10], src[11]]);
        let len = u32::from_be_bytes([src[12], src[13], src[14], src[15]]);
        if len > MAX_PAYLOAD {
            return Err(FrameError::TooLarge(len));
        }
        let total = HEADER_LEN + len as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(HEADER_LEN);
        let payload = src.split_to(len as usize).freeze();
        Ok(Some(Frame { ptype, msg_id, seq, payload }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        encode_into(&item, dst);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("bad magic")]
    BadMagic,
    #[error("bad version {0}")]
    BadVersion(u8),
    #[error("bad ptype {0}")]
    BadPType(u8),
    #[error("payload too large: {0}")]
    TooLarge(u32),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
