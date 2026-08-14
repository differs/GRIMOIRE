pub mod pb {
    tonic::include_proto!("grimoire");

    /// protobuf 消息编码（业务服务统一出口）
    pub fn encode_message<M: prost::Message>(m: &M) -> Vec<u8> {
        let mut buf = Vec::with_capacity(m.encoded_len());
        m.encode(&mut buf).unwrap();
        buf
    }

    /// protobuf 消息解码
    pub fn decode_message<M: prost::Message + Default>(bytes: &[u8]) -> Result<M, prost::DecodeError> {
        M::decode(bytes)
    }
}

pub use pb::*;
