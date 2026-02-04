use bincode::Options;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum payload size for both network frames and WAL records.
pub const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024; // 16 MB

/// All operations the KV store supports.
///
/// **Forward-compatibility invariant:** New variants MUST only be appended
/// at the end. Bincode encodes enum variants by positional index (u32).
/// Reordering or inserting variants will break WAL replay and network
/// protocol compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Get { key: String },       // variant index 0
    Set { key: String, value: Vec<u8> }, // variant index 1
    Delete { key: String },    // variant index 2
    Ping,                      // variant index 3
}

/// Server response to a client command.
///
/// **Forward-compatibility invariant:** Same as `Command` — append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,                        // variant index 0
    Value(Option<Vec<u8>>),    // variant index 1
    Error(String),             // variant index 2
    Pong,                      // variant index 3
}

/// Bincode options matching the legacy `bincode::serialize` format
/// but with a size limit to prevent OOM on untrusted input.
fn bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_varint_encoding() // matches legacy bincode::serialize format
        .with_limit(MAX_PAYLOAD_SIZE as u64)
}

/// A codec that frames messages as `[4-byte big-endian length][bincode payload]`.
///
/// Works for any `Serialize + DeserializeOwned` pair. We use two concrete
/// instances: one for the client side (sends Commands, receives Responses)
/// and one for the server side (receives Commands, sends Responses).
pub struct KvCodec;

impl Decoder for KvCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        // Peek at the length without advancing the cursor.
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Guard against absurdly large frames (16 MB limit).
        if len > 16 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {len} bytes"),
            ));
        }

        if src.len() < 4 + len {
            // Reserve space so the next read doesn't re-allocate.
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        // Consume length header + payload.
        src.advance(4);
        let payload = src.split_to(len).freeze();
        Ok(Some(payload))
    }
}

impl Encoder<Bytes> for KvCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.len();
        if len > 16 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {len} bytes"),
            ));
        }
        dst.reserve(4 + len);
        dst.put_u32(len as u32);
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// Serialize a `Command` to `Bytes` via bincode.
pub fn encode_command(cmd: &Command) -> Result<Bytes, bincode::Error> {
    let v = bincode_options().serialize(cmd)?;
    Ok(Bytes::from(v))
}

/// Deserialize a `Command` from a byte slice (size-limited).
pub fn decode_command(data: &[u8]) -> Result<Command, bincode::Error> {
    bincode_options().deserialize(data)
}

/// Serialize a `Response` to `Bytes` via bincode.
pub fn encode_response(resp: &Response) -> Result<Bytes, bincode::Error> {
    let v = bincode_options().serialize(resp)?;
    Ok(Bytes::from(v))
}

/// Deserialize a `Response` from a byte slice (size-limited).
pub fn decode_response(data: &[u8]) -> Result<Response, bincode::Error> {
    bincode_options().deserialize(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_command() {
        let cmd = Command::Set {
            key: "hello".into(),
            value: b"world".to_vec(),
        };
        let encoded = encode_command(&cmd).unwrap();
        let decoded = decode_command(&encoded).unwrap();
        match decoded {
            Command::Set { key, value } => {
                assert_eq!(key, "hello");
                assert_eq!(value, b"world");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn roundtrip_response() {
        let resp = Response::Value(Some(b"data".to_vec()));
        let encoded = encode_response(&resp).unwrap();
        let decoded = decode_response(&encoded).unwrap();
        match decoded {
            Response::Value(Some(v)) => assert_eq!(v, b"data"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn codec_framing() {
        let mut codec = KvCodec;
        let payload = Bytes::from_static(b"hello");

        // Encode
        let mut buf = BytesMut::new();
        codec.encode(payload.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), 4 + 5);

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, payload);
    }
}
