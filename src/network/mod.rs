use crate::protocol::{
    decode_command, encode_response, Command, KvCodec, Response,
};
use crate::storage::ShardedDb;
use crate::wal::{WalWrite, WalError};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

/// Shared state passed to every connection handler.
pub struct ServerState {
    pub db: Arc<ShardedDb>,
    pub wal_tx: mpsc::Sender<WalWrite>,
}

/// Start the TCP server on the given address.
pub async fn run_server(addr: SocketAddr, state: Arc<ServerState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {addr}");

    loop {
        let (socket, peer) = listener.accept().await?;
        debug!("New connection from {peer}");

        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, state).await {
                warn!("Connection {peer} error: {e}");
            }
            debug!("Connection {peer} closed");
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(socket, KvCodec);

    while let Some(frame_result) = framed.next().await {
        let frame = match frame_result {
            Ok(f) => f,
            Err(e) => {
                error!("Frame decode error from {peer}: {e}");
                return Err(e.into());
            }
        };

        let cmd = match decode_command(&frame) {
            Ok(c) => c,
            Err(e) => {
                let resp = Response::Error(format!("bad command: {e}"));
                send_response(&mut framed, &resp).await?;
                continue;
            }
        };

        let response = process_command(cmd, &state).await;
        send_response(&mut framed, &response).await?;
    }

    Ok(())
}

async fn process_command(cmd: Command, state: &ServerState) -> Response {
    match cmd {
        Command::Ping => Response::Pong,

        Command::Get { ref key } => {
            let value = state.db.get(key).await;
            Response::Value(value)
        }

        Command::Set { key, value } => {
            // Clone for WAL; move originals into db after confirmation.
            let wal_cmd = Command::Set {
                key: key.clone(),
                value: value.clone(),
            };
            match wal_write(&state.wal_tx, wal_cmd).await {
                Ok(()) => {
                    state.db.set(key, value).await;
                    Response::Ok
                }
                Err(e) => Response::Error(format!("WAL error: {e}")),
            }
        }

        Command::Delete { key } => {
            let wal_cmd = Command::Delete { key: key.clone() };
            match wal_write(&state.wal_tx, wal_cmd).await {
                Ok(()) => {
                    state.db.delete(&key).await;
                    Response::Ok
                }
                Err(e) => Response::Error(format!("WAL error: {e}")),
            }
        }
    }
}

/// Send a write request to the WAL manager and wait for confirmation.
async fn wal_write(
    wal_tx: &mpsc::Sender<WalWrite>,
    command: Command,
) -> Result<(), WalError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    wal_tx
        .send(WalWrite {
            command,
            reply: reply_tx,
        })
        .await
        .map_err(|_| {
            WalError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL manager shut down",
            ))
        })?;

    reply_rx.await.map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "WAL reply dropped",
        ))
    })?
}

async fn send_response(
    framed: &mut Framed<TcpStream, KvCodec>,
    resp: &Response,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_response(resp)?;
    framed.send(encoded).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{decode_response, encode_command};
    use crate::wal;
    use tempfile::TempDir;
    use tokio::net::TcpStream;

    async fn start_test_server() -> (SocketAddr, Arc<ServerState>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let db = Arc::new(ShardedDb::new());
        let (wal_tx, wal_rx) = mpsc::channel(1024);
        tokio::spawn(wal::wal_manager(wal_path, wal_rx));

        let state = Arc::new(ServerState { db, wal_tx });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let state_clone = state.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((socket, peer)) = listener.accept().await {
                    let state = state_clone.clone();
                    tokio::spawn(async move {
                        let _ = handle_connection(socket, peer, state).await;
                    });
                }
            }
        });

        (addr, state, tmp)
    }

    #[tokio::test]
    async fn integration_set_get() {
        let (addr, _state, _tmp) = start_test_server().await;

        let socket = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(socket, KvCodec);

        // SET
        let cmd = Command::Set {
            key: "foo".into(),
            value: b"bar".to_vec(),
        };
        framed.send(encode_command(&cmd).unwrap()).await.unwrap();
        let resp_bytes = framed.next().await.unwrap().unwrap();
        let resp: Response = decode_response(&resp_bytes).unwrap();
        assert!(matches!(resp, Response::Ok));

        // GET
        let cmd = Command::Get { key: "foo".into() };
        framed.send(encode_command(&cmd).unwrap()).await.unwrap();
        let resp_bytes = framed.next().await.unwrap().unwrap();
        let resp: Response = decode_response(&resp_bytes).unwrap();
        match resp {
            Response::Value(Some(v)) => assert_eq!(v, b"bar"),
            other => panic!("expected Value, got {other:?}"),
        }

        // DELETE
        let cmd = Command::Delete { key: "foo".into() };
        framed.send(encode_command(&cmd).unwrap()).await.unwrap();
        let resp_bytes = framed.next().await.unwrap().unwrap();
        let resp: Response = decode_response(&resp_bytes).unwrap();
        assert!(matches!(resp, Response::Ok));

        // GET after DELETE
        let cmd = Command::Get { key: "foo".into() };
        framed.send(encode_command(&cmd).unwrap()).await.unwrap();
        let resp_bytes = framed.next().await.unwrap().unwrap();
        let resp: Response = decode_response(&resp_bytes).unwrap();
        assert!(matches!(resp, Response::Value(None)));
    }

    #[tokio::test]
    async fn ping_pong() {
        let (addr, _state, _tmp) = start_test_server().await;

        let socket = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(socket, KvCodec);

        let cmd = Command::Ping;
        framed.send(encode_command(&cmd).unwrap()).await.unwrap();
        let resp_bytes = framed.next().await.unwrap().unwrap();
        let resp: Response = decode_response(&resp_bytes).unwrap();
        assert!(matches!(resp, Response::Pong));
    }
}
