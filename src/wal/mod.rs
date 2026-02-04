use crate::protocol::{Command, MAX_PAYLOAD_SIZE};
use bincode::Options;
use bytes::{BufMut, BytesMut};
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

/// On-disk record format:
///   [4 bytes: payload length (big-endian)]
///   [4 bytes: CRC32 checksum of payload]
///   [N bytes: bincode-serialized Command]
const HEADER_SIZE: usize = 8;

/// Maximum batch size before forcing a flush.
const BATCH_SIZE_LIMIT: usize = 16 * 1024; // 16 KB

/// Maximum time to wait before flushing a partial batch.
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

/// A request sent to the WAL manager actor.
pub struct WalWrite {
    pub command: Command,
    pub reply: oneshot::Sender<Result<(), WalError>>,
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Serialize(bincode::Error),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {e}"),
            WalError::Serialize(e) => write!(f, "WAL serialization error: {e}"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

impl From<bincode::Error> for WalError {
    fn from(e: bincode::Error) -> Self {
        WalError::Serialize(e)
    }
}

/// Bincode options matching the protocol module's format.
fn wal_bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_varint_encoding()
        .with_limit(MAX_PAYLOAD_SIZE as u64)
}

/// Encode a single WAL record into bytes: [len][crc32][payload].
fn encode_record(cmd: &Command) -> Result<Vec<u8>, bincode::Error> {
    let payload = wal_bincode_options().serialize(cmd)?;
    let crc = crc32fast::hash(&payload);
    let total = HEADER_SIZE + payload.len();

    let mut buf = Vec::with_capacity(total);
    buf.put_u32(payload.len() as u32);
    buf.put_u32(crc);
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// The WAL manager actor. Receives write requests, batches them, and
/// flushes to disk with a single `fsync` per batch (group commit).
pub async fn wal_manager(path: PathBuf, mut rx: mpsc::Receiver<WalWrite>) {
    let file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open WAL file {}: {e}", path.display());
            return;
        }
    };

    let mut file = file;
    let mut batch_buf = BytesMut::with_capacity(BATCH_SIZE_LIMIT * 2);
    let mut waiters: Vec<oneshot::Sender<Result<(), WalError>>> = Vec::new();

    loop {
        batch_buf.clear();
        waiters.clear();

        // Wait for the first write request (blocking).
        let first = match rx.recv().await {
            Some(w) => w,
            None => {
                info!("WAL channel closed, shutting down");
                return;
            }
        };

        // Encode the first record.
        match encode_record(&first.command) {
            Ok(rec) => {
                batch_buf.extend_from_slice(&rec);
                waiters.push(first.reply);
            }
            Err(e) => {
                let _ = first.reply.send(Err(WalError::Serialize(e)));
            }
        }

        // Drain more requests until batch limit or timeout.
        let deadline = time::sleep(BATCH_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                maybe = rx.recv() => {
                    match maybe {
                        Some(w) => {
                            match encode_record(&w.command) {
                                Ok(rec) => {
                                    batch_buf.extend_from_slice(&rec);
                                    waiters.push(w.reply);
                                }
                                Err(e) => {
                                    let _ = w.reply.send(Err(WalError::Serialize(e)));
                                }
                            }
                            if batch_buf.len() >= BATCH_SIZE_LIMIT {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                () = &mut deadline => break,
            }
        }

        // Flush the batch to disk.
        if !batch_buf.is_empty() {
            let result = flush_batch(&mut file, &batch_buf).await;
            for waiter in waiters.drain(..) {
                let _ = waiter.send(match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(WalError::Io(io::Error::new(e.kind(), e.to_string()))),
                });
            }
            if let Err(e) = &result {
                error!("WAL flush error: {e}");
            } else {
                debug!("WAL flushed {} bytes", batch_buf.len());
            }
        }
    }
}

async fn flush_batch(file: &mut File, data: &[u8]) -> Result<(), io::Error> {
    file.write_all(data).await?;
    file.sync_all().await?;
    Ok(())
}

/// Replay the WAL file, returning all valid commands in order.
/// Stops at the first corrupted or incomplete record (tail corruption from crash).
pub async fn replay_wal(path: &Path) -> Result<Vec<Command>, WalError> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let data = fs::read(path).await?;
    let mut commands = Vec::new();
    let mut offset = 0;

    while offset + HEADER_SIZE <= data.len() {
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;

        let stored_crc = u32::from_be_bytes([
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);

        let payload_start = offset + HEADER_SIZE;
        let payload_end = payload_start + len;

        if len > MAX_PAYLOAD_SIZE {
            warn!("WAL record at offset {offset} claims {len} bytes, exceeds limit");
            break;
        }

        if payload_end > data.len() {
            warn!(
                "WAL truncated at offset {offset}: expected {len} bytes, only {} available",
                data.len() - payload_start
            );
            break;
        }

        let payload = &data[payload_start..payload_end];
        let actual_crc = crc32fast::hash(payload);

        if actual_crc != stored_crc {
            warn!("WAL CRC mismatch at offset {offset}, stopping replay");
            break;
        }

        match wal_bincode_options().deserialize::<Command>(payload) {
            Ok(cmd) => commands.push(cmd),
            Err(e) => {
                warn!("WAL deserialization error at offset {offset}: {e}");
                break;
            }
        }

        offset = payload_end;
    }

    info!("WAL replay: recovered {} commands", commands.len());
    Ok(commands)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn wal_write_and_replay() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Start WAL manager.
        let (tx, rx) = mpsc::channel(1024);
        let path_clone = wal_path.clone();
        let handle = tokio::spawn(wal_manager(path_clone, rx));

        // Write some commands.
        for i in 0..10 {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(WalWrite {
                command: Command::Set {
                    key: format!("key-{i}"),
                    value: format!("val-{i}").into_bytes(),
                },
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        }

        // Shut down WAL manager.
        drop(tx);
        handle.await.unwrap();

        // Replay and verify.
        let commands = replay_wal(&wal_path).await.unwrap();
        assert_eq!(commands.len(), 10);
        for (i, cmd) in commands.iter().enumerate() {
            match cmd {
                Command::Set { key, value } => {
                    assert_eq!(key, &format!("key-{i}"));
                    assert_eq!(value, format!("val-{i}").as_bytes());
                }
                _ => panic!("expected Set"),
            }
        }
    }

    #[tokio::test]
    async fn wal_handles_truncation() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("truncated.wal");

        // Write a valid record then garbage.
        let cmd = Command::Set {
            key: "a".into(),
            value: b"b".to_vec(),
        };
        let record = encode_record(&cmd).unwrap();
        let mut data = record.clone();
        // Append incomplete header to simulate crash.
        data.extend_from_slice(&[0xFF, 0xFF]);

        fs::write(&wal_path, &data).await.unwrap();

        let commands = replay_wal(&wal_path).await.unwrap();
        assert_eq!(commands.len(), 1);
    }
}
