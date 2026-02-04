use clap::Parser;
use rustkvstore::network::{run_server, ServerState};
use rustkvstore::protocol::Command;
use rustkvstore::storage::ShardedDb;
use rustkvstore::wal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "rustkvstore", about = "High-performance async key-value store")]
struct Cli {
    /// Address to bind the server to.
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    addr: SocketAddr,

    /// Path to the Write-Ahead Log file.
    #[arg(short, long, default_value = "data.wal")]
    wal_path: PathBuf,

    /// Number of shards for the in-memory store.
    #[arg(short, long, default_value_t = 64)]
    shards: usize,

    /// WAL channel buffer size (controls backpressure).
    #[arg(long, default_value_t = 4096)]
    wal_buffer: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // --- Recovery phase ---
    info!("Replaying WAL from {}", cli.wal_path.display());
    let recovered = wal::replay_wal(&cli.wal_path).await?;
    let db = Arc::new(ShardedDb::with_shard_count(cli.shards));

    let mut applied = 0usize;
    for cmd in recovered {
        match cmd {
            Command::Set { key, value } => {
                db.set(key, value).await;
                applied += 1;
            }
            Command::Delete { key } => {
                db.delete(&key).await;
                applied += 1;
            }
            _ => {
                warn!("Skipping non-mutating command during WAL replay");
            }
        }
    }
    info!("WAL replay complete: {applied} mutations applied");

    // --- Start WAL manager ---
    let (wal_tx, wal_rx) = mpsc::channel(cli.wal_buffer);
    tokio::spawn(wal::wal_manager(cli.wal_path.clone(), wal_rx));

    // --- Start server ---
    let state = Arc::new(ServerState { db, wal_tx });
    run_server(cli.addr, state).await?;

    Ok(())
}
