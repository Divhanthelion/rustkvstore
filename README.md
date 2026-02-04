# rustkvstore

A high-performance async key-value store written in Rust, built on Tokio. Features a sharded in-memory store, write-ahead log (WAL) with group commit, a length-prefixed bincode wire protocol, and a ratatui-based TUI client.

## Architecture

```
┌──────────┐    TCP    ┌──────────────┐     ┌────────────┐
│  kvcli   │◄────────►│    Server     │────►│ ShardedDb  │
│  (TUI)   │          │  (per-conn   │     │ (64 shards,│
└──────────┘          │   tasks)     │     │  RwLock)   │
                      └──────┬───────┘     └────────────┘
                             │
                       mpsc channel
                             │
                      ┌──────▼───────┐
                      │ WAL Manager  │
                      │ (group commit│
                      │  + fsync)    │
                      └──────────────┘
```

### Modules

| Module | Description |
|--------|-------------|
| `src/main.rs` | Server binary — CLI args, WAL replay on startup, TCP listener |
| `src/bin/kvcli.rs` | TUI client — ratatui interactive shell with scrollable history |
| `src/storage/` | `ShardedDb` — concurrent hash map split across N shards (default 64), each behind a `tokio::sync::RwLock` |
| `src/protocol/` | `Command`/`Response` enums, bincode serialization, length-prefixed `KvCodec` (4-byte big-endian length + payload) |
| `src/network/` | TCP accept loop, per-connection framed I/O, WAL-gated mutations |
| `src/wal/` | Write-ahead log — CRC32-checksummed records, batched writes with 5ms/16KB group commit, crash-safe replay |
| `benches/` | Criterion benchmarks for serialization and sharded DB operations |

## Usage

### Start the server

```bash
cargo run --bin rustkvstore
```

Options:

- `-a, --addr <ADDR>` — bind address (default: `127.0.0.1:6379`)
- `-w, --wal-path <PATH>` — WAL file path (default: `data.wal`)
- `-s, --shards <N>` — number of shards (default: `64`)
- `--wal-buffer <N>` — WAL channel buffer size (default: `4096`)

### Start the TUI client

```bash
cargo run --bin kvcli [ADDR]
```

Address defaults to `127.0.0.1:6379` if omitted.

### Client commands

| Command | Description |
|---------|-------------|
| `set <key> <value>` | Store a key-value pair |
| `get <key>` | Retrieve a value by key |
| `del <key>` | Delete a key |
| `ping` | Server health check |
| `connect [addr]` | Reconnect (optionally to a new address) |
| `help` | Show available commands |
| `quit` / `exit` | Exit the client |

Scroll history with **Up/Down** and **PageUp/PageDown**. Press **Esc** or **Ctrl+C** to quit.

## Wire protocol

All messages use a `[4-byte BE length][bincode payload]` framing. Commands and responses are bincode-serialized enums with varint encoding and a 16 MB size limit. Enum variants are append-only for forward compatibility.

## Write-ahead log

Mutations (`Set`, `Delete`) are written to the WAL before being applied in memory. Records use the format `[4B length][4B CRC32][payload]`. The WAL manager batches writes and issues a single `fsync` per batch (up to 16 KB or 5ms), then notifies all waiting clients. On startup, the server replays the WAL to recover state.

## Testing

```bash
cargo test
```

## Benchmarks

```bash
cargo bench
```

Benchmarks cover bincode serialization round-trips and `ShardedDb` get/set throughput across different shard counts.
