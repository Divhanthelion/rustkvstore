use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rustkvstore::protocol::{Command, encode_command, decode_command, encode_response, decode_response, Response};
use rustkvstore::storage::ShardedDb;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");

    let cmd = Command::Set {
        key: "benchmark-key".into(),
        value: vec![0u8; 256],
    };

    group.bench_function("encode_command", |b| {
        b.iter(|| encode_command(&cmd).unwrap());
    });

    let encoded = encode_command(&cmd).unwrap();
    group.bench_function("decode_command", |b| {
        b.iter(|| decode_command(&encoded).unwrap());
    });

    let resp = Response::Value(Some(vec![0u8; 256]));
    group.bench_function("encode_response", |b| {
        b.iter(|| encode_response(&resp).unwrap());
    });

    let encoded_resp = encode_response(&resp).unwrap();
    group.bench_function("decode_response", |b| {
        b.iter(|| decode_response(&encoded_resp).unwrap());
    });

    group.finish();
}

fn bench_sharded_db(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sharded_db");

    for shard_count in [1, 16, 64, 256] {
        let db = Arc::new(ShardedDb::with_shard_count(shard_count));

        // Pre-populate
        rt.block_on(async {
            for i in 0..1000 {
                db.set(format!("key-{i}"), format!("val-{i}").into_bytes()).await;
            }
        });

        group.bench_with_input(
            BenchmarkId::new("get", shard_count),
            &shard_count,
            |b, _| {
                let db = db.clone();
                b.to_async(&rt).iter(|| {
                    let db = db.clone();
                    async move {
                        db.get("key-500").await;
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("set", shard_count),
            &shard_count,
            |b, _| {
                let db = db.clone();
                b.to_async(&rt).iter(|| {
                    let db = db.clone();
                    async move {
                        db.set("bench-key".into(), b"bench-val".to_vec()).await;
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_serialization, bench_sharded_db);
criterion_main!(benches);
