use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use tokio::sync::RwLock;

const DEFAULT_SHARD_COUNT: usize = 64;

/// A sharded concurrent key-value map.
///
/// The key space is partitioned into `N` independent shards, each protected
/// by its own `RwLock`. This reduces lock contention by a factor of `N`
/// compared to a single global lock.
pub struct ShardedDb {
    shards: Vec<RwLock<HashMap<String, Vec<u8>>>>,
    shard_count: usize,
}

impl ShardedDb {
    pub fn new() -> Self {
        Self::with_shard_count(DEFAULT_SHARD_COUNT)
    }

    pub fn with_shard_count(n: usize) -> Self {
        assert!(n > 0, "shard count must be positive");
        let shards = (0..n).map(|_| RwLock::new(HashMap::new())).collect();
        Self {
            shards,
            shard_count: n,
        }
    }

    /// Deterministically route a key to its shard.
    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shard_count
    }

    fn shard(&self, key: &str) -> &RwLock<HashMap<String, Vec<u8>>> {
        &self.shards[self.shard_index(key)]
    }

    /// Retrieve the value for a key. Returns `None` if the key doesn't exist.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let shard = self.shard(key).read().await;
        shard.get(key).cloned()
    }

    /// Insert or overwrite a key-value pair.
    pub async fn set(&self, key: String, value: Vec<u8>) {
        let mut shard = self.shard(&key).write().await;
        shard.insert(key, value);
    }

    /// Remove a key. Returns the old value if it existed.
    pub async fn delete(&self, key: &str) -> Option<Vec<u8>> {
        let mut shard = self.shard(key).write().await;
        shard.remove(key)
    }

    /// Total number of entries across all shards.
    pub async fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.read().await.len();
        }
        total
    }

    /// Whether the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl Default for ShardedDb {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn basic_get_set_delete() {
        let db = ShardedDb::new();
        assert!(db.get("foo").await.is_none());

        db.set("foo".into(), b"bar".to_vec()).await;
        assert_eq!(db.get("foo").await.unwrap(), b"bar");

        let old = db.delete("foo").await;
        assert_eq!(old.unwrap(), b"bar");
        assert!(db.get("foo").await.is_none());
    }

    #[tokio::test]
    async fn overwrite() {
        let db = ShardedDb::new();
        db.set("k".into(), b"v1".to_vec()).await;
        db.set("k".into(), b"v2".to_vec()).await;
        assert_eq!(db.get("k").await.unwrap(), b"v2");
    }

    #[tokio::test]
    async fn concurrent_writes() {
        let db = Arc::new(ShardedDb::new());
        let mut handles = vec![];
        for i in 0..200 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.set(format!("key-{i}"), format!("val-{i}").into_bytes())
                    .await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(db.len().await, 200);
    }

    #[tokio::test]
    async fn concurrent_reads_and_writes() {
        let db = Arc::new(ShardedDb::new());
        // Pre-populate
        for i in 0..100 {
            db.set(format!("key-{i}"), format!("val-{i}").into_bytes())
                .await;
        }

        let mut handles = vec![];
        // Concurrent readers
        for i in 0..100 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let val = db.get(&format!("key-{i}")).await;
                assert!(val.is_some());
            }));
        }
        // Concurrent writers
        for i in 100..200 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.set(format!("key-{i}"), format!("val-{i}").into_bytes())
                    .await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(db.len().await, 200);
    }
}
