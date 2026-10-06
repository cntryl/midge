//! A client's acknowledged inserts survive the warmup/measurement boundary.

use super::{deterministic_u64, make_key, make_value, retry_write_stall_observed};
use cntryl_midge::{
    ColumnFamilyId, Engine, MidgeError, MidgeResult, Query, TransactionMode, WriteOptions,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct InsertInventory {
    initial_keys: u64,
    client_id: usize,
    committed: Arc<AtomicU64>,
    read_hits: Arc<AtomicU64>,
}

impl InsertInventory {
    pub fn new(initial_keys: usize, client_id: usize) -> Self {
        assert!(initial_keys > 0, "read-latest requires initial rows");
        Self {
            initial_keys: u64::try_from(initial_keys).expect("initial dataset fits u64"),
            client_id,
            committed: Arc::new(AtomicU64::new(0)),
            read_hits: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    pub fn read_hits(&self) -> u64 {
        self.read_hits.load(Ordering::Acquire)
    }

    fn physical_insert(&self, ordinal: u64) -> u64 {
        assert!(ordinal < (1_u64 << 32), "client insert namespace exhausted");
        self.initial_keys
            .checked_add(u64::try_from(self.client_id).unwrap() << 32)
            .and_then(|base| base.checked_add(ordinal))
            .expect("physical insert ID fits u64")
    }

    pub fn read_key(&self, hot: bool, entropy: u64) -> [u8; super::KEY_SIZE] {
        let count = self.initial_keys + self.committed();
        let position = if hot {
            count - 1 - entropy % (count / 10).max(1)
        } else {
            entropy % count
        };
        let physical = if position < self.initial_keys {
            position
        } else {
            self.physical_insert(position - self.initial_keys + 1)
        };
        make_key(physical)
    }

    pub fn insert(
        &self,
        engine: &Engine,
        cf_id: ColumnFamilyId,
        stop: &AtomicBool,
        op_index: u64,
        write_opts: WriteOptions,
    ) -> MidgeResult<bool> {
        self.insert_with(engine, cf_id, stop, op_index, |key, value| {
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
            tx.put(key.to_vec(), value.to_vec(), None)?;
            tx.commit(write_opts)
        })
    }

    fn insert_with<F>(
        &self,
        engine: &Engine,
        cf_id: ColumnFamilyId,
        stop: &AtomicBool,
        op_index: u64,
        mut commit: F,
    ) -> MidgeResult<bool>
    where
        F: FnMut(&[u8], &[u8]) -> MidgeResult<()>,
    {
        let next = self
            .committed()
            .checked_add(1)
            .expect("insert count fits u64");
        let key = make_key(self.physical_insert(next));
        let value = make_value(u8::try_from(op_index % 251).unwrap());
        let completed = retry_write_stall_observed(engine, cf_id, stop, || commit(&key, &value))?;
        if completed {
            self.committed.store(next, Ordering::Release);
        }
        Ok(completed)
    }

    #[allow(clippy::too_many_arguments)] // Mirrors the real duration-worker callback and write policy.
    pub fn read_latest_step(
        &self,
        engine: &Engine,
        cf_id: ColumnFamilyId,
        stop: &AtomicBool,
        seed: u64,
        op_index: u64,
        write_opts: WriteOptions,
    ) -> MidgeResult<bool> {
        let mix = deterministic_u64(seed, self.client_id, op_index, 0) % 100;
        if mix >= 95 {
            return self.insert(engine, cf_id, stop, op_index, write_opts);
        }
        let hot = mix < 90;
        let entropy = deterministic_u64(seed, self.client_id, op_index, if hot { 1 } else { 2 });
        let key = self.read_key(hot, entropy);
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        if tx.get(&key)?.is_none() {
            return Err(MidgeError::Internal(
                "YCSB D selected an absent inventory key".into(),
            ));
        }
        self.read_hits.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

pub fn verify_inventory(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    initial_keys: usize,
    clients: &[InsertInventory],
) -> MidgeResult<u64> {
    let expected = u64::try_from(initial_keys).unwrap()
        + clients.iter().map(InsertInventory::committed).sum::<u64>();
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let mut rows = 0_u64;
    for row in tx.scan(&Query::new())? {
        row?;
        rows += 1;
    }
    if rows != expected {
        return Err(MidgeError::Internal(format!(
            "YCSB fresh-insert inventory has {rows} rows; expected {expected}"
        )));
    }
    Ok(rows)
}

#[cfg(all(test, feature = "internal-testing"))]
mod tests {
    #[allow(unused_imports)] // Harnessless benches compile this module without test bodies.
    use super::*;

    #[test]
    fn should_advance_inventory_when_stop_arrives_after_actual_commit() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let opts = cntryl_midge::OpenOptions::local(dir.path())
            .build()
            .unwrap();
        let mut engine = Engine::open(opts.clone()).unwrap();
        let cf = engine.create_column_family("cf1").unwrap();
        super::super::load_initial_dataset(&engine, &cf, 16);
        let inventory = InsertInventory::new(16, 63);
        let stop = AtomicBool::new(false);

        // Act: a real successful commit is followed by stop before the wrapper returns.
        let completed = inventory
            .insert_with(&engine, cf.id(), &stop, 0, |key, value| {
                let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
                tx.put(key.to_vec(), value.to_vec(), None)?;
                tx.commit(WriteOptions::sync())?;
                stop.store(true, Ordering::Release);
                Ok(())
            })
            .unwrap();
        let rows =
            verify_inventory(&engine, cf.id(), 16, std::slice::from_ref(&inventory)).unwrap();
        engine.shutdown(std::time::Duration::from_secs(30)).unwrap();
        drop(engine);
        let mut reopened = Engine::open(opts).unwrap();
        let cf = reopened.get_column_family("cf1").unwrap();
        let recovered =
            verify_inventory(&reopened, cf.id(), 16, std::slice::from_ref(&inventory)).unwrap();
        reopened
            .shutdown(std::time::Duration::from_secs(30))
            .unwrap();

        // Assert
        assert!(completed);
        assert_eq!(inventory.committed(), 1);
        assert_eq!(rows, 17);
        assert_eq!(recovered, rows);
    }
}
