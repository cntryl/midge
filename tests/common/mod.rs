#![allow(dead_code, unused_imports)]

use cntryl_midge::{
    ColumnFamilyHandle, Engine, Goal, MemoryBudget, MidgeEngine, MidgeResult, OpenOptions, Storage,
    WorkloadProfile, WriteOptions,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub enum StorageMode {
    Memory,
    LocalDisk { db_path: PathBuf },
    CloudBacked { local_cache_path: PathBuf },
}

#[derive(Clone)]
pub struct MidgeOptions {
    pub storage_mode: StorageMode,
    pub wal_sync: bool,
    pub wal_batch_config: Option<cntryl_midge::wal::policy::BatchConfig>,
    pub memtable_size: usize,
    pub compression: bool,
    pub enable_compaction: bool,
    pub memory_budget: Option<usize>,
}

impl Default for MidgeOptions {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::Memory,
            wal_sync: false,
            wal_batch_config: None,
            memtable_size: 64 * 1024 * 1024,
            compression: false,
            enable_compaction: true,
            memory_budget: None,
        }
    }
}

impl MidgeOptions {
    pub fn memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget = Some(bytes);
        self
    }

    pub fn to_open_options(&self) -> OpenOptions {
        let storage = match &self.storage_mode {
            StorageMode::Memory => Storage::InMemory,
            StorageMode::LocalDisk { db_path } => Storage::Local {
                path: db_path.clone(),
            },
            StorageMode::CloudBacked { local_cache_path } => Storage::CloudSimulated {
                local_cache_path: local_cache_path.clone(),
                bucket: "test-bucket".to_string(),
                prefix: "test-prefix/".to_string(),
            },
        };

        let mut open_opts = match storage {
            Storage::InMemory => OpenOptions::in_memory(),
            Storage::Local { path } => OpenOptions::local(path),
            Storage::CloudSimulated {
                local_cache_path,
                bucket,
                prefix,
            } => OpenOptions::cloud_simulated(local_cache_path, bucket, prefix),
            Storage::Cloud { .. } => unreachable!("tests/common uses simulated cloud storage"),
        };

        open_opts = open_opts
            .memory_budget(match self.memory_budget {
                Some(n) => MemoryBudget::Bytes(n),
                None => MemoryBudget::Auto,
            })
            .workload(WorkloadProfile::default())
            .goal(Goal::default())
            .build();
        open_opts
    }
}

pub fn all_storage_modes_new() -> Vec<&'static str> {
    vec!["memory", "local", "cloud"]
}

pub fn durable_storage_modes() -> &'static [&'static str] {
    &["local", "cloud"]
}

pub fn memory_storage_modes() -> Vec<&'static str> {
    vec!["memory"]
}

pub fn memory_opts() -> MidgeOptions {
    opts_for_mode("memory")
}

pub fn opts_for_mode(mode: &str) -> MidgeOptions {
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    let unique_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);

    match mode {
        "memory" => MidgeOptions {
            storage_mode: StorageMode::Memory,
            wal_sync: false,
            wal_batch_config: None,
            memtable_size: 64 * 1024,
            compression: false,
            enable_compaction: false,
            memory_budget: None,
        },
        "local" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let test_dir = PathBuf::from(format!(
                "target/tmp/midge_test_local_{}_{}_{}",
                std::process::id(),
                unique_id,
                timestamp
            ));
            let _ = std::fs::create_dir_all(&test_dir);
            MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: test_dir },
                wal_sync: false,
                wal_batch_config: None,
                memtable_size: 64 * 1024,
                compression: false,
                enable_compaction: false,
                memory_budget: None,
            }
        }
        "cloud" => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let test_dir = PathBuf::from(format!(
                "target/tmp/midge_test_cloud_{}_{}_{}",
                std::process::id(),
                unique_id,
                timestamp
            ));
            let _ = std::fs::create_dir_all(&test_dir);
            MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: test_dir,
                },
                wal_sync: true,
                wal_batch_config: None,
                memtable_size: 2 * 1024 * 1024,
                compression: false,
                enable_compaction: false,
                memory_budget: None,
            }
        }
        _ => panic!("unknown storage mode: {}", mode),
    }
}

pub fn for_each_storage_mode<F>(modes: &[&str], test_fn: F)
where
    F: Fn(&str, MidgeOptions),
{
    for mode in modes {
        if let Some(only) = std::env::var_os("MIDGE_TEST_ONLY_MODE") {
            if only.as_os_str() != std::ffi::OsStr::new(mode) {
                continue;
            }
        }

        if std::env::var_os("MIDGE_TEST_TRACE_MODES").is_some() {
            eprintln!("[midge-test] mode={mode}");
        }
        test_fn(mode, opts_for_mode(mode));
    }
}

pub fn test_temp_dir() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("Failed to create temp dir")
}

pub fn open_with_mode(opts: MidgeOptions, _mode: &str) -> Engine {
    Engine::open(opts.to_open_options()).expect("failed to open engine")
}

pub struct DurabilityTestContext {
    _private: (),
}

impl DurabilityTestContext {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for DurabilityTestContext {
    fn default() -> Self {
        Self::new()
    }
}

pub fn populate_multi_level_data(
    _engine: &Engine,
    _cf: &ColumnFamilyHandle,
    _levels: usize,
) -> MidgeResult<()> {
    Ok(())
}

pub mod test_helpers {
    use std::time::Duration;

    pub fn wait_for_signal_default<T>(rx: std::sync::mpsc::Receiver<T>) -> Option<T> {
        rx.recv_timeout(Duration::from_secs(5)).ok()
    }
}

pub fn with_engine_restart<F1, F2>(opts: MidgeOptions, before_restart: F1, after_restart: F2)
where
    F1: FnOnce(&Engine),
    F2: FnOnce(&Engine),
{
    {
        let engine = Engine::open(opts.clone().to_open_options()).expect("open");
        before_restart(&engine);
        drop(engine);
    }

    {
        let engine = Engine::open(opts.to_open_options()).expect("reopen");
        after_restart(&engine);
    }
}

pub fn assert_get_equals(
    engine: &MidgeEngine,
    cf: &ColumnFamilyHandle,
    key: &[u8],
    expected: &[u8],
) {
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin_tx failed");
    let result = tx.get(key).expect("get failed");
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(expected));
}

pub fn assert_key_absent(engine: &MidgeEngine, cf: &ColumnFamilyHandle, key: &[u8]) {
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin_tx failed");
    let result = tx.get(key).expect("get failed");
    assert!(
        result.is_none(),
        "Expected key to be absent, but found value"
    );
}

pub fn bulk_put(
    engine: &MidgeEngine,
    cf: &ColumnFamilyHandle,
    kvs: &[(&[u8], &[u8])],
) -> MidgeResult<()> {
    for (key, value) in kvs {
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx.put(key.to_vec(), value.to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;
    }
    Ok(())
}
