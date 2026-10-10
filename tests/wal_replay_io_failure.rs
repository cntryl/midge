//! Audit V1 (LR-F1): local salvage recovery on a non-corruption replay error.
//!
//! Lives in its own test binary because it changes TMPDIR for the process.

#![cfg(feature = "internal-testing")]

use cntryl_midge::__internal::wal::{encoding, frame, WalOpKind, WalRecord};
use cntryl_midge::{
    Engine, MemoryBudget, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn options(path: &Path) -> OpenOptions {
    OpenOptions::local(path)
        .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
        .transaction_memory_pool_size(8 * 1024)
        .background_compaction(false)
        .recovery_policy(RecoveryPolicy::Salvage)
        .build()
        .expect("options")
}

fn read_records(path: &Path) -> Vec<WalRecord> {
    let bytes = std::fs::read(path).expect("read WAL");
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let header_end = offset + frame::WAL_FRAME_HEADER_LEN;
        let (payload_len, _) =
            frame::decode_frame_header(&bytes[offset..header_end]).expect("frame header");
        let payload = &bytes[header_end..header_end + payload_len];
        records.push(encoding::decode(payload).expect("decode"));
        offset = header_end + payload_len;
    }
    records
}

fn get(engine: &Engine, key: &[u8]) -> Option<usize> {
    let cf = engine.get_column_family("default").expect("default");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");
    tx.get(key).expect("get").map(|value| value.len())
}

fn salvaged_wal_logs(db: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(db.join("wal"))
        .expect("read wal dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("salvaged-"))
        .map(|entry| entry.path().join("wal.log"))
        .filter(|path| path.exists())
        .collect()
}

#[test]
#[ignore = "#808 PR 5: noncorruption-salvage"]
fn should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error() {
    // Arrange: sync-acknowledged, unflushed writes, one of them a spilled
    // (split-marker) transaction whose replay needs an anonymous temp spool.
    let temp = tempfile::TempDir::new().expect("temp");
    let db = temp.path().join("db");
    let mut engine = Engine::open(options(&db)).expect("open");
    let cf = engine.get_column_family("default").expect("default");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin");
    tx.put(b"acked-small".to_vec(), b"v".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync()).expect("commit small");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin");
    for index in 0..12 {
        tx.put(
            format!("acked-big-{index:03}").into_bytes(),
            vec![b'x'; 8 * 1024],
            None,
        )
        .expect("put big");
    }
    tx.commit(WriteOptions::sync()).expect("commit spilled");
    engine.shutdown(Duration::from_secs(10)).expect("shutdown");
    drop(engine);
    let before = read_records(&db.join("wal").join("wal.log"));
    assert!(
        before.iter().any(|record| record.op == WalOpKind::TxnBegin),
        "need a split txn"
    );
    let acked_max_seq = before.iter().map(|record| record.seq).max().unwrap();

    // Act: reopen while the temp directory is unusable (full/read-only/
    // missing tmpfs), so replay's TxnSpool::new fails with Io.
    let original_tmpdir = std::env::var_os("TMPDIR");
    std::env::set_var("TMPDIR", temp.path().join("missing-tmpdir"));
    let reopened = Engine::open(options(&db));
    match original_tmpdir {
        Some(value) => std::env::set_var("TMPDIR", value),
        None => std::env::remove_var("TMPDIR"),
    }

    // Assert: open fails retryably, or the acknowledged writes are present.
    let mut engine = match reopened {
        Err(error) => {
            eprintln!("open failed (acceptable): {error:?}");
            return;
        }
        Ok(engine) => engine,
    };
    let small = get(&engine, b"acked-small");
    let big = get(&engine, b"acked-big-000");
    let cf = engine.get_column_family("default").expect("default");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin");
    tx.put(b"after-reopen".to_vec(), b"n".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync())
        .expect("commit after reopen");
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown reopened");
    drop(engine);
    let new_seqs: Vec<u64> = read_records(&db.join("wal").join("wal.log"))
        .iter()
        .map(|record| record.seq)
        .collect();
    let salvaged = salvaged_wal_logs(&db);
    let mut third = Engine::open(options(&db)).expect("third open with a healthy TMPDIR");
    let small_after_healthy_reopen = get(&third, b"acked-small");
    let new_after_healthy_reopen = get(&third, b"after-reopen");
    third
        .shutdown(Duration::from_secs(10))
        .expect("shutdown third");
    eprintln!(
        "after a later healthy reopen: acked-small {small_after_healthy_reopen:?}, \
         after-reopen {new_after_healthy_reopen:?}"
    );
    eprintln!(
        "acked-small {small:?}, acked-big-000 {big:?}; set-aside WAL {salvaged:?}; \
         acked max seq {acked_max_seq}; new wal.log seqs {new_seqs:?}"
    );
    assert_eq!(
        (small, big),
        (Some(1), Some(8 * 1024)),
        "acknowledged writes vanished after salvage open; new writes got seqs {new_seqs:?} \
         (acknowledged max seq {acked_max_seq})"
    );
}
