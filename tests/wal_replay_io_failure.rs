//! #808: non-corruption replay errors must preserve acknowledged WAL data.
//! A separate binary isolates the process-global replay failpoint.

#![cfg(all(feature = "internal-testing", feature = "failpoints"))]

use cntryl_midge::__internal::wal::{encoding, frame, WalOpKind};
use cntryl_midge::{
    Engine, MemoryBudget, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

fn assert_acknowledged_writes(engine: &Engine) {
    let cf = engine.get_column_family("default").expect("default");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");
    assert_eq!(
        tx.get(b"acked-small").expect("get small").as_deref(),
        Some(b"v".as_slice()),
        "acknowledged writes vanished after salvage open"
    );
    for index in 0..12 {
        assert_eq!(
            tx.get(format!("acked-big-{index:03}").as_bytes())
                .expect("get big")
                .as_deref(),
            Some(vec![b'x'; 8 * 1024].as_slice()),
            "acknowledged writes vanished after salvage open: member {index}"
        );
    }
}

#[test]
#[ignore = "#808 PR 5: noncorruption-salvage"]
fn should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error() {
    // Arrange: sync-acknowledged writes include a split transaction requiring
    // the shipping replay spool. The healthy control proves all members exist.
    let temp = tempfile::tempdir().expect("temp");
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
    assert_acknowledged_writes(&engine);
    engine.shutdown(Duration::from_secs(10)).expect("shutdown");
    drop(engine);
    let bytes = std::fs::read(db.join("wal/wal.log")).expect("read WAL");
    let mut offset = 0;
    let mut split = false;
    while offset < bytes.len() {
        let end = offset + frame::WAL_FRAME_HEADER_LEN;
        let (length, _) = frame::decode_frame_header(&bytes[offset..end]).expect("frame");
        split |= encoding::decode(&bytes[end..end + length])
            .expect("record")
            .op
            == WalOpKind::TxnBegin;
        offset = end + length;
    }
    assert!(split, "the fixture must exercise spool creation");
    let mut control = Engine::open(options(&db)).expect("healthy recovery control");
    assert_acknowledged_writes(&control);
    control
        .shutdown(Duration::from_secs(10))
        .expect("control shutdown");
    drop(control);

    // Act: inject Io at spool creation on every platform. Observe the error
    // branch itself; a misspelled or removed injection must fail this fixture.
    let scenario = fail::FailScenario::setup();
    let hits = Arc::new(AtomicUsize::new(0));
    let observed = hits.clone();
    fail::cfg_callback("midge::recovery::spool_create_io_injected", move || {
        observed.fetch_add(1, Ordering::SeqCst);
    })
    .expect("observe replay fault");
    fail::cfg("midge::recovery::inject_spool_create_io", "return").expect("inject replay Io");
    let reopened = Engine::open(options(&db));
    fail::remove("midge::recovery::inject_spool_create_io");
    fail::remove("midge::recovery::spool_create_io_injected");
    scenario.teardown();
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "the spool creation Io fault must fire"
    );
    let recovered = match reopened {
        Ok(mut engine) => {
            engine
                .shutdown(Duration::from_secs(10))
                .expect("shutdown salvage open");
            Some(engine)
        }
        Err(error) => {
            eprintln!("faulted open returned {error:?}");
            None
        }
    };
    drop(recovered);

    // Assert: both Err and Ok paths must permit a later healthy recovery of
    // every acknowledged value. Quarantining WAL and then returning Err fails.
    let mut healthy = Engine::open(options(&db)).expect("healthy open after injected Io");
    assert_acknowledged_writes(&healthy);
    healthy
        .shutdown(Duration::from_secs(10))
        .expect("healthy shutdown");
}
