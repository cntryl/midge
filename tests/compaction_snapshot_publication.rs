//! Compaction snapshot publication ordering regressions.

use bytes::Bytes;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::{Arc, Barrier};

#[test]
fn should_publish_replacement_snapshot_before_obsolete_sst_deletion() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine
        .get_column_family("default")
        .expect("default column family");

    for batch in 0..4 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        for index in 0..25 {
            let key = format!("snapshot_key_{batch}_{index:04}");
            tx.put(
                key.into_bytes(),
                format!("value_{batch}").into_bytes(),
                None,
            )
            .expect("put compaction seed");
        }
        tx.commit(WriteOptions::buffered()).expect("commit batch");
        engine.flush_cf(&cf).expect("flush L0 generation");
    }

    let scenario = fail::FailScenario::setup();
    let gc_reached = Arc::new(Barrier::new(2));
    let allow_compaction_to_finish = Arc::new(Barrier::new(2));
    let callback_gc_reached = Arc::clone(&gc_reached);
    let callback_allow_finish = Arc::clone(&allow_compaction_to_finish);
    fail::cfg_callback("midge::compaction::after_input_sst_gc", move || {
        callback_gc_reached.wait();
        callback_allow_finish.wait();
    })
    .expect("configure post-GC pause");

    // Act
    let read_results = std::thread::scope(|scope| {
        let compaction = scope.spawn(|| engine.compact_all());
        gc_reached.wait();

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction while compaction is paused");
        let reads = (0..4)
            .map(|batch| {
                let key = format!("snapshot_key_{batch}_0000");
                (batch, tx.get(key.as_bytes()))
            })
            .collect::<Vec<_>>();

        allow_compaction_to_finish.wait();
        compaction
            .join()
            .expect("join compaction thread")
            .expect("complete compaction");
        reads
    });

    fail::remove("midge::compaction::after_input_sst_gc");
    scenario.teardown();

    // Assert
    for (batch, result) in read_results {
        assert_eq!(
            result.expect("read while obsolete SSTs are being collected"),
            Some(Bytes::from(format!("value_{batch}"))),
            "replacement snapshot must serve batch {batch} before old inputs disappear"
        );
    }
}
