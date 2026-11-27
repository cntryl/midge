//! Genuine Engine owner-lifetime regression, staged before producer hooks.

#![cfg(feature = "internal-testing")]

use cntryl_midge::__internal::checkpoint::{metrics_handle, Medium, Origin};
use cntryl_midge::{Engine, OpenOptions, Query, TransactionMode, WriteOptions};
use std::time::Duration;

fn verify(engine: &Engine) {
    let family = engine.get_column_family("default").unwrap();
    let transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .unwrap();
    for index in 0..32 {
        let key = format!("row-{index:04}");
        let value = vec![u8::try_from(index).unwrap(); 128];
        assert_eq!(
            transaction.get(key.as_bytes()).unwrap().unwrap().as_ref(),
            value.as_slice()
        );
    }
    let mut count = 0;
    for row in transaction.scan(&Query::new()).unwrap() {
        let (key, value) = row.unwrap();
        assert_eq!(key.as_ref(), format!("row-{count:04}").as_bytes());
        assert_eq!(
            value.as_ref(),
            vec![u8::try_from(count).unwrap(); 128].as_slice()
        );
        count += 1;
    }
    assert_eq!(count, 32);
}

#[test]
fn should_release_actual_engine_lease_when_retained_checkpoint_metrics_outlive_shutdown() {
    // Arrange: one real persistent engine and its counter-only handle.
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .with_memtable_size_limit(8 * 1024 * 1024)
        .build()
        .unwrap();
    let mut engine = Engine::open(options.clone()).unwrap();
    let retained = metrics_handle(&engine);
    let before = retained.snapshot();
    let family = engine.get_column_family("default").unwrap();
    let mut transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..32 {
        transaction
            .put(
                format!("row-{index:04}").into_bytes(),
                vec![u8::try_from(index).unwrap(); 128],
                None,
            )
            .unwrap();
    }

    // Act: strict acknowledgement, actual worker publication, shutdown and same-path reacquisition.
    transaction.commit(WriteOptions::sync()).unwrap();
    engine.flush_cf(&family).unwrap();
    verify(&engine);
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let after_shutdown = retained.snapshot();
    let mut reopened = Engine::open(options).unwrap();
    let reopened_handle = metrics_handle(&reopened);
    verify(&reopened);
    reopened.shutdown(Duration::from_secs(30)).unwrap();
    drop(reopened);

    // Assert: retaining first-owner counters does not retain its writer or runtime.
    assert_ne!(
        retained.snapshot().owner_id,
        reopened_handle.snapshot().owner_id
    );
    assert_eq!(
        serde_json::to_value(retained.snapshot()).unwrap(),
        serde_json::to_value(&after_shutdown).unwrap()
    );
    let delta = after_shutdown.delta(&before).unwrap();
    let counters = &delta
        .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
        .counters;
    assert_eq!(counters.flush_committed_count, 1);
    assert!(counters.flush_committed_sst_bytes > 0);
    assert!(counters.flush_full_publication_elapsed_ns > 0);
    assert!(counters.issued_bytes[0] > 0);
    assert!(counters.issued_bytes[1] > 0);
    assert_eq!(counters.checkpoint_complete_count, 1);
}
