//! Forced origins from real Engine publication and same-path exact recovery.

use super::*;
use crate::metadata::accounting::{Medium, Origin};

fn write_rows(engine: &Engine, start: u8, end: u8) {
    let cf = engine.get_column_family("default").expect("default family");
    let mut transaction = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in start..end {
        transaction
            .put(vec![index], vec![index; 128], None)
            .unwrap();
    }
    let write_options = if engine.cloud_mode {
        WriteOptions::cloud_strict()
    } else {
        WriteOptions::sync()
    };
    transaction
        .commit(write_options)
        .expect("actual acknowledgement");
}

fn verify_rows(engine: &Engine) {
    let cf = engine.get_column_family("default").expect("default family");
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    for index in 0..32_u8 {
        assert_eq!(
            tx.get(&[index]).unwrap().as_deref(),
            Some(vec![index; 128].as_slice())
        );
    }
    let rows = tx
        .scan(&Query::new())
        .unwrap()
        .collect::<MidgeResult<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 32);
    for (index, (key, value)) in rows.iter().enumerate() {
        let index = u8::try_from(index).unwrap();
        assert_eq!(key.as_ref(), [index]);
        assert_eq!(value.as_ref(), vec![index; 128].as_slice());
    }
}

#[test]
fn should_attribute_cloud_shutdown_flush_when_acknowledged_rows_remain_active() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: actual strict-ack cloud rows stay below the normal threshold.
        let directory = tempfile::tempdir().unwrap();
        let options = OpenOptions::cloud_simulated(directory.path(), "accounting", "shutdown")
            .background_compaction(false)
            .with_memtable_size_limit(8 * 1024 * 1024)
            .build()
            .unwrap();
        let mut engine = Engine::open(options.clone()).unwrap();
        let retained = engine.checkpoint_metrics();
        let before = retained.snapshot();
        write_rows(&engine, 0, 32);
        verify_rows(&engine);
        assert_eq!(
            retained
                .snapshot()
                .bucket(Origin::CloudFlush, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );

        // Act: actual shutdown seals WAL and checkpoints the active generation.
        engine.shutdown(Duration::from_secs(30)).unwrap();
        drop(engine);
        let after = retained.snapshot();
        let mut reopened = Engine::open(options).unwrap();
        verify_rows(&reopened);
        reopened.shutdown(Duration::from_secs(30)).unwrap();
        drop(reopened);

        // Assert: exact data survives; forced flush never enters ordinary denominators.
        let delta = after.delta(&before).unwrap();
        let shutdown = &delta.bucket(Origin::Shutdown, Medium::Persistent).counters;
        assert_eq!(shutdown.flush_committed_count, 1);
        assert_eq!(shutdown.publication_attempts, 1);
        assert_eq!(shutdown.publication_failures, 0);
        assert!(shutdown.flush_committed_sst_bytes > 0);
        assert!(shutdown.checkpoint_complete_count > 0);
        assert_eq!(
            delta
                .bucket(Origin::CloudFlush, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );
        assert_eq!(
            delta
                .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );
        assert_eq!(
            serde_json::to_value(retained.snapshot()).unwrap(),
            serde_json::to_value(after).unwrap()
        );
    });
}

#[test]
fn should_attribute_compaction_checkpoint_when_real_local_outputs_replace_inputs() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: two genuine strict-ack flushes create distinct local inputs.
        let directory = tempfile::tempdir().unwrap();
        let options = OpenOptions::local(directory.path())
            .background_compaction(false)
            .with_memtable_size_limit(8 * 1024 * 1024)
            .build()
            .unwrap();
        let mut engine = Engine::open(options.clone()).unwrap();
        let retained = engine.checkpoint_metrics();
        let before = retained.snapshot();
        let family = engine.get_column_family("default").unwrap();
        write_rows(&engine, 0, 16);
        engine.flush_cf(&family).unwrap();
        write_rows(&engine, 16, 32);
        engine.flush_cf(&family).unwrap();
        let compactions_before = engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .compactions_run;

        // Act: use the public caller through real compute and publication.
        engine.compact_all().unwrap();
        let compactions_after = engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .compactions_run;
        verify_rows(&engine);
        let measured = retained.snapshot().delta(&before).unwrap();
        engine.shutdown(Duration::from_secs(30)).unwrap();
        drop(engine);
        let mut reopened = Engine::open(options).unwrap();
        verify_rows(&reopened);
        reopened.shutdown(Duration::from_secs(30)).unwrap();

        // Assert: actual compaction count and forced metadata cost remain distinct.
        assert!(compactions_after > compactions_before);
        let forced = &measured
            .bucket(Origin::CompactionBeforeGc, Medium::Persistent)
            .counters;
        assert_eq!(
            forced.compaction_committed_count,
            compactions_after - compactions_before
        );
        assert!(forced.compaction_committed_sst_bytes > 0);
        assert!(forced.checkpoint_complete_count > 0);
        assert_eq!(forced.flush_committed_count, 0);
        assert_eq!(
            measured
                .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
                .counters
                .flush_committed_count,
            2
        );
        assert_eq!(
            measured
                .bucket(Origin::Recovery, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );
        assert_eq!(
            measured
                .bucket(Origin::Shutdown, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );
        assert_eq!(measured.incomplete_observations, 0);
    });
}
