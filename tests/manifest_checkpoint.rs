//! Flushed SSTs and manifest checkpoints must preserve every published row.

use cntryl_midge::{Engine, MidgeResult, OpenOptions, Query, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;

fn assert_rows(engine: &Engine, rounds: usize) {
    let family = engine.get_column_family("data").expect("data family");
    let read = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .unwrap();
    let rows = read
        .scan(&Query::new())
        .unwrap()
        .collect::<MidgeResult<Vec<_>>>()
        .unwrap();
    let expected: Vec<_> = (0..rounds)
        .flat_map(|round| {
            (0..4).map(move |index| {
                (
                    format!("key-{round}-{index}").into_bytes(),
                    format!("value-{round}-{index}").into_bytes(),
                )
            })
        })
        .collect();
    assert_eq!(rows.len(), expected.len());
    for ((key, value), (expected_key, expected_value)) in rows.iter().zip(expected) {
        assert_eq!(key.as_ref(), expected_key);
        assert_eq!(value.as_ref(), expected_value);
    }
}

fn checkpoint_horizon(path: &Path) -> u64 {
    let snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("manifest.snapshot.json")).unwrap())
            .unwrap();
    snapshot["edit_checkpoint_id"].as_u64().unwrap()
}

#[test]
fn should_preserve_every_flushed_row_when_manifest_checkpoints_advance_through_compaction() {
    for cloud in [false, true] {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let options = if cloud {
            OpenOptions::cloud_simulated(directory.path(), "bucket", "checkpoint")
        } else {
            OpenOptions::local(directory.path())
        }
        .background_compaction(false)
        .build()
        .unwrap();
        let mut engine = Engine::open(options.clone()).unwrap();
        let family = engine.create_column_family("data").unwrap();
        let write = if cloud {
            WriteOptions::cloud_strict()
        } else {
            WriteOptions::sync()
        };
        let mut previous_horizon = 0;

        // Act: publish distinct SSTs, then replace them through compaction.
        for round in 0..3 {
            for index in 0..4 {
                let mut transaction = engine
                    .begin_tx(family.id(), TransactionMode::ReadWrite)
                    .unwrap();
                transaction
                    .put(
                        format!("key-{round}-{index}").into_bytes(),
                        format!("value-{round}-{index}").into_bytes(),
                        None,
                    )
                    .unwrap();
                transaction.commit(write).unwrap();
                engine.flush_cf(&family).unwrap();
            }
            let flushed_horizon = checkpoint_horizon(directory.path());
            assert!(flushed_horizon >= previous_horizon, "cloud={cloud}");
            let before_compactions = engine
                .metrics()
                .get_runtime_metrics()
                .unwrap()
                .compactions_run;
            engine.compact_all().unwrap();

            // Assert: every durable checkpoint advances and retains earlier publications.
            let horizon = checkpoint_horizon(directory.path());
            assert!(horizon > flushed_horizon, "cloud={cloud}");
            assert!(
                engine
                    .metrics()
                    .get_runtime_metrics()
                    .unwrap()
                    .compactions_run
                    > before_compactions
            );
            previous_horizon = horizon;
            assert_rows(&engine, round + 1);
        }
        engine.shutdown(Duration::from_secs(30)).unwrap();
        drop(engine);
        let mut reopened = Engine::open(options).unwrap();
        assert!(checkpoint_horizon(directory.path()) >= previous_horizon);
        assert_rows(&reopened, 3);
        reopened.shutdown(Duration::from_secs(30)).unwrap();
    }
}
