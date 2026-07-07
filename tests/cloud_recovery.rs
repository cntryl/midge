//! Reopen Consistency After Flush And Background Upload Activity
//!
//! These tests cover clean-shutdown reopen behavior after flushes, compaction
//! requests, and short background-upload windows across durable storage modes.
//! They do not inject real cloud failures, partial uploads, or process crashes.
//!
//! **Storage Modes**: Durable modes only (local, cloud)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
mod common;
use cntryl_midge::TransactionMode;
use common::*;
use std::thread;
use std::time::Duration;

// ============================================================================
// TEST GROUP: Cloud Recovery Scenarios
// ============================================================================

#[test]
fn should_preserve_flushed_values_when_reopening_after_short_upload_window() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let cloud_cache_path = match &opts.storage_mode {
            StorageMode::CloudBacked { local_cache_path } => Some(local_cache_path.clone()),
            _ => None,
        };
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("partial_upload_key_{i:04}");
                tx.put(
                    key.as_bytes().to_vec(),
                    b"value_before_upload".to_vec(),
                    None,
                )
                .expect("put value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            engine.flush_cf(&cf).expect("flush");
            thread::sleep(Duration::from_millis(50));
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..100 {
                let key = format!("partial_upload_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get reopened value"),
                    Some(Bytes::from_static(b"value_before_upload")),
                    "mode: {mode} key: {key}"
                );
            }
        }

        if let Some(local_cache_path) = cloud_cache_path {
            assert!(
                !local_cache_path.join("cloud_recovery").exists(),
                "cloud reopen should not leave recovery staging directories behind"
            );
        }
    });
}

#[test]
fn should_preserve_both_flushed_batches_when_reopening_after_compaction_request() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("manifest_fail_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None)
                    .expect("put first flushed batch");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 50..100 {
                let key = format!("manifest_fail_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None)
                    .expect("put second flushed batch");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            engine.compact_all().ok();
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..100 {
                let key = format!("manifest_fail_key_{i:04}");
                let expected = if i < 50 {
                    Bytes::from_static(b"v1")
                } else {
                    Bytes::from_static(b"v2")
                };
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get reopened batch value"),
                    Some(expected),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_flushed_values_when_reopening_after_background_upload_delay() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..75 {
                let key = format!("retry_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"retry_value".to_vec(), None)
                    .expect("put retry value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            engine.flush_cf(&cf).expect("flush");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            thread::sleep(Duration::from_millis(200)); // Wait for background retry

            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..75 {
                let key = format!("retry_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes())
                        .expect("get retry value after reopen"),
                    Some(Bytes::from_static(b"retry_value")),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_snapshot_visibility_when_flushing_with_snapshot_open() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Write data
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("exposure_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"safe_value".to_vec(), None)
                .expect("put safe value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        // Act
        engine.flush_cf(&cf).expect("flush");

        // Assert
        for i in 0..100 {
            let key = format!("exposure_key_{i:04}");
            assert_eq!(
                snapshot.get(key.as_bytes()).expect("snapshot get"),
                Some(Bytes::from_static(b"safe_value")),
                "snapshot saw unexpected value in mode: {mode} key: {key}"
            );
        }
    });
}

#[test]
fn should_preserve_both_generations_when_reopening_after_compaction_request() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("midcompact_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"gen1".to_vec(), None)
                    .expect("put generation 1 value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 100..200 {
                let key = format!("midcompact_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"gen2".to_vec(), None)
                    .expect("put generation 2 value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            engine.compact_all().ok();
            thread::sleep(Duration::from_millis(50)); // Let compaction start
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..200 {
                let key = format!("midcompact_key_{i:04}");
                let expected = if i < 100 {
                    Bytes::from_static(b"gen1")
                } else {
                    Bytes::from_static(b"gen2")
                };
                assert_eq!(
                    tx.get(key.as_bytes())
                        .expect("get reopened generation value"),
                    Some(expected),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_values_when_reopening_after_flush_attempt() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("cloud_offline_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"offline_value".to_vec(), None)
                    .expect("put offline value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            engine.flush_cf(&cf).ok();
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..50 {
                let key = format!("cloud_offline_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get reopened offline value"),
                    Some(Bytes::from_static(b"offline_value")),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_multiple_flushed_batches_when_reopening_after_short_upload_window() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("resume_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"batch1".to_vec(), None)
                    .expect("put batch1 value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 50..100 {
                let key = format!("resume_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"batch2".to_vec(), None)
                    .expect("put batch2 value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            thread::sleep(Duration::from_millis(50));
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            thread::sleep(Duration::from_millis(300));

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..100 {
                let key = format!("resume_key_{i:04}");
                let expected = if i < 50 {
                    Bytes::from_static(b"batch1")
                } else {
                    Bytes::from_static(b"batch2")
                };
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get reopened resume value"),
                    Some(expected),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_flushed_values_when_reopening_after_short_retry_window() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..60 {
                let key = format!("dedup_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"dedup_value".to_vec(), None)
                    .expect("put dedup value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            engine.flush_cf(&cf).expect("flush");
            thread::sleep(Duration::from_millis(50));
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            thread::sleep(Duration::from_millis(200));

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            for i in 0..60 {
                let key = format!("dedup_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get reopened dedup value"),
                    Some(Bytes::from_static(b"dedup_value")),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}
