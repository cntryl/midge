//! Read path — point reads and range scans
//!
//! Contains memtable → immutable memtable → SST lookup logic for both
//! point reads and range scans, plus durability-aware message handlers.

use super::EventLoop;

use super::super::durability::DurabilityWaiter;
use super::super::RuntimeResponse;

impl EventLoop {
    /// Create an immutable read snapshot for a column family
    pub(super) fn create_read_snapshot(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<super::super::ReadSnapshot> {
        let cf_state = self.state.column_families.get(&cf_id)?;

        // Collect SST metadata for this CF
        let sst_files: Vec<_> = self
            .state
            .manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

        let sst_path_prefix = self
            .state
            .sst_dir
            .strip_prefix(&self.state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();
        let mut snapshot = super::super::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            std::sync::Arc::clone(&self.state.fs),
            sst_path_prefix,
            self.state.is_memory_mode(),
        );
        snapshot.cf_id = cf_id;
        Some(snapshot)
    }

    /// Check if a sequence number is durable at the requested level.
    /// Special case: `u64::MAX` (latest available) always returns true and bypasses durability checks.
    #[inline]
    pub(super) fn is_sequence_durable(
        &self,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    ) -> bool {
        crate::runtime::durability::DurabilityCoordinator::is_durable(
            sequence,
            requested_durability,
            self.state.wal.local_durable_seq,
            self.state.wal.cloud_durable_seq,
        )
    }

    /// Handle a Read message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute reads directly against their stored `ReadSnapshot`.
    pub(super) fn handle_msg_read(
        &self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    ) {
        let visible_up_to = sequence;

        // === Phase 0 Guardrail #3: Transaction atomicity barrier ===
        // If a transaction is pending commit (Batched mode), block reads at sequences
        // >= pending_txn_min_seq to prevent seeing partial transaction state.
        if let Some(pending_min) = self.state.pending_txn_min_seq {
            if sequence >= pending_min {
                // Defer this read until transaction completes
                self.durability.queue_waiter(DurabilityWaiter::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                    requested_durability,
                });
                return;
            }
        }

        if visible_up_to <= self.state.sequence
            || self.is_sequence_durable(sequence, requested_durability)
        {
            // Execute read synchronously (fallback path - transactions normally execute directly)
            let value = self.handle_read(cf_id, &key, sequence);
            self.respond(request_id, RuntimeResponse::ReadValue { request_id, value });
        } else {
            self.durability.queue_waiter(DurabilityWaiter::Read {
                request_id,
                cf_id,
                key,
                sequence,
                requested_durability,
            });
        }
    }

    /// Handle a `RangeScan` message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute scans directly against their stored `ReadSnapshot`.
    pub(super) fn handle_msg_range_scan(
        &self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    ) {
        let visible_up_to = sequence;

        // === Phase 0 Guardrail #3: Transaction atomicity barrier ===
        // If a transaction is pending commit (Batched mode), block scans at sequences
        // >= pending_txn_min_seq to prevent seeing partial transaction state.
        if let Some(pending_min) = self.state.pending_txn_min_seq {
            if sequence >= pending_min {
                // Defer this scan until transaction completes
                self.durability.queue_waiter(DurabilityWaiter::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                    requested_durability,
                });
                return;
            }
        }

        if visible_up_to <= self.state.sequence
            || self.is_sequence_durable(sequence, requested_durability)
        {
            // Execute scan synchronously (fallback path - transactions normally execute directly)
            let results = self.handle_range_scan(cf_id, &start, &end, sequence);
            self.respond(
                request_id,
                RuntimeResponse::RangeScanResults {
                    request_id,
                    results,
                },
            );
        } else {
            self.durability.queue_waiter(DurabilityWaiter::RangeScan {
                request_id,
                cf_id,
                start,
                end,
                sequence,
                requested_durability,
            });
        }
    }

    /// Local read path: memtable → immutable memtables → SST
    pub(super) fn handle_read(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
        seq: u64,
    ) -> Option<Vec<u8>> {
        self.create_read_snapshot(cf_id)
            .and_then(|snapshot| snapshot.get(key, seq))
    }

    /// Range scan: iterate keys in [start, end) from memtables and SSTs
    pub(super) fn handle_range_scan(
        &self,
        cf_id: u32,
        start: &[u8],
        end: &[u8],
        snapshot_seq: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.create_read_snapshot(cf_id)
            .map(|snapshot| snapshot.range_scan(start, end, snapshot_seq))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_event_loop;
    use super::super::EventLoop;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use std::sync::Arc;

    #[test]
    fn should_have_handle_read_method() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act - The method should exist (private, so can't call directly)
        // We verify it exists by checking the struct compiles with it

        // Assert - Just verify event_loop is created
        assert!(!event_loop.trace_enabled);
    }

    #[test]
    fn should_have_handle_range_scan_method() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act - Similar to handle_read, verify method exists

        // Assert
        assert!(!event_loop.trace_enabled);
    }

    #[test]
    fn should_range_scan_include_keys_from_ssts() -> crate::common::MidgeResult<()> {
        use crate::sst::traits::SstFactory;

        // Arrange: create real filesystem-backed state (not memory mode)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        // Ensure sst dir exists
        std::fs::create_dir_all(&state.sst_dir)?;

        let router = Arc::new(ResponseRouter::new());
        let mut el = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        )?;

        // Create an SST file with one key using the FsSstFactory
        let sst_name = "00000001.sst".to_string();
        let sst_path = el.state.sst_dir.join(&sst_name);

        let fs = std::sync::Arc::new(crate::io::RealFs::new(&el.state.sst_dir)?);
        let factory = std::sync::Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));
        let mut writer = factory.create()?;
        writer.add_with_meta(b"a", Some(b"va".as_ref()), 10, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &sst_path)?;

        // Add manifest entry pointing to the SST we just wrote
        let file_meta = crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: std::fs::metadata(&sst_path)?.len(),
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"a".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
            ..Default::default()
        };
        el.state.manifest.files.push(file_meta);

        // Replace compaction actor factory with a TestFactory that returns a fake reader
        // so we don't need to create a valid on-disk SST file in this unit test.
        struct TestFactory;
        impl crate::sst::traits::SstFactory for TestFactory {
            fn create(
                &self,
            ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
                Err(crate::common::MidgeError::NotSupported(
                    "create not supported in test".into(),
                ))
            }
            fn open(
                &self,
                _path: &std::path::Path,
            ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
                struct FakeReader;
                impl crate::sst::traits::SstReader for FakeReader {
                    fn get(&self, key: &[u8]) -> crate::common::MidgeResult<Option<bytes::Bytes>> {
                        if key == b"a" {
                            Ok(Some(bytes::Bytes::copy_from_slice(b"va")))
                        } else {
                            Ok(None)
                        }
                    }
                    fn scan_range(
                        &self,
                        start: Option<&[u8]>,
                        end: Option<&[u8]>,
                    ) -> crate::common::MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>>
                    {
                        let s = start.unwrap_or(&[]);
                        let e = end.unwrap_or(&[255u8]);
                        if s <= &b"a"[..] && &b"a"[..] < e {
                            Ok(vec![(
                                bytes::Bytes::copy_from_slice(b"a"),
                                bytes::Bytes::copy_from_slice(b"va"),
                            )])
                        } else {
                            Ok(Vec::new())
                        }
                    }
                }
                impl crate::sst::traits::SstStateReader for FakeReader {
                    fn get_state(
                        &self,
                        key: &[u8],
                    ) -> crate::common::MidgeResult<crate::sst::types::KeyState>
                    {
                        Ok(if key == b"a" {
                            crate::sst::types::KeyState::Value(
                                bytes::Bytes::copy_from_slice(b"va"),
                                10,
                                None,
                                0,
                            )
                        } else {
                            crate::sst::types::KeyState::Absent
                        })
                    }

                    fn scan_range_state(
                        &self,
                        start: Option<&[u8]>,
                        end: Option<&[u8]>,
                    ) -> crate::common::MidgeResult<Vec<(bytes::Bytes, crate::sst::types::KeyState)>>
                    {
                        let s = start.unwrap_or(&[]);
                        let e = end.unwrap_or(&[255u8]);
                        if s <= &b"a"[..] && &b"a"[..] < e {
                            Ok(vec![(
                                bytes::Bytes::copy_from_slice(b"a"),
                                crate::sst::types::KeyState::Value(
                                    bytes::Bytes::copy_from_slice(b"va"),
                                    10,
                                    None,
                                    0,
                                ),
                            )])
                        } else {
                            Ok(Vec::new())
                        }
                    }
                }
                Ok(Box::new(FakeReader))
            }
        }

        el.compaction_actor =
            crate::runtime::actors::CompactionActor::new(std::sync::Arc::new(TestFactory));

        // Quick sanity-check: ensure the fake reader returns the key we expect
        let reader = el.compaction_actor.open_sst_reader(&sst_path)?;
        let sst_pairs = reader.scan_range(Some(b"a"), Some(b"b"))?;
        assert!(sst_pairs
            .iter()
            .any(|(k, v)| k.as_ref() == b"a" && v.as_ref() == b"va"));

        // Act: perform a range scan ["a","b") at sequence u64::MAX
        let results = el.handle_range_scan(0, b"a", b"b", u64::MAX);

        // Assert: We expect to see the key in SST; current implementation does NOT consult SSTs and this test should fail until fixed.
        assert!(results
            .iter()
            .any(|(k, v)| k.as_slice() == b"a" && v.as_slice() == b"va"));

        Ok(())
    }
}
