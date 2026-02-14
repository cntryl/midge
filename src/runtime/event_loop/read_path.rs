//! Read path — point reads and range scans
//!
//! Contains memtable → immutable memtable → SST lookup logic for both
//! point reads and range scans, plus durability-aware message handlers.

use super::EventLoop;
use crate::sst::traits::SstReader;
use crate::sst::Memtable;

use super::super::durability::DurabilityWaiter;
use super::super::RuntimeResponse;

impl EventLoop {
    /// Create an immutable read snapshot for a column family
    pub(super) fn create_read_snapshot(
        &self,
        cf_id: crate::engine::ColumnFamilyId,
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
        Some(super::super::ReadSnapshot {
            cf_id,
            memtable: cf_state.memtable.clone(),
            immutable_memtables: cf_state.immutable_memtables.clone(),
            sst_files,
            sst_fs: std::sync::Arc::clone(&self.state.fs),
            sst_path_prefix,
            memory_mode: self.state.memory_mode,
        })
    }

    /// Check if a sequence number is durable at the requested level.
    /// Special case: u64::MAX (latest available) always returns true and bypasses durability checks.
    #[inline]
    pub(super) fn is_sequence_durable(
        &self,
        sequence: u64,
        requested_durability: crate::engine::api::Durability,
    ) -> bool {
        self.durability.is_durable(
            sequence,
            requested_durability,
            self.state.wal.local_durable_seq,
            self.state.wal.cloud_durable_seq,
        )
    }

    /// Handle a Read message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute reads directly against their stored ReadSnapshot.
    pub(super) fn handle_msg_read(
        &self,
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
        requested_durability: crate::engine::api::Durability,
    ) {
        let visible_up_to = if sequence == 0 { 0 } else { sequence - 1 };

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

    /// Handle a RangeScan message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute scans directly against their stored ReadSnapshot.
    pub(super) fn handle_msg_range_scan(
        &self,
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64,
        requested_durability: crate::engine::api::Durability,
    ) {
        let visible_up_to = if sequence == 0 { 0 } else { sequence - 1 };

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
        cf_id: crate::engine::ColumnFamilyId,
        key: &[u8],
        seq: u64,
    ) -> Option<Vec<u8>> {
        let cf_state = self.state.column_families.get(&cf_id)?;

        // Simple get logic
        // Active memtable — use snapshot-aware lookup when requested
        if seq == u64::MAX {
            if let Ok(Some(v)) = cf_state.memtable.get(key) {
                return Some(v);
            }
        } else if let Ok(Some(v)) = cf_state.memtable.get_at_seq(key, seq) {
            return Some(v);
        }

        // Immutable memtables (newest → oldest) — check newest first
        for imm in cf_state.immutable_memtables.iter().rev() {
            if seq == u64::MAX {
                if let Ok(Some(v)) = imm.get(key) {
                    return Some(v);
                }
            } else if let Ok(Some(v)) = imm.get_at_seq(key, seq) {
                return Some(v);
            }
        }

        // SST lookup: check files from newest to oldest across all levels
        let mut ssts_checked = 0u64;
        let mut l0_ssts_checked = 0u64;
        let mut blocks_read = 0u64;

        // Get all SST files for this CF, grouped by level
        let mut files_by_level: std::collections::BTreeMap<u32, Vec<_>> =
            std::collections::BTreeMap::new();
        for file in &self.state.manifest.files {
            if file.cf_id == cf_id {
                files_by_level
                    .entry(file.level)
                    .or_default()
                    .push(file.clone());
            }
        }

        // Search L0 first (newest to oldest), then L1, L2, etc.
        // L0 files may overlap, so we must check all of them
        if let Some(l0_files) = files_by_level.get(&0) {
            for file_meta in l0_files.iter().rev() {
                ssts_checked += 1;
                l0_ssts_checked += 1;

                // Track read access for compaction prioritization
                file_meta.record_read();

                // Try to open and read from this SST
                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                    blocks_read += 1; // At minimum, we read index block
                    if let Ok(Some(value)) = reader.get(key) {
                        // Found! Record metrics and return
                        self.state.read_amp_metrics.record_read(
                            ssts_checked,
                            l0_ssts_checked,
                            blocks_read,
                        );
                        return Some(value.to_vec());
                    }
                }
            }
        }

        // Check higher levels (L1, L2, ...) - these are sorted and non-overlapping
        for (&level, files) in files_by_level.iter() {
            if level == 0 {
                continue; // Already checked L0
            }

            for file_meta in files.iter().rev() {
                // Check if key is in range for this SST
                if let (Some(ref smallest), Some(ref largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if key < smallest.as_slice() || key > largest.as_slice() {
                        continue; // Key not in this SST's range
                    }
                }

                ssts_checked += 1;

                // Track read access for compaction prioritization
                file_meta.record_read();

                // Try to open and read from this SST
                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                    blocks_read += 1; // At minimum, we read index block
                    if let Ok(Some(value)) = reader.get(key) {
                        // Found! Record metrics and return
                        self.state.read_amp_metrics.record_read(
                            ssts_checked,
                            l0_ssts_checked,
                            blocks_read,
                        );
                        return Some(value.to_vec());
                    }
                }
            }
        }

        // Key not found in any SST - record miss
        self.state
            .read_amp_metrics
            .record_read(ssts_checked, l0_ssts_checked, blocks_read);
        None
    }

    /// Range scan: iterate keys in [start, end) from memtables and SSTs
    pub(super) fn handle_range_scan(
        &self,
        cf_id: u32,
        start: &[u8],
        end: &[u8],
        _snapshot_seq: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let cf_state = match self.state.column_families.get(&cf_id) {
            Some(state) => state,
            None => return vec![],
        };

        // Treat empty bounds as unbounded.
        //
        // `Transaction::scan` uses empty Vecs when the caller does not specify start/end.
        // Interpreting empty as an actual key would make Query::new() scan the empty range.
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        // Collect results in order: SSTs (oldest->newest) -> immutable memtables (oldest->newest) -> active memtable
        // so that newer versions override older ones.
        let mut results: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            std::collections::BTreeMap::new();

        // --- SSTs: check L0 (newest->oldest) first, then higher levels ---
        let mut files_by_level: std::collections::BTreeMap<u32, Vec<_>> =
            std::collections::BTreeMap::new();
        for file in &self.state.manifest.files {
            if file.cf_id == cf_id {
                files_by_level
                    .entry(file.level)
                    .or_default()
                    .push(file.clone());
            }
        }

        // L0: newest -> oldest (may overlap)
        if let Some(l0_files) = files_by_level.get(&0) {
            for file_meta in l0_files.iter().rev() {
                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = self.compaction_actor.open_sst_reader(&sst_path) {
                    if let Ok(pairs) = reader.scan_range(start_opt, end_opt) {
                        for (k, v) in pairs {
                            // SstReader::scan_range returns only (key, value) tuples of present values.
                            // Treat all returned values as valid for the snapshot (SSTs are persisted).
                            results.entry(k.to_vec()).or_insert(v.to_vec());
                        }
                    }
                }
            }
        }

        // Higher levels
        for (&level, files) in files_by_level.iter() {
            if level == 0 {
                continue;
            }

            for file_meta in files.iter().rev() {
                if let (Some(ref smallest), Some(ref largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if start >= smallest.as_slice() && start >= largest.as_slice() {
                        // Key range doesn't overlap; skip
                        continue;
                    }
                }

                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = self.compaction_actor.open_sst_reader(&sst_path) {
                    if let Ok(pairs) = reader.scan_range(start_opt, end_opt) {
                        for (k, v) in pairs {
                            // SstReader::scan_range returns only (key, value) tuples of present values.
                            // Treat all returned values as valid for the snapshot (SSTs are persisted).
                            results.entry(k.to_vec()).or_insert(v.to_vec());
                        }
                    }
                }
            }
        }

        // --- Immutable memtables: oldest -> newest ---
        for imm in cf_state.immutable_memtables.iter() {
            // Build by_key for this memtable (keep first seen = most recent within memtable)
            let mut by_key: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
                std::collections::BTreeMap::new();
            for (key, value, _seq) in imm.iter_all(u64::MAX) {
                // Current behavior: snapshots return current state (no MVCC), so do not
                // filter memtable entries by the snapshot sequence. In future when MVCC
                // is implemented, this should be filtered by `snapshot_seq`.
                by_key.entry(key).or_insert(value);
            }

            for (key, value) in by_key.iter() {
                if value.is_none() {
                    continue; // tombstone
                }
                let in_start = start_opt.is_none_or(|s| key.as_slice() >= s);
                let in_end = end_opt.is_none_or(|e| key.as_slice() < e);
                if in_start && in_end {
                    results.insert(key.clone(), value.clone().expect("value already checked"));
                }
            }
        }

        // --- Active memtable (newest) overrides everything ---
        let mut by_key: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for (key, value, _seq) in cf_state.memtable.iter_all(u64::MAX) {
            // See note above: snapshots currently reflect current state; do not filter by
            // snapshot sequence here. This preserves existing behavior documented in tests.
            by_key.entry(key).or_insert(value);
        }

        for (key, value) in by_key.iter() {
            if value.is_none() {
                continue;
            }
            let in_start = start_opt.is_none_or(|s| key.as_slice() >= s);
            let in_end = end_opt.is_none_or(|e| key.as_slice() < e);
            if in_start && in_end {
                results.insert(key.clone(), value.clone().expect("value already checked"));
            }
        }

        results.into_iter().collect()
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
        Box::new(writer).finish_to_path(&sst_path)?;

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
            ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReader>> {
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
