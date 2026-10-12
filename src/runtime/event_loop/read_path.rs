//! Read path — point reads and range scans
//!
//! Contains memtable → immutable memtable → SST lookup logic for both
//! point reads and range scans, plus durability-aware message handlers.

use super::EventLoop;

#[cfg(test)]
use super::super::durability::{DurabilityWaiter, TestDurabilityWaiter};
#[cfg(test)]
use super::super::RuntimeResponse;

impl EventLoop {
    /// Create an immutable read snapshot for a column family
    pub(super) fn create_read_snapshot(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<super::super::ReadSnapshot> {
        let cf_state = self.state.column_families.get(&cf_id)?;

        let sst_view = self.state.manifest.read_view_for(cf_id);

        let sst_path_prefix = std::path::PathBuf::from(crate::sst::fs::SST_DIR);
        let snapshot = super::super::ReadSnapshot::new_with_view_resources(
            cf_id,
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_view,
            std::sync::Arc::clone(&self.state.fs),
            sst_path_prefix,
            self.state.is_memory_mode(),
            self.state.observed_time_millis(),
            self.read_resources.clone(),
        );
        Some(snapshot)
    }

    /// Check if a sequence number is durable at the requested level.
    /// Special case: `u64::MAX` (latest available) always returns true and bypasses durability checks.
    #[inline]
    #[cfg(test)]
    pub(super) fn is_sequence_durable(
        &self,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    ) -> bool {
        crate::runtime::durability::DurabilityCoordinator::is_durable(
            sequence,
            requested_durability,
            self.state.wal.frontiers.local_durable(),
        )
    }

    /// Handle a Read message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute reads directly against their stored `ReadSnapshot`.
    #[cfg(test)]
    pub(super) fn handle_msg_read(
        &self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    ) {
        let visible_up_to = sequence;

        if visible_up_to <= self.state.sequence
            || self.is_sequence_durable(sequence, requested_durability)
        {
            // Execute read synchronously (fallback path - transactions normally execute directly)
            let value = self.handle_read(cf_id, &key, sequence);
            self.respond(request_id, RuntimeResponse::ReadValue { request_id, value });
        } else {
            self.durability
                .queue_waiter(DurabilityWaiter::Test(TestDurabilityWaiter::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                }));
        }
    }

    /// Handle a `RangeScan` message: check durability frontier or queue for later.
    /// NOTE: This is a fallback path - transactions now execute scans directly against their stored `ReadSnapshot`.
    #[cfg(test)]
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
            self.durability
                .queue_waiter(DurabilityWaiter::Test(TestDurabilityWaiter::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                }));
        }
    }

    /// Local read path: memtable → immutable memtables → SST
    #[cfg(test)]
    pub(super) fn handle_read(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
        seq: u64,
    ) -> Option<Vec<u8>> {
        self.create_read_snapshot(cf_id)
            .and_then(|snapshot| snapshot.get(key, seq).ok().flatten())
    }

    /// Range scan: iterate keys in [start, end) from memtables and SSTs
    #[cfg(test)]
    pub(super) fn handle_range_scan(
        &self,
        cf_id: u32,
        start: &[u8],
        end: &[u8],
        snapshot_seq: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.create_read_snapshot(cf_id)
            .and_then(|snapshot| snapshot.range_scan(start, end, snapshot_seq).ok())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_event_loop;
    use super::super::EventLoop;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use crate::sst::traits::SstFactory;
    use crate::types::EntryType;
    use std::sync::Arc;

    struct FakeReader;

    impl crate::sst::traits::SstStateReader for FakeReader {
        crate::sst::traits::test_reader_required_methods!();

        fn range_tombstones(&self) -> Vec<crate::types::RangeTombstone> {
            Vec::new()
        }

        fn range_tombstone_memory_usage(&self) -> usize {
            0
        }

        fn get_state(&self, key: &[u8]) -> crate::common::MidgeResult<crate::types::KeyState> {
            Ok(if key == b"a" {
                crate::types::KeyState::Value(
                    bytes::Bytes::copy_from_slice(b"va"),
                    10,
                    None,
                    crate::types::EntryType::Put,
                )
            } else {
                crate::types::KeyState::Absent
            })
        }

        #[cfg(test)]
        fn scan_range_state(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> crate::common::MidgeResult<Vec<(bytes::Bytes, crate::types::KeyState)>> {
            let s = start.unwrap_or(&[]);
            let e = end.unwrap_or(&[255u8]);
            if s <= &b"a"[..] && &b"a"[..] < e {
                Ok(vec![(
                    bytes::Bytes::copy_from_slice(b"a"),
                    crate::types::KeyState::Value(
                        bytes::Bytes::copy_from_slice(b"va"),
                        10,
                        None,
                        crate::types::EntryType::Put,
                    ),
                )])
            } else {
                Ok(Vec::new())
            }
        }

        #[cfg(test)]
        fn scan_range_raw_state(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> crate::common::MidgeResult<Vec<(bytes::Bytes, crate::types::KeyState)>> {
            self.scan_range_state(start, end)
        }
    }

    struct TestFactory;

    impl crate::sst::traits::SstFactory for TestFactory {
        fn output_fs(&self) -> std::sync::Arc<dyn crate::io::Fs> {
            std::sync::Arc::new(crate::io::MockFs::new())
        }

        fn compaction_scratch_cleanup_verified(&self) -> bool {
            false
        }

        fn create_for_compaction(
            &self,
            _budget: crate::common::resource_budget::ResourceBudget,
        ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
            Err(crate::common::MidgeError::NotSupported(
                "create not supported in test".into(),
            ))
        }

        fn open_for_compaction(
            &self,
            path: &std::path::Path,
            _budget: crate::common::resource_budget::ResourceBudget,
        ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
            self.open(path)
        }

        #[cfg(test)]
        fn create(&self) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
            Err(crate::common::MidgeError::NotSupported(
                "create not supported in test".into(),
            ))
        }

        fn open(
            &self,
            _path: &std::path::Path,
        ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
            Ok(Box::new(FakeReader))
        }
    }

    fn create_event_loop_with_test_sst(
    ) -> crate::common::MidgeResult<(tempfile::TempDir, EventLoop, std::path::PathBuf)> {
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        std::fs::create_dir_all(state.host_sst_dir())?;

        let router = Arc::new(ResponseRouter::new());
        let mut el = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;

        let sst_name = "00000001.sst".to_string();
        let sst_path = el.state.host_sst_dir().join(&sst_name);
        let fs = std::sync::Arc::new(
            crate::io::RealFs::new(el.state.host_sst_dir())
                .map_err(crate::io::FsError::into_midge)?,
        );
        let factory = std::sync::Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));
        let mut writer = factory.create()?;
        writer.add_with_meta(b"a", Some(b"va".as_ref()), 10, EntryType::Put, None)?;
        writer.add_range_tombstone(b"b", b"z", 9)?;
        crate::sst::fs::finish_writer_to_path(writer, &sst_path)?;

        el.state
            .manifest
            .test_mut()
            .files
            .push(crate::metadata::FileMeta {
                name: sst_name,
                level: 0,
                size_bytes: std::fs::metadata(&sst_path)?.len(),
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"a".to_vec()),
                smallest_seq: Some(10),
                largest_seq: Some(10),
                ..Default::default()
            });
        el.compaction_actor =
            crate::runtime::actors::CompactionActor::new(std::sync::Arc::new(TestFactory));
        Ok((tmp, el, sst_path))
    }

    #[test]
    fn should_handle_read_return_value_written_to_memtable() {
        // Arrange
        let mut event_loop = create_test_event_loop().expect("Should create event loop");
        event_loop
            .state
            .get_cf_mut(0)
            .expect("default cf")
            .memtable
            .put_with_seq(b"k1".to_vec(), b"v1".to_vec(), 5, None)
            .expect("seed memtable");

        // Act
        let found = event_loop.handle_read(0, b"k1", 5);
        let missing = event_loop.handle_read(0, b"absent", 5);

        // Assert
        assert_eq!(found, Some(b"v1".to_vec()));
        assert_eq!(missing, None);
    }

    #[test]
    fn should_handle_range_scan_return_keys_written_to_memtable() {
        // Arrange
        let mut event_loop = create_test_event_loop().expect("Should create event loop");
        {
            let cf = event_loop.state.get_cf_mut(0).expect("default cf");
            cf.memtable
                .put_with_seq(b"a".to_vec(), b"va".to_vec(), 1, None)
                .expect("seed a");
            cf.memtable
                .put_with_seq(b"b".to_vec(), b"vb".to_vec(), 2, None)
                .expect("seed b");
            cf.memtable
                .put_with_seq(b"z".to_vec(), b"vz".to_vec(), 3, None)
                .expect("seed z outside range");
        }

        // Act
        let results = event_loop.handle_range_scan(0, b"a", b"c", 3);

        // Assert
        assert_eq!(
            results,
            vec![
                (b"a".to_vec(), b"va".to_vec()),
                (b"b".to_vec(), b"vb".to_vec())
            ]
        );
    }

    #[test]
    fn should_range_scan_include_keys_from_ssts() -> crate::common::MidgeResult<()> {
        // Arrange
        let (_tmp, el, sst_path) = create_event_loop_with_test_sst()?;
        let reader = el.compaction_actor.open_sst_reader(&sst_path)?;
        let sst_pairs = reader.scan_range_state(Some(b"a"), Some(b"b"))?;
        // Act
        // Assert
        assert!(sst_pairs
            .iter()
            .any(|(k, state)| k.as_ref() == b"a" && matches!(state, crate::types::KeyState::Value(value, _, _, _) if value.as_ref() == b"va")));

        let results = el.handle_range_scan(0, b"a", b"b", u64::MAX);

        assert!(results
            .iter()
            .any(|(k, v)| k.as_slice() == b"a" && v.as_slice() == b"va"));

        Ok(())
    }
}
