#[cfg(test)]
mod tests {
    use super::*;

    struct MetadataFailFs {
        inner: crate::io::MockFs,
    }

    impl crate::io::ReadFs for MetadataFailFs {
        fn open_read(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
            crate::io::Fs::open(self, path, crate::io::OpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
        }

        fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(
            &self,
            _path: &crate::io::FsPath,
        ) -> crate::io::FsResult<crate::io::traits::Metadata> {
            Err(crate::io::FsError::Io(
                "injected output metadata failure".into(),
            ))
        }

        fn list_dir(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl crate::io::Fs for MetadataFailFs {
        fn open(
            &self,
            path: &crate::io::FsPath,
            opts: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
            self.inner.open(path, opts)
        }
        fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_file(path)
        }
        fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.create_dir_all(path)
        }
        fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_dir_all(path)
        }
        fn sync_dir(
            &self,
            path: &crate::io::FsPath,
            durability: crate::io::Durability,
        ) -> crate::io::FsResult<()> {
            self.inner.sync_dir(path, durability)
        }
        fn rename_atomic(
            &self,
            from: &crate::io::FsPath,
            to: &crate::io::FsPath,
        ) -> crate::io::FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_fail_partition_when_output_metadata_cannot_be_read() -> MidgeResult<()> {
        // Arrange
        let mock = crate::io::MockFs::new();
        let factory = crate::sst::FsSstFactoryIo::new(
            std::sync::Arc::new(MetadataFailFs {
                inner: mock.clone(),
            }),
            4096,
        );
        let mut writer = factory.create()?;
        writer.add_with_meta(
            b"key",
            Some(b"value"),
            1,
            crate::types::EntryType::Put,
            None,
        )?;
        let fs = factory.output_fs();

        // Act
        let result = finish_partition(
            writer,
            1,
            &[],
            None,
            None,
            PartitionIdentity {
                cf_id: 0,
                target_level: 1,
                generation: 1,
                ordinal: 0,
            },
            std::path::Path::new("output"),
            None,
            Some(usize::MAX),
            &fs,
        );

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Io(_))));
        let name = crate::cloud_layout::compaction_file_name(0, 1, 1, 0);
        assert!(mock.get_file(&format!("output/{name}")).is_none());
        Ok(())
    }

    #[test]
    fn should_remove_partial_output_through_injected_fs_when_compaction_aborts() -> MidgeResult<()>
    {
        // Arrange
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let factory = crate::sst::FsSstFactoryIo::new(mock.clone(), 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(
            b"key",
            Some(b"value"),
            1,
            crate::types::EntryType::Put,
            None,
        )?;
        writer.finish_to_path(std::path::Path::new("input.sst"))?;
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(2);
        plan.add_test_source("input.sst");
        let sink = |_name: &str,
                    _path: &std::path::Path,
                    _budget: &crate::common::resource_budget::ResourceBudget| {
            Err(crate::common::MidgeError::Aborted(
                "injected after first output".into(),
            ))
        };

        // Act
        let result = crate::compaction::execute_compaction_with_output_sink(
            &plan,
            &factory,
            std::path::Path::new("output"),
            None,
            Some(&sink),
            None,
        );

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Aborted(_))));
        assert!(mock.get_file("input.sst").is_some());
        let output = crate::cloud_layout::compaction_file_name(0, 1, 2, 0);
        assert!(mock.get_file(&format!("output/{output}")).is_none());
        Ok(())
    }

    #[test]
    fn should_initialize_merge_with_one_raw_version_per_input() {
        use crate::sst::traits::RawSstVersionCursor;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // Arrange
        struct InstrumentedCursor {
            versions: std::vec::IntoIter<CompactionVersion>,
            advances: Arc<AtomicUsize>,
        }

        impl Iterator for InstrumentedCursor {
            type Item = MidgeResult<CompactionVersion>;

            fn next(&mut self) -> Option<Self::Item> {
                self.versions.next().map(|version| {
                    self.advances.fetch_add(1, Ordering::SeqCst);
                    Ok(version)
                })
            }
        }

        let left_advances = Arc::new(AtomicUsize::new(0));
        let right_advances = Arc::new(AtomicUsize::new(0));
        let cursors: Vec<RawSstVersionCursor> = vec![
            Box::new(InstrumentedCursor {
                versions: vec![
                    mk_version("a", 3, false, Some("a3"), None),
                    mk_version("c", 1, false, Some("c1"), None),
                ]
                .into_iter(),
                advances: Arc::clone(&left_advances),
            }),
            Box::new(InstrumentedCursor {
                versions: vec![
                    mk_version("b", 2, false, Some("b2"), None),
                    mk_version("d", 1, false, Some("d1"), None),
                ]
                .into_iter(),
                advances: Arc::clone(&right_advances),
            }),
        ];

        // Act
        let _merge = VersionMergeIterator::new(
            cursors,
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize lazy merge");

        // Assert
        assert_eq!(left_advances.load(Ordering::SeqCst), 1);
        assert_eq!(right_advances.load(Ordering::SeqCst), 1);
    }

    fn mk_version<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        key: K,
        seq: u64,
        is_tombstone: bool,
        value: Option<V>,
        expiration: Option<u64>,
    ) -> CompactionVersion {
        CompactionVersion {
            key: key.as_ref().to_vec(),
            seq,
            is_tombstone,
            value: value.map(|v| v.as_ref().to_vec()),
            expiration,
        }
    }

    fn cursor_from_versions(mut versions: Vec<CompactionVersion>) -> RawSstVersionCursor {
        versions.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| right.seq.cmp(&left.seq))
        });
        Box::new(versions.into_iter().map(Ok))
    }

    #[test]
    fn should_merge_version_streams_by_key_then_descending_sequence() {
        // Arrange
        let left = vec![
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("c", 1, false, Some("c1"), None),
        ];
        let right = vec![
            mk_version("a", 4, false, Some("a4"), None),
            mk_version("b", 2, false, Some("b2"), None),
        ];

        // Act
        let mut merge = VersionMergeIterator::new(
            vec![cursor_from_versions(left), cursor_from_versions(right)],
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize merge");
        let mut merged = Vec::new();
        while let Some(version) = merge.next_version().expect("advance merge") {
            merged.push(version);
        }

        // Assert
        assert_eq!(
            merged
                .iter()
                .map(|version| (version.key.clone(), version.seq))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), 4),
                (b"a".to_vec(), 3),
                (b"b".to_vec(), 2),
                (b"c".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn should_use_input_index_given_equal_key_sequence_when_merging() {
        // Arrange
        let first = vec![mk_version("same", 7, false, Some("first"), None)];
        let second = vec![mk_version("same", 7, false, Some("second"), None)];

        // Act
        let mut merge = VersionMergeIterator::new(
            vec![cursor_from_versions(first), cursor_from_versions(second)],
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize merge");
        let mut merged = Vec::new();
        while let Some(version) = merge.next_version().expect("advance merge") {
            merged.push(version);
        }

        // Assert
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].value.as_deref(), Some(b"first".as_ref()));
        assert_eq!(merged[1].value.as_deref(), Some(b"second".as_ref()));
    }

    #[test]
    fn should_collect_tombstones_given_stateful_reader_input() {
        use crate::sst::traits::SstStateReader;

        // Arrange
        struct FakeReader;

        impl SstStateReader for FakeReader {
            crate::sst::traits::test_reader_required_methods!();

            fn range_tombstone_memory_usage(&self) -> usize {
                self.range_tombstones()
                    .iter()
                    .map(|range| {
                        range.start.capacity()
                            + range.end.capacity()
                            + std::mem::size_of::<RangeTombstone>()
                    })
                    .sum()
            }

            fn get_state(&self, _key: &[u8]) -> MidgeResult<KeyState> {
                Ok(KeyState::Absent)
            }

            #[cfg(test)]
            fn scan_range_state(
                &self,
                _start: Option<&[u8]>,
                _end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
                Ok(vec![
                    (
                        bytes::Bytes::from_static(b"alpha"),
                        KeyState::Value(
                            bytes::Bytes::from_static(b"v1"),
                            42,
                            Some(900),
                            crate::types::EntryType::Put,
                        ),
                    ),
                    (bytes::Bytes::from_static(b"beta"), KeyState::Tombstone(41)),
                ])
            }

            #[cfg(test)]
            fn scan_range_raw_state(
                &self,
                start: Option<&[u8]>,
                end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
                self.scan_range_state(start, end)
            }

            fn range_tombstones(&self) -> Vec<RangeTombstone> {
                vec![RangeTombstone::new(b"c".to_vec(), b"f".to_vec(), 40)]
            }
        }

        // Act
        let input = collect_reader_input(&FakeReader).expect("collect stateful input");

        // Assert
        assert_eq!(input.versions.len(), 2);
        assert_eq!(input.versions[0].key, b"alpha".to_vec());
        assert_eq!(input.versions[0].seq, 42);
        assert_eq!(input.versions[0].expiration, Some(900));
        assert!(input.versions[1].is_tombstone);
        assert_eq!(input.range_tombstones.len(), 1);
        assert_eq!(input.range_tombstones[0].start, b"c".to_vec());
    }

    #[test]
    fn should_normalize_duplicate_range_tombstones_when_collecting_input() {
        // Arrange
        let tombstones = vec![
            RangeTombstone::new(b"m".to_vec(), b"z".to_vec(), 7),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 5),
        ];

        // Act
        let normalized = normalize_range_tombstones(tombstones);

        // Assert
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0].start, b"a".to_vec());
        assert_eq!(normalized[0].seq, 9);
        assert_eq!(normalized[1].seq, 5);
        assert_eq!(normalized[2].start, b"m".to_vec());
    }

    fn mk_tombstone(index: usize, seq: u64) -> RangeTombstone {
        RangeTombstone::new(
            format!("key{index:06}a").into_bytes(),
            format!("key{index:06}b").into_bytes(),
            seq,
        )
    }

    #[test]
    fn should_keep_compaction_work_linear_when_partition_holds_many_range_tombstones(
    ) -> MidgeResult<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Arrange
        struct CountingWriter {
            bounds: AtomicUsize,
        }

        impl crate::sst::traits::DynSstWriter for CountingWriter {
            fn estimated_size_bytes(&self) -> usize {
                0
            }

            fn finish_to_path(self: Box<Self>, _path: &std::path::Path) -> MidgeResult<()> {
                Err(crate::common::MidgeError::NotSupported(
                    "counting writer has no filesystem".into(),
                ))
            }

            fn encoded_size_upper_bound(&self) -> Option<usize> {
                Some(0)
            }

            fn encoded_size_upper_bound_after_sorted_entry(
                &self,
                _key: &[u8],
                _value: Option<&[u8]>,
            ) -> Option<usize> {
                None
            }

            fn additional_range_tombstone_size_upper_bound(
                &self,
                start: &[u8],
                end: &[u8],
            ) -> Option<usize> {
                self.bounds.fetch_add(1, Ordering::SeqCst);
                Some(start.len().saturating_add(end.len()))
            }

            #[cfg(test)]
            fn add_with_meta(
                &mut self,
                _key: &[u8],
                _value: Option<&[u8]>,
                _seq: u64,
                _op_type: EntryType,
                _expiration: Option<u64>,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn add_sorted_with_meta(
                &mut self,
                _key: &[u8],
                _value: Option<&[u8]>,
                _seq: u64,
                _op_type: EntryType,
                _expiration: Option<u64>,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn add_range_tombstone(
                &mut self,
                _start: &[u8],
                _end: &[u8],
                _seq: u64,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
                Ok(Vec::new())
            }
        }

        let budget = crate::common::resource_budget::ResourceBudget::new(16 * 1024 * 1024);
        let writer = CountingWriter {
            bounds: AtomicUsize::new(0),
        };
        let tombstone_count = 2_000usize;
        let key_groups = 2_000usize;

        // Act
        let mut partition = PartitionTombstones::new();
        for index in 0..tombstone_count {
            let seq = u64::try_from(index).expect("sequence fits in u64") + 1;
            partition.insert(
                &mk_tombstone(index, seq),
                &budget,
                "partition range tombstone",
                Some(PendingTombstoneBound {
                    writer: &writer,
                    lower_bound: None,
                }),
            )?;
        }
        let bounds_after_inserts = writer.bounds.load(Ordering::SeqCst);
        for _ in 0..key_groups {
            let _bound = prospective_partition_size(&writer, None, partition.pending_bytes())?;
        }
        let bounds_after_key_groups = writer.bounds.load(Ordering::SeqCst);

        // Assert: each tombstone is bounded once when it joins the partition,
        // and the per-key-group size checks never revisit the retained set, so
        // the work stays linear in tombstones instead of keys x tombstones.
        assert_eq!(bounds_after_inserts, tombstone_count);
        assert_eq!(bounds_after_key_groups, tombstone_count);
        assert_eq!(
            partition.pending_bytes(),
            pending_tombstone_size(&writer, partition.tombstones(), None)?
        );
        Ok(())
    }

    #[test]
    fn should_track_encoded_bytes_when_partition_tombstones_are_added_and_cleared(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut partition = PartitionTombstones::new();
        let first = mk_tombstone(1, 7);
        let second = mk_tombstone(2, 9);

        // Act
        partition.insert(&first, &budget, "partition range tombstone", None)?;
        partition.insert(&second, &budget, "partition range tombstone", None)?;
        partition.insert(&first, &budget, "partition range tombstone", None)?;

        // Assert: the duplicate is dropped and the tracked byte bound matches
        // a full fold over the retained set.
        assert_eq!(partition.tombstones().count(), 2);
        assert!(partition.contains(&first));
        assert_eq!(
            partition.encoded_bytes(),
            encoded_tombstone_upper_bound(&first)
                .saturating_add(encoded_tombstone_upper_bound(&second))
        );
        partition.clear();
        assert!(partition.is_empty());
        assert_eq!(partition.encoded_bytes(), 0);
        assert_eq!(partition.pending_bytes(), 0);
        Ok(())
    }

    #[test]
    fn should_report_highest_sequence_cover_when_many_obsolete_tombstones_are_active(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut active = ActiveTombstones::new();
        let live = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 400);
        let highest = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 300);

        // Act
        for seq in 1..=200u64 {
            active.insert(
                &RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), seq),
                true,
                &budget,
            )?;
        }
        active.insert(&highest, true, &budget)?;
        active.insert(&live, false, &budget)?;
        active.insert(&live, false, &budget)?;

        // Assert
        assert_eq!(active.highest_obsolete_cover(), Some(&highest));
        assert_eq!(active.live_tombstones().collect::<Vec<_>>(), vec![&live]);
        assert!(active.contains(&live));
        active.remove(&highest);
        active.remove(&live);
        assert!(!active.contains(&live));
        assert_eq!(
            active
                .highest_obsolete_cover()
                .map(|tombstone| tombstone.seq),
            Some(200)
        );
        Ok(())
    }

    #[test]
    fn should_carry_live_ranges_after_retiring_obsolete_ranges_between_partitions(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut tracker = RangeTombstoneTracker::new();
        let obsolete = RangeTombstone::new(b"a".to_vec(), b"m".to_vec(), 4);
        let live = RangeTombstone::new(b"b".to_vec(), b"z".to_vec(), 12);
        let policy = TombstoneGcPolicy {
            snapshot_horizon: Some(10),
            point_eligible: true,
            range_eligible: true,
        };

        // Act: the obsolete range covers GC decisions but never reaches an
        // output partition; the live range is carried across a roll until its
        // end event is observed.
        tracker.observe_starts([&obsolete, &live].into_iter(), policy, &budget, None)?;
        let first_partition = tracker.partition_tombstones().cloned().collect::<Vec<_>>();
        tracker.carry_live_tombstones(&budget, None)?;
        let carried_partition = tracker.partition_tombstones().cloned().collect::<Vec<_>>();
        let events = [
            CompactionEvent::RangeEnd(obsolete.clone()),
            CompactionEvent::RangeEnd(live.clone()),
        ];
        tracker.advance_to_key(events[..1].iter());
        let after_obsolete_end = tracker.highest_obsolete_cover().cloned();
        tracker.advance_to_key(events[1..].iter());
        tracker.carry_live_tombstones(&budget, None)?;

        // Assert
        assert_eq!(tracker.highest_obsolete_cover(), None);
        assert_eq!(after_obsolete_end, None);
        assert_eq!(first_partition, vec![live.clone()]);
        assert_eq!(carried_partition, vec![live]);
        assert!(tracker.partition_tombstones().next().is_none());
        Ok(())
    }

    #[test]
    fn should_roll_only_after_a_written_partition_reaches_its_boundary() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let factory = crate::sst::FsSstFactoryIo::new(
            std::sync::Arc::new(
                crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?,
            ),
            4096,
        );
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut roller = PartitionRoller::new(
            &factory,
            directory.path(),
            PartitionIdentity {
                cf_id: 0,
                target_level: 1,
                generation: 1,
                ordinal: 0,
            },
            1,
            &budget,
            None,
        )?;
        let tracker = RangeTombstoneTracker::new();
        let first = mk_version("a", 1, false, Some("first"), None);
        let next = mk_version("b", 2, false, Some("next"), None);
        let policy = TombstoneGcPolicy {
            snapshot_horizon: None,
            point_eligible: false,
            range_eligible: false,
        };

        // Act
        let before_write = roller.should_roll(Some(&next), std::iter::empty(), &tracker, policy)?;
        roller.write_selected(&first)?;
        let after_write = roller.should_roll(Some(&next), std::iter::empty(), &tracker, policy)?;

        // Assert: writer overhead alone cannot create an empty partition, but
        // a later key group can roll the partition once it contains a point.
        assert!(!before_write);
        assert!(after_write);
        Ok(())
    }

    #[test]
    fn should_treat_all_tombstones_as_obsolete_when_no_snapshot_horizon() {
        // Arrange
        let horizon = None;

        // Act
        let zero_is_obsolete = tombstone_is_obsolete(0, horizon);
        let large_is_obsolete = tombstone_is_obsolete(1_000_000, horizon);

        // Assert: with no snapshot horizon, every sequence is
        // eligible for GC (legacy "drop all tombstones" behavior).
        assert!(zero_is_obsolete);
        assert!(large_is_obsolete);
    }

    #[test]
    fn should_treat_tombstone_as_obsolete_when_sequence_is_at_or_below_horizon() {
        // Arrange
        let horizon = Some(150);

        // Act
        let at_horizon = tombstone_is_obsolete(150, horizon);
        let below_horizon = tombstone_is_obsolete(149, horizon);

        // Assert: sequences at or below the horizon are obsolete, since
        // no live snapshot can observe them being resurrected.
        assert!(at_horizon);
        assert!(below_horizon);
    }

    #[test]
    fn should_retain_tombstone_when_sequence_is_above_horizon() {
        // Arrange
        let horizon = Some(150);

        // Act
        let above_horizon = tombstone_is_obsolete(151, horizon);
        let maximum_sequence = tombstone_is_obsolete(u64::MAX, horizon);

        // Assert: a tombstone newer than the horizon must be preserved
        // so a snapshot reading at or below the horizon does not observe a
        // resurrected key.
        assert!(!above_horizon);
        assert!(!maximum_sequence);
    }

    mod key_group_reduction {
        use super::*;

        fn version_event(key: &str, seq: u64, is_tombstone: bool, value: &str) -> CompactionEvent {
            CompactionEvent::Version(mk_version(
                key,
                seq,
                is_tombstone,
                (!is_tombstone).then_some(value),
                None,
            ))
        }

        fn cover(start: &str, end: &str, seq: u64) -> RangeTombstone {
            RangeTombstone::new(start.as_bytes().to_vec(), end.as_bytes().to_vec(), seq)
        }

        fn policy(horizon: Option<u64>, point: bool, range: bool) -> TombstoneGcPolicy {
            TombstoneGcPolicy {
                snapshot_horizon: horizon,
                point_eligible: point,
                range_eligible: range,
            }
        }

        fn selected_seq(events: &[CompactionEvent]) -> MidgeResult<Option<u64>> {
            select_newest_version(events.iter()).map(|version| version.map(|v| v.seq))
        }

        #[test]
        fn should_select_highest_sequence_when_key_group_has_several_versions() {
            // Arrange
            let events = [
                version_event("k", 3, false, "old"),
                version_event("k", 9, false, "new"),
                version_event("k", 5, false, "mid"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(9));
        }

        #[test]
        fn should_keep_one_version_when_equal_sequence_versions_are_identical() {
            // Arrange
            let events = [
                version_event("k", 4, false, "same"),
                version_event("k", 4, false, "same"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(4));
        }

        #[test]
        fn should_reject_key_group_when_equal_sequence_versions_disagree() {
            // Arrange
            let events = [
                version_event("k", 4, false, "left"),
                version_event("k", 4, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(message))
                    if message.contains("conflicting compaction versions")
            ));
        }

        #[test]
        fn should_reject_shadowed_equal_sequence_conflict_during_compaction() {
            // Arrange
            let events = [
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "left"),
                version_event("k", 5, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(_))
            ));
        }

        #[test]
        fn should_keep_newest_version_when_shadowed_equal_sequence_copies_are_identical() {
            // Arrange
            let events = [
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "same"),
                version_event("k", 5, false, "same"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(10));
        }

        #[test]
        fn should_select_newest_version_when_chained_files_yield_older_version_first() {
            // Arrange
            let events = [
                version_event("k", 3, false, "source"),
                version_event("k", 1, false, "left-target"),
                version_event("k", 5, false, "right-target"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(5));
        }

        #[test]
        fn should_reject_shadowed_equal_sequence_conflict_when_versions_arrive_unordered() {
            // Arrange
            let events = [
                version_event("k", 5, false, "left"),
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(_))
            ));
        }

        #[test]
        fn should_select_nothing_when_key_group_holds_only_range_events() {
            // Arrange
            let events = [
                CompactionEvent::RangeStart(cover("a", "m", 7)),
                CompactionEvent::RangeEnd(cover("a", "m", 7)),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), None);
        }

        #[test]
        fn should_drop_point_tombstone_when_it_is_at_or_below_the_horizon() {
            // Arrange
            let version = mk_version("k", 5, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(5), true, true),
            );

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_point_tombstone_when_a_snapshot_can_still_see_it() {
            // Arrange
            let version = mk_version("k", 6, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(5), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_point_tombstone_when_point_gc_is_not_eligible() {
            // Arrange
            let version = mk_version("k", 1, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(100), false, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_drop_value_when_an_obsolete_active_cover_is_at_least_as_new() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);
            let active = cover("a", "z", 5);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_value_when_the_active_cover_is_older_than_the_value() {
            // Arrange
            let version = mk_version("k", 6, false, Some("v"), None);
            let active = cover("a", "z", 5);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_value_when_the_active_cover_does_not_contain_the_key() {
            // Arrange: retaining is the safe direction if the cover is misplaced.
            let version = mk_version("k", 5, false, Some("v"), None);
            let active = cover("m", "z", 9);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_drop_value_when_an_obsolete_range_start_in_the_group_covers_it() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);
            let starts = [cover("a", "z", 8)];

            // Act
            let survives =
                survives_tombstone_gc(&version, None, starts.iter(), policy(Some(10), true, true));

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_value_when_the_covering_range_start_is_not_yet_obsolete() {
            // Arrange: sequence 11 is above the horizon, so a snapshot may need the value.
            let version = mk_version("k", 5, false, Some("v"), None);
            let starts = [cover("a", "z", 11)];

            // Act
            let survives =
                survives_tombstone_gc(&version, None, starts.iter(), policy(Some(10), true, true));

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_value_when_no_tombstone_covers_it() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }
    }

    mod soft_roll {
        use super::*;

        struct Case {
            has_selected_version: bool,
            point_count: usize,
            partition_size: usize,
            tombstone_bytes: usize,
            has_tombstones: bool,
            target: usize,
        }

        fn due(case: &Case) -> bool {
            soft_roll_due(
                case.has_selected_version,
                case.point_count,
                case.partition_size,
                case.tombstone_bytes,
                case.has_tombstones,
                case.target,
            )
        }

        fn case() -> Case {
            Case {
                has_selected_version: true,
                point_count: 3,
                partition_size: 100,
                tombstone_bytes: 0,
                has_tombstones: false,
                target: 100,
            }
        }

        #[test]
        fn should_roll_when_the_partition_reaches_the_target_size() {
            // Arrange
            let case = case();

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }

        #[test]
        fn should_not_roll_when_the_partition_is_below_the_target_size() {
            // Arrange
            let case = Case {
                partition_size: 99,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_at_a_group_that_drops_its_only_version() {
            // Arrange: no surviving version means this key group adds no point.
            let case = Case {
                has_selected_version: false,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_an_empty_partition_on_writer_overhead_alone() {
            // Arrange: an empty writer's estimate includes fixed overhead, so a
            // large size with no points and no tombstones must not roll.
            let case = Case {
                point_count: 0,
                partition_size: 10_000,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_roll_a_range_only_partition_when_tombstones_reach_the_target() {
            // Arrange: no surviving points, but retained tombstones hold budget.
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 100,
                has_tombstones: true,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }

        #[test]
        fn should_not_roll_a_range_only_partition_below_the_target() {
            // Arrange
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 99,
                has_tombstones: true,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_a_range_only_partition_without_tombstones() {
            // Arrange
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 100,
                has_tombstones: false,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_treat_a_zero_target_as_one_byte() {
            // Arrange
            let case = Case {
                partition_size: 1,
                target: 0,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }
    }
}
