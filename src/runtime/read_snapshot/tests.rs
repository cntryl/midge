use super::super::*;
use super::*;
use crate::io::FsError;
use crate::sst::traits::SstFactory;

#[test]
fn should_not_open_older_ssts_when_checking_delete_range_conflict() -> MidgeResult<()> {
    // Arrange: an absent file makes an accidental open observable.
    let dir = tempfile::tempdir()?;
    let file = FileMeta {
        name: "absent-old.sst".into(),
        level: 0,
        cf_id: 0,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(5),
        key_bounds_complete: true,
        ..Default::default()
    };
    let snapshot = ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![file.clone()],
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        false,
        0,
    );

    // Act
    // Assert
    assert_eq!(snapshot.any_sequence_after_in_range(b"b", b"y", 10)?, None);

    // Incomplete legacy metadata cannot justify the skip.
    let legacy = ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![FileMeta {
            key_bounds_complete: false,
            ..file
        }],
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        false,
        0,
    );
    assert!(legacy.any_sequence_after_in_range(b"b", b"y", 10).is_err());
    Ok(())
}

#[test]
fn should_detect_range_conflict_from_newer_range_tombstone_in_sst() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"m", Some(b"old"), 5, EntryType::Put, None)?;
    writer.add_range_tombstone(b"a", b"z", 15)?;
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("range.sst"))?;
    let summary = crate::sst::fs::SstFileIo::summarize_with_fs("range.sst", Arc::clone(&fs))?;
    assert_eq!(summary.largest_seq, 15);
    let snapshot = ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![FileMeta {
            name: "range.sst".into(),
            level: 0,
            cf_id: 0,
            smallest_key: Some(summary.smallest_key),
            largest_key: Some(summary.largest_key),
            smallest_seq: Some(summary.smallest_seq),
            largest_seq: Some(summary.largest_seq),
            key_bounds_complete: true,
            ..Default::default()
        }],
        fs,
        std::path::PathBuf::new(),
        false,
        0,
    );

    // Act
    // Assert
    assert_eq!(
        snapshot.any_sequence_after_in_range(b"b", b"y", 10)?,
        Some(15)
    );
    Ok(())
}

#[test]
fn should_reject_conflicting_equal_sequence_ssts_across_reads() -> crate::common::MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut files = Vec::new();
    for (name, value, op) in [
        ("put.sst", Some(b"value".as_slice()), EntryType::Put),
        ("delete.sst", None, EntryType::Delete),
    ] {
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", value, 5, op, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &dir.path().join(name))?;
        files.push(FileMeta {
            name: name.to_string(),
            level: 0,
            cf_id: 0,
            size_bytes: std::fs::metadata(dir.path().join(name))?.len(),
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(5),
            largest_seq: Some(5),
            key_bounds_complete: true,
            ..Default::default()
        });
    }
    let snapshot = Arc::new(ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        files.clone(),
        fs,
        std::path::PathBuf::new(),
        false,
        0,
    ));

    // Act
    // Assert
    assert!(matches!(
        snapshot.get(b"key", u64::MAX),
        Err(MidgeError::Corruption(_))
    ));
    assert!(matches!(
        snapshot.range_scan(b"", b"", u64::MAX),
        Err(MidgeError::Corruption(_))
    ));
    assert!(matches!(
        snapshot.state_scan(None, None, false, u64::MAX).next(),
        Some(Err(MidgeError::Corruption(_)))
    ));

    // Recovery can legitimately replay a delete at its original sequence.
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", None, 5, EntryType::Delete, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("duplicate.sst"))?;
    let mut duplicate = files[1].clone();
    duplicate.name = "duplicate.sst".to_string();
    duplicate.size_bytes = std::fs::metadata(dir.path().join("duplicate.sst"))?.len();
    let duplicate_snapshot = Arc::new(ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![files[1].clone(), duplicate],
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        false,
        0,
    ));
    assert_eq!(duplicate_snapshot.get(b"key", u64::MAX)?, None);
    assert_eq!(duplicate_snapshot.range_scan(b"", b"", u64::MAX)?.len(), 0);
    assert!(duplicate_snapshot
        .state_scan(None, None, false, u64::MAX)
        .next()
        .is_none());

    // TTL interpretation must not hide differing persisted metadata.
    let mut expired_files = Vec::new();
    for (name, expiration) in [("expired-a.sst", 1), ("expired-b.sst", 2)] {
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 5, EntryType::Put, Some(expiration))?;
        crate::sst::fs::finish_writer_to_path(writer, &dir.path().join(name))?;
        let mut file = files[0].clone();
        file.name = name.to_string();
        file.size_bytes = std::fs::metadata(dir.path().join(name))?.len();
        expired_files.push(file);
    }
    let expired_snapshot = Arc::new(ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        expired_files,
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        false,
        0,
    ));
    assert!(matches!(
        expired_snapshot.get(b"key", u64::MAX),
        Err(MidgeError::Corruption(_))
    ));
    assert!(matches!(
        expired_snapshot
            .state_scan(None, None, false, u64::MAX)
            .next(),
        Some(Err(MidgeError::Corruption(_)))
    ));
    Ok(())
}

#[test]
fn should_accept_identical_expired_value_replayed_in_memtable() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 5, EntryType::Put, Some(1))?;
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("expired.sst"))?;
    let memtable = Arc::new(SkipListMemtable::new());
    memtable.put_bytes_with_seq(
        bytes::Bytes::from_static(b"key"),
        bytes::Bytes::from_static(b"value"),
        5,
        Some(1),
    )?;
    let snapshot = Arc::new(ReadSnapshot::new(
        memtable,
        Vec::new(),
        vec![FileMeta {
            name: "expired.sst".into(),
            level: 0,
            cf_id: 0,
            size_bytes: std::fs::metadata(dir.path().join("expired.sst"))?.len(),
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(5),
            largest_seq: Some(5),
            key_bounds_complete: true,
            ..Default::default()
        }],
        fs,
        std::path::PathBuf::new(),
        false,
        2,
    ));

    // Act
    let result = snapshot.get(b"key", u64::MAX);

    // Assert
    assert_eq!(result?, None);
    assert_eq!(snapshot.range_scan(b"", b"", u64::MAX)?.len(), 0);
    assert!(snapshot
        .state_scan(None, None, false, u64::MAX)
        .next()
        .is_none());
    Ok(())
}

#[test]
fn should_use_shared_block_cache_for_snapshot_sst_reads() -> crate::common::MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"cache-key", Some(b"cache-value"), 10, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("cache.sst"))?;

    let file_meta = FileMeta {
        name: "cache.sst".to_string(),
        level: 0,
        size_bytes: std::fs::metadata(temp_dir.path().join("cache.sst"))?.len(),
        cf_id: 0,
        smallest_key: Some(b"cache-key".to_vec()),
        largest_key: Some(b"cache-key".to_vec()),
        smallest_seq: Some(10),
        largest_seq: Some(10),
        ..Default::default()
    };
    let read_resources = Arc::new(ReadResources::new(
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        1024 * 1024,
        crate::sst::cache::CachePolicyType::Lru,
    ));
    let snapshot = ReadSnapshot::new_with_resources(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![file_meta],
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?),
        std::path::PathBuf::new(),
        false,
        0,
        Some(Arc::clone(&read_resources)),
    );
    let block_cache = read_resources.block_cache();
    assert_eq!(
        read_resources.cached_reader_count(),
        0,
        "snapshot construction must not eagerly open every SST reader"
    );

    // Act
    let first = snapshot.get(b"cache-key", u64::MAX);
    assert!(
        !block_cache.is_empty(),
        "first point read should synchronously populate shared block cache"
    );
    let hits_before = block_cache.metrics().hit_count();
    let second = snapshot.get(b"cache-key", u64::MAX);
    let hits_after = block_cache.metrics().hit_count();

    // Assert
    assert_eq!(first?, Some(b"cache-value".to_vec()));
    assert_eq!(second?, Some(b"cache-value".to_vec()));
    assert!(
        hits_after > hits_before,
        "second snapshot read should hit shared block cache"
    );
    Ok(())
}

#[test]
fn should_share_indexed_sst_view_across_consecutive_snapshots() {
    // Arrange
    let sst_view = Arc::new(SstReadView::new(0, Vec::new()));
    let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());

    // Act
    let first = ReadSnapshot::new_with_view_resources(
        0,
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        Arc::clone(&sst_view),
        Arc::clone(&fs),
        std::path::PathBuf::new(),
        true,
        1,
        None,
    );
    let second = ReadSnapshot::new_with_view_resources(
        0,
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        sst_view,
        fs,
        std::path::PathBuf::new(),
        true,
        2,
        None,
    );

    // Assert
    assert!(first.shares_sst_view_with(&second));
}

#[test]
fn should_open_only_adjacent_lower_level_readers_for_point_lookup() -> crate::common::MidgeResult<()>
{
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut files = Vec::new();
    for index in 0..128_u64 {
        let name = format!("point-{index:04}.sst");
        let key = index.to_be_bytes();
        let mut writer = factory.create()?;
        writer.add_with_meta(&key, Some(key.as_slice()), index + 1, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(&name))?;
        let size_bytes = std::fs::metadata(temp_dir.path().join(&name))?.len();
        files.push(FileMeta {
            name,
            level: 1,
            size_bytes,
            cf_id: 0,
            sst_seq: index + 1,
            smallest_key: Some(key.to_vec()),
            largest_key: Some((index + 1).to_be_bytes().to_vec()),
            smallest_seq: Some(index + 1),
            largest_seq: Some(index + 1),
            key_bounds_complete: true,
            ..Default::default()
        });
    }
    let read_resources = Arc::new(ReadResources::new(
        Arc::clone(&fs),
        std::path::PathBuf::new(),
        1024 * 1024,
        crate::sst::cache::CachePolicyType::Lru,
    ));
    let snapshot = ReadSnapshot::new_with_resources(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        files,
        fs,
        std::path::PathBuf::new(),
        false,
        0,
        Some(Arc::clone(&read_resources)),
    );
    let key = 64_u64.to_be_bytes();

    // Act
    let value = snapshot.get(&key, u64::MAX)?;

    // Assert
    assert_eq!(value.as_deref(), Some(key.as_slice()));
    assert_eq!(
        read_resources.cached_reader_count(),
        2,
        "an equality boundary may open both adjacent files, never the full level"
    );
    Ok(())
}

#[test]
fn should_use_one_sequential_cursor_per_complete_lower_level_in_both_directions(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut files = Vec::new();
    for index in 0..64_u64 {
        let name = format!("level-{index:04}.sst");
        let key = format!("key-{index:04}").into_bytes();
        let mut writer = factory.create()?;
        writer.add_with_meta(&key, Some(key.as_slice()), index + 1, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(&name))?;
        files.push(FileMeta {
            name,
            level: 1,
            size_bytes: 1,
            cf_id: 0,
            sst_seq: index + 1,
            smallest_key: Some(key.clone()),
            largest_key: Some(key),
            smallest_seq: Some(index + 1),
            largest_seq: Some(index + 1),
            key_bounds_complete: true,
            ..Default::default()
        });
    }
    let snapshot = Arc::new(ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        files,
        fs,
        std::path::PathBuf::new(),
        false,
        0,
    ));

    // Act
    let mut forward = snapshot.state_scan(None, None, false, u64::MAX);
    forward.initialize()?;
    let forward_cursor_slots = forward
        .sources
        .iter()
        .filter(|source| {
            matches!(
                &source.iterator,
                SnapshotStateIterator::Sst(_) | SnapshotStateIterator::SstLevel(_)
            )
        })
        .count();
    let forward_rows = forward.collect::<MidgeResult<Vec<_>>>()?;

    let mut reverse = snapshot.state_scan(None, None, true, u64::MAX);
    reverse.initialize()?;
    let reverse_cursor_slots = reverse
        .sources
        .iter()
        .filter(|source| {
            matches!(
                &source.iterator,
                SnapshotStateIterator::Sst(_) | SnapshotStateIterator::SstLevel(_)
            )
        })
        .count();
    let reverse_rows = reverse.collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert_eq!(forward_cursor_slots, 1);
    assert_eq!(reverse_cursor_slots, 1);
    assert_eq!(forward_rows.len(), 64);
    assert_eq!(reverse_rows.len(), 64);
    assert_eq!(
        forward_rows.first().map(|row| row.0.as_ref()),
        Some(b"key-0000".as_slice())
    );
    assert_eq!(
        forward_rows.last().map(|row| row.0.as_ref()),
        Some(b"key-0063".as_slice())
    );
    assert_eq!(
        reverse_rows.first().map(|row| row.0.as_ref()),
        Some(b"key-0063".as_slice())
    );
    assert_eq!(
        reverse_rows.last().map(|row| row.0.as_ref()),
        Some(b"key-0000".as_slice())
    );
    Ok(())
}

#[test]
fn should_merge_equal_boundary_key_once_in_each_scan_direction() -> crate::common::MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
    let mut first_writer = factory.create()?;
    first_writer.add_with_meta(b"a", Some(b"first-a"), 1, EntryType::Put, None)?;
    first_writer.add_with_meta(b"b", Some(b"old-b"), 2, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(first_writer, &temp_dir.path().join("first.sst"))?;
    let mut second_writer = factory.create()?;
    second_writer.add_with_meta(b"b", Some(b"new-b"), 3, EntryType::Put, None)?;
    second_writer.add_with_meta(b"c", Some(b"second-c"), 4, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(second_writer, &temp_dir.path().join("second.sst"))?;
    let files = vec![
        FileMeta {
            name: "first.sst".to_string(),
            level: 1,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"b".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(2),
            key_bounds_complete: true,
            ..Default::default()
        },
        FileMeta {
            name: "second.sst".to_string(),
            level: 1,
            cf_id: 0,
            smallest_key: Some(b"b".to_vec()),
            largest_key: Some(b"c".to_vec()),
            smallest_seq: Some(3),
            largest_seq: Some(4),
            key_bounds_complete: true,
            ..Default::default()
        },
    ];
    let snapshot = Arc::new(ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        files,
        fs,
        std::path::PathBuf::new(),
        false,
        0,
    ));

    // Act
    let point = snapshot.get(b"b", u64::MAX)?;
    let forward = snapshot
        .state_scan(None, None, false, u64::MAX)
        .collect::<MidgeResult<Vec<_>>>()?;
    let reverse = snapshot
        .state_scan(None, None, true, u64::MAX)
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert_eq!(point.as_deref(), Some(b"new-b".as_slice()));
    assert_eq!(forward.len(), 3);
    assert_eq!(reverse.len(), 3);
    assert_eq!(forward[1].0.as_ref(), b"b");
    assert_eq!(forward[1].1.as_ref(), b"new-b");
    assert_eq!(reverse[1].0.as_ref(), b"b");
    assert_eq!(reverse[1].1.as_ref(), b"new-b");
    Ok(())
}

#[test]
fn should_not_skip_range_tombstone_when_manifest_bounds_are_narrow(
) -> crate::common::MidgeResult<()> {
    // Arrange: model an older manifest whose second SST recorded only its
    // point key `m`, even though the SST itself contains [a, c) tombstone.
    let temp_dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(temp_dir.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);

    let mut old_writer = factory.create()?;
    old_writer.add_with_meta(b"b", Some(b"old"), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(old_writer, &temp_dir.path().join("old.sst"))?;

    let mut tombstone_writer = factory.create()?;
    tombstone_writer.add_with_meta(b"m", Some(b"new"), 3, EntryType::Put, None)?;
    tombstone_writer.add_range_tombstone(b"a", b"c", 2)?;
    crate::sst::fs::finish_writer_to_path(
        tombstone_writer,
        &temp_dir.path().join("tombstone.sst"),
    )?;

    let snapshot = ReadSnapshot::new(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        vec![
            FileMeta {
                name: "old.sst".to_string(),
                level: 0,
                size_bytes: std::fs::metadata(temp_dir.path().join("old.sst"))?.len(),
                cf_id: 0,
                smallest_key: Some(b"b".to_vec()),
                largest_key: Some(b"b".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                ..Default::default()
            },
            FileMeta {
                name: "tombstone.sst".to_string(),
                level: 0,
                size_bytes: std::fs::metadata(temp_dir.path().join("tombstone.sst"))?.len(),
                cf_id: 0,
                // Intentionally stale/narrow manifest metadata.
                smallest_key: Some(b"m".to_vec()),
                largest_key: Some(b"m".to_vec()),
                smallest_seq: Some(2),
                largest_seq: Some(3),
                ..Default::default()
            },
        ],
        fs,
        std::path::PathBuf::new(),
        false,
        0,
    );

    // Act
    let value = snapshot.get(b"b", u64::MAX)?;

    // Assert
    assert_eq!(value, None);
    Ok(())
}

/// Builds a memory-mode snapshot with `keys` point writes at sequence 1
/// and `ranges` (start, end, seq) range tombstones.
fn snapshot_with_ranges(
    keys: &[(Vec<u8>, u64)],
    ranges: &[(Vec<u8>, Vec<u8>, u64)],
) -> Arc<ReadSnapshot> {
    let memtable = SkipListMemtable::new();
    for (key, seq) in keys {
        memtable
            .put_with_seq(key.clone(), b"v".to_vec(), *seq, None)
            .expect("put");
    }
    for (start, end, seq) in ranges {
        memtable
            .delete_range_with_seq(start, end, *seq)
            .expect("delete range");
    }
    Arc::new(ReadSnapshot::new(
        Arc::new(memtable),
        Vec::new(),
        Vec::new(),
        Arc::new(crate::io::MockFs::new()),
        std::path::PathBuf::new(),
        true,
        0,
    ))
}

#[test]
fn should_bound_tombstone_work_by_keys_plus_tombstones_when_none_cover_returned_keys() {
    // Arrange: 2,000 live keys under "k", 2,000 old noncovering ranges
    // under "z" that overlap the unbounded scan but cover no key.
    let keys: Vec<(Vec<u8>, u64)> = (0..2_000u32)
        .map(|i| (format!("k{i:06}").into_bytes(), 100))
        .collect();
    let ranges: Vec<(Vec<u8>, Vec<u8>, u64)> = (0..2_000u32)
        .map(|i| {
            (
                format!("z{i:06}").into_bytes(),
                format!("z{i:06}~").into_bytes(),
                1,
            )
        })
        .collect();
    let snapshot = snapshot_with_ranges(&keys, &ranges);

    // Act
    let mut scan = snapshot.state_scan(None, None, false, u64::MAX);
    let rows = scan
        .by_ref()
        .collect::<MidgeResult<Vec<_>>>()
        .expect("scan");

    // Assert: work is linear in K + T, not K x T.
    assert_eq!(rows.len(), 2_000);
    let bound = 8 * (keys.len() + ranges.len()) as u64;
    assert!(
        scan.tombstone_work() <= bound,
        "tombstone work {} exceeds {bound}",
        scan.tombstone_work()
    );
}

#[test]
fn should_match_brute_force_oracle_when_scans_cross_overlapping_range_tombstones() {
    let key = |n: u64| format!("k{n:02}").into_bytes();
    let mut state = 0x9e37_79b9_u64;
    let mut next = move |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    for round in 0..40 {
        // Arrange: versions and tombstones share a small sequence space so
        // equal-sequence ties, nesting and overlap all occur, split
        // between the active and an immutable memtable.
        let active = SkipListMemtable::new();
        let immutable = SkipListMemtable::new();
        let mut versions: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut ranges: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
        for n in 0..30 {
            for seq in 1..=8 {
                if next(4) == 0 {
                    let table = if next(2) == 0 { &active } else { &immutable };
                    table
                        .put_with_seq(key(n), format!("v{n}-{seq}").into_bytes(), seq, None)
                        .expect("put");
                    versions.push((key(n), seq));
                }
            }
        }
        for _ in 0..10 {
            let (a, b) = (next(31), next(31));
            let (start, end, seq) = (key(a.min(b)), key(a.max(b) + 1), 1 + next(8));
            let table = if next(2) == 0 { &active } else { &immutable };
            table
                .delete_range_with_seq(&start, &end, seq)
                .expect("range");
            ranges.push((start, end, seq));
        }
        let snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(active),
            vec![Arc::new(immutable)],
            Vec::new(),
            Arc::new(crate::io::MockFs::new()),
            std::path::PathBuf::new(),
            true,
            0,
        ));

        for sequence in [3, 5, u64::MAX] {
            for (start, end) in [
                (None, None),
                (Some(key(5)), Some(key(22))),
                (Some(key(9)), None),
                (None, Some(key(14))),
            ] {
                // Oracle: newest version visible at `sequence`, hidden by
                // any visible range with an equal or higher sequence.
                let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for n in 0..30 {
                    let k = key(n);
                    if start.as_ref().is_some_and(|s| &k < s)
                        || end.as_ref().is_some_and(|e| &k >= e)
                    {
                        continue;
                    }
                    let newest = versions
                        .iter()
                        .filter(|(vk, vs)| *vk == k && *vs <= sequence)
                        .map(|(_, vs)| *vs)
                        .max();
                    let Some(newest) = newest else { continue };
                    let hidden = ranges.iter().any(|(rs, re, rq)| {
                        *rq <= sequence && *rq >= newest && *rs <= k && k < *re
                    });
                    if !hidden {
                        expected.push((k, format!("v{n}-{newest}").into_bytes()));
                    }
                }

                for reverse in [false, true] {
                    for limit in [None, Some(3_usize)] {
                        // Act
                        let scan =
                            snapshot.state_scan(start.clone(), end.clone(), reverse, sequence);
                        let rows: Vec<(Vec<u8>, Vec<u8>)> = scan
                            .take(limit.unwrap_or(usize::MAX))
                            .map(|row| {
                                let (k, v) = row.expect("row");
                                (k.to_vec(), v.to_vec())
                            })
                            .collect();

                        // Assert
                        let mut want = expected.clone();
                        if reverse {
                            want.reverse();
                        }
                        want.truncate(limit.unwrap_or(usize::MAX));
                        assert_eq!(
                                rows, want,
                                "round {round} seq {sequence} bounds {start:?}..{end:?} reverse {reverse} limit {limit:?}"
                            );
                    }
                }
            }
        }
    }
}
