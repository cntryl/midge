use super::*;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsResult, MockFs, OpenOptions};
use crate::wal::{encoding, frame, WalOpKind};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};

struct BoundedFs {
    inner: MockFs,
    max_read: usize,
    largest_read: Arc<AtomicUsize>,
    read_calls: Arc<AtomicUsize>,
    len_calls: Arc<AtomicUsize>,
}

struct BoundedFile<'a> {
    inner: Box<dyn File + 'a>,
    max_read: usize,
    largest_read: Arc<AtomicUsize>,
    read_calls: Arc<AtomicUsize>,
    len_calls: Arc<AtomicUsize>,
}

impl crate::io::ReadFile for BoundedFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        self.largest_read
            .fetch_max(usize::try_from(len).unwrap_or(usize::MAX), Ordering::SeqCst);
        if len > self.max_read as u64 {
            return Err(FsError::Io("full inventory read forbidden".into()));
        }
        self.inner.read_at(offset, len)
    }

    fn len(&self) -> FsResult<u64> {
        self.len_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.len()
    }
}

impl File for BoundedFile<'_> {
    fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, bytes)
    }
    fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
        self.inner.append(bytes)
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

impl ReadFs for BoundedFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        if path.0 == "wal" {
            Ok(true)
        } else {
            self.inner.exists(path)
        }
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path).map(|entries| {
            entries
                .into_iter()
                .map(|mut entry| {
                    entry.name = entry.name.rsplit('/').next().unwrap().to_string();
                    entry
                })
                .collect()
        })
    }
}

impl Fs for BoundedFs {
    fn try_lock_exclusive(&self, path: &FsPath) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        Ok(Box::new(BoundedFile {
            inner: self.inner.open(path, opts)?,
            max_read: self.max_read,
            largest_read: Arc::clone(&self.largest_read),
            read_calls: Arc::clone(&self.read_calls),
            len_calls: Arc::clone(&self.len_calls),
        }))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// Segments `files` under `wal/`, by id, behind a read-size bound.
fn fixture(files: &[(u64, Vec<u8>)], max_read: usize) -> BoundedFs {
    let inner = MockFs::new();
    inner.create_dir_all(&FsPath::new("wal")).unwrap();
    for (id, bytes) in files {
        inner
            .open(
                &crate::wal::segment_key(*id),
                OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )
            .unwrap()
            .write_at(0, Bytes::copy_from_slice(bytes))
            .unwrap();
    }
    BoundedFs {
        inner,
        max_read,
        largest_read: Arc::new(AtomicUsize::new(0)),
        read_calls: Arc::new(AtomicUsize::new(0)),
        len_calls: Arc::new(AtomicUsize::new(0)),
    }
}

fn put(key: impl Into<Bytes>, sequence: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        key.into(),
        Some(Bytes::from(vec![b'x'; 128])),
        sequence,
    )
}

fn encode(record: &WalRecord) -> Vec<u8> {
    let payload = encoding::encode(record).unwrap();
    let mut bytes = Vec::new();
    frame::append_frame(&mut bytes, &payload).unwrap();
    bytes
}

fn limits() -> StreamingReplayLimits {
    StreamingReplayLimits {
        max_frame_bytes: 1024,
        max_pending_txn_bytes: 32 * 1024,
        max_memtable_encoded_bytes: 20 * 1024,
        target_memtable_encoded_bytes: 20 * 1024,
    }
}

#[test]
fn should_checkpoint_single_wal_object_larger_than_configured_local_capacity() {
    // Arrange
    let bytes: Vec<_> = (1..=2000)
        .flat_map(|seq| encode(&put(format!("key-{seq:04}"), seq)))
        .collect();
    assert!(bytes.len() > 10 * limits().max_memtable_encoded_bytes);
    let storage = fixture(&[(1, bytes)], limits().max_frame_bytes);
    let mut memtables = HashMap::new();
    let mut checkpoint_count = 0;
    let mut recovered_entries = 0;

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        limits(),
        &mut |tables, _| {
            assert!(
                tables
                    .values()
                    .map(|table| table.encoded_size_upper_bound())
                    .sum::<usize>()
                    <= limits().max_memtable_encoded_bytes
            );
            checkpoint_count += 1;
            recovered_entries += tables
                .values()
                .map(|table| table.iter_all().len())
                .sum::<usize>();
            tables.clear();
            Ok(())
        },
    )
    .unwrap();
    recovered_entries += memtables
        .values()
        .map(|table| table.iter_all().len())
        .sum::<usize>();

    // Assert
    assert_eq!(stats.record_count, 2000);
    assert_eq!(stats.max_sequence, Some(2000));
    assert_eq!(recovered_entries, 2000);
    assert!(checkpoint_count > 100);
    assert!(storage.largest_read.load(Ordering::SeqCst) <= limits().max_frame_bytes);
    assert!(
        storage.read_calls.load(Ordering::SeqCst) < 1000,
        "2000 small frames should reuse bounded read-ahead windows"
    );
    assert!(
        storage.len_calls.load(Ordering::SeqCst) <= 4,
        "file length should be cached for each replay pass"
    );
}

#[test]
fn should_read_many_local_wal_frames_with_one_window_per_file_pass() {
    // Arrange
    let bytes = (1..=2000)
        .flat_map(|seq| encode(&put(format!("key-{seq:04}"), seq)))
        .collect();
    let storage = fixture(&[(1, bytes)], 1024 * 1024);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        StreamingReplayLimits::local(),
        &mut |_, _| Ok(()),
    )
    .expect("local replay");

    // Assert
    assert_eq!(stats.record_count, 2000);
    assert!(storage.read_calls.load(Ordering::SeqCst) <= 4);
    assert!(storage.len_calls.load(Ordering::SeqCst) <= 4);
    assert!(storage.largest_read.load(Ordering::SeqCst) <= 1024 * 1024);
}

#[test]
fn should_read_large_frame_directly_when_payload_exceeds_read_ahead_window() {
    // Arrange
    let payload_bytes = 2 * 1024 * 1024;
    let mut seed = 0x1234_5678_9abc_def0_u64;
    let value = (0..payload_bytes)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed.to_le_bytes()[4]
        })
        .collect::<Vec<_>>();
    let record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"large"),
        Some(Bytes::from(value)),
        1,
    );
    let storage = fixture(&[(1, encode(&record))], 3 * 1024 * 1024);
    let replay_limits = StreamingReplayLimits {
        max_frame_bytes: 3 * 1024 * 1024,
        max_pending_txn_bytes: 8 * 1024 * 1024,
        max_memtable_encoded_bytes: 8 * 1024 * 1024,
        target_memtable_encoded_bytes: 8 * 1024 * 1024,
    };
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        replay_limits,
        &mut |_, _| Ok(()),
    )
    .expect("large WAL frame");

    // Assert
    assert_eq!(stats.record_count, 1);
    assert!(storage.largest_read.load(Ordering::SeqCst) > 1024 * 1024);
    assert!(storage.read_calls.load(Ordering::SeqCst) <= 6);
}

#[test]
fn should_checkpoint_only_complete_split_transactions() {
    // Arrange
    let mut records = vec![put("seed-a", 1), put("seed-b", 2)];
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 3);
    begin.txn_id = Some(10);
    records.push(begin);
    for seq in 4..=6 {
        let mut record = put(format!("txn-{seq}"), seq);
        record.txn_id = Some(10);
        records.push(record);
    }
    let mut commit = WalRecord::new(WalOpKind::TxnCommit, Bytes::from_static(b"txn"), None, 7);
    commit.txn_id = Some(10);
    records.push(commit);
    records.push(put("after", 8));
    let bytes = records.iter().flat_map(encode).collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let mut memtables = HashMap::new();
    let config = StreamingReplayLimits {
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };
    let mut checkpoint_sequences = Vec::new();
    let mut txn_counts = Vec::new();

    // Act
    replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |tables, stats| {
            let entries = tables[&0].iter_all();
            txn_counts.push(
                entries
                    .iter()
                    .filter(|(key, _, _)| key.starts_with(b"txn-"))
                    .count(),
            );
            checkpoint_sequences.push(stats.through_sequence);
            tables.clear();
            Ok(())
        },
    )
    .unwrap();

    // Assert
    assert_eq!(txn_counts, vec![0, 3]);
    assert_eq!(checkpoint_sequences, vec![2, 7]);
    assert_eq!(memtables[&0].get(b"after").unwrap(), Some(vec![b'x'; 128]));
}

#[test]
fn should_reject_atomic_batch_before_checkpoint_when_transaction_exceeds_encoded_limit() {
    // Arrange
    let records: Vec<_> = (2..=6)
        .map(|seq| {
            let mut op = put(format!("atomic-{seq}"), seq);
            op.txn_id = Some(1);
            op
        })
        .collect();
    let payload = encoding::encode_txn_batch_payload(1, 1, 7, 0, &records).unwrap();
    let mut outer = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(payload),
        7,
    );
    outer.txn_id = Some(1);
    let storage = fixture(&[(1, encode(&outer))], 4096);
    let config = StreamingReplayLimits {
        max_frame_bytes: 4096,
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |_, _| {
            checkpoints += 1;
            Ok(())
        },
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
    assert_eq!(checkpoints, 0);
    assert!(memtables.is_empty());
}

#[test]
fn should_bound_uncommitted_split_transaction_buffers() {
    // Arrange
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1);
    begin.txn_id = Some(1);
    let mut bytes = encode(&begin);
    for seq in 2..=20 {
        let mut op = put(format!("open-{seq}"), seq);
        op.txn_id = Some(1);
        bytes.extend(encode(&op));
    }
    let storage = fixture(&[(1, bytes)], 1024);
    let mut memtables = HashMap::new();
    let config = StreamingReplayLimits {
        max_pending_txn_bytes: 2048,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |_, _| panic!("open transaction cannot checkpoint"),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(memtables.is_empty());
}

#[test]
fn should_fail_closed_when_corrupt_length_hides_verified_active_suffix() {
    // Arrange
    let first = encode(&put("first", 1));
    let middle = encode(&put("middle", 2));
    let last = encode(&put("last", 3));
    let offset = first.len();
    let mut bytes = [first, middle, last].concat();
    let bad_len = u32::try_from(bytes.len()).unwrap();
    bytes[offset..offset + 4].copy_from_slice(&bad_len.to_le_bytes());
    let storage = fixture(&[(1, bytes)], 1024);
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
        limits(),
        &mut |_, _| {
            checkpoints += 1;
            Ok(())
        },
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(message)) if message.contains("verified later frame"))
    );
    assert_eq!(checkpoints, 0, "a failed replay must not checkpoint");
}

#[test]
fn should_tolerate_only_verified_active_prefix_when_final_frame_is_torn() {
    // Arrange
    let mut bytes = encode(&put("first", 1));
    let mut torn = encode(&put("torn", 2));
    torn.truncate(torn.len() - 20);
    bytes.extend(torn);
    let storage = fixture(&[(1, bytes)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
        limits(),
        &mut |_, _| Ok(()),
    )
    .unwrap();

    // Assert
    assert_eq!(stats.record_count, 1);
    assert!(!stats.had_corruption);
    assert!(memtables[&0].get(b"first").unwrap().is_some());
    assert_eq!(memtables[&0].get(b"torn").unwrap(), None);
}

#[test]
fn should_reject_compressed_record_before_expanding_beyond_replay_budget() {
    // Arrange
    let mut record = put("compressed", 1);
    record.value = Some(Bytes::from(vec![b'x'; 128 * 1024]));
    let encoded = encode(&record);
    assert!(encoded.len() < 1024);
    let storage = fixture(&[(1, encoded)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        limits(),
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(memtables.is_empty());
}

#[test]
fn should_reject_invalid_batch_or_empty_segment_in_sealed_stream_inspector() {
    // Arrange
    let mut invalid_batch = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(Bytes::from_static(b"invalid batch")),
        3,
    );
    invalid_batch.txn_id = Some(1);
    for bytes in [encode(&invalid_batch), Vec::new()] {
        let storage = fixture(&[(1, bytes)], 1024);
        segments(&storage, &[1], None);
        let mut read_ns = 0;
        let file = open_wal_replay_file(&storage, &crate::wal::segment_key(1), &mut read_ns)
            .unwrap()
            .unwrap();

        // Act
        let result = inspect_sealed_wal_file(&*file, &crate::wal::segment_key(1), limits());

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }
}
#[test]
fn should_reject_conflicting_point_after_its_memtable_was_checkpointed() {
    // Arrange
    let mut records: Vec<_> = (1..=10).map(|seq| put(format!("key-{seq}"), seq)).collect();
    let mut duplicate = put("key-1", 1);
    duplicate.value = Some(Bytes::from_static(b"conflicting"));
    records.push(duplicate);
    let storage = fixture(&[(1, records.iter().flat_map(encode).collect())], 1024);
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;
    let config = StreamingReplayLimits {
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |tables, _| {
            checkpoints += 1;
            tables.clear();
            Ok(())
        },
    );

    // Assert
    assert!(checkpoints > 0);
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
}

#[test]
fn should_reject_conflicting_transaction_batch_after_checkpoint() {
    // Arrange
    let make_batch = |value: &'static [u8]| {
        let mut op = put("batch-key", 2);
        op.txn_id = Some(1);
        op.value = Some(Bytes::from_static(value));
        let payload = encoding::encode_txn_batch_payload(1, 1, 3, 0, &[op]).unwrap();
        let mut outer = WalRecord::new(
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            3,
        );
        outer.txn_id = Some(1);
        outer
    };
    let mut bytes = encode(&make_batch(b"first"));
    for seq in 4..=15 {
        bytes.extend(encode(&put(format!("middle-{seq}"), seq)));
    }
    bytes.extend(encode(&make_batch(b"conflicting")));
    let storage = fixture(&[(1, bytes)], 1024);
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;
    let config = StreamingReplayLimits {
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |tables, _| {
            checkpoints += 1;
            tables.clear();
            Ok(())
        },
    );

    // Assert
    assert!(checkpoints > 0);
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
}

#[test]
fn should_recover_atomic_batch_above_soft_checkpoint_target_within_hard_capacity() {
    // Arrange
    let mut operation = put("large-atomic", 3);
    operation.txn_id = Some(2);
    operation.value = Some(Bytes::from(vec![b'v'; 12 * 1024]));
    let payload = encoding::encode_txn_batch_payload(2, 2, 4, 0, &[operation]).unwrap();
    let mut outer = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(payload),
        4,
    );
    outer.txn_id = Some(2);
    let mut bytes = encode(&put("seed", 1));
    bytes.extend(encode(&outer));
    let storage = fixture(&[(1, bytes)], 32 * 1024);
    let config = StreamingReplayLimits {
        max_frame_bytes: 32 * 1024,
        max_pending_txn_bytes: 64 * 1024,
        max_memtable_encoded_bytes: 64 * 1024,
        target_memtable_encoded_bytes: 18 * 1024,
    };
    let mut memtables = HashMap::new();
    let mut checkpoints = Vec::new();

    // Act
    replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1], None),
        &mut memtables,
        ReplayPolicy::Strict,
        config,
        &mut |tables, stats| {
            assert!(tables[&0].get(b"large-atomic").unwrap().is_none());
            checkpoints.push(stats.through_sequence);
            tables.clear();
            Ok(())
        },
    )
    .unwrap();

    // Assert
    assert_eq!(checkpoints, vec![1]);
    assert_eq!(
        memtables[&0].get(b"large-atomic").unwrap(),
        Some(vec![b'v'; 12 * 1024])
    );
    assert!(memtables[&0].encoded_size_upper_bound() > config.target_memtable_encoded_bytes);
    assert!(memtables[&0].encoded_size_upper_bound() <= config.max_memtable_encoded_bytes);
}

#[test]
fn should_visit_only_validated_wal_records_before_corrupt_tail() {
    // Arrange
    let mut bytes = encode(&put("first", 1));
    bytes.extend_from_slice(&[0xff; 8]);
    let storage = fixture(&[(1, bytes)], limits().max_frame_bytes);
    segments(&storage, &[1], None);
    let path = crate::wal::segment_key(1);
    let file = storage
        .open(
            &path,
            OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .unwrap();
    let mut visited = Vec::new();

    // Act
    let result = visit_sealed_wal_records(file.as_ref(), &path, limits(), &mut |record| {
        visited.push(record.seq);
        Ok(())
    });

    // Assert
    assert!(result.is_err());
    assert_eq!(visited, vec![1]);
}

#[test]
fn should_stop_streamed_wal_inspection_when_record_visitor_rejects() {
    // Arrange
    let bytes = (1..=3)
        .flat_map(|sequence| encode(&put("key", sequence)))
        .collect();
    let storage = fixture(&[(1, bytes)], limits().max_frame_bytes);
    segments(&storage, &[1], None);
    let path = crate::wal::segment_key(1);
    let file = storage
        .open(
            &path,
            OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .unwrap();
    let mut visited = 0;

    // Act
    let result = visit_sealed_wal_records(file.as_ref(), &path, limits(), &mut |_| {
        visited += 1;
        Err(MidgeError::ResourceLimit("visitor budget".into()))
    });

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(visited, 1);
}

#[test]
fn should_checkpoint_after_abandoned_split_transaction_when_newer_epoch_records_follow() {
    // Arrange
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 3);
    begin.txn_id = Some(10);
    let mut abandoned = put("abandoned", 4);
    abandoned.txn_id = Some(10);
    let epoch_one: Vec<u8> = [put("seed", 2), begin, abandoned]
        .iter()
        .flat_map(encode)
        .collect();
    let epoch_two: Vec<u8> = (5..=400)
        .map(|seq| put(format!("next-{seq}"), seq))
        .flat_map(|record| encode(&record))
        .collect();
    // Segment 1 was written by epoch 1, segment 2 by epoch 2.
    let (epoch_one, link) = crate::wal::format::segment::SegmentBuilder::new(
        1,
        1,
        crate::wal::format::ChainLink::GENESIS,
    )
    .bytes(&epoch_one)
    .seal();
    let (epoch_two, _) = crate::wal::format::segment::SegmentBuilder::new(2, 2, link)
        .bytes(&epoch_two)
        .seal();
    let storage = fixture(&[(1, epoch_one), (2, epoch_two)], 1024);
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;
    let mut recovered = std::collections::BTreeSet::new();

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &segments(&storage, &[1, 2], None),
        &mut memtables,
        ReplayPolicy::Strict,
        limits(),
        &mut |tables, _| {
            checkpoints += 1;
            for table in tables.values() {
                recovered.extend(table.iter_all().into_iter().map(|entry| entry.0));
            }
            tables.clear();
            Ok(())
        },
    );

    // Assert
    assert!(result.is_ok(), "replay failed: {result:?}");
    assert!(checkpoints > 0);
    for table in memtables.values() {
        recovered.extend(table.iter_all().into_iter().map(|entry| entry.0));
    }
    assert!(recovered.contains(b"seed".as_slice()));
    for seq in 5..=400 {
        assert!(recovered.contains(format!("next-{seq}").as_bytes()));
    }
    assert!(!recovered.contains(b"abandoned".as_slice()));
    assert_eq!(recovered.len(), 397);
}

/// Replay `sealed` segments under `wal/` in order, then `open`, after
/// wrapping the bodies `storage` holds for them into a chain of v2 segments
/// written by epoch 1.
fn segments(storage: &BoundedFs, sealed: &[u64], open: Option<u64>) -> ReplaySegments {
    let segments = crate::wal::recovery::fixture::segments(&FsPath::new("wal"), sealed, open);
    crate::wal::recovery::fixture::wrap_chain(&storage.inner, &segments, 1)
}

#[test]
fn should_reject_segment_scan_when_bytes_follow_its_footer() {
    // Arrange: a sealed segment whose footer is followed by one more frame,
    // as recovery scans an open member a crashed rotation already sealed.
    let (mut sealed, _) = crate::wal::format::segment::SegmentBuilder::new(
        1,
        1,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&put("key", 1))
    .seal();
    sealed.extend(encode(&put("key", 2)));
    let storage = fixture(&[(1, sealed)], 1024);
    let path = crate::wal::segment_key(1);
    let file = storage
        .open(
            &path,
            OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .unwrap();

    // Act
    let result = scan_segment_file(file.as_ref(), &path, None);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("follow the footer")),
        "{result:?}"
    );
}

#[test]
fn should_replay_complete_batch_when_frontier_precedes_commit() {
    // Arrange
    let mut members = vec![put("first-member", 2), put("second-member", 3)];
    for member in &mut members {
        member.txn_id = Some(1);
    }
    let payload = encoding::encode_txn_batch_payload(1, 1, 4, 0, &members).unwrap();
    let mut batch = WalRecord::new(WalOpKind::TxnBatch, Bytes::new(), Some(payload), 4);
    batch.txn_id = Some(1);
    let storage = fixture(&[(1, encode(&batch))], 1024);
    let mut tables = HashMap::new();
    let frontiers = replay_frontiers(2);

    // Act
    replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::Strict,
        limits(),
        ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers)),
        &mut |_, _| Ok(()),
    )
    .unwrap();

    // Assert
    assert_eq!(
        tables[&0].iter_all().len(),
        2,
        "a frontier below commit must replay every member"
    );
}

#[test]
fn should_replay_complete_spooled_transaction_when_frontier_precedes_commit() {
    // Arrange
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::new(), None, 1);
    begin.txn_id = Some(1);
    let mut first = put("first-member", 2);
    first.txn_id = Some(1);
    let mut second = put("second-member", 3);
    second.txn_id = Some(1);
    let mut commit = WalRecord::new(WalOpKind::TxnCommit, Bytes::new(), None, 4);
    commit.txn_id = Some(1);
    let bytes = [begin, first, second, commit]
        .iter()
        .flat_map(encode)
        .collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let mut tables = HashMap::new();
    let frontiers = replay_frontiers(2);

    // Act
    replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::Strict,
        StreamingReplayLimits::local(),
        ReplayOptions {
            spill_pending_txns: true,
            ..ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers))
        },
        &mut |_, _| Ok(()),
    )
    .unwrap();

    // Assert
    assert_eq!(
        tables[&0].iter_all().len(),
        2,
        "a frontier below commit must replay every spooled member"
    );
}

fn replay_frontiers(sequence: u64) -> crate::metadata::frontiers::FlushFrontiers {
    serde_json::from_value(serde_json::json!([
        {"cf_id": 0, "through_sequence": sequence}
    ]))
    .unwrap()
}

#[test]
fn should_skip_complete_transaction_when_commit_is_already_flushed() {
    // Arrange
    let mut members = vec![put("first", 2), put("second", 3)];
    for member in &mut members {
        member.txn_id = Some(1);
    }
    let payload = encoding::encode_txn_batch_payload(1, 1, 4, 0, &members).unwrap();
    let mut batch = WalRecord::new(WalOpKind::TxnBatch, Bytes::new(), Some(payload), 4);
    batch.txn_id = Some(1);
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::new(), None, 1);
    begin.txn_id = Some(1);
    let mut commit = WalRecord::new(WalOpKind::TxnCommit, Bytes::new(), None, 4);
    commit.txn_id = Some(1);
    let split = [vec![begin], members, vec![commit]].concat();
    let frontiers = replay_frontiers(4);

    for (records, spool) in [(vec![batch], false), (split.clone(), false), (split, true)] {
        let bytes = records.iter().flat_map(encode).collect();
        let storage = fixture(&[(1, bytes)], 1024);
        let mut tables = HashMap::new();

        // Act
        let stats = replay_wal_with_options(
            &storage,
            &segments(&storage, &[1], None),
            &mut tables,
            ReplayPolicy::Strict,
            StreamingReplayLimits::local(),
            ReplayOptions {
                spill_pending_txns: spool,
                ..ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers))
            },
            &mut |_, _| Ok(()),
        )
        .unwrap();

        // Assert
        assert!(tables.is_empty());
        assert_eq!(stats.max_sequence, Some(4));
    }
}

#[test]
fn should_validate_transaction_shape_when_its_family_is_already_flushed() {
    // Arrange
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::new(), None, 1);
    begin.txn_id = Some(1);
    let mut member = put("wrong-family", 2);
    member.txn_id = Some(1);
    member.cf_id = 7;
    let bytes = [begin, member].iter().flat_map(encode).collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let frontiers = replay_frontiers(9);

    // Act
    let result = replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut HashMap::new(),
        ReplayPolicy::Strict,
        StreamingReplayLimits::local(),
        ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers)),
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
}

#[test]
fn should_refuse_missing_family_authority_when_replaying_under_salvage_policy() {
    // Arrange
    let mut record = put("unknown-family", 1);
    record.cf_id = 7;
    let storage = fixture(&[(1, encode(&record))], 1024);
    let frontiers = replay_frontiers(0);
    let mut tables = HashMap::new();

    // Act
    let result = replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::SalvageValidPrefix,
        StreamingReplayLimits::local(),
        ReplayOptions {
            salvage_record_errors: true,
            ..ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers))
        },
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    assert!(tables.is_empty());
}

fn overlapping_transaction_batch() -> WalRecord {
    let mut members = vec![put("uncommitted-member", 2), put("prior-member", 3)];
    for member in &mut members {
        member.txn_id = Some(1);
    }
    let payload = encoding::encode_txn_batch_payload(1, 1, 4, 0, &members).unwrap();
    let mut batch = WalRecord::new(WalOpKind::TxnBatch, Bytes::new(), Some(payload), 4);
    batch.txn_id = Some(1);
    batch
}

#[test]
fn should_preserve_complete_units_when_a_batch_overlaps_prior_frames_under_strict_replay() {
    // Arrange
    let bytes = [put("prior-member", 3), overlapping_transaction_batch()]
        .iter()
        .flat_map(encode)
        .collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let mut tables = HashMap::new();
    let mut checkpoints = 0;

    // Act
    let result = replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::Strict,
        StreamingReplayLimits {
            target_memtable_encoded_bytes: 1,
            ..limits()
        },
        ReplayOptions::new(crate::wal::replay::ReplaySelection::All),
        &mut |tables, _| {
            checkpoints += 1;
            tables.clear();
            Ok(())
        },
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    assert_eq!(
        checkpoints, 0,
        "a rejected unit must not start a checkpoint"
    );
    assert!(tables[&0].get(b"prior-member").unwrap().is_some());
    assert!(
        tables[&0].get(b"uncommitted-member").unwrap().is_none(),
        "a rejected batch must not expose any member"
    );
}

#[test]
fn should_preserve_complete_units_when_a_batch_overlaps_prior_frames_under_salvage_replay() {
    // Arrange
    let bytes = [put("prior-member", 3), overlapping_transaction_batch()]
        .iter()
        .flat_map(encode)
        .collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let mut tables = HashMap::new();
    let mut checkpoints = 0;

    // Act
    let stats = replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::SalvageValidPrefix,
        limits(),
        ReplayOptions {
            salvage_record_errors: true,
            ..ReplayOptions::new(crate::wal::replay::ReplaySelection::All)
        },
        &mut |_, _| {
            checkpoints += 1;
            Ok(())
        },
    )
    .unwrap();

    // Assert
    assert!(stats.had_corruption);
    assert_eq!(
        checkpoints, 0,
        "a rejected unit must not start a checkpoint"
    );
    assert!(tables[&0].get(b"prior-member").unwrap().is_some());
    assert!(
        tables[&0].get(b"uncommitted-member").unwrap().is_none(),
        "salvage must return only complete accepted units"
    );
}

#[test]
fn should_validate_overlapping_batch_sequences_when_its_family_is_already_flushed() {
    // Arrange
    let bytes = [put("prior-member", 3), overlapping_transaction_batch()]
        .iter()
        .flat_map(encode)
        .collect();
    let storage = fixture(&[(1, bytes)], 1024);
    let mut tables = HashMap::new();
    let frontiers = replay_frontiers(4);

    // Act
    let result = replay_wal_with_options(
        &storage,
        &segments(&storage, &[1], None),
        &mut tables,
        ReplayPolicy::Strict,
        limits(),
        ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers)),
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    assert!(tables.is_empty());
}

#[test]
fn should_reject_batch_spans_before_the_segment_first_sequence() {
    // Arrange
    for (begin_sequence, accepted) in [(9, false), (10, true)] {
        let mut members = vec![
            put("first", begin_sequence + 1),
            put("second", begin_sequence + 2),
        ];
        for member in &mut members {
            member.txn_id = Some(1);
        }
        let commit_sequence = begin_sequence + 3;
        let payload =
            encoding::encode_txn_batch_payload(1, begin_sequence, commit_sequence, 0, &members)
                .unwrap();
        let mut batch = WalRecord::new(
            WalOpKind::TxnBatch,
            Bytes::new(),
            Some(payload),
            commit_sequence,
        );
        batch.txn_id = Some(1);
        let bytes = crate::wal::format::segment::SegmentBuilder::new(
            1,
            1,
            crate::wal::format::ChainLink::GENESIS,
        )
        .first_seq(10)
        .record(&batch)
        .seal()
        .0;
        let storage = fixture(&[(1, bytes)], 1024);
        let mut tables = HashMap::new();

        // Act
        let result = replay_wal_with_options(
            &storage,
            &segments(&storage, &[1], None),
            &mut tables,
            ReplayPolicy::Strict,
            limits(),
            ReplayOptions::new(crate::wal::replay::ReplaySelection::All),
            &mut |_, _| Ok(()),
        );

        // Assert
        if accepted {
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(tables[&0].iter_all().len(), 2);
        } else {
            assert!(
                matches!(result, Err(MidgeError::Corruption(_))),
                "{result:?}"
            );
            assert!(tables.is_empty());
        }
    }
}
