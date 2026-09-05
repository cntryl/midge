use super::*;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, FsError, FsResult, MockFs, OpenOptions};
use crate::wal::{encoding, frame, WalOpKind};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};

struct BoundedFs {
    inner: MockFs,
    max_read: usize,
    largest_read: Arc<AtomicUsize>,
}

struct BoundedFile<'a> {
    inner: Box<dyn File + 'a>,
    max_read: usize,
    largest_read: Arc<AtomicUsize>,
}

impl File for BoundedFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.largest_read
            .fetch_max(usize::try_from(len).unwrap_or(usize::MAX), Ordering::SeqCst);
        if len > self.max_read as u64 {
            return Err(FsError::Io("full inventory read forbidden".into()));
        }
        self.inner.read_at(offset, len)
    }
    fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, bytes)
    }
    fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
        self.inner.append(bytes)
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

impl Fs for BoundedFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        Ok(Box::new(BoundedFile {
            inner: self.inner.open(path, opts)?,
            max_read: self.max_read,
            largest_read: Arc::clone(&self.largest_read),
        }))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
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
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
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

fn fixture(files: &[(&str, Vec<u8>)], max_read: usize) -> BoundedFs {
    let inner = MockFs::new();
    inner.create_dir_all(&FsPath::new("wal")).unwrap();
    for (name, bytes) in files {
        inner
            .open(
                &FsPath::new(format!("wal/{name}")),
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
    }
}

fn put(key: impl Into<Bytes>, sequence: u64, epoch: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        key.into(),
        Some(Bytes::from(vec![b'x'; 128])),
        sequence,
        epoch,
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
        .flat_map(|seq| encode(&put(format!("key-{seq:04}"), seq, 1)))
        .collect();
    assert!(bytes.len() > 10 * limits().max_memtable_encoded_bytes);
    let storage = fixture(&[("1.wal", bytes)], limits().max_frame_bytes);
    let mut memtables = HashMap::new();
    // The prior snapshot entrypoint fails the same bounded-read contract.
    assert!(super::super::replay_wal_with_policy(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict
    )
    .is_err());
    storage.largest_read.store(0, Ordering::SeqCst);
    let mut checkpoint_count = 0;
    let mut recovered_entries = 0;

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
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
                .map(|table| table.iter_all(u64::MAX).len())
                .sum::<usize>();
            tables.clear();
            Ok(())
        },
    )
    .unwrap();
    recovered_entries += memtables
        .values()
        .map(|table| table.iter_all(u64::MAX).len())
        .sum::<usize>();

    // Assert
    assert_eq!(stats.record_count, 2000);
    assert_eq!(stats.max_sequence, Some(2000));
    assert_eq!(recovered_entries, 2000);
    assert!(checkpoint_count > 100);
    assert!(storage.largest_read.load(Ordering::SeqCst) <= limits().max_frame_bytes);
}

#[test]
fn should_checkpoint_only_complete_split_transactions() {
    // Arrange
    let mut records = vec![put("seed-a", 1, 1), put("seed-b", 2, 1)];
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 3, 1);
    begin.txn_id = Some(10);
    records.push(begin);
    for seq in 4..=6 {
        let mut record = put(format!("txn-{seq}"), seq, 1);
        record.txn_id = Some(10);
        records.push(record);
    }
    let mut commit = WalRecord::new(WalOpKind::TxnCommit, Bytes::from_static(b"txn"), None, 7, 1);
    commit.txn_id = Some(10);
    records.push(commit);
    records.push(put("after", 8, 1));
    let bytes = records.iter().flat_map(encode).collect();
    let storage = fixture(&[("1.wal", bytes)], 1024);
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
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        config,
        &mut |tables, stats| {
            let entries = tables[&0].iter_all(u64::MAX);
            txn_counts.push(
                entries
                    .iter()
                    .filter(|(key, _, _)| key.starts_with(b"txn-"))
                    .count(),
            );
            checkpoint_sequences.push(stats.max_sequence);
            tables.clear();
            Ok(())
        },
    )
    .unwrap();

    // Assert
    assert_eq!(txn_counts, vec![0, 3]);
    assert_eq!(checkpoint_sequences, vec![Some(2), Some(7)]);
    assert_eq!(memtables[&0].get(b"after").unwrap(), Some(vec![b'x'; 128]));
}

#[test]
fn should_reject_atomic_batch_before_checkpoint_when_transaction_exceeds_encoded_limit() {
    // Arrange
    let records: Vec<_> = (2..=6)
        .map(|seq| {
            let mut op = put(format!("atomic-{seq}"), seq, 1);
            op.txn_id = Some(1);
            op
        })
        .collect();
    let payload = encoding::encode_txn_batch_payload(1, 1, 7, 1, &records).unwrap();
    let mut outer = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(payload),
        7,
        1,
    );
    outer.txn_id = Some(1);
    let storage = fixture(&[("1.wal", encode(&outer))], 4096);
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
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
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
    let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1, 1);
    begin.txn_id = Some(1);
    let mut bytes = encode(&begin);
    for seq in 2..=20 {
        let mut op = put(format!("open-{seq}"), seq, 1);
        op.txn_id = Some(1);
        bytes.extend(encode(&op));
    }
    let storage = fixture(&[("1.wal", bytes)], 1024);
    let mut memtables = HashMap::new();
    let config = StreamingReplayLimits {
        max_pending_txn_bytes: 2048,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        config,
        &mut |_, _| panic!("open transaction cannot checkpoint"),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(memtables.is_empty());
}

#[test]
fn should_preserve_epoch_ordinals_when_duplicate_rotation_frames_are_rescanned() {
    // Arrange
    let duplicate = encode(&put("duplicate", 1, 1));
    let active = [
        duplicate.clone(),
        encode(&put("fresh", 10, 2)),
        encode(&put("stale", 2, 1)),
    ]
    .concat();
    let storage = fixture(&[("1.wal", duplicate), ("wal.log", active)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        limits(),
        &mut |_, _| panic!("fixture fits memtable"),
    )
    .unwrap();

    // Assert
    assert_eq!(stats.stale_records_skipped, 1);
    assert_eq!(stats.record_count, 2);
    assert_eq!(stats.max_epoch_seen, 2);
    assert!(memtables[&0].get(b"duplicate").unwrap().is_some());
    assert!(memtables[&0].get(b"fresh").unwrap().is_some());
    assert_eq!(memtables[&0].get(b"stale").unwrap(), None);
}

#[test]
fn should_fail_closed_when_corrupt_length_hides_verified_active_suffix() {
    // Arrange
    let first = encode(&put("first", 1, 1));
    let middle = encode(&put("middle", 2, 1));
    let last = encode(&put("last", 3, 1));
    let offset = first.len();
    let mut bytes = [first, middle, last].concat();
    let bad_len = u32::try_from(bytes.len()).unwrap();
    bytes[offset..offset + 4].copy_from_slice(&bad_len.to_le_bytes());
    let storage = fixture(&[("wal.log", bytes)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        limits(),
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(message)) if message.contains("verified later frame"))
    );
    assert!(
        memtables.is_empty(),
        "epoch discovery must reject before checkpointing"
    );
}

#[test]
fn should_tolerate_only_verified_active_prefix_when_final_frame_is_torn() {
    // Arrange
    let mut bytes = encode(&put("first", 1, 1));
    let mut torn = encode(&put("torn", 2, 1));
    torn.truncate(torn.len() - 20);
    bytes.extend(torn);
    let storage = fixture(&[("wal.log", bytes)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
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
    let mut record = put("compressed", 1, 1);
    record.value = Some(Bytes::from(vec![b'x'; 128 * 1024]));
    let encoded = encode(&record);
    assert!(encoded.len() < 1024);
    let storage = fixture(&[("1.wal", encoded)], 1024);
    let mut memtables = HashMap::new();

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        limits(),
        &mut |_, _| Ok(()),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(memtables.is_empty());
}

#[test]
fn should_reject_mixed_epoch_or_invalid_batch_in_sealed_stream_inspector() {
    // Arrange
    let mixed = [encode(&put("old", 1, 1)), encode(&put("new", 2, 2))].concat();
    let mut invalid_batch = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(Bytes::from_static(b"invalid batch")),
        3,
        1,
    );
    invalid_batch.txn_id = Some(1);
    for bytes in [mixed, encode(&invalid_batch), Vec::new()] {
        let storage = fixture(&[("1.wal", bytes)], 1024);
        let mut read_ns = 0;
        let file = open_wal_replay_file(&storage, &FsPath::new("wal/1.wal"), &mut read_ns)
            .unwrap()
            .unwrap();

        // Act
        let result = inspect_sealed_wal_file(&*file, &FsPath::new("wal/1.wal"), limits());

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }
}

#[test]
fn should_ignore_fenced_overlap_when_prior_epoch_uses_same_key_and_sequence() {
    // Arrange
    let old = encode(&put("same-key", 7, 1));
    let mut fresh = put("same-key", 7, 2);
    fresh.value = Some(Bytes::from_static(b"fresh"));
    let storage = fixture(&[("1.wal", old), ("2.wal", encode(&fresh))], 1024);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        limits(),
        &mut |_, _| Ok(()),
    )
    .unwrap();

    // Assert
    assert_eq!(stats.stale_records_skipped, 1);
    assert_eq!(
        memtables[&0].get(b"same-key").unwrap(),
        Some(b"fresh".to_vec())
    );
}

#[test]
fn should_reject_conflicting_point_after_its_memtable_was_checkpointed() {
    // Arrange
    let mut records: Vec<_> = (1..=10)
        .map(|seq| put(format!("key-{seq}"), seq, 1))
        .collect();
    let mut duplicate = put("key-1", 1, 1);
    duplicate.value = Some(Bytes::from_static(b"conflicting"));
    records.push(duplicate);
    let storage = fixture(
        &[("1.wal", records.iter().flat_map(encode).collect())],
        1024,
    );
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;
    let config = StreamingReplayLimits {
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
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
        let mut op = put("batch-key", 2, 1);
        op.txn_id = Some(1);
        op.value = Some(Bytes::from_static(value));
        let payload = encoding::encode_txn_batch_payload(1, 1, 3, 1, &[op]).unwrap();
        let mut outer = WalRecord::new(
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            3,
            1,
        );
        outer.txn_id = Some(1);
        outer
    };
    let mut bytes = encode(&make_batch(b"first"));
    for seq in 4..=15 {
        bytes.extend(encode(&put(format!("middle-{seq}"), seq, 1)));
    }
    bytes.extend(encode(&make_batch(b"conflicting")));
    let storage = fixture(&[("1.wal", bytes)], 1024);
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;
    let config = StreamingReplayLimits {
        max_memtable_encoded_bytes: 18 * 1024,
        ..limits()
    };

    // Act
    let result = replay_wal_with_checkpoint(
        &storage,
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
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
    let mut operation = put("large-atomic", 3, 1);
    operation.txn_id = Some(2);
    operation.value = Some(Bytes::from(vec![b'v'; 12 * 1024]));
    let payload = encoding::encode_txn_batch_payload(2, 2, 4, 1, &[operation]).unwrap();
    let mut outer = WalRecord::new(
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(payload),
        4,
        1,
    );
    outer.txn_id = Some(2);
    let mut bytes = encode(&put("seed", 1, 1));
    bytes.extend(encode(&outer));
    let storage = fixture(&[("1.wal", bytes)], 32 * 1024);
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
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        config,
        &mut |tables, stats| {
            assert!(tables[&0].get(b"large-atomic").unwrap().is_none());
            checkpoints.push(stats.max_sequence);
            tables.clear();
            Ok(())
        },
    )
    .unwrap();

    // Assert
    assert_eq!(checkpoints, vec![Some(1)]);
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
    let mut bytes = encode(&put("first", 1, 1));
    bytes.extend_from_slice(&[0xff; 8]);
    let storage = fixture(&[("1.wal", bytes)], limits().max_frame_bytes);
    let path = FsPath::new("wal/1.wal");
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
        .flat_map(|sequence| encode(&put("key", sequence, 1)))
        .collect();
    let storage = fixture(&[("1.wal", bytes)], limits().max_frame_bytes);
    let path = FsPath::new("wal/1.wal");
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
