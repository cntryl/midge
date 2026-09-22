use super::*;
use crate::io::traits::{DirEntry, Durability, File, FsError, FsResult, Metadata, OpenOptions};
use crate::io::FsPath;
use crate::types::{EntryType, KeyState};

/// Records every staging operation an SST publication performs and can
/// fail the parent-directory sync, so tests observe that persistence runs
/// on the filesystem the factory was injected with.
struct RecordingFile<'a> {
    name: String,
    inner: Box<dyn File + 'a>,
    events: Arc<parking_lot::Mutex<Vec<String>>>,
}

impl File for RecordingFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
        self.inner.read_at(offset, len)
    }

    fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)?;
        self.events.lock().push(format!("write {}", self.name));
        Ok(())
    }

    fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)?;
        self.events.lock().push(format!("file sync {}", self.name));
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

struct RecordingFs {
    inner: crate::io::RealFs,
    events: Arc<parking_lot::Mutex<Vec<String>>>,
    fail_directory_sync: bool,
}

impl RecordingFs {
    fn new(root: &Path, fail_directory_sync: bool) -> MidgeResult<Self> {
        Ok(Self {
            inner: crate::io::RealFs::new(root).map_err(crate::common::MidgeError::from)?,
            events: Arc::new(parking_lot::Mutex::new(Vec::new())),
            fail_directory_sync,
        })
    }
}

impl Fs for RecordingFs {
    fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.events.lock().push(format!("open {}", path.0));
        Ok(Box::new(RecordingFile {
            name: path.0.clone(),
            inner: self.inner.open(path, options)?,
            events: Arc::clone(&self.events),
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.events.lock().push(format!("remove {}", path.0));
        self.inner.remove_file(path)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.events.lock().push(format!("sync_dir {}", path.0));
        if self.fail_directory_sync {
            return Err(FsError::Unavailable(
                "injected directory sync failure".to_string(),
            ));
        }
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)?;
        self.events
            .lock()
            .push(format!("rename {} -> {}", from.0, to.0));
        Ok(())
    }
}

#[test]
fn should_route_sst_staging_sync_through_injected_fs_when_finishing_writer() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let fs = Arc::new(RecordingFs::new(directory.path(), true)?);
    let events = Arc::clone(&fs.events);
    let factory = FsSstFactoryIo::new(Arc::clone(&fs) as Arc<dyn Fs>, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 1, EntryType::Put, None)?;

    // Act
    let result =
        crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("routed.sst"));

    // Assert
    let error = result.expect_err("injected directory sync failure must surface");
    assert!(
        format!("{error}").contains("injected directory sync failure"),
        "unexpected error: {error}"
    );
    let events = events.lock().clone();
    assert!(
        events.contains(&"write routed.sst.tmp".to_string()),
        "{events:?}"
    );
    assert!(
        events.contains(&"file sync routed.sst.tmp".to_string()),
        "{events:?}"
    );
    assert!(
        events.contains(&"rename routed.sst.tmp -> routed.sst".to_string()),
        "{events:?}"
    );
    assert!(events.contains(&"sync_dir .".to_string()), "{events:?}");
    Ok(())
}

#[test]
fn should_route_streaming_sst_staging_through_injected_fs_when_finishing_flush_writer(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let fs = Arc::new(RecordingFs::new(directory.path(), false)?);
    let events = Arc::clone(&fs.events);
    let factory = FsSstFactoryIo::new(Arc::clone(&fs) as Arc<dyn Fs>, 4096)
        .with_compaction_scratch_directory(directory.path().join("scratch"));
    let budget = crate::common::resource_budget::ResourceBudget::new(4 * 1024 * 1024);
    let mut writer = factory.create_for_flush(budget)?;
    for sequence in 0_u64..64 {
        writer.add_sorted_with_meta(
            &sequence.to_be_bytes(),
            Some(b"value"),
            sequence,
            EntryType::Put,
            None,
        )?;
    }

    // Act
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("streamed.sst"))?;

    // Assert
    let events = events.lock().clone();
    assert!(
        events.contains(&"file sync streamed.sst.tmp".to_string()),
        "{events:?}"
    );
    assert!(
        events.contains(&"rename streamed.sst.tmp -> streamed.sst".to_string()),
        "{events:?}"
    );
    assert!(events.contains(&"sync_dir .".to_string()), "{events:?}");
    assert!(directory.path().join("streamed.sst").exists());
    Ok(())
}

#[test]
fn should_produce_identical_bytes_when_same_entries_written_via_create_and_create_for_flush(
) -> MidgeResult<()> {
    // Arrange
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096)
        .with_compression_policy(CompressionPolicy::Fixed(crate::codec::CompressionAlgo::Lz4));
    let entries = (0_u64..192)
        .map(|index| {
            let key = format!("account/customer/region/{index:04}").into_bytes();
            let value = if index % 17 == 0 {
                None
            } else {
                Some(format!("value-{index:04}").into_bytes())
            };
            let entry_type = if value.is_some() {
                EntryType::Put
            } else {
                EntryType::Delete
            };
            let expiration = (index % 11 == 0).then_some(4_000_000_000_000 + index);
            (key, value, index, entry_type, expiration)
        })
        .collect::<Vec<_>>();
    let mut buffered = factory.create()?;
    let mut flush = factory.create_for_flush(
        crate::common::resource_budget::ResourceBudget::new(4 * 1024 * 1024),
    )?;

    // Act
    for (key, value, sequence, entry_type, expiration) in entries.iter().rev() {
        buffered.add_with_meta(key, value.as_deref(), *sequence, *entry_type, *expiration)?;
    }
    for (key, value, sequence, entry_type, expiration) in &entries {
        flush.add_sorted_with_meta(key, value.as_deref(), *sequence, *entry_type, *expiration)?;
    }
    for writer in [&mut buffered, &mut flush] {
        writer.add_range_tombstone(
            b"account/customer/region/0020",
            b"account/customer/region/0030",
            400,
        )?;
        writer.add_range_tombstone(
            b"account/customer/region/0150",
            b"account/customer/region/0160",
            401,
        )?;
    }
    let buffered_bytes = buffered.finish_bytes()?;
    let flush_bytes = flush.finish_bytes()?;

    // Assert
    assert_eq!(buffered_bytes, flush_bytes);
    Ok(())
}

#[test]
fn should_keep_sst_encoding_policy_in_the_shared_block_pipeline() {
    // Arrange
    let writer = include_str!("../factory_io.rs");
    let pipeline = include_str!("pipeline.rs");
    let sink = include_str!("sink.rs");

    // Act
    let legacy_streaming_helpers = [
        "append_block_to_stream",
        "flush_streaming_current_block",
        "append_range_tombstone_block_to_stream",
        "append_trie_block_to_stream",
        "append_metadata_index_and_footer_to_stream",
    ];

    // Assert
    assert!(pipeline.contains("fn append_block<S: BlockSink>"));
    assert!(pipeline.contains("fn append_metadata_index_and_footer<S: BlockSink>"));
    assert!(!pipeline.contains("fn finalize_data_blocks"));
    assert!(legacy_streaming_helpers
        .iter()
        .all(|helper| !writer.contains(helper) && !pipeline.contains(helper)));
    assert!(!sink.contains("CompressionPolicy"));
    assert!(!sink.contains("encode_readable_block"));
}

#[test]
fn should_reject_sst_target_when_path_escapes_injected_filesystem_root() -> MidgeResult<()> {
    // Arrange
    let root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(root.path())?), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 1, EntryType::Put, None)?;

    // Act
    let result = crate::sst::fs::finish_writer_to_path(writer, &outside.path().join("escaped.sst"));

    // Assert
    assert!(
        matches!(&result, Err(crate::common::MidgeError::Internal(message))
            if message.contains("outside the filesystem root")),
        "unexpected result: {result:?}"
    );
    assert_eq!(
        result
            .expect_err("escaping target must be rejected")
            .severity(),
        crate::common::Severity::Defect,
        "a target outside the root is an engine fault, not a caller fault"
    );
    assert!(!outside.path().join("escaped.sst").exists());
    assert!(!root.path().join("escaped.sst").exists());
    Ok(())
}

#[test]
fn should_publish_sst_into_injected_mock_filesystem_when_finishing_writer() -> MidgeResult<()> {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());
    let factory = FsSstFactoryIo::new(Arc::clone(&fs) as Arc<dyn Fs>, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 1, EntryType::Put, None)?;

    // Act
    crate::sst::fs::finish_writer_to_path(writer, Path::new("mocked.sst"))?;

    // Assert
    assert!(fs
        .exists(&FsPath::new("mocked.sst"))
        .map_err(crate::common::MidgeError::from)?);
    assert!(!fs
        .exists(&FsPath::new("mocked.sst.tmp"))
        .map_err(crate::common::MidgeError::from)?);
    let reader = super::super::SstFileIo::open("mocked.sst", Arc::clone(&fs) as Arc<dyn Fs>)?;
    assert_eq!(
        crate::sst::SstReader::get(&reader, b"key")?.as_deref(),
        Some(b"value".as_slice())
    );
    Ok(())
}

#[test]
fn should_stream_flush_larger_than_its_shared_buffer_allowance() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let factory = FsSstFactoryIo::new(
        Arc::new(crate::io::RealFs::new(directory.path())?),
        64 * 1024,
    )
    .with_compaction_scratch_directory(directory.path().to_path_buf())
    .with_compression_policy(CompressionPolicy::Fixed(
        crate::codec::CompressionAlgo::None,
    ));
    let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
    let mut writer = factory.create_for_flush(budget.clone())?;
    let value = vec![7; 16 * 1024];
    let output = directory.path().join("large.sst");

    // Act
    for sequence in 0_u64..1024 {
        writer.add_sorted_with_meta(
            &sequence.to_be_bytes(),
            Some(&value),
            sequence,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &output)?;

    // Assert
    assert!(std::fs::metadata(&output)?.len() > 16 * 1024 * 1024);
    assert!(budget.peak() > 0 && budget.peak() <= budget.limit());
    assert_eq!(budget.used(), 0);
    assert!(factory.compaction_scratch_cleanup_verified());
    let reader = super::super::SstFileIo::open_with_real_fs(&output)?;
    for sequence in 0_u64..1024 {
        assert_eq!(
            crate::sst::SstReader::get(&reader, &sequence.to_be_bytes())?.as_deref(),
            Some(value.as_slice())
        );
    }
    Ok(())
}

#[test]
fn should_reject_flush_entry_when_shared_writer_memory_is_exhausted() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096)
        .with_compaction_scratch_directory(directory.path().to_path_buf());
    let budget = crate::common::resource_budget::ResourceBudget::new(0);
    let mut writer = factory.create_for_flush(budget.clone())?;

    // Act
    let result = writer.add_sorted_with_meta(b"key", Some(b"value"), 1, EntryType::Put, None);
    drop(writer);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::ResourceLimit(_))
    ));
    assert_eq!(budget.used(), 0);
    assert!(factory.compaction_scratch_cleanup_verified());
    Ok(())
}

#[test]
fn should_release_final_block_reservations_before_reserving_finalization_workspace(
) -> MidgeResult<()> {
    // Arrange
    const ENTRY_COUNT: usize = 8;
    let first_key = b"k0";
    let value = b"v";
    let entry_reservations = ENTRY_COUNT
        * (std::mem::size_of::<PendingEntry>() + first_key.len() * 4 + value.len() + 64);
    let index_entry_bytes =
        std::mem::size_of::<(Vec<u8>, crate::sst::types::BlockHandle)>() + first_key.len();
    let persistent_metadata_bytes = index_entry_bytes + ENTRY_COUNT * 16;
    let finalization_bytes = index_entry_bytes * 4 + 16 * 1024;
    let budget = crate::common::resource_budget::ResourceBudget::new(
        finalization_bytes + persistent_metadata_bytes,
    );
    assert!(
        16 * 1024 + entry_reservations > budget.limit(),
        "the fixture must reject if finalization overlaps retained entries"
    );
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create_for_flush(budget.clone())?;
    for index in 0..ENTRY_COUNT {
        let key = format!("k{index}");
        writer.add_sorted_with_meta(
            key.as_bytes(),
            Some(value),
            u64::try_from(index).expect("small fixture index"),
            EntryType::Put,
            None,
        )?;
    }

    // Act
    let bytes = writer.finish_bytes()?;

    // Assert
    assert!(!bytes.is_empty());
    assert_eq!(budget.used(), 0);
    assert!(budget.peak() <= budget.limit());
    Ok(())
}

#[test]
fn should_track_compaction_scratch_in_recoverable_directory_until_confirmed_cleanup(
) -> MidgeResult<()> {
    for cleanup_fails in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let scratch_directory = directory.path().join("sst/.flush-staging");
        let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096)
            .with_compaction_scratch_directory(scratch_directory.clone());
        let writer = factory.create_for_compaction(
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )?;
        assert!(!factory.compaction_scratch_cleanup_verified());
        let scratch_path = std::fs::read_dir(&scratch_directory)?
            .next()
            .expect("scratch file")?
            .path();
        if cleanup_fails {
            std::fs::remove_file(&scratch_path)?;
            std::fs::create_dir(&scratch_path)?;
        }
        // Act
        drop(writer);
        // Assert
        assert_eq!(
            factory.compaction_scratch_cleanup_verified(),
            !cleanup_fails
        );
        assert_eq!(scratch_path.exists(), cleanup_fails);
    }
    Ok(())
}

#[test]
fn should_release_compaction_reservations_when_legacy_entry_exceeds_budget() -> MidgeResult<()> {
    // Arrange
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
    let mut writer = factory.create_for_compaction(budget.clone())?;
    let legacy_value = vec![b'v'; crate::codec::MAX_DECOMPRESSED_BLOCK_SIZE];
    // Act
    let result =
        writer.add_sorted_with_meta(b"legacy", Some(&legacy_value), 7, EntryType::Put, None);
    drop(writer);
    // Assert
    assert!(
        matches!(result, Err(crate::MidgeError::ResourceLimit(message)) if message.contains("SST current block entry"))
    );
    assert!(budget
        .reserve(budget.limit(), "released writer budget")
        .is_ok());
    Ok(())
}

#[test]
fn should_reject_unrepresentable_compressed_block_length_before_prefix_encoding() {
    // Arrange
    let too_large = usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX);

    // Act
    let result = FsSstWriter::checked_block_payload_len(too_large);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::ResourceLimit(_))
    ));
}

#[test]
fn should_bound_final_file_size_when_point_indexes_and_compression_are_present() -> MidgeResult<()>
{
    // Arrange
    use crate::codec::CompressionAlgo;
    for algorithm in [
        CompressionAlgo::None,
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd3,
    ] {
        for streaming in [false, true] {
            let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096)
                .with_compression_policy(CompressionPolicy::Fixed(algorithm));
            let mut writer = factory.create()?;
            for index in 0..256_u64 {
                let key = format!("structured-prefix-{index:020}");
                let value = vec![u8::try_from(index).expect("small fixture index"); 256];
                let predicted = writer
                    .encoded_size_upper_bound_after_sorted_entry(key.as_bytes(), Some(&value))
                    .expect("next entry bound");
                if streaming {
                    writer.add_sorted_with_meta(
                        key.as_bytes(),
                        Some(&value),
                        index,
                        EntryType::Put,
                        Some(u64::MAX),
                    )?;
                } else {
                    writer.add_with_meta(
                        key.as_bytes(),
                        Some(&value),
                        index,
                        EntryType::Put,
                        Some(u64::MAX),
                    )?;
                }
                assert!(
                    predicted
                        >= writer
                            .encoded_size_upper_bound()
                            .expect("current file bound")
                );
            }

            // Act
            let bound = writer
                .encoded_size_upper_bound()
                .expect("filesystem writer bound");
            let bytes = writer.finish_bytes()?;

            // Assert
            assert!(
                bound >= bytes.len(),
                "bound {bound} omitted {} encoded bytes",
                bytes.len()
            );
        }
    }
    Ok(())
}

#[test]
fn should_bound_encoded_output_when_range_tombstones_dominate_the_sst() -> MidgeResult<()> {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());
    let factory = FsSstFactoryIo::new(fs, 4096).with_compression_policy(CompressionPolicy::Fixed(
        crate::codec::CompressionAlgo::None,
    ));
    for streaming in [false, true] {
        let mut writer = factory.create()?;
        if streaming {
            writer.add_sorted_with_meta(b"point", Some(b"value"), 1, EntryType::Put, None)?;
        }
        for index in 0..128_u32 {
            let mut start = vec![b'a'; 512];
            start[..4].copy_from_slice(&index.to_be_bytes());
            let mut end = start.clone();
            end.push(b'z');
            let predicted = writer
                .encoded_size_upper_bound()
                .expect("current file bound")
                .saturating_add(
                    writer
                        .additional_range_tombstone_size_upper_bound(&start, &end)
                        .expect("next range bound"),
                );
            writer.add_range_tombstone(&start, &end, u64::from(index) + 1)?;
            assert!(
                predicted
                    >= writer
                        .encoded_size_upper_bound()
                        .expect("current file bound")
            );
        }

        // Act
        let bound = writer
            .encoded_size_upper_bound()
            .expect("filesystem writer bound");
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(
            bound >= bytes.len(),
            "bound {bound} omitted {} encoded bytes",
            bytes.len()
        );
    }
    Ok(())
}

#[test]
fn should_create_factory_with_mock_fs() {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());

    // Act
    let factory = FsSstFactoryIo::new(fs, 4096);

    // Assert
    assert_eq!(factory.block_size, 4096);
}

#[test]
fn should_create_factory_with_real_fs() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

    // Act
    let factory = FsSstFactoryIo::new(fs, 4096);

    // Assert
    assert_eq!(factory.block_size, 4096);
    Ok(())
}

#[test]
fn should_support_method_chaining() {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());

    // Act
    let factory = FsSstFactoryIo::new(fs, 4096).with_block_size(8192);

    // Assert
    assert_eq!(factory.block_size, 8192);
}

#[test]
fn should_roundtrip_stateful_entries_when_sst_contains_range_tombstones() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = FsSstFactoryIo::new(fs, 4096);
    let path = temp_dir.path().join("stateful.sst");

    let mut writer = factory.create()?;
    writer.add_with_meta(
        b"alpha",
        Some(b"value-a"),
        10,
        EntryType::Put,
        Some(4_000_000_000_000),
    )?;
    writer.add_with_meta(b"alpha", None, 9, EntryType::Delete, None)?;
    writer.add_with_meta(b"beta", Some(b"value-b"), 8, EntryType::Insert, None)?;
    writer.add_range_tombstone(b"cat", b"cow", 7)?;
    crate::sst::fs::finish_writer_to_path(writer, &path)?;

    // Act
    let reader = factory.open(std::path::Path::new("stateful.sst"))?;
    let states = reader.scan_range_state(None, None)?;

    // Assert
    assert_eq!(states.len(), 3);
    match &states[0].1 {
        KeyState::Value(value, seq, expiration, op_type) => {
            assert_eq!(states[0].0.as_ref(), b"alpha");
            assert_eq!(value.as_ref(), b"value-a");
            assert_eq!(*seq, 10);
            assert_eq!(*expiration, Some(4_000_000_000_000));
            assert_eq!(*op_type, EntryType::Put);
        }
        other => panic!("expected value state, got {other:?}"),
    }

    match &states[1].1 {
        KeyState::Tombstone(seq) => {
            assert_eq!(states[1].0.as_ref(), b"alpha");
            assert_eq!(*seq, 9);
        }
        other => panic!("expected tombstone state, got {other:?}"),
    }

    assert_eq!(reader.range_tombstones().len(), 1);
    assert_eq!(reader.range_tombstones()[0].start, b"cat".to_vec());
    assert_eq!(reader.range_tombstones()[0].end, b"cow".to_vec());

    Ok(())
}

#[test]
fn should_roundtrip_large_key_when_sst_entry_key_delta_exceeds_inline_limit() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = FsSstFactoryIo::new(fs, 4096);
    let path = temp_dir.path().join("large-key.sst");
    let oversized_key = vec![b'k'; 65_536];

    // Act
    let mut writer = factory.create()?;
    writer.add_with_meta(&oversized_key, Some(b"value"), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let reader = factory.open(std::path::Path::new("large-key.sst"))?;
    let states = reader.scan_range_state(None, None)?;

    // Assert
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].0.as_ref(), oversized_key.as_slice());
    match &states[0].1 {
        KeyState::Value(value, sequence, expiration, op_type) => {
            assert_eq!(value.as_ref(), b"value");
            assert_eq!(*sequence, 1);
            assert_eq!(*expiration, None);
            assert_eq!(*op_type, EntryType::Put);
        }
        other => panic!("expected value state, got {other:?}"),
    }
    Ok(())
}

#[test]
fn should_roundtrip_empty_value_when_sst_entry_is_put() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = FsSstFactoryIo::new(fs, 4096);
    let path = temp_dir.path().join("empty-value.sst");

    // Act
    let mut writer = factory.create()?;
    writer.add_with_meta(b"empty", Some(b""), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let reader = factory.open(std::path::Path::new("empty-value.sst"))?;
    let states = reader.scan_range_state(None, None)?;

    // Assert
    assert_eq!(states.len(), 1);
    match &states[0].1 {
        KeyState::Value(value, sequence, expiration, op_type) => {
            assert_eq!(states[0].0.as_ref(), b"empty");
            assert_eq!(value.as_ref(), b"");
            assert_eq!(*sequence, 1);
            assert_eq!(*expiration, None);
            assert_eq!(*op_type, EntryType::Put);
        }
        other => panic!("expected empty value state, got {other:?}"),
    }
    Ok(())
}

#[test]
fn should_roundtrip_multiple_blocks_when_sorted_compaction_spills() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = FsSstFactoryIo::new(fs, 4096);
    let path = temp_dir.path().join("streamed.sst");
    let mut writer = factory.create()?;

    // Act
    for index in 0..2_000 {
        let key = format!("key-{index:06}");
        writer.add_sorted_with_meta(key.as_bytes(), Some(b"value"), index, EntryType::Put, None)?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let reader = factory.open(std::path::Path::new("streamed.sst"))?;
    let states = reader.scan_range_state(None, None)?;

    // Assert
    assert_eq!(states.len(), 2_000);
    assert_eq!(
        states.first().map(|(key, _)| key.as_ref()),
        Some(&b"key-000000"[..])
    );
    assert_eq!(
        states.last().map(|(key, _)| key.as_ref()),
        Some(&b"key-001999"[..])
    );
    Ok(())
}

#[test]
fn should_reject_out_of_order_entries_on_sorted_writer_path() -> MidgeResult<()> {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());
    let factory = FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_sorted_with_meta(b"b", Some(b"value"), 2, EntryType::Put, None)?;

    // Act
    let result = writer.add_sorted_with_meta(b"a", Some(b"value"), 1, EntryType::Put, None);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::InvalidArgument(_))
    ));
    Ok(())
}

#[test]
fn should_accept_writable_op_types_when_adding_sst_entries() -> MidgeResult<()> {
    // Arrange
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create()?;

    // Act
    writer.add_with_meta(b"a", Some(b"put"), 3, EntryType::Put, None)?;
    writer.add_with_meta(b"b", Some(b"insert"), 2, EntryType::Insert, None)?;
    writer.add_with_meta(b"c", None, 1, EntryType::Delete, None)?;

    // Assert
    assert!(!writer.finish_bytes()?.is_empty());
    Ok(())
}

#[test]
fn should_reject_merge_entry_when_adding_sst_entries() -> MidgeResult<()> {
    // Arrange
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut unsorted = factory.create()?;
    let mut sorted = factory.create()?;

    // Act
    let unsorted_result = unsorted.add_with_meta(b"key", Some(b"value"), 1, EntryType::Merge, None);
    let sorted_result =
        sorted.add_sorted_with_meta(b"key", Some(b"value"), 1, EntryType::Merge, None);

    // Assert
    // The writer must refuse at the call, not buffer the entry and fail later
    // when the block spills.
    assert!(
        matches!(
            unsorted_result,
            Err(crate::common::MidgeError::InvalidArgument(_))
        ),
        "add_with_meta must reject Merge, got {unsorted_result:?}"
    );
    assert!(
        matches!(
            sorted_result,
            Err(crate::common::MidgeError::InvalidArgument(_))
        ),
        "add_sorted_with_meta must reject Merge, got {sorted_result:?}"
    );
    Ok(())
}

#[test]
fn should_reject_merge_entry_when_encoding_pending_sst_entry() {
    // Arrange
    let entry = PendingEntry {
        key: b"key".to_vec(),
        value: Some(b"value".to_vec()),
        sequence: 1,
        op_type: EntryType::Merge,
        expiration: None,
    };

    // Act
    let result = FsSstWriter::encode_pending_entry(b"", &entry);

    // Assert
    assert!(
        matches!(result, Err(crate::common::MidgeError::InvalidArgument(_))),
        "encoder must never emit EntryType::Merge, got {result:?}"
    );
}
