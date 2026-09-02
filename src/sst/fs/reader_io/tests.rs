use super::*;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenOptions};
use crate::sst::compression::{CompressionAlgo, CompressionPolicy, BLOCK_TRAILER_SIZE};
use crate::sst::traits::{SstFactory, SstReader, SstStateReader};
use std::collections::HashSet;
use std::sync::Mutex;

struct CountingFs {
    inner: crate::io::RealFs,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
    persistent_handles: bool,
}

impl CountingFs {
    fn new(root: &std::path::Path) -> FsResult<Self> {
        Ok(Self {
            inner: crate::io::RealFs::new(root)?,
            reads: Arc::new(Mutex::new(Vec::new())),
            persistent_handles: true,
        })
    }

    fn without_persistent_handles(root: &std::path::Path) -> FsResult<Self> {
        Ok(Self {
            persistent_handles: false,
            ..Self::new(root)?
        })
    }

    fn clear_reads(&self) {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn reads(&self) -> Vec<(u64, u64)> {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct CountingFile<'a> {
    inner: Box<dyn File + 'a>,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl File for CountingFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((offset, len));
        if len > 1024 * 1024 {
            return Err(crate::io::FsError::Corruption(
                "test filesystem refused an oversized read".to_string(),
            ));
        }
        self.inner.read_at(offset, len)
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        self.inner.sync(dur)
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

impl Fs for CountingFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        Ok(Box::new(CountingFile {
            inner: self.inner.open(path, opts)?,
            reads: Arc::clone(&self.reads),
        }))
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        if !self.persistent_handles {
            return Err(FsError::Unsupported(
                "test backend uses path-based SST reads".to_string(),
            ));
        }
        self.inner.open_persistent_handle(path, opts)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
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

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

fn write_unique_key_sst(temp_dir: &tempfile::TempDir, name: &str) -> MidgeResult<()> {
    write_marked_key_sst(temp_dir, name, b'x')
}

fn write_marked_key_sst(temp_dir: &tempfile::TempDir, name: &str, marker: u8) -> MidgeResult<()> {
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    let value = vec![marker; 256];

    for i in 0..96u64 {
        let key = format!("key_{i:04}");
        writer.add_with_meta(key.as_bytes(), Some(&value), i + 1, 0, None)?;
    }

    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))
}

fn write_single_value_sst(
    temp_dir: &tempfile::TempDir,
    name: &str,
    value: &[u8],
) -> MidgeResult<()> {
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(value), 1, 0, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))
}

fn write_keyed_sst(
    temp_dir: &tempfile::TempDir,
    name: &str,
    block_size: usize,
    keys: &[Vec<u8>],
) -> MidgeResult<()> {
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, block_size);
    let mut writer = factory.create()?;
    let value = vec![b'v'; 256];

    for (index, key) in keys.iter().enumerate() {
        writer.add_with_meta(
            key,
            Some(&value),
            u64::try_from(index + 1).unwrap_or(u64::MAX),
            0,
            None,
        )?;
    }

    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))
}

fn structured_keys() -> Vec<Vec<u8>> {
    (0..192)
        .map(|index| format!("tenant/shared/static-segment/{index:04}").into_bytes())
        .collect()
}

fn open_counting_reader(
    temp_dir: &tempfile::TempDir,
    name: &str,
) -> MidgeResult<(Arc<CountingFs>, SstFileIo)> {
    let counting_fs = Arc::new(CountingFs::new(temp_dir.path())?);
    let fs: Arc<dyn Fs> = counting_fs.clone();
    let reader = SstFileIo::open(name, fs)?;
    Ok((counting_fs, reader))
}

fn data_block_reads(reads: &[(u64, u64)], index: &[(Vec<u8>, BlockHandle)]) -> Vec<(u64, u64)> {
    let data_offsets = index
        .iter()
        .map(|(_key, handle)| handle.offset)
        .collect::<HashSet<_>>();
    reads
        .iter()
        .copied()
        .filter(|(offset, _len)| data_offsets.contains(offset))
        .collect()
}

#[test]
fn should_create_new_reader_with_io_fs() {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());

    // Act
    let reader = SstFileIo::new("test.sst", fs);

    // Assert
    assert!(reader.footer.is_none());
}

#[test]
fn should_have_proper_type_safety() {
    // Arrange
    let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());

    // Act
    let reader = SstFileIo::new("test.sst", fs);

    // Assert
    assert!(reader.footer.is_none());
}

#[test]
fn should_require_generation_identity_when_attaching_block_cache() {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());
    let reader = SstFileIo::new("test.sst", fs);
    let cache = Arc::new(crate::sst::cache::BlockCache::new(
        1024,
        1,
        crate::sst::cache::CachePolicyType::Lru,
    ));

    // Act
    let cached = reader.with_block_cache(cache, 42);

    // Assert
    assert_eq!(cached.sst_id, 42);
    assert!(cached.block_cache.is_some());
}

#[test]
fn should_isolate_replaced_sst_cache_entries_by_generation_identity() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let cache = Arc::new(crate::sst::cache::BlockCache::new_default(1024 * 1024));
    let shared_fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    write_single_value_sst(&temp_dir, "replaced.sst", b"old")?;
    let first = SstFileIo::open("replaced.sst", Arc::clone(&shared_fs))?
        .with_block_cache(Arc::clone(&cache), 100);
    assert_eq!(first.get(b"key")?.as_deref(), Some(b"old".as_slice()));

    write_single_value_sst(&temp_dir, "replaced.sst", b"new")?;
    let replacement = SstFileIo::open("replaced.sst", shared_fs)?.with_block_cache(cache, 101);

    // Act
    let value = replacement.get(b"key")?;

    // Assert
    assert_eq!(value.as_deref(), Some(b"new".as_slice()));
    Ok(())
}

#[cfg(unix)]
#[test]
fn should_finish_scan_from_open_handle_when_backing_sst_is_unlinked_mid_scan() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "unlinked-scan.sst")?;
    let reader = Arc::new(SstFileIo::open(
        "unlinked-scan.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);
    let mut scan = reader.state_scan(None, None, false, u64::MAX, 0);
    let first = scan.next().expect("first scan item")?;
    std::fs::remove_file(temp_dir.path().join("unlinked-scan.sst"))?;

    // Act
    let remaining = scan.collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert_eq!(first.0.as_ref(), b"key_0000");
    assert_eq!(remaining.len(), 95);
    assert_eq!(
        remaining.last().map(|(key, _)| key.as_ref()),
        Some(b"key_0095".as_slice())
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn should_finish_scan_from_original_handle_when_sst_path_is_replaced_mid_scan() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_marked_key_sst(&temp_dir, "active.sst", b'o')?;
    write_marked_key_sst(&temp_dir, "replacement.sst", b'n')?;
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let reader = Arc::new(SstFileIo::open("active.sst", Arc::clone(&fs))?);
    let mut scan = reader.state_scan(None, None, false, u64::MAX, 0);
    let first = scan.next().expect("first scan item")?;
    std::fs::rename(
        temp_dir.path().join("replacement.sst"),
        temp_dir.path().join("active.sst"),
    )?;

    // Act
    let remaining = scan.collect::<MidgeResult<Vec<_>>>()?;
    let replacement = SstFileIo::open("active.sst", fs)?;

    // Assert
    let old_value = vec![b'o'; 256];
    let new_value = vec![b'n'; 256];
    assert!(
        matches!(first.1, KeyState::Value(ref value, ..) if value.as_ref() == old_value.as_slice())
    );
    assert_eq!(remaining.len(), 95);
    assert!(remaining.iter().all(|(_, state)| {
        matches!(state, KeyState::Value(value, ..) if value.as_ref() == old_value.as_slice())
    }));
    assert_eq!(
        replacement.get(b"key_0001")?.as_deref(),
        Some(new_value.as_slice())
    );
    Ok(())
}

#[test]
fn should_finish_scan_with_visible_error_when_backing_sst_is_deleted_mid_range_scan(
) -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "deleted-scan.sst")?;
    let fs = Arc::new(CountingFs::without_persistent_handles(temp_dir.path())?);
    let reader = Arc::new(SstFileIo::open("deleted-scan.sst", fs)?);
    let mut scan = reader.state_scan(None, None, false, u64::MAX, 0);
    let first = scan.next().expect("first scan item")?;
    std::fs::remove_file(temp_dir.path().join("deleted-scan.sst"))?;

    // Act
    let first_failure = scan
        .find_map(Result::err)
        .expect("path-based scan must observe deletion at a block transition");
    let replayed_once = scan.next().expect("failed scan replays its error");
    let replayed_twice = scan.next().expect("failed scan remains failed");

    // Assert
    assert_eq!(first.0.as_ref(), b"key_0000");
    assert!(matches!(first_failure, MidgeError::NotFound));
    assert!(matches!(replayed_once, Err(MidgeError::NotFound)));
    assert!(matches!(replayed_twice, Err(MidgeError::NotFound)));
    assert!(matches!(scan.lifecycle, SstScanLifecycle::Failed(_)));
    Ok(())
}

#[test]
fn should_classify_truncated_nonlegacy_sst_as_corruption() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    std::fs::write(temp_dir.path().join("truncated.sst"), [0_u8; 32])?;
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

    // Act
    let Err(error) = SstFileIo::open("truncated.sst", fs) else {
        panic!("truncated SST must not open");
    };

    // Assert
    assert!(matches!(error, MidgeError::Corruption(_)));
    Ok(())
}

#[test]
fn should_reject_legacy_v2_sst_without_falling_back_to_unchecksummed_blocks() -> MidgeResult<()> {
    // Arrange
    let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compatibility/v2_compressed_sst_db/sst");
    let file_name = "000001_00_00000000000000000001.sst";
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(&fixture_dir)?);

    // Act
    let Err(error) = SstFileIo::open(file_name, fs) else {
        panic!("legacy SST must not open in a V4-only build");
    };

    // Assert
    assert!(matches!(error, MidgeError::CompatibilityError(_)));
    Ok(())
}

#[test]
fn should_get_state_at_read_only_candidate_block_when_key_present() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "candidate.sst")?;
    let (counting_fs, reader) = open_counting_reader(&temp_dir, "candidate.sst")?;
    let index = reader.index_entries()?;
    assert!(
        index.len() >= 3,
        "test SST should contain multiple data blocks"
    );

    let target_block_idx = 1;
    let target_handle = index[target_block_idx].1;
    let target_block = reader.read_block(&target_handle)?;
    let entries = reader.scan_block_entries_from_bytes(&target_block)?;
    assert!(
        entries.len() >= 2,
        "target block should contain multiple keys"
    );
    let target_key = entries[1].key.clone();

    counting_fs.clear_reads();

    // Act
    let state = reader.get_state_at(&target_key, u64::MAX)?;

    // Assert
    assert!(matches!(state, KeyState::Value(_, _, _, _)));
    let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
    assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
    Ok(())
}

#[test]
fn should_get_state_at_read_only_candidate_block_when_key_missing() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "candidate-missing.sst")?;
    let (counting_fs, reader) = open_counting_reader(&temp_dir, "candidate-missing.sst")?;
    let index = reader.index_entries()?;
    assert!(
        index.len() >= 3,
        "test SST should contain multiple data blocks"
    );

    let target_block_idx = 1;
    let target_handle = index[target_block_idx].1;
    let target_block = reader.read_block(&target_handle)?;
    let entries = reader.scan_block_entries_from_bytes(&target_block)?;
    assert!(
        entries.len() >= 2,
        "target block should contain multiple keys"
    );
    let block_bloom = reader
        .block_bloom_filter
        .as_ref()
        .expect("writer should persist and reader should load block blooms");
    assert_eq!(block_bloom.num_blocks(), index.len());
    let missing_key = (0..256u16)
        .map(|suffix| {
            let mut key = entries[1].key.clone();
            key.extend_from_slice(format!("-missing-{suffix}").as_bytes());
            key
        })
        .find(|key| {
            block_bloom
                .might_contain_in_block(target_block_idx, key)
                .definitely_not_present()
        })
        .expect("test should find a deterministic bloom-negative key");

    counting_fs.clear_reads();

    // Act
    let state = reader.get_state_at(&missing_key, u64::MAX)?;

    // Assert
    assert_eq!(state, KeyState::Absent);
    let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
    assert!(
        reads.is_empty(),
        "persisted block bloom should reject the in-range miss before data I/O"
    );
    Ok(())
}

#[test]
fn should_select_trie_metadata_for_structured_keys() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = structured_keys();
    write_keyed_sst(&temp_dir, "structured.sst", 4096, &keys)?;

    // Act
    let reader = SstFileIo::open(
        "structured.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;

    // Assert
    assert_eq!(reader.index_kind, IndexKind::Trie);
    assert!(reader.trie_reader.is_some());
    assert_eq!(reader.smallest_key.as_deref(), Some(keys[0].as_slice()));
    assert_eq!(
        reader.largest_key.as_deref(),
        Some(keys[keys.len() - 1].as_slice())
    );
    Ok(())
}

#[test]
fn should_keep_binary_index_metadata_for_small_ssts() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = (0..64)
        .map(|index| format!("random-key-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    write_keyed_sst(&temp_dir, "small.sst", 4096, &keys)?;

    // Act
    let reader = SstFileIo::open(
        "small.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;

    // Assert
    assert_eq!(reader.index_kind, IndexKind::Sparse);
    assert!(reader.trie_reader.is_none());
    Ok(())
}

#[test]
fn should_seek_to_first_key_at_or_after_bound_given_sparse_index_when_reading() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = (0..96)
        .map(|index| format!("random-key-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    write_keyed_sst(&temp_dir, "forward-seek.sst", 4096, &keys)?;
    let reader = Arc::new(SstFileIo::open(
        "forward-seek.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);
    assert_eq!(reader.index_kind, IndexKind::Sparse);
    assert!(
        reader.index_entries()?.len() >= 3,
        "forward seek must cross a real sparse-index boundary"
    );
    let between_keys = b"random-key-0031\0".to_vec();

    // Act
    let first = reader
        .state_scan(Some(between_keys), None, false, u64::MAX, 0)
        .next()
        .expect("bounded forward scan has a row")?;

    // Assert
    assert_eq!(first.0.as_ref(), b"random-key-0032");
    Ok(())
}

#[test]
fn should_return_empty_scan_given_start_bound_after_all_keys_when_reading() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = (0..96)
        .map(|index| format!("random-key-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    write_keyed_sst(&temp_dir, "start-after-all.sst", 4096, &keys)?;
    let reader = Arc::new(SstFileIo::open(
        "start-after-all.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);

    // Act
    let rows = reader
        .state_scan(Some(b"zzzz".to_vec()), None, false, u64::MAX, 0)
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn should_return_empty_scan_given_end_bound_before_all_keys_when_reading() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = (0..96)
        .map(|index| format!("random-key-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    write_keyed_sst(&temp_dir, "end-before-all.sst", 4096, &keys)?;
    let reader = Arc::new(SstFileIo::open(
        "end-before-all.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);

    // Act
    let rows = reader
        .state_scan(None, Some(b"a".to_vec()), false, u64::MAX, 0)
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn should_seek_to_last_key_at_or_before_bound_given_reverse_scan_when_reading() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = (0..96)
        .map(|index| format!("random-key-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    write_keyed_sst(&temp_dir, "reverse-seek.sst", 4096, &keys)?;
    let reader = Arc::new(SstFileIo::open(
        "reverse-seek.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);
    assert_eq!(reader.index_kind, IndexKind::Sparse);
    assert!(
        reader.index_entries()?.len() >= 3,
        "reverse seek must cross a real sparse-index boundary"
    );
    let inclusive_end = b"random-key-0031\0".to_vec();

    // Act
    let mut scan = reader.state_scan(None, Some(inclusive_end), true, u64::MAX, 0);
    let first = scan.next().expect("bounded reverse scan has a row")?;
    let second = scan.next().expect("bounded reverse scan has another row")?;

    // Assert
    assert_eq!(first.0.as_ref(), b"random-key-0031");
    assert_eq!(second.0.as_ref(), b"random-key-0030");
    Ok(())
}

#[test]
fn should_reject_corrupt_sparse_index_given_nonmonotonic_offsets_when_opening() -> MidgeResult<()> {
    // Arrange: fixed no-compression keeps the index payload byte-addressable.
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096)
        .with_compression_policy(CompressionPolicy::Fixed(CompressionAlgo::None));
    let mut writer = factory.create()?;
    let value = vec![b'v'; 256];
    for index in 0..96u64 {
        let key = format!("random-key-{index:04}");
        writer.add_with_meta(key.as_bytes(), Some(&value), index + 1, 0, None)?;
    }
    let path = temp_dir.path().join("nonmonotonic-index.sst");
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let reader = SstFileIo::open(
        "nonmonotonic-index.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;
    assert_eq!(reader.index_kind, IndexKind::Sparse);
    let index = reader.index_entries()?;
    assert!(index.len() >= 2, "test SST must contain multiple blocks");
    let index_handle = reader.footer.as_ref().expect("V4 footer").index_handle;
    let first_data_offset = index[0].1.offset;

    let mut bytes = std::fs::read(&path)?;
    let block_start = usize::try_from(index_handle.offset).expect("index offset fits usize");
    let block_end = block_start
        .checked_add(usize::try_from(index_handle.size).expect("index size fits usize"))
        .expect("index end fits usize");
    let payload_start = block_start + size_of::<u32>();
    let first_key_len = u32::from_le_bytes(
        bytes[payload_start..payload_start + size_of::<u32>()]
            .try_into()
            .expect("first key length"),
    ) as usize;
    let second_entry = payload_start + size_of::<u32>() + first_key_len + 2 * size_of::<u64>();
    let second_key_len = u32::from_le_bytes(
        bytes[second_entry..second_entry + size_of::<u32>()]
            .try_into()
            .expect("second key length"),
    ) as usize;
    let second_offset = second_entry + size_of::<u32>() + second_key_len;
    bytes[second_offset..second_offset + size_of::<u64>()]
        .copy_from_slice(&first_data_offset.to_le_bytes());
    let crc_offset = block_end - size_of::<u32>();
    assert_eq!(
        bytes[crc_offset - 1],
        CompressionAlgo::None.to_u8(),
        "test index must remain uncompressed"
    );
    let crc = crc32c::crc32c(&bytes[payload_start..crc_offset]);
    bytes[crc_offset..block_end].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes)?;

    // Act
    let Err(error) = SstFileIo::open(
        "nonmonotonic-index.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    ) else {
        panic!("nonmonotonic data-block offsets must fail SST open");
    };

    // Assert
    assert!(matches!(error, MidgeError::Corruption(_)));
    assert!(error.to_string().contains("non-overlapping"));
    Ok(())
}

#[test]
fn should_use_trie_accelerator_when_structured_key_is_present() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = structured_keys();
    write_keyed_sst(&temp_dir, "structured-candidate.sst", 4096, &keys)?;
    let (counting_fs, reader) = open_counting_reader(&temp_dir, "structured-candidate.sst")?;
    assert_eq!(reader.index_kind, IndexKind::Trie);
    let index = reader.index_entries()?;
    let target_block_idx = 1;
    let target_handle = index[target_block_idx].1;
    let target_block = reader.read_block(&target_handle)?;
    let entries = reader.scan_block_entries_from_bytes(&target_block)?;
    let target_key = entries[1].key.clone();

    counting_fs.clear_reads();

    // Act
    let state = reader.get_state_at(&target_key, u64::MAX)?;

    // Assert
    assert!(matches!(state, KeyState::Value(_, _, _, _)));
    let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
    assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
    Ok(())
}

#[test]
fn should_skip_range_scan_when_requested_keys_are_outside_persisted_bounds() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = structured_keys();
    write_keyed_sst(&temp_dir, "range-bounds.sst", 4096, &keys)?;
    let (counting_fs, reader) = open_counting_reader(&temp_dir, "range-bounds.sst")?;
    counting_fs.clear_reads();

    // Act
    let rows = reader.scan_range(Some(b"zzz"), Some(b"zzzz"))?;

    // Assert
    assert!(rows.is_empty());
    assert!(counting_fs.reads().is_empty());
    Ok(())
}

#[test]
fn should_fail_open_when_trie_block_is_corrupted() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let keys = structured_keys();
    write_keyed_sst(&temp_dir, "corrupt-trie.sst", 4096, &keys)?;
    let reader = SstFileIo::open(
        "corrupt-trie.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;
    let trie_handle = reader
        .footer
        .as_ref()
        .and_then(|footer| footer.trie_handle)
        .expect("structured SST should persist a trie block");
    let path = temp_dir.path().join("corrupt-trie.sst");
    let mut bytes = std::fs::read(&path)?;
    let corrupt_offset = usize::try_from(trie_handle.offset + 4).unwrap_or(usize::MAX);
    bytes[corrupt_offset] ^= 0xFF;
    std::fs::write(&path, bytes)?;

    // Act
    let Err(error) = SstFileIo::open(
        "corrupt-trie.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    ) else {
        panic!("corrupted trie block should fail to open");
    };

    // Assert
    assert!(matches!(error, MidgeError::Corruption(_)));
    Ok(())
}

#[test]
fn should_get_state_at_return_newest_visible_version_across_duplicate_blocks() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"aaa", Some(&vec![b'a'; 256]), 100, 0, None)?;
    for seq in 1..=32u64 {
        let value = vec![u8::try_from(seq).unwrap_or(u8::MAX); 512];
        writer.add_with_meta(b"dup", Some(&value), seq, 0, None)?;
    }
    writer.add_with_meta(b"zzz", Some(&vec![b'z'; 256]), 100, 0, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("versions.sst"))?;

    let reader = SstFileIo::open(
        "versions.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;
    let index = reader.index_entries()?;
    let duplicate_blocks = index
        .iter()
        .filter(|(first_key, _handle)| first_key.as_slice() == b"dup")
        .count();
    assert!(
        duplicate_blocks >= 2,
        "duplicate versions should span multiple blocks"
    );

    // Act
    let state_at_5 = reader.get_state_at(b"dup", 5)?;
    let latest = reader.get_state_at(b"dup", u64::MAX)?;

    // Assert
    match state_at_5 {
        KeyState::Value(value, seq, _, _) => {
            assert_eq!(seq, 5);
            assert_eq!(value[0], 5);
        }
        other => panic!("expected visible value at seq 5, got {other:?}"),
    }
    match latest {
        KeyState::Value(value, seq, _, _) => {
            assert_eq!(seq, 32);
            assert_eq!(value[0], 32);
        }
        other => panic!("expected latest visible value, got {other:?}"),
    }
    Ok(())
}

#[test]
fn should_stream_raw_versions_in_compaction_order_across_blocks() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"aaa", Some(b"first"), 100, 0, None)?;
    for sequence in 1..=32u64 {
        let value = vec![u8::try_from(sequence).unwrap_or(u8::MAX); 512];
        writer.add_with_meta(b"dup", Some(&value), sequence, 0, None)?;
    }
    writer.add_with_meta(b"zzz", Some(b"last"), 100, 0, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("raw-versions.sst"))?;
    let reader = Box::new(SstFileIo::open(
        "raw-versions.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);

    // Act
    let versions = reader
        .raw_version_cursor(None, None)?
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    assert_eq!(versions.len(), 34);
    assert_eq!(
        versions.first().map(|version| version.key.as_slice()),
        Some(b"aaa".as_slice())
    );
    assert_eq!(
        versions.last().map(|version| version.key.as_slice()),
        Some(b"zzz".as_slice())
    );
    let duplicate_sequences = versions
        .iter()
        .filter(|version| version.key == b"dup")
        .map(|version| version.seq)
        .collect::<Vec<_>>();
    assert_eq!(duplicate_sequences, (1..=32u64).rev().collect::<Vec<_>>());
    Ok(())
}

#[test]
fn should_fail_raw_cursor_before_decoded_block_exceeds_compaction_pool() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "budgeted-raw-cursor.sst")?;
    let path = temp_dir.path().join("budgeted-raw-cursor.sst");
    let reader = Box::new(SstFileIo::open(
        "budgeted-raw-cursor.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?);
    let budget = crate::common::resource_budget::ResourceBudget::new(1024);
    let mut cursor = reader.raw_version_cursor_with_budget(None, None, Some(budget))?;

    // Act
    let result = cursor.next().expect("cursor reports resource failure");

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(
        path.is_file(),
        "resource failure must retain the authoritative SST"
    );
    Ok(())
}

#[test]
fn should_get_state_at_return_newest_visible_version_across_many_trie_blocks() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    for index in 0..128u64 {
        let key = format!("tenant/shared/key/{index:04}");
        writer.add_with_meta(key.as_bytes(), Some(b"filler"), index + 1, 0, None)?;
    }
    let target_key = b"tenant/shared/key/0064-target";
    for sequence in 1..=32u64 {
        let value = vec![u8::try_from(sequence).unwrap_or(u8::MAX); 8192];
        writer.add_with_meta(target_key, Some(&value), sequence + 1_000, 0, None)?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("trie-versions.sst"))?;

    let reader = SstFileIo::open(
        "trie-versions.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;
    let index = reader.index_entries()?;
    let duplicate_blocks = index
        .iter()
        .filter(|(first_key, _handle)| first_key.as_slice() == target_key)
        .count();
    assert_eq!(reader.index_kind, IndexKind::Trie);
    assert!(
        duplicate_blocks >= 3,
        "duplicate versions should span at least three trie-indexed blocks"
    );

    // Act
    let latest = reader.get_state_at(target_key, u64::MAX)?;

    // Assert
    match latest {
        KeyState::Value(value, sequence, _, _) => {
            assert_eq!(sequence, 1_032);
            assert_eq!(value[0], 32);
        }
        other => panic!("expected latest visible trie value, got {other:?}"),
    }
    Ok(())
}

#[test]
fn should_preserve_tombstone_ttl_semantics_when_get_state_at_reads() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"dead", Some(b"old"), 4, 0, None)?;
    writer.add_with_meta(b"dead", None, 9, 2, None)?;
    writer.add_with_meta(b"ttl", Some(b"expired"), 11, 0, Some(1))?;
    crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("state.sst"))?;
    let reader = SstFileIo::open(
        "state.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;

    // Act
    let old_dead = reader.get_state_at(b"dead", 4)?;
    let deleted_dead = reader.get_state_at(b"dead", u64::MAX)?;
    let expired = reader.get_state_at(b"ttl", u64::MAX)?;
    let current_expired = reader.get_state(b"ttl")?;
    let direct_expired = reader.get(b"ttl")?;
    let direct_rows = reader.scan_range(None, None)?;

    // Assert
    assert!(matches!(old_dead, KeyState::Value(_, 4, _, _)));
    assert_eq!(deleted_dead, KeyState::Tombstone(9));
    assert_eq!(expired, KeyState::Tombstone(11));
    assert_eq!(current_expired, KeyState::Tombstone(11));
    assert_eq!(direct_expired, None);
    assert!(
        direct_rows.is_empty(),
        "expired and deleted states must mask scans"
    );
    Ok(())
}

#[test]
fn should_reject_current_format_block_with_crc_mismatch() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"crc-key", Some(b"crc-value"), 7, 0, None)?;
    let path = temp_dir.path().join("crc.sst");
    crate::sst::fs::finish_writer_to_path(writer, &path)?;

    {
        use std::io::{Read, Seek, Write};

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        file.seek(std::io::SeekFrom::Start(4))?;
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte)?;
        file.seek(std::io::SeekFrom::Start(4))?;
        file.write_all(&[byte[0] ^ 0x01])?;
        file.sync_all()?;
    }

    let reader = SstFileIo::open(
        "crc.sst",
        Arc::new(crate::io::RealFs::new(temp_dir.path())?),
    )?;

    // Act
    let error = reader
        .get_state_at(b"crc-key", u64::MAX)
        .expect_err("current-format SST blocks must enforce CRC");

    // Assert
    assert!(
        error.to_string().contains("CRC32C mismatch"),
        "expected CRC mismatch error, got {error}"
    );
    Ok(())
}

#[test]
fn should_report_corruption_given_shipping_codec_payload_when_reading_through_full_sst_path(
) -> MidgeResult<()> {
    // Arrange
    let pattern = b"account=0042|region=east|status=active|segment=business|";
    let value = pattern
        .iter()
        .copied()
        .cycle()
        .take(16 * 1024)
        .collect::<Vec<_>>();
    for algorithm in [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd3,
        CompressionAlgo::Zstd9,
    ] {
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096)
            .with_compression_policy(CompressionPolicy::Fixed(algorithm));
        let mut writer = factory.create()?;
        writer.add_with_meta(b"codec-key", Some(&value), 1, 0, None)?;
        let path = temp_dir.path().join("codec.sst");
        crate::sst::fs::finish_writer_to_path(writer, &path)?;
        let reader = SstFileIo::open("codec.sst", Arc::clone(&fs))?;
        let (_, handle) = reader
            .index_entries()?
            .first()
            .cloned()
            .expect("persisted SST has one data handle");
        let mut bytes = std::fs::read(&path)?;
        let data_start = usize::try_from(handle.offset).expect("data offset fits usize");
        let data_size = usize::try_from(handle.size).expect("data size fits usize");
        let data_end = data_start + data_size;
        let payload_start = data_start + 4;
        let algorithm_offset = data_end - BLOCK_TRAILER_SIZE;
        let crc_offset = data_end - size_of::<u32>();
        assert_eq!(bytes[algorithm_offset], algorithm.to_u8());
        match algorithm {
            CompressionAlgo::Lz4 => {
                bytes[payload_start..payload_start + 4]
                    .copy_from_slice(&(64 * 1024 * 1024_u32 + 1).to_le_bytes());
            }
            CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => bytes[payload_start] ^= 0xff,
            CompressionAlgo::None => panic!("test requires a genuinely compressed block"),
        }
        let crc = crc32c::crc32c(&bytes[payload_start..crc_offset]);
        bytes[crc_offset..data_end].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, bytes)?;

        // Act
        let point_error = SstFileIo::open("codec.sst", Arc::clone(&fs))?
            .get(b"codec-key")
            .expect_err("point read must surface compressed payload corruption");
        let scan_error = SstFileIo::open("codec.sst", Arc::clone(&fs))?
            .scan_range(None, None)
            .expect_err("range read must surface compressed payload corruption");
        let verify_error = SstFileIo::open("codec.sst", Arc::clone(&fs))?
            .verify_all_blocks()
            .expect_err("verification must surface compressed payload corruption");

        // Assert
        for error in [point_error, scan_error, verify_error] {
            assert!(matches!(error, MidgeError::Corruption(_)));
            assert!(
                !error.to_string().contains("CRC32C mismatch"),
                "{algorithm:?} must reach the codec decoder: {error}"
            );
        }
    }
    Ok(())
}

#[test]
fn should_reject_oversized_block_handle_before_issuing_filesystem_read() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "oversized-handle.sst")?;
    let (counting_fs, reader) = open_counting_reader(&temp_dir, "oversized-handle.sst")?;
    counting_fs.clear_reads();

    // Act
    let result = reader.read_block(&BlockHandle::new(0, u64::MAX));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert!(
        counting_fs.reads().is_empty(),
        "corrupt handles must be rejected before a pre-read allocation is requested"
    );
    Ok(())
}

#[test]
fn should_reject_out_of_order_readahead_handles_without_panicking() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    write_unique_key_sst(&temp_dir, "reversed-handles.sst")?;
    let (_counting_fs, reader) = open_counting_reader(&temp_dir, "reversed-handles.sst")?;
    let index = reader.index_entries()?;
    assert!(index.len() >= 2);
    let handles = [index[1].1, index[0].1];

    // Act
    let result = reader.read_blocks_contiguous(&handles);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_reject_unreferenced_gap_between_v4_sst_blocks() {
    // Arrange
    let mut reader = SstFileIo::new("gap.sst", Arc::new(crate::io::MockFs::new()));
    reader.block_region_end = 30;
    reader.footer = Some(Footer::new(
        BlockHandle::new(0, 9),
        BlockHandle::new(20, 10),
    ));

    // Act
    let result = reader.validate_nonoverlapping_references(&[]);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
}
