use super::*;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsPath, FsResult, OpenOptions};
use crate::sst::traits::{SstFactory, SstReader, SstStateReader};
use std::collections::HashSet;
use std::sync::Mutex;

struct CountingFs {
    inner: crate::io::RealFs,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl CountingFs {
    fn new(root: &std::path::Path) -> FsResult<Self> {
        Ok(Self {
            inner: crate::io::RealFs::new(root)?,
            reads: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn clear_reads(&self) {
        self.reads.lock().expect("read log lock").clear();
    }

    fn reads(&self) -> Vec<(u64, u64)> {
        self.reads.lock().expect("read log lock").clone()
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
            .expect("read log lock")
            .push((offset, len));
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
    let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    let value = vec![b'x'; 256];

    for i in 0..96u64 {
        let key = format!("key_{i:04}");
        writer.add_with_meta(key.as_bytes(), Some(&value), i + 1, 0, None)?;
    }

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
fn should_chain_with_sst_id() {
    // Arrange
    let fs = Arc::new(crate::io::MockFs::new());
    let reader = SstFileIo::new("test.sst", fs);

    // Act
    let with_id = reader.with_sst_id(42);

    // Assert
    assert_eq!(with_id.sst_id, 42);
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
    let mut missing_key = entries[1].key.clone();
    missing_key.push(b'a');

    counting_fs.clear_reads();

    // Act
    let state = reader.get_state_at(&missing_key, u64::MAX)?;

    // Assert
    assert_eq!(state, KeyState::Absent);
    let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
    assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
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
fn should_keep_sparse_metadata_for_small_ssts() -> MidgeResult<()> {
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
