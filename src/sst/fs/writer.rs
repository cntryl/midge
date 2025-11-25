use crate::common::codec::CompressionType;
use crate::error::MidgeResult;
use crate::sst::format::{
    Block, BlockHandle, BlockType, DataBlockBuilder, Footer, IndexBlockBuilder,
};
use crate::sst::traits::RangeTombstone;
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::{Path, PathBuf};

/// Streaming filesystem-backed DynSstWriter.
///
/// Writes encoded blocks directly to a temporary file as they are produced
/// to avoid keeping the full SST image in memory.
pub struct FsDynWriter {
    file: std::fs::File,
    temp_path: PathBuf,
    block_size: usize,
    compression: CompressionType,
    use_internal_keys: bool,

    // current block builder
    cur_block: DataBlockBuilder,
    last_key_in_block: Option<Vec<u8>>,

    // collected metadata for index/bloom
    offsets: Vec<(Vec<u8>, BlockHandle)>,
    index: IndexBlockBuilder,
    bloom_builder: crate::sst::bloom::BloomFilterBuilder,
    range_tombstones: Vec<RangeTombstone>,

    // current file offset
    offset: u64,
    // Optional test hooks for instrumentation/fault-injection
    test_hooks: Option<crate::common::test_hooks::TestHooks>,
}

impl FsDynWriter {
    pub fn new(
        temp_dir: &Path,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
    ) -> MidgeResult<Self> {
        let id = uuid::Uuid::new_v4().to_string();
        let temp_path = temp_dir.join(format!("{}.sst.tmp", id));
        // Create file with write+read to allow finalization and possible readback
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&temp_path)?;
        Ok(Self {
            file,
            temp_path,
            block_size,
            compression,
            use_internal_keys: use_internal,
            cur_block: DataBlockBuilder::new(16),
            last_key_in_block: None,
            offsets: Vec::new(),
            index: IndexBlockBuilder::new_with_internal_keys(use_internal),
            bloom_builder: crate::sst::bloom::BloomFilterBuilder::with_bits_per_key(10),
            range_tombstones: Vec::new(),
            offset: 0,
            test_hooks,
        })
    }

    /// Create a new FsDynWriter with a specific SST sequence for deterministic temp file naming.
    pub fn new_with_seq(
        temp_dir: &Path,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        sst_seq: u64,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
    ) -> MidgeResult<Self> {
        let temp_path = temp_dir.join(format!("{:016}.sst.tmp", sst_seq));
        // Create file with write+read to allow finalization and possible readback
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&temp_path)?;
        Ok(Self {
            file,
            temp_path,
            block_size,
            compression,
            use_internal_keys: use_internal,
            cur_block: DataBlockBuilder::new(16),
            last_key_in_block: None,
            offsets: Vec::new(),
            index: IndexBlockBuilder::new_with_internal_keys(use_internal),
            bloom_builder: crate::sst::bloom::BloomFilterBuilder::with_bits_per_key(10),
            range_tombstones: Vec::new(),
            offset: 0,
            test_hooks,
        })
    }

    fn flush_block_if_needed_inner(&mut self) -> MidgeResult<()> {
        if self.cur_block.is_empty() {
            return Ok(());
        }
        let last_key = self.last_key_in_block.clone().unwrap_or_default();
        let builder = std::mem::replace(&mut self.cur_block, DataBlockBuilder::new(16));
        let payload = builder.finish();
        let block = Block::new(payload, BlockType::Data, self.compression);
        let encoded = block.encode()?;
        // Use write_all to ensure full buffer is written; amortize syscalls by writing
        // larger encoded buffers at once.
        crate::fs::write_all_with_hooks(&mut self.file, &encoded, self.test_hooks.as_ref())?;
        let written = encoded.len() as u64;
        let handle = BlockHandle::new(self.offset, written);
        self.offset = self.offset.saturating_add(written);
        self.offsets.push((last_key, handle));
        Ok(())
    }
}

impl crate::sst::DynSstWriter for FsDynWriter {
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.add_with_meta(key, Some(value), 0, 0, None)
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let tombstone = op_type == 2;
        // If adding this entry would exceed block, flush current block to disk
        if self.cur_block.estimated_size() + key.len() + value.unwrap_or(&[]).len() + 16
            > self.block_size
        {
            self.flush_block_if_needed_inner()?;
        }

        let write_key: Vec<u8> = key.to_vec();
        if self.use_internal_keys {
            if let Some((user, _s, _t)) = crate::common::internal_key::decode_internal_key(key) {
                self.cur_block
                    .add_with_meta(key, value, seq, op_type, true, expiration)?;
                // Store full internal key (not just user key) for sparse index uniqueness
                self.last_key_in_block = Some(key.to_vec());
                self.bloom_builder.add_key(&user);
            } else {
                let ik = crate::common::internal_key::encode_internal_key(key, seq, tombstone);
                self.cur_block
                    .add_with_meta(&ik, value, seq, op_type, true, expiration)?;
                // Store encoded internal key for sparse index
                self.last_key_in_block = Some(ik);
                self.bloom_builder.add_key(key);
            }
        } else {
            self.cur_block
                .add_with_meta(&write_key, value, seq, op_type, false, expiration)?;
            self.last_key_in_block = Some(write_key.clone());
            self.bloom_builder.add_key(&write_key);
        }
        Ok(())
    }

    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        self.range_tombstones.push(RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq,
        });
        Ok(())
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        // Default behavior: finalize to a temp file and read bytes back
        let mut s = *self;
        // flush remaining block
        s.flush_block_if_needed_inner()?;

        // Build index block and other metadata and append to file
        // Index
        for (k, h) in &s.offsets {
            s.index.add_index_entry(k.as_ref(), *h)?;
        }
        let index_payload = s.index.finish();
        let index_block =
            Block::new(index_payload, BlockType::Index, CompressionType::None).encode()?;
        let index_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &index_block, s.test_hooks.as_ref())?;
        let index_handle = BlockHandle::new(index_off, index_block.len() as u64);
        s.offset += index_block.len() as u64;

        // Bloom
        let bloom = s.bloom_builder.finish();
        let bloom_bytes = bloom.encode();
        let bloom_block =
            Block::new(bloom_bytes, BlockType::Filter, CompressionType::None).encode()?;
        let bloom_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &bloom_block, s.test_hooks.as_ref())?;
        let bloom_handle = BlockHandle::new(bloom_off, bloom_block.len() as u64);
        s.offset += bloom_block.len() as u64;

        // Tombstones
        let tomb_handle_opt = if !s.range_tombstones.is_empty() {
            let tomb_bytes =
                crate::sst::range_tombstone::encode_range_tombstones(&s.range_tombstones)?;
            let tomb_block =
                Block::new(tomb_bytes, BlockType::Filter, CompressionType::None).encode()?;
            let tomb_off = s.offset;
            crate::fs::write_all_with_hooks(&mut s.file, &tomb_block, s.test_hooks.as_ref())?;
            s.offset += tomb_block.len() as u64;
            Some(BlockHandle::new(tomb_off, tomb_block.len() as u64))
        } else {
            None
        };

        // Meta index
        let mut meta_builder = DataBlockBuilder::new(1);
        meta_builder.add(b"filter.bloom", &bloom_handle.encode())?;
        if s.use_internal_keys {
            meta_builder.add(b"format.internal_keys", b"1")?;
        }
        if let Some(tomb_handle) = tomb_handle_opt {
            meta_builder.add(b"tombstones.range", &tomb_handle.encode())?;
        }
        let meta_payload = meta_builder.finish();
        let meta_block =
            Block::new(meta_payload, BlockType::MetaIndex, CompressionType::None).encode()?;
        let meta_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &meta_block, s.test_hooks.as_ref())?;
        let meta_handle = BlockHandle::new(meta_off, meta_block.len() as u64);
        s.offset += meta_block.len() as u64;

        // Footer
        let footer = Footer::new(index_handle, meta_handle).encode();
        crate::fs::write_all_with_hooks(&mut s.file, &footer, s.test_hooks.as_ref())?;
        s.offset += footer.len() as u64;

        // Ensure all bytes flushed (honor test hooks when present)
        crate::fs::sync_data_only(&s.file, s.test_hooks.as_ref())?;

        // Read file bytes back
        let mut buf = Vec::with_capacity(s.offset as usize);
        s.file.rewind().ok();
        use std::io::Read;
        s.file.read_to_end(&mut buf)?;
        // Attempt to remove temp file after reading
        let _ = std::fs::remove_file(&s.temp_path);
        Ok(buf)
    }

    fn finish_to_path(self: Box<Self>, path: &std::path::Path) -> MidgeResult<()> {
        let mut s = *self;
        s.flush_block_if_needed_inner()?;

        // Build index block and other metadata and append to file
        for (k, h) in &s.offsets {
            s.index.add_index_entry(k.as_ref(), *h)?;
        }
        let index = s.index.finish();
        let index_payload = index;
        let index_block =
            Block::new(index_payload, BlockType::Index, CompressionType::None).encode()?;
        let index_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &index_block, s.test_hooks.as_ref())?;
        let index_handle = BlockHandle::new(index_off, index_block.len() as u64);
        s.offset += index_block.len() as u64;

        // Bloom filter
        let bloom = s.bloom_builder.finish();
        let bloom_bytes = bloom.encode();
        let bloom_block =
            Block::new(bloom_bytes, BlockType::Filter, CompressionType::None).encode()?;
        let bloom_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &bloom_block, s.test_hooks.as_ref())?;
        let bloom_handle = BlockHandle::new(bloom_off, bloom_block.len() as u64);
        s.offset += bloom_block.len() as u64;

        // Tombstones
        let tomb_handle_opt = if !s.range_tombstones.is_empty() {
            let tomb_bytes =
                crate::sst::range_tombstone::encode_range_tombstones(&s.range_tombstones)?;
            let tomb_block =
                Block::new(tomb_bytes, BlockType::Filter, CompressionType::None).encode()?;
            let tomb_off = s.offset;
            crate::fs::write_all_with_hooks(&mut s.file, &tomb_block, s.test_hooks.as_ref())?;
            s.offset += tomb_block.len() as u64;
            Some(BlockHandle::new(tomb_off, tomb_block.len() as u64))
        } else {
            None
        };

        // Meta index
        let mut meta_builder = DataBlockBuilder::new(1);
        meta_builder.add(b"filter.bloom", &bloom_handle.encode())?;
        if s.use_internal_keys {
            meta_builder.add(b"format.internal_keys", b"1")?;
        }
        if let Some(tomb_handle) = tomb_handle_opt {
            meta_builder.add(b"tombstones.range", &tomb_handle.encode())?;
        }
        let meta_payload = meta_builder.finish();
        let meta_block =
            Block::new(meta_payload, BlockType::MetaIndex, CompressionType::None).encode()?;
        let meta_off = s.offset;
        crate::fs::write_all_with_hooks(&mut s.file, &meta_block, s.test_hooks.as_ref())?;
        let meta_handle = BlockHandle::new(meta_off, meta_block.len() as u64);
        s.offset += meta_block.len() as u64;

        // Footer
        let footer = Footer::new(index_handle, meta_handle).encode();
        crate::fs::write_all_with_hooks(&mut s.file, &footer, s.test_hooks.as_ref())?;
        s.offset += footer.len() as u64;

        crate::fs::sync_data_only(&s.file, s.test_hooks.as_ref())?;
        drop(s.file);

        // Move temp file into place (atomic rename preferred)
        tracing::debug!(
            "finalizing SST: renaming {} -> {}",
            s.temp_path.display(),
            path.display()
        );
        if let Err(e) = std::fs::rename(&s.temp_path, path) {
            // fallback: try to copy then remove
            std::fs::copy(&s.temp_path, path)?;
            let _ = std::fs::remove_file(&s.temp_path);
            tracing::warn!("rename temp sst failed, copied instead: {}", e);
        }

        // Best-effort: ensure the directory entry for the new file is persisted
        if let Err(e) = crate::fs::sync_parent(path) {
            tracing::warn!("failed to sync parent dir for {}: {}", path.display(), e);
        } else {
            tracing::debug!("synced parent dir for {}", path.display());
        }

        Ok(())
    }
}
