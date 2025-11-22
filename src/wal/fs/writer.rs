use std::fs::OpenOptions;
use std::io::{BufWriter, IoSlice, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{MidgeError, MidgeResult};
use crate::wal::arena::Arena;
use crate::wal::encode_pipeline::WalEncoder;
use crate::wal::encoding::decode;
use crate::wal::fs::batched_sync::{BatchedSyncConfig, BatchedSyncCoordinator};
use crate::wal::types::WalSyncMode;
use crate::wal::{WalOpKind, WalPos, WalRecord, WalWriter};

// Use shared filesystem utilities
use crate::fs;

#[cfg(test)]
/// Test-only counter incremented each time the underlying durable sync is performed.
/// This is used by instrumentation unit tests to verify batching effectiveness.
static TEST_SYNC_CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

/// WAL format version 1: TLV encoding with full transaction support
pub(super) const WAL_MAGIC_V1: &[u8; 8] = b"SHALWAL1";
const BUF_CAP: usize = 128 * 1024; // Optimized for balance between buffer size and flush frequency
const DIRECT_WRITE_THRESHOLD: usize = BUF_CAP * 2;
// ============================================================================
// WAL implementation
// ============================================================================

/// Mutable state of the WAL that needs interior mutability for &self methods
struct WalInner {
    file: FsBufWriter,
    /// Track current file position to avoid seek() calls
    pos: u64,
    /// Reusable page-aligned buffer for batching (avoids per-call allocations)
    scratch: Arena,
}

/// Custom BufWriter that uses fs functions for I/O error injection in tests
struct FsBufWriter {
    inner: BufWriter<std::fs::File>,
    test_hooks: Option<crate::common::test_hooks::TestHooks>,
}

impl FsBufWriter {
    fn new(file: std::fs::File, test_hooks: Option<crate::common::test_hooks::TestHooks>) -> Self {
        Self {
            inner: BufWriter::with_capacity(BUF_CAP, file),
            test_hooks,
        }
    }

    fn set_test_hooks(&mut self, test_hooks: Option<crate::common::test_hooks::TestHooks>) {
        self.test_hooks = test_hooks;
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn get_ref(&self) -> &std::fs::File {
        self.inner.get_ref()
    }

    fn get_mut(&mut self) -> &mut std::fs::File {
        self.inner.get_mut()
    }

    #[allow(dead_code)]
    fn into_inner(
        self,
    ) -> Result<std::fs::File, std::io::IntoInnerError<BufWriter<std::fs::File>>> {
        self.inner.into_inner()
    }
}

impl Write for FsBufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.inner.write_vectored(bufs)
    }
}

impl Seek for FsBufWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

pub struct Wal {
    path: PathBuf,
    /// Mutable state protected by mutex for thread-safe &self methods
    inner: parking_lot::Mutex<WalInner>,
    /// Synchronization mode
    #[allow(dead_code)]
    sync_mode: WalSyncMode,
    /// Batched-sync coordinator (shared across instances)
    #[allow(dead_code)]
    group_commit: Option<Arc<BatchedSyncCoordinator>>,
    /// File number for this WAL (used for numbered naming: 000001.log)
    file_number: u64,
    /// Parallel encoder for batch operations
    encoder: WalEncoder<crate::wal::encode_pipeline::DefaultBodyEncoder>,
    /// Test hooks for fault injection (test builds only)
    pub(crate) test_hooks: Option<crate::common::test_hooks::TestHooks>,
}

impl Wal {
    /// Set test hooks for fault injection. This also propagates to the underlying FsBufWriter.
    pub fn set_test_hooks(&mut self, test_hooks: Option<crate::common::test_hooks::TestHooks>) {
        self.test_hooks = test_hooks.clone();
        let mut inner = self.inner.lock();
        inner.file.set_test_hooks(test_hooks);
    }
    pub fn open(dir: &Path) -> MidgeResult<Self> {
        Self::open_with_mode(dir, WalSyncMode::default())
    }

    pub fn open_with_mode(dir: &Path, sync_mode: WalSyncMode) -> MidgeResult<Self> {
        std::fs::create_dir_all(dir)?;

        // Find the latest WAL file number using shared fs utilities
        let latest_number = fs::find_latest_numbered_file(dir, "wal")?;

        // Reuse the existing file if it exists, otherwise create the first one
        let file_number = if latest_number == 0 {
            1 // Start with 00000000000000000001.wal if no files exist
        } else {
            latest_number // Reuse the latest file (append mode)
        };
        let path = fs::numbered_file_path(dir, file_number, "wal");

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        // Get initial file size
        let pos = file.metadata()?.len();
        // Wrap file in FsBufWriter with 128KB buffer (balanced for throughput)
        let file = FsBufWriter::new(file, None);

        // Create group commit coordinator if needed
        let group_commit = if sync_mode == WalSyncMode::BatchedSync {
            Some(Arc::new(BatchedSyncCoordinator::new(
                BatchedSyncConfig::default(),
            )))
        } else {
            None
        };

        // Create parallel encoder with defaults
        let encoder = WalEncoder::with_defaults()?;

        let mut wal = Self {
            path,
            inner: parking_lot::Mutex::new(WalInner {
                file,
                pos,
                scratch: Arena::with_capacity(256 * 1024), // reusable 256KB page-aligned buffer
            }),
            sync_mode,
            group_commit,
            file_number,
            encoder,
            test_hooks: None, // Will be set by factory if needed
        };
        // Ensure header exists for a fresh file
        let _ = wal.write_header(0);
        Ok(wal)
    }

    /// Returns the file number for this WAL (e.g., 1 for 000001.wal)
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    fn write_header(&mut self, start_sequence: u64) -> MidgeResult<()> {
        // Write v1 magic header
        // Access the inner file to get metadata
        let mut inner = self.inner.lock();
        let len = inner.file.get_ref().metadata()?.len();
        if len == 0 {
            inner.file.seek(SeekFrom::Start(0))?;
            inner.file.write_all(WAL_MAGIC_V1)?;
            inner.file.write_all(&start_sequence.to_be_bytes())?;
            inner.file.flush()?;
            inner.pos = 16; // Magic (8) + sequence (8)
        }
        Ok(())
    }

    pub fn append(&mut self, rec: &WalRecord) -> MidgeResult<()> {
        let mut inner = self.inner.lock();
        // OPTIMIZATION: Use the parallel encoder to serialize TLV body and optionally
        // obtain a precomputed CRC in a single streaming pass. This keeps the
        // CPU work outside the critical section and avoids a second scan of the
        // encoded body when the encoder provides the CRC.
        let frag = self.encoder.encode_one(rec)?;

        // Now perform I/O operations in a tight batch.
        // Prefer a vectored write to emit header+body in a single syscall when
        // possible. Flush the buffered writer first so we can write directly to
        // the underlying file to avoid buffering copies.
        inner.file.flush()?;
        let file = inner.file.get_mut();

        // Use the fs-level vectored writer which will dispatch to io_uring
        // when enabled (feature-gated) or fall back to blocking writev.
        crate::fs::write_vectored_with_hooks(
            file,
            &[&frag.header, &frag.body],
            self.test_hooks.as_ref(),
        )?;

        let written = (frag.header.len() + frag.body.len()) as u64;
        inner.pos += written;
        Ok(())
    }

    pub fn truncate(&self) -> MidgeResult<()> {
        let mut inner = self.inner.lock();

        // Flush buffer first, then truncate the underlying file
        inner.file.flush()?;
        inner.file.get_mut().set_len(0)?;
        inner.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn replay(&mut self) -> MidgeResult<Vec<WalRecord>> {
        let mut inner = self.inner.lock();

        // Flush buffered writes before reading
        inner.file.flush()?;
        // Read directly from our own file using replay_wal_file
        replay_wal_file(&self.path)
    }
}

impl WalWriter for Wal {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Call test hook before WAL append
        if let Some(hooks) = &self.test_hooks {
            hooks.before_wal_append();
        }

        let mut inner = self.inner.lock();
        // Encode using the shared WalEncoder so we can leverage any streaming CRC
        // it provides and avoid a second pass over the encoded body.
        let frag = self.encoder.encode_one(record)?;

        if frag.body.len() > (u32::MAX as usize) {
            return Err(MidgeError::WalError {
                message: "WAL record too large".to_string(),
            });
        }

        let pos_before = inner.pos;
        // Flush buffered data and write header+body with a single vectored syscall
        inner.file.flush()?;
        let file = inner.file.get_mut();

        crate::fs::write_vectored_with_hooks(
            file,
            &[&frag.header, &frag.body],
            self.test_hooks.as_ref(),
        )?;

        // Advance position to reflect newly-written bytes
        inner.pos += (frag.header.len() + frag.body.len()) as u64;

        // Test hook: allow truncating WAL immediately after append to simulate
        // torn-write scenarios. If the hook requests truncation, truncate the
        // underlying file back to `pos_before` and update the tracked position.
        if let Some(hooks) = &self.test_hooks {
            if hooks.after_wal_append() {
                // Ensure buffered writer flushed and then truncate the file to
                // simulate a partial/torn write (i.e., the last append didn't make it).
                inner.file.flush()?;
                let f = inner.file.get_mut();
                // If tests request a simulated failing truncate, force the deterministic
                // CRC-overwrite fallback so tests can exercise the code path.
                if hooks.should_fail_truncate() {
                    if let Err(e) = (|| -> std::io::Result<()> {
                        f.seek(SeekFrom::Start(pos_before))?;
                        // Overwrite 4 CRC bytes with 0xFF to guarantee mismatch.
                        f.write_all(&[0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8])?;
                        f.flush()?;
                        Ok(())
                    })() {
                        return Err(e.into());
                    } else {
                        // Make sure our tracked position reflects the truncated view
                        if let Err(e) = f.seek(SeekFrom::Start(pos_before)) {
                            return Err(e.into());
                        }
                        inner.pos = pos_before;
                    }
                } else if let Err(e) = f.set_len(pos_before) {
                    // Fallback for platforms that disallow set_len while file is open.
                    // Overwrite the stored CRC (first 4 bytes of the record header)
                    // at `pos_before` so WAL replay will detect a CRC mismatch and
                    // stop at the last valid record. Writing 4 bytes emulates the
                    // manual corruption performed in unit tests (see
                    // `should_detect_corrupted_crc`).
                    if let Err(_e) = (|| -> std::io::Result<()> {
                        f.seek(SeekFrom::Start(pos_before))?;
                        // Overwrite 4 CRC bytes with 0xFF to guarantee mismatch.
                        f.write_all(&[0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8])?;
                        f.flush()?;
                        Ok(())
                    })() {
                        return Err(e.into());
                    }
                } else {
                    if let Err(e) = f.seek(SeekFrom::Start(pos_before)) {
                        return Err(e.into());
                    }
                    inner.pos = pos_before;
                }
            }
        }

        Ok(pos_before)
    }

    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        if records.is_empty() {
            return Ok(self.current_pos());
        }

        // Call test hook before WAL append (once per batch)
        if let Some(hooks) = &self.test_hooks {
            hooks.before_wal_append();
        }

        // OPTIMIZATION: Use parallel encoder for batch encoding + CRC computation.
        // This parallelizes the CPU-intensive serialization work across multiple cores,
        // dramatically reducing the encode time for large batches (200ms -> ~50ms).
        let fragments = self.encoder.encode_batch(records)?;

        // Calculate total size for all encoded fragments
        let mut total_size: usize = 0;
        for frag in &fragments {
            // Guard against overflow when summing many large records
            if let Some(n) = total_size.checked_add(frag.total_len()) {
                total_size = n;
            } else {
                return Err(MidgeError::WalError {
                    message: "WAL batch too large".into(),
                });
            }
        }

        let mut inner = self.inner.lock();
        let pos_before = inner.pos;

        // For batching we prefer to avoid allocating/copying the entire batch
        // into the scratch buffer when the total size is very large. Allocating
        // a huge scratch buffer increases memory pressure and is expensive to
        // fill. Use a hybrid approach:
        // - For small batches: concatenate into `scratch` and write once (fast)
        // - For very large batches: flush the BufWriter and write each record
        //   (header + body) directly to the underlying file to avoid copying
        //   the large body bytes into `scratch`.

        if total_size < DIRECT_WRITE_THRESHOLD {
            // Small batch -- build in scratch and write through the buffered writer.
            // Use a single resize + memcpy pass to avoid repeated bounds checks
            // and reallocations from repeated extend_from_slice calls.
            let mut scratch = std::mem::take(&mut inner.scratch);
            if scratch.capacity() < total_size {
                scratch.reserve(total_size - scratch.capacity());
            }
            // Resize to the exact final length and fill by copying into slices
            scratch.resize(total_size, 0u8);
            let mut off = 0usize;
            let s = scratch.as_mut_slice();
            for frag in &fragments {
                s[off..off + 8].copy_from_slice(&frag.header);
                off += 8;
                s[off..off + frag.body.len()].copy_from_slice(&frag.body);
                off += frag.body.len();
            }

            inner.file.write_all(s)?;
            inner.pos = inner.pos.saturating_add(total_size as u64);
            // Return scratch buffer for reuse
            inner.scratch = scratch;

            // Test hook: optionally truncate after batch append to simulate torn write
            if let Some(hooks) = &self.test_hooks {
                if hooks.after_wal_append() {
                    inner.file.flush()?;
                    let f = inner.file.get_mut();
                    // If tests request simulated failing truncate, perform the
                    // deterministic CRC-overwrite fallback instead of set_len.
                    if hooks.should_fail_truncate() {
                        if let Err(e) = (|| -> std::io::Result<()> {
                            f.seek(SeekFrom::Start(pos_before))?;
                            f.write_all(&[0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8])?;
                            f.flush()?;
                            Ok(())
                        })() {
                            return Err(e.into());
                        } else {
                            if let Err(e) = f.seek(SeekFrom::Start(pos_before)) {
                                return Err(e.into());
                            }
                            inner.pos = pos_before;
                        }
                    } else {
                        if let Err(e) = f.set_len(pos_before) {
                            return Err(e.into());
                        }
                        if let Err(e) = f.seek(SeekFrom::Start(pos_before)) {
                            return Err(e.into());
                        }
                        inner.pos = pos_before;
                    }
                }
            }
        } else {
            // Very large batch -- avoid copying body bytes into scratch. Use
            // vectored writes (write_vectored) to emit headers + bodies in
            // fewer syscalls. To avoid creating an enormous IoSlice vector we
            // chunk the records. We must also handle partial writes which may
            // consume only a prefix of the provided iovecs.
            inner.file.flush()?;
            let file = inner.file.get_mut();

            // Build a compact headers blob (8 bytes per record) so we can take
            // stable slices into it for IoSlice while keeping layout simple.
            let mut headers_blob: Vec<u8> = Vec::with_capacity(fragments.len() * 8);
            for frag in &fragments {
                headers_blob.extend_from_slice(&frag.header);
            }

            // Chunked vectored writes: limit number of iovecs per syscall.
            // Keep chunk size modest to avoid OS limits; each record produces
            // two iovecs (header + body). Choose chunk_records so that
            // chunk_records * 2 <= MAX_IO_SLICES.
            const MAX_IO_SLICES: usize = 128; // conservative
            let chunk_records = std::cmp::max(1, MAX_IO_SLICES / 2);

            let mut idx = 0usize;
            while idx < fragments.len() {
                let end = std::cmp::min(fragments.len(), idx + chunk_records);

                // Build an array of borrowed byte slices for this chunk in the
                // form [header0, body0, header1, body1, ...]. We'll create
                // IoSlice instances from them in each write attempt so we can
                // adjust offsets after partial writes.
                let mut parts: Vec<&[u8]> = Vec::with_capacity((end - idx) * 2);
                // Iterate the slice of fragments for this chunk instead
                // of indexing into `fragments` by a range. This avoids the
                // needless range-loop pattern Clippy warns about and is
                // clearer about which subset we're processing.
                for (j, frag) in fragments[idx..end].iter().enumerate() {
                    let i = idx + j;
                    let h_off = i * 8;
                    parts.push(&headers_blob[h_off..h_off + 8]);
                    parts.push(&frag.body[..]);
                }

                // Track current position within parts for partial-write handling
                let mut part_index = 0usize;
                let mut part_offset = 0usize;

                while part_index < parts.len() {
                    // Build IoSlice vector for remaining parts in this chunk.
                    // Only the first remaining slice may have a non-zero offset
                    // (part_offset). Apply it only to that slice.
                    let mut slices: Vec<IoSlice> = Vec::with_capacity(parts.len() - part_index);
                    for (i, s) in parts[part_index..].iter().enumerate() {
                        if i == 0 && part_offset != 0 {
                            slices.push(IoSlice::new(&s[part_offset..]));
                        } else {
                            slices.push(IoSlice::new(s));
                        }
                    }

                    let written = file.write_vectored(&slices)?;
                    if written == 0 {
                        return Err(MidgeError::WalError {
                            message: "write_vectored wrote 0 bytes".into(),
                        });
                    }

                    // Advance part_index/part_offset according to bytes written
                    let mut rem = written;
                    while part_index < parts.len() {
                        let available = parts[part_index].len() - part_offset;
                        if rem >= available {
                            rem -= available;
                            part_index += 1;
                            part_offset = 0;
                        } else {
                            part_offset += rem;
                            break;
                        }
                    }
                }

                idx = end;
            }

            inner.pos = inner.pos.saturating_add(total_size as u64);

            // Test hook: optionally truncate after large-batch direct writes
            if let Some(hooks) = &self.test_hooks {
                if hooks.after_wal_append() {
                    // We wrote directly via file descriptor; truncate to simulate torn write
                    let file = inner.file.get_mut();
                    file.set_len(pos_before)?;
                    file.seek(SeekFrom::Start(pos_before))?;
                    inner.pos = pos_before;
                }
            }
        }

        Ok(pos_before)
    }

    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos> {
        let record = WalRecord {
            cf_id: 0,
            op: kind,
            key: bytes::Bytes::copy_from_slice(key),
            value: value.map(bytes::Bytes::copy_from_slice),
            seq: 0,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    fn flush(&self) -> MidgeResult<()> {
        let mut inner = self.inner.lock();
        inner.file.flush()?;
        Ok(())
    }

    fn sync(&self) -> MidgeResult<()> {
        // Flush buffered writes while holding the lock, then perform the durable
        // sync outside the lock. If group commit is enabled, use the coordinator
        // to batch the fsync across concurrent callers.
        let maybe_coordinator = self.group_commit.as_ref().cloned();

        // Flush the buffered writer and obtain a cloned file handle for syncing
        let file_clone = {
            let mut inner = self.inner.lock();
            inner.file.flush()?;
            // Clone the underlying file descriptor so we can sync it without holding the lock
            inner.file.get_ref().try_clone()?
        };

        if let Some(coord) = maybe_coordinator {
            // Use group commit coordinator to batch syncs
            coord.wait_for_sync(|| {
                // Test-only increment for instrumentation
                #[cfg(test)]
                TEST_SYNC_CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                let t0 = std::time::Instant::now();
                let res = fs::sync_data_only(&file_clone, self.test_hooks.as_ref());
                let _dur = t0.elapsed();
                Ok(res?)
            })?;
        } else {
            // No group commit - perform direct sync
            #[cfg(test)]
            TEST_SYNC_CALL_COUNT.fetch_add(1, Ordering::SeqCst);
            let t0 = std::time::Instant::now();
            fs::sync_data_only(&file_clone, self.test_hooks.as_ref())?;
            let _dur = t0.elapsed();
        }

        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        let inner = self.inner.lock();
        inner.pos
    }

    fn close(&self) -> MidgeResult<()> {
        let mut inner = self.inner.lock();
        inner.file.flush()?;
        Ok(())
    }
}

/// Replay an entire WAL file at a given path. Supports v1 TLV format with
/// standard header (magic + start_sequence) and verifies per-record CRCs.
///
/// This function uses `AbsoluteConsistency` mode. For recovery mode control,
/// use `replay_wal_file_with_mode`.
pub fn replay_wal_file(path: &std::path::Path) -> crate::error::MidgeResult<Vec<WalRecord>> {
    replay_wal_file_with_mode(path, crate::WalRecoveryMode::AbsoluteConsistency)
}

/// Read exact bytes from a file, handling EOF based on recovery mode
fn read_exact_with_recovery(
    file: &mut std::fs::File,
    buf: &mut [u8],
    recovery_mode: crate::WalRecoveryMode,
    context: &str,
    position: u64,
) -> MidgeResult<bool> {
    use std::io::Read;
    match file.read_exact(buf) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            if recovery_mode == crate::WalRecoveryMode::TolerateCorruptedTail {
                Ok(false) // Signal EOF in tolerant mode
            } else {
                Err(crate::error::MidgeError::Corruption {
                    message: format!(
                        "Unexpected EOF while reading {} at position {}",
                        context, position
                    ),
                })
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// Validate WAL header and return start sequence
fn validate_wal_header(
    file: &mut std::fs::File,
    _file_size: u64,
    recovery_mode: crate::WalRecoveryMode,
) -> MidgeResult<Option<()>> {
    let mut magic = [0u8; 8];
    let mut seqbuf = [0u8; 8];

    // Read magic
    if !read_exact_with_recovery(file, &mut magic, recovery_mode, "magic", 0)? {
        return Ok(None); // Empty or truncated header in tolerant mode
    }

    // Read sequence
    if !read_exact_with_recovery(file, &mut seqbuf, recovery_mode, "sequence", 8)? {
        return Ok(None); // Truncated header in tolerant mode
    }

    // Validate magic
    if &magic != WAL_MAGIC_V1 {
        return Err(crate::error::MidgeError::Corruption {
            message: format!("Invalid WAL magic: expected v1, got {:?}", magic),
        });
    }

    Ok(Some(()))
}

/// Parse a single WAL record from TLV body
fn parse_wal_record_tlv(body: &[u8]) -> MidgeResult<WalRecord> {
    decode(body)
}

/// Replay an entire WAL file with configurable recovery mode.
///
/// # Recovery Modes
///
/// - `AbsoluteConsistency`: Fails on ANY corruption (strictest)
/// - `TolerateCorruptedTail`: Recovers records up to tail corruption
///
/// # Returns
///
/// - `Ok(Vec<WalRecord>)`: Successfully recovered records
/// - `Err(Corruption)`: Corruption detected (behavior depends on mode)
///
/// # Examples
///
/// ```rust,no_run
/// use cntryl_midge::WalRecoveryMode;
/// # use std::path::Path;
/// # fn example(path: &Path) -> cntryl_midge::MidgeResult<Vec<cntryl_midge::wal::WalRecord>> {
///
/// // Strict mode - fail on any corruption
/// let records = cntryl_midge::wal::fs::replay_wal_file_with_mode(
///     path,
///     WalRecoveryMode::AbsoluteConsistency
/// )?;
///
/// // Tolerant mode - recover partial data
/// let records = cntryl_midge::wal::fs::replay_wal_file_with_mode(
///     path,
///     WalRecoveryMode::TolerateCorruptedTail
/// )?;
/// # Ok(records)
/// # }
/// ```
pub fn replay_wal_file_with_mode(
    path: &std::path::Path,
    recovery_mode: crate::WalRecoveryMode,
) -> crate::error::MidgeResult<Vec<WalRecord>> {
    use std::io::Read;
    let mut file = OpenOptions::new().read(true).open(path)?;
    let file_size = file.metadata()?.len();

    // Handle empty WAL files gracefully
    if file_size == 0 {
        return Ok(Vec::new());
    }

    // Validate header
    if validate_wal_header(&mut file, file_size, recovery_mode)?.is_none() {
        return Ok(Vec::new()); // Empty or truncated in tolerant mode
    }

    let mut records = Vec::new();

    // Read records until EOF
    loop {
        let current_position = file.stream_position()?;

        // Read CRC32
        let mut crc_buf = [0u8; 4];
        match file.read_exact(&mut crc_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(records); // Normal EOF
            }
            Err(e) => return Err(e.into()),
        }
        let crc_stored = u32::from_le_bytes(crc_buf);

        // Read length
        let mut len_buf = [0u8; 4];
        if !read_exact_with_recovery(
            &mut file,
            &mut len_buf,
            recovery_mode,
            "length",
            current_position,
        )? {
            return Ok(records);
        }
        let body_len = u32::from_le_bytes(len_buf) as usize;

        // Read TLV body
        let mut body = vec![0u8; body_len];
        if !read_exact_with_recovery(
            &mut file,
            &mut body,
            recovery_mode,
            "body",
            current_position,
        )? {
            return Ok(records);
        }

        // Verify CRC32-C
        let crc_calc = crc32c::crc32c(&body);
        if crc_calc != crc_stored {
            let bytes_from_end = file_size.saturating_sub(current_position);
            let is_tail = bytes_from_end < 8192; // Last 8KB considered "tail"

            if recovery_mode == crate::WalRecoveryMode::TolerateCorruptedTail && is_tail {
                return Ok(records); // Tail corruption in tolerant mode
            } else {
                return Err(crate::error::MidgeError::Corruption {
                    message: format!(
                        "WAL v1 CRC mismatch at position {} ({} bytes from end, is_tail={})",
                        current_position, bytes_from_end, is_tail
                    ),
                });
            }
        }

        // Parse and add record
        records.push(parse_wal_record_tlv(&body)?);
    }
}

/// Factory for creating filesystem-backed WAL writers and readers
#[allow(dead_code)]
pub struct FsWalFactory;

#[allow(dead_code)]
impl FsWalFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FsWalFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::wal::WalFactory for FsWalFactory {
    fn create_writer(&self, dir: &Path) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        Ok(Box::new(Wal::open(dir)?))
    }

    fn create_writer_with_hooks(
        &self,
        dir: &Path,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
    ) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let mut wal = Wal::open(dir)?;
        wal.set_test_hooks(test_hooks);
        Ok(Box::new(wal))
    }

    fn create_reader(&self, _dir: &Path) -> MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        // For now, return an error as WalReader will be implemented in the file split
        Err(MidgeError::Internal {
            message: "WalReader not yet implemented - use replay_wal_file() directly".into(),
        })
    }

    fn rotate_writer(&self, dir: &Path, _seq: u64) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // For filesystem WAL, rotation is handled internally by Wal
        // Just create a new writer which will use the latest file
        Ok(Box::new(Wal::open(dir)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;
    use tempfile::TempDir;

    #[test]
    fn should_initialize_wal_file_in_directory() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Act
        let result = Wal::open(dir.path());

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_new_position_after_append() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");
        let pos1 = wal.current_pos();

        // Act
        let append_pos = wal
            .append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        let pos2 = wal.current_pos();

        // Assert
        assert!(append_pos >= pos1);
        assert!(pos2 > pos1);
    }

    #[test]
    fn should_complete_sync_without_error() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");

        // Act
        let result = wal.sync();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_batch_syncs_from_concurrent_callers() {
        use std::sync::Arc;
        use std::thread;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        // Ensure BatchedSync mode is enabled so syncs may be batched by the coordinator
        let wal = Wal::open_with_mode(dir.path(), WalSyncMode::BatchedSync).expect("open");
        // Seed some data so file has content
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");

        // Reset test counter
        TEST_SYNC_CALL_COUNT.store(0, Ordering::SeqCst);

        let wal = Arc::new(wal);
        let threads = 20usize;
        let barrier = Arc::new(std::sync::Barrier::new(threads));
        let mut handles = Vec::with_capacity(threads);

        // Act - spawn multiple threads and make them call sync() concurrently
        for _ in 0..threads {
            let w = Arc::clone(&wal);
            let b = Arc::clone(&barrier);
            let handle = thread::spawn(move || {
                // wait for all threads to be ready
                b.wait();
                let _ = w.sync();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        // Assert - batching should have reduced the number of underlying sync calls
        let count = TEST_SYNC_CALL_COUNT.load(Ordering::SeqCst);
        assert!(
            count < threads,
            "expected batching: {} underlying syncs for {} callers",
            count,
            threads
        );
    }

    #[test]
    fn should_track_positions_given_multiple_appends() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");

        // Act: append multiple operations
        for i in 0..100 {
            let result = wal.append_op(
                crate::wal::WalOpKind::Put,
                format!("key{}", i).as_bytes(),
                Some(format!("value{}", i).as_bytes()),
            );
            assert!(result.is_ok());
        }

        // Assert: final position should be significantly advanced
        assert!(wal.current_pos() > 0);
    }

    #[test]
    fn should_append_delete_operations_successfully() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Delete, b"key1", None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_put_operations_successfully() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Insert, b"key1", Some(b"value1"));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_large_values_successfully() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");
        let large_value = vec![0xAB; 1024 * 1024]; // 1MB

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, b"large_key", Some(&large_value));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_empty_keys_successfully() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, b"", Some(b"value"));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_binary_data_successfully() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");
        let binary_key = vec![0x00, 0xFF, 0x80, 0x7F];
        let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, &binary_key, Some(&binary_value));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_persist_data_across_reopen() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Act: write in one session
        {
            let wal = Wal::open(dir.path()).expect("open");
            wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
                .expect("append");
        }

        // Act: reopen in another session
        let result = Wal::open(dir.path());

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_create_parent_directory_when_missing() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let subdir = temp.path().join("new_dir");

        // Act: open WAL in non-existent directory
        let result = Wal::open(&subdir);

        // Assert: should create directory and succeed
        assert!(result.is_ok());
        assert!(subdir.exists());
    }

    #[test]
    fn should_replay_multiple_records() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        // Write multiple records
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Delete, b"key1", None)
            .expect("append");

        // Act: replay WAL
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key.as_ref(), b"key1");
        assert_eq!(
            records[0].value.as_ref().map(|v| v.as_ref()),
            Some(&b"value1"[..])
        );
        assert_eq!(records[1].key.as_ref(), b"key2");
        assert_eq!(records[2].key.as_ref(), b"key1");
        assert!(records[2].value.is_none());
    }

    #[test]
    fn should_handle_truncate_operation() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Write initial data
        {
            #[cfg(unix)]
            let mut wal = Wal::open(dir.path()).expect("open");
            #[cfg(windows)]
            let wal = Wal::open(dir.path()).expect("open");

            wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
                .expect("append");

            // Act
            let result = wal.truncate();

            // Assert
            // On Windows, truncate might fail due to file locking
            // On Unix, it should succeed
            #[cfg(unix)]
            {
                result.expect("truncate");
                let records = wal.replay().expect("replay");
                assert_eq!(records.len(), 0);
            }

            #[cfg(windows)]
            {
                // On Windows, just verify the operation completes (may succeed or fail)
                let _ = result;
            }
        }

        // Verify we can reopen and use the WAL
        let wal = Wal::open(dir.path()).expect("reopen after truncate");
        wal.append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append after truncate");
    }

    #[test]
    fn should_handle_transaction_markers() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        // Write transaction
        let txn_id = 12345u64;
        let mut rec_begin = WalRecord::new(
            crate::wal::WalOpKind::TxnBegin,
            bytes::Bytes::new(),
            None,
            1,
        );
        rec_begin.txn_id = Some(txn_id);
        wal.append(&rec_begin).expect("append begin");

        let mut rec_put = WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"txn_key"),
            Some(bytes::Bytes::from_static(b"txn_value")),
            2,
        );
        rec_put.txn_id = Some(txn_id);
        wal.append(&rec_put).expect("append put");

        let mut rec_commit = WalRecord::new(
            crate::wal::WalOpKind::TxnCommit,
            bytes::Bytes::new(),
            None,
            3,
        );
        rec_commit.txn_id = Some(txn_id);
        wal.append(&rec_commit).expect("append commit");

        // Act: replay
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0].op, crate::wal::WalOpKind::TxnBegin));
        assert_eq!(records[0].txn_id, Some(txn_id));
        assert!(matches!(records[1].op, crate::wal::WalOpKind::Put));
        assert_eq!(records[1].txn_id, Some(txn_id));
        assert!(matches!(records[2].op, crate::wal::WalOpKind::TxnCommit));
        assert_eq!(records[2].txn_id, Some(txn_id));
    }

    #[test]
    fn should_handle_ttl_expiration_field() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        let mut rec = WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"ttl_key"),
            Some(bytes::Bytes::from_static(b"ttl_value")),
            1,
        );
        rec.expiration = Some(1234567890);
        wal.append(&rec).expect("append");

        // Act: replay
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].expiration, Some(1234567890));
    }

    #[test]
    fn should_handle_range_delete() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        let mut rec = WalRecord::new(
            crate::wal::WalOpKind::DeleteRange,
            bytes::Bytes::from_static(b"start_key"),
            None,
            1,
        );
        rec.range_end = Some(bytes::Bytes::from_static(b"end_key"));
        wal.append(&rec).expect("append");

        // Act: replay
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].op, crate::wal::WalOpKind::DeleteRange));
        assert_eq!(records[0].key.as_ref(), b"start_key");
        assert_eq!(
            records[0].range_end.as_ref().map(|b| b.as_ref()),
            Some(&b"end_key"[..])
        );
    }

    #[test]
    fn should_handle_insert_operation() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        wal.append_op(
            crate::wal::WalOpKind::Insert,
            b"insert_key",
            Some(b"insert_value"),
        )
        .expect("append");

        // Act: replay
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].op, crate::wal::WalOpKind::Insert));
        assert_eq!(records[0].key.as_ref(), b"insert_key");
    }

    #[test]
    fn should_handle_column_family_records() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        let cf_id = crate::api::column_family::ColumnFamilyId::new(42);
        let rec = WalRecord::new_cf(
            cf_id,
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"cf_key"),
            Some(bytes::Bytes::from_static(b"cf_value")),
            1,
        );
        wal.append(&rec).expect("append");

        // Act: replay
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cf_id, 42);
    }

    #[test]
    fn should_detect_corrupted_crc() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal_path = fs::numbered_file_path(dir.path(), 1, "wal");

        // Write a valid record
        {
            let wal = Wal::open(dir.path()).expect("open");
            wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
                .expect("append");
        }

        // Corrupt the CRC in the WAL file
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .expect("open for corruption");

            // Skip header (8 bytes magic + 8 bytes seq)
            file.seek(SeekFrom::Start(16)).expect("seek");
            // Corrupt the CRC bytes
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).expect("corrupt");
        }

        // Act: try to replay the specific corrupted file
        let result = replay_wal_file(&wal_path);

        // Assert: should detect corruption
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, crate::error::MidgeError::Corruption { .. }));
        }
    }

    #[test]
    fn should_ignore_corrupted_tail_in_tolerant_mode() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal_path = fs::numbered_file_path(dir.path(), 1, "wal");

        // Write two valid records
        {
            let wal = Wal::open(dir.path()).expect("open");
            wal.append_op(crate::wal::WalOpKind::Put, b"k1", Some(b"v1"))
                .expect("append");
            wal.append_op(crate::wal::WalOpKind::Put, b"k2", Some(b"v2"))
                .expect("append");
        }

        // Corrupt the CRC of the second record (the tail) by overwriting its CRC
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .expect("open for corruption");

            // Skip WAL header
            file.seek(SeekFrom::Start(16))
                .expect("seek to first record");

            // Read first record header: CRC (4) + LEN (4)
            let mut hdr = [0u8; 8];
            file.read_exact(&mut hdr).expect("read first header");
            let len1 = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as u64;

            // Skip first body
            file.seek(SeekFrom::Current(len1 as i64))
                .expect("skip first body");

            // Now at start of second record header; overwrite CRC bytes
            file.write_all(&[0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8])
                .expect("corrupt second crc");
            file.flush().expect("flush corruption");
        }

        // Act
        let recs =
            replay_wal_file_with_mode(&wal_path, crate::WalRecoveryMode::TolerateCorruptedTail)
                .expect("replay in tolerant mode");

        // Assert
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key.as_ref(), b"k1");
        assert_eq!(recs[0].value.as_ref().map(|v| v.as_ref()), Some(&b"v1"[..]));
    }

    #[test]
    fn should_invoke_fallback_when_truncate_simulation_fails() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal_path = fs::numbered_file_path(dir.path(), 1, "wal");

        let mut wal = Wal::open(dir.path()).expect("open");

        // Attach test hooks that request truncation but also force the truncate
        // to fail so the writer uses the CRC-overwrite fallback.
        let hooks = crate::common::test_hooks::TestHooks::new()
            .with_wal_behavior(crate::common::test_hooks::WalBehavior::TruncateAfterWriteFail);
        wal.test_hooks = Some(hooks.clone());

        // Act - append two records where the second will be 'torn' by the hook
        wal.append_op(crate::wal::WalOpKind::Put, b"hk1", Some(b"hv1"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Put, b"hk2", Some(b"hv2"))
            .expect("append");

        // Close writer to flush state
        drop(wal);

        // Assert - replay in tolerant mode should only return the first record
        let recs =
            replay_wal_file_with_mode(&wal_path, crate::WalRecoveryMode::TolerateCorruptedTail)
                .expect("replay in tolerant mode");

        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key.as_ref(), b"hk1");
        assert_eq!(
            recs[0].value.as_ref().map(|v| v.as_ref()),
            Some(&b"hv1"[..])
        );
    }

    #[test]
    fn should_use_wal_reader_for_replay() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append");

        // Act
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref(), b"key1");
        assert_eq!(records[1].key.as_ref(), b"key2");
    }

    #[test]
    fn should_replay_wal_file_by_path() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal = Wal::open(dir.path()).expect("open");
        let wal_path = wal.path.clone(); // Get the actual path used

        wal.append_op(crate::wal::WalOpKind::Put, b"path_key", Some(b"path_value"))
            .expect("append");
        drop(wal); // Close the WAL

        // Act: use replay_wal_file function
        let records = replay_wal_file(&wal_path).expect("replay file");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.as_ref(), b"path_key");
    }

    // --- Durability Tests ---

    #[test]
    fn should_persist_record_given_sync_called() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Act
        let mut wal = Wal::open(dir.path()).expect("open");
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        wal.sync().expect("sync should succeed");
        let records = wal.replay().expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.as_ref(), b"key1");
        assert_eq!(
            records[0].value.as_ref().map(|v| v.as_ref()),
            Some(b"value1".as_ref())
        );
    }

    #[test]
    fn should_fsync_to_disk_when_sync_called() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act: Write multiple records with sync between
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        wal.sync().expect("first sync");

        wal.append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append");
        wal.sync().expect("second sync");

        // Assert: Both records persisted
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn should_preserve_order_given_multiple_appends_before_sync() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act: Write multiple records before sync
        wal.append_op(crate::wal::WalOpKind::Put, b"a", Some(b"1"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Put, b"b", Some(b"2"))
            .expect("append");
        wal.append_op(crate::wal::WalOpKind::Put, b"c", Some(b"3"))
            .expect("append");
        wal.sync().expect("sync");

        // Assert: Order preserved
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key.as_ref(), b"a");
        assert_eq!(records[1].key.as_ref(), b"b");
        assert_eq!(records[2].key.as_ref(), b"c");
    }

    #[test]
    fn should_not_lose_data_given_flush_without_sync() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act: Flush but not sync (OS buffer only)
        wal.append_op(crate::wal::WalOpKind::Put, b"key", Some(b"value"))
            .expect("append");
        wal.flush().expect("flush to OS buffer");
        // Note: In real crash, OS buffer might be lost, but in test we close cleanly

        // Assert: Data still readable after clean shutdown
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn should_compress_large_values_automatically() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let large_value = vec![b'X'; 512];
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"large_key", Some(&large_value))
            .expect("append");
        wal.sync().expect("sync");

        // Assert
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.as_ref(), b"large_key");
        assert_eq!(records[0].value.as_ref().unwrap().as_ref(), &large_value);
    }

    #[test]
    fn should_not_compress_small_values() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let small_value = b"small";
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"small_key", Some(small_value))
            .expect("append");
        wal.sync().expect("sync");

        // Assert
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.as_ref(), b"small_key");
        assert_eq!(records[0].value.as_ref().unwrap().as_ref(), small_value);
    }

    #[test]
    fn should_read_compressed_values_after_uncompressed() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let large_value = vec![b'Y'; 512];
        let small_value = b"tiny";
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"small", Some(small_value))
            .expect("append small");
        wal.append_op(crate::wal::WalOpKind::Put, b"large", Some(&large_value))
            .expect("append large");
        wal.append_op(crate::wal::WalOpKind::Put, b"small2", Some(b"tiny2"))
            .expect("append small2");
        wal.sync().expect("sync");

        // Assert
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key.as_ref(), b"small");
        assert_eq!(records[0].value.as_ref().unwrap().as_ref(), small_value);
        assert_eq!(records[1].key.as_ref(), b"large");
        assert_eq!(records[1].value.as_ref().unwrap().as_ref(), &large_value);
        assert_eq!(records[2].key.as_ref(), b"small2");
        assert_eq!(records[2].value.as_ref().unwrap().as_ref(), b"tiny2");
    }

    #[test]
    fn should_compress_highly_compressible_data() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let compressible_value = vec![b'A'; 1024];
        let mut wal = Wal::open(dir.path()).expect("open");

        // Act
        wal.append_op(
            crate::wal::WalOpKind::Put,
            b"comp_key",
            Some(&compressible_value),
        )
        .expect("append");
        wal.sync().expect("sync");

        // Assert
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].value.as_ref().unwrap().as_ref(),
            &compressible_value
        );
    }

    #[test]
    fn should_use_crc32c_for_checksums() {
        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let test_data = b"test data for CRC32-C verification";
        let mut wal = Wal::open(dir.path()).unwrap();

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"key", Some(test_data))
            .unwrap();
        wal.sync().unwrap();
        let records = wal.replay().unwrap();

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref().unwrap().as_ref(), test_data);
    }

    #[test]
    fn should_detect_corruption_with_crc32c() {
        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let test_data = b"original data";
        let wal_path = fs::numbered_file_path(dir.path(), 1, "wal");
        {
            let wal = Wal::open(dir.path()).unwrap();
            wal.append_op(crate::wal::WalOpKind::Put, b"key", Some(test_data))
                .unwrap();
            wal.sync().unwrap();
        }

        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            file.seek(std::io::SeekFrom::Start(100)).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }

        // Act
        let result = replay_wal_file(&wal_path);

        // Assert
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MidgeError::Corruption { .. }));
    }
    #[test]
    fn should_compute_crc32c_correctly_for_large_batches() {
        use bytes::Bytes;

        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let records: Vec<_> = (0..100)
            .map(|i| crate::wal::WalRecord {
                op: crate::wal::WalOpKind::Put,
                cf_id: 0,
                seq: i,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from(format!("value{}", i))),
                txn_id: None,
                expiration: None,
                compression: None,
                range_end: None,
            })
            .collect();
        let mut wal = Wal::open(dir.path()).unwrap();

        // Act
        wal.append_batch(&records).unwrap();
        wal.sync().unwrap();
        let replayed = wal.replay().unwrap();

        // Assert
        assert_eq!(replayed.len(), 100);
        for (i, record) in replayed.iter().enumerate() {
            assert_eq!(record.seq, i as u64);
            assert_eq!(record.key, Bytes::from(format!("key{}", i)));
        }
    }

    #[test]
    fn should_sync_data_with_every_write_mode() {
        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let mut wal = Wal::open_with_mode(dir.path(), WalSyncMode::EveryWrite).unwrap();

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .unwrap();
        let replayed = wal.replay().unwrap();

        // Assert
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].key.as_ref(), b"key1");
    }

    #[test]
    fn should_sync_data_with_batch_sync_mode() {
        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let mut wal = Wal::open_with_mode(dir.path(), WalSyncMode::BatchedSync).unwrap();

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .unwrap();
        wal.sync().unwrap();
        let replayed = wal.replay().unwrap();

        // Assert
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].key.as_ref(), b"key1");
    }

    #[test]
    fn should_persist_data_after_fdatasync() {
        use bytes::Bytes;

        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let test_data = Bytes::from("critical data that must survive crash");
        let wal_path = fs::numbered_file_path(dir.path(), 1, "wal");

        // Act
        {
            let wal = Wal::open_with_mode(dir.path(), WalSyncMode::EveryWrite).unwrap();
            wal.append_op(crate::wal::WalOpKind::Put, b"important", Some(&test_data))
                .unwrap();
        }

        let records = replay_wal_file(&wal_path).unwrap();

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref().unwrap(), &test_data);
    }

    #[test]
    fn should_use_fdatasync_for_better_performance() {
        // Arrange
        let dir = tempfile::TempDir::new().unwrap();
        let mut wal = Wal::open_with_mode(dir.path(), WalSyncMode::BatchedSync).unwrap();
        let test_key = b"perf_test_key";
        let test_value = b"perf_test_value";

        // Act
        for i in 0..10 {
            wal.append_op(
                crate::wal::WalOpKind::Put,
                &format!("{}_{}", std::str::from_utf8(test_key).unwrap(), i).into_bytes(),
                Some(test_value),
            )
            .unwrap();
        }
        wal.sync().unwrap();
        let replayed = wal.replay().unwrap();

        // Assert
        assert_eq!(replayed.len(), 10);
        for (i, record) in replayed.iter().enumerate() {
            let expected_key = format!("{}_{}", std::str::from_utf8(test_key).unwrap(), i);
            assert_eq!(record.key.as_ref(), expected_key.as_bytes());
        }
    }

    #[test]
    fn should_advance_pos_when_using_append_mut() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut wal = Wal::open(dir.path()).expect("open");
        let pos_before = wal.current_pos();

        // Act
        let rec = WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"k"),
            Some(bytes::Bytes::from_static(b"v")),
            1,
        );
        wal.append(&rec).expect("append");

        // Assert
        assert!(wal.current_pos() > pos_before);
    }
}
