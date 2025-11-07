//! Parallel WAL encode + CRC32C pipeline with zero-copy batch encoding.
//!
//! - Encodes `WalRecord` bodies into a contiguous arena (zero per-record allocations).
//! - Computes CRC32C streaming during encode (no second pass).
//! - Returns headers + single contiguous body buffer suitable for vectored I/O.
//! - Thread-safe parallel path uses TLS buffers to avoid locks.
//! - Optimized for both sequential and parallel workloads.

use crc32c::crc32c;
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::cell::RefCell;

use crate::common::codec::Compressor;
use crate::error::{MidgeError, MidgeResult};
use crate::wal::WalRecord;

thread_local! {
    static TLS_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(8 * 1024));
}

// Global reusable ThreadPool to avoid repeated thread creation across benchmarks
static GLOBAL_WAL_POOL: OnceCell<ThreadPool> = OnceCell::new();

fn get_global_pool(num_threads: usize) -> &'static ThreadPool {
    GLOBAL_WAL_POOL.get_or_init(|| {
        ThreadPoolBuilder::new()
            .thread_name(|i| format!("wal-enc-{}", i))
            .num_threads(num_threads)
            .build()
            .expect("failed to build global wal encode thread pool")
    })
}

/// What the WAL writer needs to write: header (8) + body.
#[derive(Debug)]
pub struct EncodedFragment {
    pub header: [u8; 8], // [CRC32C LE (4)] [LEN LE (4)]
    pub body: Vec<u8>,
}

impl EncodedFragment {
    #[inline]
    pub fn total_len(&self) -> usize {
        self.header.len() + self.body.len()
    }
}

/// Batch of encoded WAL records with contiguous body arena.
///
/// This layout enables zero-copy vectored I/O: headers and body slices
/// can be written directly via `write_vectored` without concatenation.
#[derive(Debug)]
pub struct EncodedBatch {
    /// Pre-computed headers: [CRC32C (4 bytes LE) | LEN (4 bytes LE)]
    pub headers: Vec<[u8; 8]>,
    /// All encoded bodies back-to-back in a single contiguous buffer
    pub bodies: Vec<u8>,
    /// (start_offset, length) for each record's body in the bodies buffer
    pub offsets: Vec<(usize, usize)>,
}

impl EncodedBatch {
    /// Total bytes (headers + bodies)
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.headers.len() * 8 + self.bodies.len()
    }

    /// Number of records in this batch
    #[inline]
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
}

/// Pluggable body encoder with zero-copy interface.
///
/// Writes encoded body into the provided buffer and returns bytes written.
/// Additionally it may return a precomputed CRC32C for the appended slice to
/// avoid a second pass over the data (streaming CRC). If `None` is returned
/// the caller will compute the CRC by scanning the appended slice.
pub trait BodyEncoder: Send + Sync {
    /// Encode record body into `dst`, appending to existing content.
    /// Returns (bytes_written, optional_precomputed_crc32c).
    fn encode_body_into(
        &self,
        rec: &WalRecord,
        dst: &mut Vec<u8>,
    ) -> MidgeResult<(usize, Option<u32>)>;
}

/// Default encoder using existing TLV encoder.
pub struct DefaultBodyEncoder;

impl BodyEncoder for DefaultBodyEncoder {
    #[inline]
    fn encode_body_into(
        &self,
        rec: &WalRecord,
        dst: &mut Vec<u8>,
    ) -> MidgeResult<(usize, Option<u32>)> {
        let start = dst.len();
        crate::wal::encoding::encode_into(rec, dst)?;
        Ok((dst.len() - start, None))
    }
}

/// Streaming encoder that writes TLV fields directly into the destination buffer
/// while updating a running CRC32C (no second pass).
#[derive(Default)]
pub struct StreamingBodyEncoder;

impl StreamingBodyEncoder {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }

    // Helper: encode varint32 into local small buffer and push to dst,
    // updating crc via crc32c::crc32c_append.
    #[inline(always)]
    fn write_varint32_and_update_crc(dst: &mut Vec<u8>, crc: &mut u32, mut v: u32) {
        // varint32 fits in at most 5 bytes
        let mut tmp = [0u8; 5];
        let mut i = 0usize;
        while v >= 0x80 {
            tmp[i] = (v as u8) | 0x80;
            v >>= 7;
            i += 1;
        }
        tmp[i] = v as u8;
        i += 1;
        // update crc and append
        *crc = crc32c::crc32c_append(*crc, &tmp[..i]);
        dst.extend_from_slice(&tmp[..i]);
    }

    #[inline(always)]
    fn write_u8_and_update_crc(dst: &mut Vec<u8>, crc: &mut u32, tag: u8, val: u8) {
        let buf = [tag, val];
        *crc = crc32c::crc32c_append(*crc, &buf);
        dst.extend_from_slice(&buf);
    }

    #[inline(always)]
    fn write_u32_be_and_update_crc(dst: &mut Vec<u8>, crc: &mut u32, tag: u8, val: u32) {
        let mut buf = [0u8; 5];
        buf[0] = tag;
        buf[1..5].copy_from_slice(&val.to_be_bytes());
        *crc = crc32c::crc32c_append(*crc, &buf);
        dst.extend_from_slice(&buf);
    }

    #[inline(always)]
    fn write_u64_be_and_update_crc(dst: &mut Vec<u8>, crc: &mut u32, tag: u8, val: u64) {
        let mut buf = [0u8; 9];
        buf[0] = tag;
        buf[1..9].copy_from_slice(&val.to_be_bytes());
        *crc = crc32c::crc32c_append(*crc, &buf);
        dst.extend_from_slice(&buf);
    }

    #[inline(always)]
    fn write_bytes_and_update_crc(dst: &mut Vec<u8>, crc: &mut u32, tag: u8, data: &[u8]) {
        // tag
        *crc = crc32c::crc32c_append(*crc, &[tag]);
        dst.push(tag);
        // varint32 length
        Self::write_varint32_and_update_crc(dst, crc, data.len() as u32);
        // payload
        *crc = crc32c::crc32c_append(*crc, data);
        dst.extend_from_slice(data);
    }
}

impl BodyEncoder for StreamingBodyEncoder {
    fn encode_body_into(
        &self,
        rec: &WalRecord,
        dst: &mut Vec<u8>,
    ) -> MidgeResult<(usize, Option<u32>)> {
        use crate::common::codec::Lz4Codec;
        use crate::common::tlv::tags;

        let start = dst.len();
        // running crc (crc32c append supports incremental updates)
        let mut crc: u32 = 0;

        // Required fields
        Self::write_u8_and_update_crc(dst, &mut crc, tags::OPERATION, rec.op.to_wire_format());
        Self::write_u32_be_and_update_crc(dst, &mut crc, tags::CF_ID, rec.cf_id);
        Self::write_u64_be_and_update_crc(dst, &mut crc, tags::SEQUENCE, rec.seq);
        Self::write_bytes_and_update_crc(dst, &mut crc, tags::KEY, &rec.key);

        // Optional value field with compression support
        if let Some(ref value) = rec.value {
            // mirror logic from write_value_with_compression
            if value.len() < crate::wal::encoding::COMPRESSION_THRESHOLD {
                Self::write_bytes_and_update_crc(dst, &mut crc, tags::VALUE, value);
            } else {
                let codec = Lz4Codec;
                match codec.compress(value) {
                    Ok(compressed) if compressed.len() < value.len() => {
                        // write compression tag then compressed value
                        Self::write_u8_and_update_crc(dst, &mut crc, tags::COMPRESSION, 2);
                        Self::write_bytes_and_update_crc(
                            dst,
                            &mut crc,
                            tags::VALUE_COMPRESSED,
                            &compressed,
                        );
                    }
                    _ => {
                        Self::write_bytes_and_update_crc(dst, &mut crc, tags::VALUE, value);
                    }
                }
            }
        }

        // Optional expiration
        if let Some(expiration) = rec.expiration {
            Self::write_u64_be_and_update_crc(dst, &mut crc, tags::EXPIRATION, expiration);
        }

        // Optional range_end
        if let Some(ref range_end) = rec.range_end {
            Self::write_bytes_and_update_crc(dst, &mut crc, tags::RANGE_END, range_end);
        }

        // Optional txn_id
        if let Some(txn_id) = rec.txn_id {
            Self::write_u64_be_and_update_crc(dst, &mut crc, tags::TRANSACTION_ID, txn_id);
        }

        let written = dst.len() - start;
        Ok((written, Some(crc)))
    }
}

// Helper functions for zero-copy encoding

/// Encode a single record into an existing buffer, returning its header.
#[inline]
fn encode_one_into<E: BodyEncoder>(
    enc: &E,
    max_len: usize,
    rec: &WalRecord,
    out_body: &mut Vec<u8>,
) -> MidgeResult<[u8; 8]> {
    let start = out_body.len();
    let (written, opt_crc) = enc.encode_body_into(rec, out_body)?;
    let body_len = written;

    if body_len > max_len {
        out_body.truncate(start);
        return Err(MidgeError::WalError {
            message: format!("WAL record too large: {} bytes", body_len),
        });
    }

    let body = &out_body[start..start + body_len];
    // If encoder provided a precomputed CRC use it, otherwise compute over the body slice
    let crc = match opt_crc {
        Some(c) => c,
        None => crc32c(body),
    };
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(&crc.to_le_bytes());
    h[4..8].copy_from_slice(&(body_len as u32).to_le_bytes());
    Ok(h)
}

/// Configuration for the pipeline.
#[derive(Debug, Clone)]
pub struct EncodeConfig {
    /// How many worker threads to use (1 = single-threaded).
    pub parallelism: usize,
    /// Hard cap for a single body (u32 length in header).
    pub max_body_len: usize,
    /// Minimum total bytes across the batch to trigger parallel encoding (avoid overhead for tiny batches).
    pub parallel_threshold_bytes: usize,
}

impl Default for EncodeConfig {
    fn default() -> Self {
        Self {
            parallelism: std::cmp::max(1, num_cpus::get_physical()),
            max_body_len: u32::MAX as usize,
            // Crossover point: parallel wins when total batch size >= 128 KiB (configurable).
            // Below this, thread coordination cost (locks, atomics, work-stealing) dominates.
            // Use bytes as the trigger gives better behavior for variable-length payloads.
            parallel_threshold_bytes: 128 * 1024,
        }
    }
}

/// Encoder with optional rayon pool.
pub struct WalEncoder<E: BodyEncoder> {
    cfg: EncodeConfig,
    enc: E,
    // Use a static reference to a global pool to minimize thread creation overhead.
    pool: Option<&'static ThreadPool>,
}

impl<E: BodyEncoder> WalEncoder<E> {
    pub fn new(cfg: EncodeConfig, enc: E) -> MidgeResult<Self> {
        let pool = if cfg.parallelism > 1 {
            // Initialize (or get) the global pool with requested parallelism. If multiple
            // callers request different sizes, the first wins. This trades flexibility
            // for the benefit of reusing threads across encoders/bench runs.
            Some(get_global_pool(cfg.parallelism))
        } else {
            None
        };

        Ok(Self { cfg, enc, pool })
    }

    /// Encode + checksum a single record (legacy API, returns EncodedFragment).
    ///
    /// For batch operations, prefer `encode_batch_arena()` which avoids per-record allocations.
    #[inline]
    pub fn encode_one(&self, rec: &WalRecord) -> MidgeResult<EncodedFragment> {
        let mut body = Vec::new();
        let header = encode_one_into(&self.enc, self.cfg.max_body_len, rec, &mut body)?;
        Ok(EncodedFragment { header, body })
    }

    /// Encode + checksum a batch with zero-copy arena allocation.
    ///
    /// Returns a contiguous buffer suitable for vectored I/O. Sequential path uses
    /// a single arena (no per-record allocations). Parallel path uses TLS buffers
    /// per thread, then stitches results into a contiguous buffer.
    pub fn encode_batch_arena(&self, recs: &[WalRecord]) -> MidgeResult<EncodedBatch> {
        if recs.is_empty() {
            return Ok(EncodedBatch {
                headers: Vec::new(),
                bodies: Vec::new(),
                offsets: Vec::new(),
            });
        }

        // Estimate total bytes: key + value + ~16 bytes TLV overhead per record
        let est_bytes: usize = recs
            .iter()
            .map(|r| r.key.len() + r.value.as_ref().map_or(0, |v| v.len()) + 16)
            .sum();

        let use_parallel = self.pool.is_some() && est_bytes >= self.cfg.parallel_threshold_bytes;

        if !use_parallel {
            // SEQUENTIAL PATH: single arena, zero extra copies
            let mut headers = Vec::with_capacity(recs.len());
            let mut bodies = Vec::with_capacity(est_bytes);
            let mut offsets = Vec::with_capacity(recs.len());

            for rec in recs {
                let start = bodies.len();
                let header = encode_one_into(&self.enc, self.cfg.max_body_len, rec, &mut bodies)?;
                let len = bodies.len() - start;
                headers.push(header);
                offsets.push((start, len));
            }

            return Ok(EncodedBatch {
                headers,
                bodies,
                offsets,
            });
        }

        // PARALLEL PATH: TLS buffers, then stitch into contiguous buffer
        let pool = self
            .pool
            .expect("parallel encode path requires a ThreadPool; invariant violated");
        let enc = &self.enc;
        let max_len = self.cfg.max_body_len;

        let n = recs.len();
        let threads = std::cmp::min(self.cfg.parallelism, n);
        let chunk_size = n.div_ceil(threads);

        pool.install(|| {
            // Parallelize at the chunk (per-worker) level to avoid per-record Vec copies
            let chunks = recs
                .par_chunks(chunk_size)
                .map(|chunk| -> MidgeResult<EncodedBatch> {
                    // Estimate chunk bytes to reserve
                    let est: usize = chunk
                        .iter()
                        .map(|r| r.key.len() + r.value.as_ref().map_or(0, |v| v.len()) + 16)
                        .sum();
                    let mut headers = Vec::with_capacity(chunk.len());
                    let mut bodies = Vec::with_capacity(est);
                    let mut offsets = Vec::with_capacity(chunk.len());

                    for rec in chunk {
                        let start = bodies.len();
                        let header = encode_one_into(enc, max_len, rec, &mut bodies)?;
                        let len = bodies.len() - start;
                        headers.push(header);
                        offsets.push((start, len));
                    }

                    Ok(EncodedBatch {
                        headers,
                        bodies,
                        offsets,
                    })
                })
                .collect::<MidgeResult<Vec<EncodedBatch>>>()?;

            // Stitch chunk results into a single contiguous EncodedBatch (preserve order)
            let total_records: usize = chunks.iter().map(|c| c.len()).sum();
            let total_body_bytes: usize = chunks.iter().map(|c| c.bodies.len()).sum();

            let mut headers = Vec::with_capacity(total_records);
            let mut bodies = Vec::with_capacity(total_body_bytes);
            let mut offsets = Vec::with_capacity(total_records);

            for chunk in chunks {
                for (i, h) in chunk.headers.into_iter().enumerate() {
                    let (s, l) = chunk.offsets[i];
                    let start = bodies.len();
                    bodies.extend_from_slice(&chunk.bodies[s..s + l]);
                    headers.push(h);
                    offsets.push((start, l));
                }
            }

            Ok(EncodedBatch {
                headers,
                bodies,
                offsets,
            })
        })
    }

    /// Legacy batch encode API (returns Vec<EncodedFragment>).
    ///
    /// Prefer `encode_batch_arena()` for better performance with large batches.
    pub fn encode_batch(&self, recs: &[WalRecord]) -> MidgeResult<Vec<EncodedFragment>> {
        let batch = self.encode_batch_arena(recs)?;

        // Convert EncodedBatch to Vec<EncodedFragment> for compatibility
        let mut fragments = Vec::with_capacity(batch.len());
        for (i, header) in batch.headers.iter().enumerate() {
            let (start, len) = batch.offsets[i];
            let body = batch.bodies[start..start + len].to_vec();
            fragments.push(EncodedFragment {
                header: *header,
                body,
            });
        }

        Ok(fragments)
    }
}

// ---------- convenience constructors ----------

impl WalEncoder<DefaultBodyEncoder> {
    /// Use your default TLV encoder.
    pub fn with_defaults() -> MidgeResult<Self> {
        Self::new(EncodeConfig::default(), DefaultBodyEncoder)
    }

    pub fn with_config(cfg: EncodeConfig) -> MidgeResult<Self> {
        Self::new(cfg, DefaultBodyEncoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalOpKind;
    use bytes::Bytes;

    #[test]
    fn should_encode_single_record() {
        // Arrange
        let encoder = WalEncoder::with_defaults().unwrap();
        let rec = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
        );

        // Act
        let result = encoder.encode_one(&rec);

        // Assert
        assert!(result.is_ok());
        let fragment = result.unwrap();
        assert_eq!(fragment.header.len(), 8);
        assert!(!fragment.body.is_empty());
    }

    #[test]
    fn should_encode_empty_batch() {
        // Arrange
        let encoder = WalEncoder::with_defaults().unwrap();

        // Act
        let result = encoder.encode_batch(&[]);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn should_encode_small_batch_sequentially() {
        // Arrange
        let encoder = WalEncoder::with_defaults().unwrap();
        let recs = vec![
            WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                1,
            ),
            WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"value2")),
                2,
            ),
        ];

        // Act
        let result = encoder.encode_batch(&recs);

        // Assert
        assert!(result.is_ok());
        let fragments = result.unwrap();
        assert_eq!(fragments.len(), 2);
    }

    #[test]
    fn should_encode_large_batch_in_parallel() {
        // Arrange
        let cfg = EncodeConfig {
            parallelism: 4,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        };
        let encoder = WalEncoder::new(cfg, DefaultBodyEncoder).unwrap();

        let recs: Vec<_> = (0..100)
            .map(|i| {
                WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from(format!("key{}", i)),
                    Some(Bytes::from(format!("value{}", i))),
                    i as u64,
                )
            })
            .collect();

        // Act
        let result = encoder.encode_batch(&recs);

        // Assert
        assert!(result.is_ok());
        let fragments = result.unwrap();
        assert_eq!(fragments.len(), 100);
    }

    #[test]
    fn should_reject_oversized_record() {
        // Arrange
        let cfg = EncodeConfig {
            parallelism: 1,
            max_body_len: 50, // Very small limit (needs to be smaller than encoded size)
            parallel_threshold_bytes: 1,
        };
        let encoder = WalEncoder::new(cfg, DefaultBodyEncoder).unwrap();

        let large_value = vec![0u8; 100]; // This will create an encoded body > 50 bytes
        let rec = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from(large_value)),
            1,
        );

        // Act
        let result = encoder.encode_one(&rec);

        // Assert
        assert!(result.is_err());
        match result {
            Err(MidgeError::WalError { message }) => {
                assert!(message.contains("too large"));
            }
            _ => panic!("Expected WalError"),
        }
    }

    #[test]
    fn should_preserve_order_in_parallel_encoding() {
        // Arrange
        let cfg = EncodeConfig {
            parallelism: 4,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        };
        let encoder = WalEncoder::new(cfg, DefaultBodyEncoder).unwrap();

        let recs: Vec<_> = (0..10)
            .map(|i| {
                WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from(format!("key{:03}", i)),
                    Some(Bytes::from(format!("value{:03}", i))),
                    i as u64,
                )
            })
            .collect();

        // Act
        let result = encoder.encode_batch(&recs);

        // Assert
        assert!(result.is_ok());
        let fragments = result.unwrap();

        // Decode each fragment and verify order
        for (i, fragment) in fragments.iter().enumerate() {
            let decoded = crate::wal::encoding::decode(&fragment.body).unwrap();
            assert_eq!(decoded.seq, i as u64);
        }
    }

    #[test]
    fn should_use_single_threaded_mode_when_parallelism_is_one() {
        // Arrange
        let cfg = EncodeConfig {
            parallelism: 1,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        };
        let encoder = WalEncoder::new(cfg, DefaultBodyEncoder).unwrap();

        // Assert
        assert!(encoder.pool.is_none());
    }
}
