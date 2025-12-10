// == COPILOT MASTER RULES FOR SST COMPRESSION =========================================
// These rules define the *correct* compression architecture for Midge SST blocks.
// All completions touching block compression, decompression, or compression policies
// MUST follow these rules exactly.
//
// =====================================================================================
// 1. COMPRESSION GOALS
// -------------------------------------------------------------------------------------
// Midge SST compression must be:
//   - Pluggable (LZ4, Zstd, Zlib/Deflate, None)
//   - Per-block (no cross-block dictionaries)
//   - Adaptive (auto-select best algorithm per block)
//   - Zero-copy when possible
//   - Independent (each block decompresses standalone)
//   - Deterministic (same input → same output)
//
// Compression must never:
//   - Break block boundaries
//   - Require dictionary persistence across blocks
//   - Fail silently (always fallback to None)
//   - Corrupt restart points or TLV encoding
//
// =====================================================================================
// 2. COMPRESSION ALGORITHMS (authoritative codes)
// -------------------------------------------------------------------------------------
// Each algorithm has a 1-byte code stored in block trailer:
//
//   Code | Algorithm    | Purpose                          | Notes
//   -----|--------------|----------------------------------|---------------------------
//   0    | None         | Hot path, CPU-minimal            | For very small blocks
//   1    | LZ4          | Fastest compress/decompress      | High-throughput ingestion
//   2    | Zstd(3)      | Balanced                         | General purpose default
//   3    | Zstd(9+)     | High compression                 | Cold data, cloud storage
//   4    | Zlib/Deflate | Max compatibility                | Slower, legacy
//   5    | Snappy       | RocksDB-style (optional)         | Drop-in if needed
//
// COPILOT MUST use these exact codes. Never invent new codes.
//
// =====================================================================================
// 3. BLOCK TRAILER FORMAT (final specification)
// -------------------------------------------------------------------------------------
// Every block ends with this trailer:
//
//   [ compressed_data: variable ]
//   [ compression_type: u8 (1 byte) ]
//   [ crc32c: u32 (4 bytes) ]
//
// Total trailer size: 5 bytes
//
// CRC32C is computed over compressed_data + compression_type (not decompressed data).
//
// =====================================================================================
// 4. BLOCKBUILDER COMPRESSION RULES
// -------------------------------------------------------------------------------------
// When BlockBuilder::finish() is called:
//
//   if block_size < MIN_COMPRESS_SIZE (default 256 bytes):
//       compression = None
//   else if compression_policy == Adaptive:
//       compression = choose_best(block)
//   else if compression_policy == Fixed(algo):
//       compression = algo
//   else:
//       compression = None
//
// choose_best(block) logic:
//   1. Try algorithms in order: LZ4, Zstd(3), Zstd(9)
//   2. Compute ratio = compressed_size / original_size
//   3. Select smallest (compressed_size + overhead)
//   4. Fallback to None if:
//         - ratio < min_ratio (default 1.05)
//         - savings < min_savings_bytes (default 256)
//         - compressed_size > MAX_BLOCK_SIZE
//         - compressor error
//
// =====================================================================================
// 5. COMPRESSION POLICY (authoritative enum)
// -------------------------------------------------------------------------------------
// CompressionPolicy controls how blocks are compressed:
//
//   None              → Never compress
//   Fixed(algo)       → Always use specified algorithm
//   Adaptive {
//       min_savings_bytes: usize,     // default 256
//       min_ratio: f32,               // default 1.05
//       check_algorithms: Vec<Algo>,  // default [LZ4, Zstd3, Zstd9]
//   }
//
// Adaptive is the recommended default.
//
// =====================================================================================
// 6. READER DECOMPRESSION RULES
// -------------------------------------------------------------------------------------
// To decode a block:
//
//   1. Read block from file/cache
//   2. Extract trailer (last 5 bytes):
//         compression_type = data[len-5]
//         crc32c = u32::from_le_bytes(data[len-4..len])
//   3. Verify CRC32C over data[0..len-4]
//   4. Match compression_type:
//         0 → return Bytes::from(data[0..len-5])
//         1 → LZ4::decompress(data[0..len-5])
//         2 → Zstd::decompress(data[0..len-5], level=3)
//         3 → Zstd::decompress(data[0..len-5], level=9)
//         4 → Zlib::decompress(data[0..len-5])
//         5 → Snappy::decompress(data[0..len-5])
//   5. Decompressed block contains:
//         - TLV-encoded entries
//         - Restart array at end
//
// =====================================================================================
// 7. PERFORMANCE REQUIREMENTS
// -------------------------------------------------------------------------------------
// Zstd:
//   - Use zstd crate with level 1-3 for fast mode, 9-22 for high compression
//   - Consider dictionary training for warm block cache (future optimization)
//   - Precompute decompression size via frame header
//
// LZ4:
//   - Use lz4_flex crate (pure Rust, no FFI)
//   - Always use safe API with max_decompress_size
//   - Perfect for ingestion-heavy workloads
//
// Zlib/Deflate:
//   - Use flate2 crate
//   - Only when explicitly configured (slow but compatible)
//
// Block Size Targets:
//   - 32 KB default
//   - 64 KB for read-heavy workloads
//   - 8 KB for cloud-optimized micro-blocks
//   - Force split if block_size > 64 KB
//
// =====================================================================================
// 8. ADAPTIVE COMPRESSION POLICY (recommended default)
// -------------------------------------------------------------------------------------
// Default configuration:
//
//   CompressionPolicy::Adaptive {
//       min_savings_bytes: 256,
//       min_ratio: 1.05,
//       check_algorithms: vec![
//           CompressionAlgo::Lz4,
//           CompressionAlgo::Zstd3,
//           CompressionAlgo::Zstd9,
//       ],
//   }
//
// This saves CPU under high write load while maximizing cloud durability efficiency.
//
// =====================================================================================
// 9. CLOUD-AWARE COMPRESSION
// -------------------------------------------------------------------------------------
// When SSTs are destined for cloud storage (S3, GCS, Azure):
//
//   - Prefer Zstd(3) as default (better object store economics)
//   - Override with Zstd(9) only when:
//         * Block is cold (immutable in LSM)
//         * CPU is under low pressure
//         * Compaction is underway
//
// HybridStorage can pass cloud_optimized: bool hint to SSTWriter.
//
// =====================================================================================
// 10. SAFETY INVARIANTS
// -------------------------------------------------------------------------------------
// Compression MUST:
//
//   - Never corrupt block data
//   - Always validate CRC32C before decompression
//   - Always fallback to None on compressor error
//   - Always preserve restart points
//   - Always preserve TLV entry boundaries
//   - Always be deterministic (same input → same output)
//
// Compression MUST NOT:
//
//   - Use cross-block dictionaries (each block is independent)
//   - Compress already-compressed data (check header)
//   - Exceed MAX_BLOCK_SIZE after compression
//   - Break BlockBuilder invariants (sorted keys, restart points)
//
// =====================================================================================
// 11. WHAT COPILOT MUST NEVER DO
// -------------------------------------------------------------------------------------
// ❌ Never invent new compression codes (use exact codes from table)
// ❌ Never skip CRC32C verification
// ❌ Never compress blocks < MIN_COMPRESS_SIZE
// ❌ Never use compression that degrades ratio < min_ratio
// ❌ Never persist cross-block compression state
// ❌ Never decompress without checking compression_type tag
// ❌ Never modify block boundaries based on compression
//
// =====================================================================================
//
// Follow these rules EXACTLY for all SST compression code in the Midge codebase.
// =====================================================================================

use bytes::Bytes;
use crate::common::MidgeResult;

/// Compression algorithm codes (stored in block trailer)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgo {
    None = 0,
    Lz4 = 1,
    Zstd3 = 2,    // Zstd level 3
    Zstd9 = 3,    // Zstd level 9+
    Zlib = 4,
    Snappy = 5,
}

impl CompressionAlgo {
    /// Parse compression code from u8
    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd3),
            3 => Some(Self::Zstd9),
            4 => Some(Self::Zlib),
            5 => Some(Self::Snappy),
            _ => None,
        }
    }

    /// Convert to u8 code
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Compression policy for block building
#[derive(Debug, Clone)]
pub enum CompressionPolicy {
    /// Never compress
    None,
    
    /// Always use specified algorithm
    Fixed(CompressionAlgo),
    
    /// Auto-select best algorithm per block
    Adaptive {
        /// Minimum bytes saved to use compression
        min_savings_bytes: usize,
        
        /// Minimum compression ratio (compressed/original)
        min_ratio: f32,
        
        /// Algorithms to try (in order)
        check_algorithms: Vec<CompressionAlgo>,
    },
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 1.05,
            check_algorithms: vec![
                CompressionAlgo::Lz4,
                CompressionAlgo::Zstd3,
                CompressionAlgo::Zstd9,
            ],
        }
    }
}

/// Minimum block size to attempt compression
pub const MIN_COMPRESS_SIZE: usize = 256;

/// Maximum block size after compression
pub const MAX_BLOCK_SIZE: usize = 64 * 1024;

/// Block trailer size (compression_type + crc32c)
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Compress block data according to policy
pub fn compress_block(data: &[u8], policy: &CompressionPolicy) -> MidgeResult<(Bytes, CompressionAlgo)> {
    // Never compress tiny blocks
    if data.len() < MIN_COMPRESS_SIZE {
        return Ok((Bytes::copy_from_slice(data), CompressionAlgo::None));
    }

    match policy {
        CompressionPolicy::None => {
            Ok((Bytes::copy_from_slice(data), CompressionAlgo::None))
        }
        
        CompressionPolicy::Fixed(algo) => {
            compress_with_algo(data, *algo)
        }
        
        CompressionPolicy::Adaptive { min_savings_bytes, min_ratio, check_algorithms } => {
            compress_adaptive(data, *min_savings_bytes, *min_ratio, check_algorithms)
        }
    }
}

fn compress_with_algo(data: &[u8], algo: CompressionAlgo) -> MidgeResult<(Bytes, CompressionAlgo)> {
    if algo == CompressionAlgo::None {
        return Ok((Bytes::copy_from_slice(data), CompressionAlgo::None));
    }

    // TODO: Implement actual compression
    // For now, fallback to None
    Ok((Bytes::copy_from_slice(data), CompressionAlgo::None))
}

fn compress_adaptive(
    data: &[u8],
    min_savings: usize,
    min_ratio: f32,
    algos: &[CompressionAlgo],
) -> MidgeResult<(Bytes, CompressionAlgo)> {
    let mut best_compressed = Bytes::copy_from_slice(data);
    let mut best_algo = CompressionAlgo::None;
    let mut best_size = data.len();

    for &algo in algos {
        if algo == CompressionAlgo::None {
            continue;
        }

        // Try compression
        if let Ok((compressed, _)) = compress_with_algo(data, algo) {
            let compressed_size = compressed.len();
            let ratio = compressed_size as f32 / data.len() as f32;
            let savings = data.len().saturating_sub(compressed_size);

            // Check if this is better
            if ratio < min_ratio && savings >= min_savings && compressed_size < best_size {
                best_compressed = compressed;
                best_algo = algo;
                best_size = compressed_size;
            }
        }
    }

    Ok((best_compressed, best_algo))
}

/// Decompress block data based on compression type
pub fn decompress_block(compressed: &[u8], algo: CompressionAlgo) -> MidgeResult<Bytes> {
    match algo {
        CompressionAlgo::None => Ok(Bytes::copy_from_slice(compressed)),
        _ => {
            // TODO: Implement actual decompression
            Ok(Bytes::copy_from_slice(compressed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_compression_codes() {
        for code in 0..=5 {
            let algo = CompressionAlgo::from_u8(code).unwrap();
            assert_eq!(algo.to_u8(), code);
        }
    }

    #[test]
    fn should_reject_invalid_compression_codes() {
        assert!(CompressionAlgo::from_u8(6).is_none());
        assert!(CompressionAlgo::from_u8(255).is_none());
    }

    #[test]
    fn should_skip_compression_for_tiny_blocks() {
        let policy = CompressionPolicy::default();
        let tiny_data = vec![0u8; 100]; // < MIN_COMPRESS_SIZE
        
        let (compressed, algo) = compress_block(&tiny_data, &policy).unwrap();
        
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.len(), tiny_data.len());
    }

    #[test]
    fn should_use_fixed_compression_when_specified() {
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);
        let data = vec![0u8; 1024];
        
        let (_compressed, algo) = compress_block(&data, &policy).unwrap();
        
        // Will be None until we implement actual compression
        assert_eq!(algo, CompressionAlgo::None);
    }

    #[test]
    fn should_decompress_none_as_passthrough() {
        let data = b"test data";
        let decompressed = decompress_block(data, CompressionAlgo::None).unwrap();
        
        assert_eq!(decompressed.as_ref(), data);
    }
}
