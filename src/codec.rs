//! Shared persisted block and WAL compression helpers and policies.

use crate::common::MidgeResult;
use bytes::Bytes;

/// Compression algorithm codes (stored in block trailer)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgo {
    None = 0,
    Lz4 = 1,
    Zstd3 = 2, // Zstd level 3
    Zstd9 = 3, // Zstd level 9+
}

impl CompressionAlgo {
    /// Parse compression code from u8
    #[must_use]
    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd3),
            3 => Some(Self::Zstd9),
            _ => None,
        }
    }

    /// Convert to u8 code
    #[must_use]
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Compression policy for block building
#[derive(Debug, Clone, PartialEq)]
pub enum CompressionPolicy {
    /// Never compress
    None,

    /// Use the specified algorithm for inputs at or above
    /// [`MIN_COMPRESSION_INPUT_BYTES`]. Smaller inputs remain raw because the
    /// framing and codec overhead dominates at that size.
    Fixed(CompressionAlgo),

    /// Auto-select best algorithm per block
    Adaptive {
        /// Minimum bytes saved to use compression
        min_savings_bytes: usize,

        /// Inclusive compressed/original ratio threshold
        min_ratio: f32,

        /// Algorithms to try (in order)
        check_algorithms: Vec<CompressionAlgo>,
    },
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 0.95,
            check_algorithms: vec![
                CompressionAlgo::Lz4,
                CompressionAlgo::Zstd3,
                CompressionAlgo::Zstd9,
            ],
        }
    }
}

/// Minimum SST block or WAL value size at which compression is attempted.
pub const MIN_COMPRESSION_INPUT_BYTES: usize = 256;

/// Maximum block size after compression
pub const MAX_BLOCK_SIZE: usize = 64 * 1024;

/// Hard ceiling for decompressed data from one persisted block.
pub const MAX_DECOMPRESSED_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// Block trailer size (`compression_type` + crc32c)
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Distinct safe bounds for decoding a compressed payload and reserving its
/// destination buffer.
///
/// Frames without a zstd content size need a deliberately smaller decoder
/// bound than their conservative reservation bound. Keeping both values avoids
/// a drift between the WAL streaming preflight and the SST read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecompressedLenHint {
    pub(crate) decode_limit: usize,
    pub(crate) reservation_limit: usize,
}

/// Determine the bounded decoded-size hint for one compressed payload.
///
/// The returned reservation limit is suitable for accounting before a decode;
/// the decode limit is the maximum output passed to the decoder itself.
pub(crate) fn decompressed_len_hint(
    algo: CompressionAlgo,
    compressed: &[u8],
) -> MidgeResult<DecompressedLenHint> {
    match algo {
        CompressionAlgo::None => Ok(DecompressedLenHint {
            decode_limit: compressed.len(),
            reservation_limit: compressed.len(),
        }),
        CompressionAlgo::Lz4 => {
            if compressed.len() < 4 {
                return Err(crate::common::MidgeError::Corruption(
                    "LZ4 block is missing its size prefix".to_string(),
                ));
            }
            let declared_size =
                u32::from_le_bytes([compressed[0], compressed[1], compressed[2], compressed[3]])
                    as usize;
            let declared_size = enforce_decompressed_size(declared_size, "LZ4")?;
            Ok(DecompressedLenHint {
                decode_limit: declared_size,
                reservation_limit: declared_size,
            })
        }
        CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => {
            let declared_size = match zstd::zstd_safe::get_frame_content_size(compressed) {
                Ok(Some(size)) => {
                    let size = usize::try_from(size).map_err(|_| {
                        crate::common::MidgeError::Corruption(format!(
                            "Zstd frame content size {size} exceeds addressable memory"
                        ))
                    })?;
                    Some(enforce_decompressed_size(size, "Zstd")?)
                }
                Ok(None) => None,
                Err(error) => {
                    return Err(crate::common::MidgeError::Corruption(format!(
                        "Zstd frame content size unavailable: {error}"
                    )))
                }
            };
            Ok(match declared_size {
                Some(size) => DecompressedLenHint {
                    decode_limit: size,
                    reservation_limit: size,
                },
                None => DecompressedLenHint {
                    decode_limit: MAX_BLOCK_SIZE,
                    reservation_limit: MAX_DECOMPRESSED_BLOCK_SIZE,
                },
            })
        }
    }
}

/// Compress block data according to policy
///
/// # Errors
///
/// Returns an error when the selected compression algorithm fails or is unsupported.
pub fn compress_block(
    data: &[u8],
    policy: &CompressionPolicy,
) -> MidgeResult<(Bytes, CompressionAlgo)> {
    if data.len() < MIN_COMPRESSION_INPUT_BYTES {
        return Ok((Bytes::copy_from_slice(data), CompressionAlgo::None));
    }

    match policy {
        CompressionPolicy::None => Ok((Bytes::copy_from_slice(data), CompressionAlgo::None)),

        CompressionPolicy::Fixed(algo) => compress_with_algo(data, *algo),

        CompressionPolicy::Adaptive {
            min_savings_bytes,
            min_ratio,
            check_algorithms,
        } => Ok(compress_adaptive(
            data,
            *min_savings_bytes,
            *min_ratio,
            check_algorithms,
        )),
    }
}

#[inline]
fn compress_with_algo(data: &[u8], algo: CompressionAlgo) -> MidgeResult<(Bytes, CompressionAlgo)> {
    match algo {
        CompressionAlgo::None => Ok((Bytes::copy_from_slice(data), CompressionAlgo::None)),

        CompressionAlgo::Lz4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            Ok((Bytes::from(compressed), CompressionAlgo::Lz4))
        }

        CompressionAlgo::Zstd3 => {
            let compressed = zstd::bulk::compress(data, 3).map_err(|e| {
                crate::common::MidgeError::Internal(format!("zstd(3) compress failed: {e}"))
            })?;
            Ok((Bytes::from(compressed), CompressionAlgo::Zstd3))
        }

        CompressionAlgo::Zstd9 => {
            let compressed = zstd::bulk::compress(data, 9).map_err(|e| {
                crate::common::MidgeError::Internal(format!("zstd(9) compress failed: {e}"))
            })?;
            Ok((Bytes::from(compressed), CompressionAlgo::Zstd9))
        }
    }
}

fn compress_adaptive(
    data: &[u8],
    min_savings: usize,
    min_ratio: f32,
    algos: &[CompressionAlgo],
) -> (Bytes, CompressionAlgo) {
    let mut best: Option<(Bytes, CompressionAlgo)> = None;

    for &algo in algos {
        if algo == CompressionAlgo::None {
            continue;
        }

        if let Ok((compressed, _)) = compress_with_algo(data, algo) {
            let compressed_size = compressed.len();
            if compression_qualifies(data.len(), compressed_size, min_savings, min_ratio)
                && best
                    .as_ref()
                    .is_none_or(|(current, _)| compressed_size < current.len())
            {
                best = Some((compressed, algo));
            }
        }
    }

    best.unwrap_or_else(|| (Bytes::copy_from_slice(data), CompressionAlgo::None))
}

fn compression_qualifies(
    original_size: usize,
    compressed_size: usize,
    min_savings: usize,
    min_ratio: f32,
) -> bool {
    compressed_size < original_size
        && original_size.saturating_sub(compressed_size) >= min_savings
        && ratio_at_or_below_f32_threshold(original_size, compressed_size, min_ratio)
}

fn ratio_at_or_below_f32_threshold(
    original_size: usize,
    compressed_size: usize,
    threshold: f32,
) -> bool {
    if threshold.is_nan() || threshold.is_sign_negative() || original_size == 0 {
        return false;
    }

    let threshold = if threshold.is_infinite() {
        f64::INFINITY
    } else {
        let threshold_value = f64::from(threshold);
        let next_value = f64::from(f32::from_bits(threshold.to_bits().saturating_add(1)));
        threshold_value + (next_value - threshold_value) / 2.0
    };
    let original_size = f64::from(u32::try_from(original_size).unwrap_or(u32::MAX));
    let compressed_size = f64::from(u32::try_from(compressed_size).unwrap_or(u32::MAX));
    compressed_size / original_size <= threshold
}

/// Decompress block data based on compression type.
///
/// # Errors
///
/// Returns an error when the compressed data is corrupt or the algorithm is unsupported.
#[inline]
pub fn decompress_block(compressed: &[u8], algo: CompressionAlgo) -> MidgeResult<Bytes> {
    match algo {
        CompressionAlgo::None => Ok(Bytes::copy_from_slice(compressed)),

        CompressionAlgo::Lz4 => {
            let hint = decompressed_len_hint(algo, compressed)?;
            let decompressed = lz4_flex::decompress_size_prepended(compressed).map_err(|e| {
                crate::common::MidgeError::Corruption(format!("LZ4 decompression failed: {e}"))
            })?;
            debug_assert!(decompressed.len() <= hint.decode_limit);
            Ok(Bytes::from(decompressed))
        }

        CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => {
            let hint = decompressed_len_hint(algo, compressed)?;
            let decompressed =
                zstd::bulk::decompress(compressed, hint.decode_limit).map_err(|e| {
                    crate::common::MidgeError::Corruption(format!("Zstd decompression failed: {e}"))
                })?;
            Ok(Bytes::from(decompressed))
        }
    }
}

fn enforce_decompressed_size(size: usize, algorithm: &str) -> MidgeResult<usize> {
    if size > MAX_DECOMPRESSED_BLOCK_SIZE {
        return Err(crate::common::MidgeError::Corruption(format!(
            "{algorithm} declared output size {size} exceeds {MAX_DECOMPRESSED_BLOCK_SIZE} byte limit"
        )));
    }
    Ok(size)
}

/// Compress block data and append a trailer (`[compressed_data][algo:u8][crc32c:u32 LE]`).
///
/// CRC32C covers `compressed_data + compression_type`.
///
/// # Errors
///
/// Returns an error when block compression fails or the chosen algorithm is unsupported.
pub fn compress_block_with_trailer(data: &[u8], policy: &CompressionPolicy) -> MidgeResult<Bytes> {
    let (compressed, algo) = compress_block(data, policy)?;

    let algo_byte = algo.to_u8();
    let mut out = Vec::with_capacity(compressed.len() + BLOCK_TRAILER_SIZE);
    out.extend_from_slice(&compressed);
    out.push(algo_byte);

    // CRC32C over compressed_data + compression_type
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    Ok(Bytes::from(out))
}

/// Strip the block trailer, verify CRC32C, and decompress.
///
/// Input layout: `[compressed_data][algo:u8][crc32c:u32 LE]`
///
/// # Errors
///
/// Returns an error when the trailer is invalid, the CRC does not match, or
/// decompression fails.
pub fn decompress_block_with_trailer(block: &[u8]) -> MidgeResult<Bytes> {
    if block.len() < BLOCK_TRAILER_SIZE {
        return Err(crate::common::MidgeError::Corruption(
            "block too small for trailer".into(),
        ));
    }

    let data_plus_algo_len = block.len() - 4; // everything except CRC
    let compressed_data_len = block.len() - BLOCK_TRAILER_SIZE;

    // Extract trailer fields
    let algo_byte = block[compressed_data_len];
    let stored_crc = u32::from_le_bytes([
        block[data_plus_algo_len],
        block[data_plus_algo_len + 1],
        block[data_plus_algo_len + 2],
        block[data_plus_algo_len + 3],
    ]);

    // Verify CRC32C over compressed_data + algo byte
    let computed_crc = crc32c::crc32c(&block[..data_plus_algo_len]);
    if computed_crc != stored_crc {
        return Err(crate::common::MidgeError::Corruption(format!(
            "block CRC32C mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
        )));
    }

    let algo = CompressionAlgo::from_u8(algo_byte).ok_or_else(|| {
        crate::common::MidgeError::Corruption(format!(
            "unknown compression algorithm code: {algo_byte}"
        ))
    })?;

    decompress_block(&block[..compressed_data_len], algo)
}

/// Return the maximum decoded allocation required by a checksummed block.
/// Compaction uses this before decompression so its output buffer is reserved
/// against the internal memory pool first.
pub(crate) fn decompressed_size_with_trailer(block: &[u8]) -> MidgeResult<usize> {
    if block.len() < BLOCK_TRAILER_SIZE {
        return Err(crate::common::MidgeError::Corruption(
            "block too small for trailer".into(),
        ));
    }
    let compressed_data_len = block.len() - BLOCK_TRAILER_SIZE;
    let algo = CompressionAlgo::from_u8(block[compressed_data_len]).ok_or_else(|| {
        crate::common::MidgeError::Corruption(format!(
            "unknown compression algorithm code: {}",
            block[compressed_data_len]
        ))
    })?;
    let compressed = &block[..compressed_data_len];
    Ok(decompressed_len_hint(algo, compressed)?.reservation_limit)
}

/// Quick entropy check to detect if value is likely compressible.
///
/// Samples the first 256 bytes (or the full value if smaller) to detect
/// repetitive patterns that compress well. Avoids expensive LZ4 compression
/// for incompressible data like random/encrypted values.
///
/// Returns true if value is likely worth compressing.
#[inline]
fn is_likely_compressible(value: &[u8]) -> bool {
    // The sample must be large enough to tell random from structured data:
    // a 256-byte uniformly random sample holds about 162 distinct bytes,
    // while text and structured values hold far fewer. (A 32-byte sample can
    // hold at most 32, which made the old `< 200` threshold always true.)
    const SAMPLE_LEN: usize = 256;
    const MAX_DISTINCT_FOR_COMPRESSIBLE: u32 = 128;

    if value.is_empty() {
        return false;
    }
    let sample = &value[..value.len().min(SAMPLE_LEN)];
    let mut seen = [false; 256];
    let mut distinct = 0u32;
    for &byte in sample {
        if !seen[usize::from(byte)] {
            seen[usize::from(byte)] = true;
            distinct += 1;
        }
    }
    distinct <= MAX_DISTINCT_FOR_COMPRESSIBLE
}

/// Compress a WAL record value using LZ4 (fast, latency-optimal).
///
/// Returns `(compressed_value, compression_algo_byte)` or `(original_value, None)`
/// if the value is too small to benefit from compression or is incompressible.
///
/// Performs a quick entropy check before expensive LZ4 compression to avoid
/// CPU waste on random/encrypted data in constrained environments.
#[must_use]
pub fn compress_wal_value(value: &[u8]) -> (Bytes, Option<u8>) {
    if value.len() < MIN_COMPRESSION_INPUT_BYTES {
        return (Bytes::copy_from_slice(value), None);
    }

    // Fast entropy check: skip expensive compression for incompressible data
    if !is_likely_compressible(value) {
        return (Bytes::copy_from_slice(value), None);
    }

    let compressed = lz4_flex::compress_prepend_size(value);

    // Only use compression if it actually saves space
    if compressed.len() >= value.len() {
        return (Bytes::copy_from_slice(value), None);
    }

    (Bytes::from(compressed), Some(CompressionAlgo::Lz4.to_u8()))
}

/// Decompress a WAL record value based on the compression byte.
///
/// If `compression` is `None` or `Some(0)` (None algo), the value is returned as-is.
///
/// # Errors
///
/// Returns an error when the compression code is unknown or decompression fails.
pub fn decompress_wal_value(value: &[u8], compression: Option<u8>) -> MidgeResult<Bytes> {
    match compression {
        None | Some(0) => Ok(Bytes::copy_from_slice(value)),
        Some(algo_byte) => {
            let algo = CompressionAlgo::from_u8(algo_byte).ok_or_else(|| {
                crate::common::MidgeError::Corruption(format!(
                    "unknown WAL compression algorithm code: {algo_byte}"
                ))
            })?;
            decompress_block(value, algo)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ====================== CompressionAlgo Tests ======================

    #[test]
    fn should_roundtrip_none_code() {
        // Arrange
        let algo = CompressionAlgo::from_u8(0).unwrap();

        // Act

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(algo.to_u8(), 0);
    }

    #[test]
    fn should_roundtrip_lz4_code() {
        // Arrange
        let algo = CompressionAlgo::from_u8(1).unwrap();

        // Act

        // Assert
        assert_eq!(algo, CompressionAlgo::Lz4);
        assert_eq!(algo.to_u8(), 1);
    }

    #[test]
    fn should_roundtrip_zstd3_code() {
        // Arrange
        let algo = CompressionAlgo::from_u8(2).unwrap();

        // Act

        // Assert
        assert_eq!(algo, CompressionAlgo::Zstd3);
        assert_eq!(algo.to_u8(), 2);
    }

    #[test]
    fn should_roundtrip_zstd9_code() {
        // Arrange
        let algo = CompressionAlgo::from_u8(3).unwrap();

        // Act

        // Assert
        assert_eq!(algo, CompressionAlgo::Zstd9);
        assert_eq!(algo.to_u8(), 3);
    }

    #[test]
    fn should_roundtrip_all_valid_codes() {
        // Arrange
        let valid_codes = (0..=3).collect::<Vec<_>>();

        // Act
        for code in valid_codes {
            let algo = CompressionAlgo::from_u8(code).expect("valid code");
            assert_eq!(algo.to_u8(), code, "roundtrip failed for code {code}");
        }

        // Assert
    }

    #[test]
    fn should_reject_removed_compression_code_4() {
        // Arrange
        let result = CompressionAlgo::from_u8(4);

        // Act

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_reject_invalid_compression_code_255() {
        // Arrange
        let result = CompressionAlgo::from_u8(255);

        // Act

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_reject_all_invalid_codes() {
        // Arrange
        for code in 4..=255 {
            // Act
            let result = CompressionAlgo::from_u8(code);

            // Assert
            assert!(result.is_none(), "code {code} should be invalid");
        }
    }

    #[test]
    fn should_have_exact_u8_repr() {
        // Arrange
        // Act
        // Assert
        assert_eq!(CompressionAlgo::None.to_u8(), 0);
        assert_eq!(CompressionAlgo::Lz4.to_u8(), 1);
        assert_eq!(CompressionAlgo::Zstd3.to_u8(), 2);
        assert_eq!(CompressionAlgo::Zstd9.to_u8(), 3);
    }

    // ====================== CompressionPolicy Tests ======================

    #[test]
    fn should_create_none_policy() {
        // Arrange
        let policy = CompressionPolicy::None;

        // Act

        // Assert - should not panic
        assert!(matches!(policy, CompressionPolicy::None));
    }

    #[test]
    fn should_create_fixed_policy() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);

        // Act

        // Assert
        assert!(matches!(
            policy,
            CompressionPolicy::Fixed(CompressionAlgo::Lz4)
        ));
    }

    #[test]
    fn should_create_adaptive_policy_with_custom_params() {
        // Arrange
        let policy = CompressionPolicy::Adaptive {
            min_savings_bytes: 512,
            min_ratio: 1.1,
            check_algorithms: vec![CompressionAlgo::Zstd3],
        };

        // Act

        // Assert
        match policy {
            CompressionPolicy::Adaptive {
                min_savings_bytes,
                min_ratio,
                check_algorithms,
            } => {
                assert_eq!(min_savings_bytes, 512);
                assert!(min_ratio > 1.09 && min_ratio < 1.11);
                assert_eq!(check_algorithms.len(), 1);
                assert_eq!(check_algorithms[0], CompressionAlgo::Zstd3);
            }
            _ => panic!("wrong policy variant"),
        }
    }

    #[test]
    fn should_have_default_adaptive_policy() {
        // Arrange
        let policy = CompressionPolicy::default();

        // Act

        // Assert
        match policy {
            CompressionPolicy::Adaptive {
                min_savings_bytes,
                min_ratio,
                check_algorithms,
            } => {
                assert_eq!(min_savings_bytes, 256);
                assert!((min_ratio - 0.95).abs() < f32::EPSILON);
                assert!(check_algorithms.contains(&CompressionAlgo::Lz4));
                assert!(check_algorithms.contains(&CompressionAlgo::Zstd3));
                assert!(check_algorithms.contains(&CompressionAlgo::Zstd9));
                assert_eq!(check_algorithms.len(), 3);
            }
            _ => panic!("should be adaptive by default"),
        }
    }

    #[test]
    fn should_be_cloneable_policy() {
        // Arrange
        let policy = CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 1.05,
            check_algorithms: vec![CompressionAlgo::Zstd3],
        };

        // Act
        let cloned = policy.clone();

        // Assert
        assert_eq!(policy, cloned, "cloned policy should equal original");
    }

    // ====================== compress_block Tests ======================

    #[test]
    fn should_store_tiny_block_uncompressed_when_fixed_policy_requests_compression() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Zstd9);
        let tiny_data = vec![0u8; MIN_COMPRESSION_INPUT_BYTES - 1];

        // Act
        let (compressed, algo) = compress_block(&tiny_data, &policy).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.len(), tiny_data.len());
        assert_eq!(compressed.as_ref(), tiny_data.as_slice());
    }

    #[test]
    fn should_skip_compression_at_min_compress_size_boundary() {
        // Arrange
        let policy = CompressionPolicy::default();
        let boundary_data = vec![0u8; MIN_COMPRESSION_INPUT_BYTES - 1];

        // Act
        let (compressed, algo) = compress_block(&boundary_data, &policy).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.len(), boundary_data.len());
    }

    #[test]
    fn should_compress_at_min_compress_size() {
        // Arrange
        let policy = CompressionPolicy::default();
        let boundary_data = vec![0u8; MIN_COMPRESSION_INPUT_BYTES];

        // Act
        let (compressed, algo) = compress_block(&boundary_data, &policy).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.len(), boundary_data.len());
    }

    #[test]
    fn should_use_none_policy_without_compression() {
        // Arrange
        let policy = CompressionPolicy::None;
        let data = vec![1u8; 1024];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_use_fixed_none_policy_without_compression() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::None);
        let data = vec![2u8; 1024];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_use_fixed_lz4_policy() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);
        let data = vec![3u8; 1024];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert - LZ4 compression is now implemented
        assert_eq!(algo, CompressionAlgo::Lz4);
        assert_ne!(compressed.as_ref(), data.as_slice());
        // Roundtrip
        let decompressed = decompress_block(&compressed, algo).unwrap();
        assert_eq!(decompressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_use_fixed_zstd3_policy() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Zstd3);
        let data = vec![4u8; 1024];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert - Zstd3 compression is now implemented
        assert_eq!(algo, CompressionAlgo::Zstd3);
        assert_ne!(compressed.as_ref(), data.as_slice());
        // Roundtrip
        let decompressed = decompress_block(&compressed, algo).unwrap();
        assert_eq!(decompressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_use_fixed_zstd9_policy() {
        // Arrange
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Zstd9);
        let data = vec![5u8; 2048];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert - Zstd9 compression is now implemented
        assert_eq!(algo, CompressionAlgo::Zstd9);
        assert_ne!(compressed.as_ref(), data.as_slice());
        // Roundtrip
        let decompressed = decompress_block(&compressed, algo).unwrap();
        assert_eq!(decompressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_handle_adaptive_policy() {
        // Arrange
        let policy = CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 1.05,
            check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
        };
        let data = vec![7u8; 2048];

        // Act
        let (compressed, algo) = compress_block(&data, &policy).unwrap();

        // Assert - should return something
        assert!(!compressed.is_empty());
        assert!(
            algo == CompressionAlgo::None
                || algo == CompressionAlgo::Lz4
                || algo == CompressionAlgo::Zstd3
        );
    }

    #[test]
    fn should_include_exact_adaptive_eligibility_boundaries() {
        // Arrange
        let original_size = 1_000;
        let compressed_size = 950;

        // Act
        let exact = compression_qualifies(original_size, compressed_size, 50, 0.95);
        let too_few_bytes = compression_qualifies(original_size, compressed_size, 51, 0.95);
        let too_weak_ratio = compression_qualifies(original_size, compressed_size, 50, 0.949);

        // Assert
        assert!(exact);
        assert!(!too_few_bytes);
        assert!(!too_weak_ratio);
    }

    #[test]
    fn should_keep_production_adaptive_selection_exhaustive_after_rejected_candidate() {
        // Arrange
        let data = vec![b'A'; 16 * 1024];
        let policy = CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 0.95,
            check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
        };
        let (lz4, _) = compress_with_algo(&data, CompressionAlgo::Lz4).expect("compress LZ4");
        let (zstd3, _) = compress_with_algo(&data, CompressionAlgo::Zstd3).expect("compress Zstd3");

        // Act
        let (compressed, algorithm) = compress_block(&data, &policy).expect("compress adaptively");

        // Assert
        assert_eq!(algorithm, CompressionAlgo::Zstd3);
        assert_eq!(compressed.len(), lz4.len().min(zstd3.len()));
    }

    #[test]
    fn should_preserve_data_on_none_compression() {
        // Arrange
        let data = vec![8u8; 512];

        // Act
        let (compressed, _algo) = compress_block(&data, &CompressionPolicy::None).unwrap();

        // Assert
        assert_eq!(compressed.as_ref(), data.as_slice());
    }

    #[test]
    fn should_handle_empty_data() {
        // Arrange
        let data: Vec<u8> = vec![];

        // Act
        let (compressed, algo) = compress_block(&data, &CompressionPolicy::default()).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.len(), 0);
    }

    #[test]
    fn should_handle_single_byte() {
        // Arrange
        let data = vec![42u8];

        // Act
        let (compressed, algo) = compress_block(&data, &CompressionPolicy::default()).unwrap();

        // Assert
        assert_eq!(algo, CompressionAlgo::None);
        assert_eq!(compressed.as_ref(), [42u8].as_ref());
    }

    #[test]
    fn should_handle_large_block() {
        // Arrange
        let data = vec![9u8; 32 * 1024]; // 32KB

        // Act
        let (compressed, _algo) = compress_block(&data, &CompressionPolicy::default()).unwrap();

        // Assert
        assert!(!compressed.is_empty());
    }

    #[test]
    fn should_handle_max_block_size() {
        // Arrange
        let data = vec![10u8; MAX_BLOCK_SIZE];

        // Act
        let (compressed, _algo) = compress_block(&data, &CompressionPolicy::default()).unwrap();

        // Assert
        assert!(!compressed.is_empty());
    }

    #[test]
    fn should_reject_compressed_block_given_declared_size_mismatch_when_reading() {
        // Arrange
        let declared_size = u32::try_from(MAX_DECOMPRESSED_BLOCK_SIZE + 1).unwrap();
        let mut forged = declared_size.to_le_bytes().to_vec();
        forged.extend_from_slice(&[0_u8; 4]);

        // Act
        let result = decompress_block(&forged, CompressionAlgo::Lz4);

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Corruption(message)) if message.contains("exceeds"))
        );
    }

    #[test]
    fn should_reject_zstd_block_with_oversized_declared_output() {
        // Arrange
        let declared_size = MAX_DECOMPRESSED_BLOCK_SIZE + 1;

        // Act
        let result = enforce_decompressed_size(declared_size, "Zstd");

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Corruption(message)) if message.contains("exceeds"))
        );
    }

    // ====================== decompress_block Tests ======================

    #[test]
    fn should_decompress_none_as_passthrough() {
        // Arrange
        let data = b"test data for decompression";

        // Act
        let decompressed = decompress_block(data, CompressionAlgo::None).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn should_decompress_empty_data_with_none() {
        // Arrange
        let data: &[u8] = b"";

        // Act
        let decompressed = decompress_block(data, CompressionAlgo::None).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), b"");
    }

    #[test]
    fn should_roundtrip_decompress_lz4() {
        // Arrange
        let original = b"hello world this is some test data for lz4 roundtrip";
        let (compressed, algo) = compress_with_algo(original, CompressionAlgo::Lz4).unwrap();
        assert_eq!(algo, CompressionAlgo::Lz4);

        // Act
        let decompressed = decompress_block(&compressed, CompressionAlgo::Lz4).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), original.as_slice());
    }

    #[test]
    fn should_roundtrip_decompress_zstd3() {
        // Arrange
        let original = b"hello world this is some test data for zstd3 roundtrip";
        let (compressed, algo) = compress_with_algo(original, CompressionAlgo::Zstd3).unwrap();
        assert_eq!(algo, CompressionAlgo::Zstd3);

        // Act
        let decompressed = decompress_block(&compressed, CompressionAlgo::Zstd3).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), original.as_slice());
    }

    #[test]
    fn should_roundtrip_decompress_zstd9() {
        // Arrange
        let original = b"hello world this is some test data for zstd9 roundtrip";
        let (compressed, algo) = compress_with_algo(original, CompressionAlgo::Zstd9).unwrap();
        assert_eq!(algo, CompressionAlgo::Zstd9);

        // Act
        let decompressed = decompress_block(&compressed, CompressionAlgo::Zstd9).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), original.as_slice());
    }

    #[test]
    fn should_decompress_large_data_with_none() {
        // Arrange
        let data = vec![11u8; 16 * 1024]; // 16KB

        // Act
        let decompressed = decompress_block(&data, CompressionAlgo::None).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), data.as_slice());
    }

    // ====================== Round-trip Tests ======================

    #[test]
    fn should_roundtrip_compress_decompress_none() {
        // Arrange
        let original = b"round trip test data";

        // Act
        let (compressed, algo) = compress_block(original, &CompressionPolicy::None).unwrap();
        let decompressed = decompress_block(&compressed, algo).unwrap();

        // Assert
        assert_eq!(decompressed.as_ref(), original);
    }

    #[test]
    fn should_roundtrip_with_various_policies() {
        // Arrange
        let data = vec![12u8; 512];
        let policies = vec![
            CompressionPolicy::None,
            CompressionPolicy::Fixed(CompressionAlgo::None),
            CompressionPolicy::Adaptive {
                min_savings_bytes: 64,
                min_ratio: 1.05,
                check_algorithms: vec![CompressionAlgo::Lz4],
            },
        ];

        // Act
        for policy in policies {
            // Assert
            let (compressed, algo) = compress_block(&data, &policy).unwrap();
            let decompressed = decompress_block(&compressed, algo).unwrap();
            assert_eq!(decompressed.as_ref(), data.as_slice());
        }
    }

    // ====================== Determinism Tests ======================

    #[test]
    fn should_be_deterministic_with_none_policy() {
        // Arrange
        let data = vec![13u8; 1024];
        let policy = CompressionPolicy::None;

        // Act
        let (compressed1, algo1) = compress_block(&data, &policy).unwrap();
        let (compressed2, algo2) = compress_block(&data, &policy).unwrap();

        // Assert
        assert_eq!(compressed1, compressed2);
        assert_eq!(algo1, algo2);
    }

    #[test]
    fn should_be_deterministic_with_fixed_policy() {
        // Arrange
        let data = vec![14u8; 1024];
        let policy = CompressionPolicy::Fixed(CompressionAlgo::Lz4);

        // Act
        let (compressed1, algo1) = compress_block(&data, &policy).unwrap();
        let (compressed2, algo2) = compress_block(&data, &policy).unwrap();

        // Assert
        assert_eq!(compressed1, compressed2);
        assert_eq!(algo1, algo2);
    }

    #[test]
    fn should_be_deterministic_with_adaptive_policy() {
        // Arrange
        let data = vec![15u8; 2048];
        let policy = CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 1.05,
            check_algorithms: vec![CompressionAlgo::Zstd3],
        };

        // Act
        let (compressed1, algo1) = compress_block(&data, &policy).unwrap();
        let (compressed2, algo2) = compress_block(&data, &policy).unwrap();

        // Assert
        assert_eq!(compressed1, compressed2);
        assert_eq!(algo1, algo2);
    }

    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        // xorshift64: deterministic, high-entropy, no extra dependency.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    #[test]
    fn should_skip_wal_compression_when_value_is_random() {
        // Arrange: encrypted or already-compressed values look uniformly random.
        let value = pseudo_random_bytes(4096);

        // Act
        let likely = super::is_likely_compressible(&value);
        let (stored, codec) = super::compress_wal_value(&value);

        // Assert
        assert!(!likely, "random bytes must not be treated as compressible");
        assert_eq!(codec, None);
        assert_eq!(stored.as_ref(), value.as_slice());
    }

    #[test]
    fn should_compress_wal_value_when_value_is_repetitive_text() {
        // Arrange
        let value = "{\"user\":\"alice\",\"role\":\"admin\"}".repeat(64);

        // Act
        let likely = super::is_likely_compressible(value.as_bytes());
        let (stored, codec) = super::compress_wal_value(value.as_bytes());

        // Assert
        assert!(likely);
        assert!(codec.is_some());
        assert!(stored.len() < value.len());
    }

    #[test]
    fn should_apply_distinct_decode_reservation_bounds_for_zstd_without_content_size() {
        use std::io::Write;

        // Arrange
        let input = b"unknown zstd frame size stays bounded during decode".repeat(32);
        let mut encoder =
            zstd::stream::write::Encoder::new(Vec::new(), 3).expect("create zstd test encoder");
        encoder
            .set_pledged_src_size(Some(u64::try_from(input.len()).expect("input length fits")))
            .expect("set zstd test input size");
        encoder
            .include_contentsize(false)
            .expect("omit zstd content size");
        encoder.write_all(&input).expect("write zstd test input");
        let compressed = encoder.finish().expect("finish zstd test encoder");
        assert!(matches!(
            zstd::zstd_safe::get_frame_content_size(&compressed),
            Ok(None)
        ));

        // Act
        let hint = decompressed_len_hint(CompressionAlgo::Zstd3, &compressed)
            .expect("derive zstd size hint");
        let decoded = decompress_block(&compressed, CompressionAlgo::Zstd3)
            .expect("decode zstd frame without content size");
        let mut block = compressed;
        block.push(CompressionAlgo::Zstd3.to_u8());
        block.extend_from_slice(&crc32c::crc32c(&block).to_le_bytes());
        let reservation = decompressed_size_with_trailer(&block)
            .expect("reserve zstd block without content size");

        // Assert
        assert_eq!(hint.decode_limit, MAX_BLOCK_SIZE);
        assert_eq!(hint.reservation_limit, MAX_DECOMPRESSED_BLOCK_SIZE);
        assert_eq!(reservation, MAX_DECOMPRESSED_BLOCK_SIZE);
        assert_eq!(decoded.as_ref(), input.as_slice());
    }
}
