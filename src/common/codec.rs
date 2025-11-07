//! Compression primitives and traits

/// Minimal compression abstraction used by SST format
///
/// Compression strategy by layer (per performance table):
/// - WAL: None or Lz4 (fast, level 0 equivalent)
/// - L0 SST: Lz4 (ultra-low latency for hot data)
/// - L1-L3 SST: Zstd1-3 (balanced ratio vs CPU for warm data)
/// - L4+ SST: Zstd5-9 (max density for cold/archival data)
/// - Indexes/filters: Lz4 or Zstd1 (fast decompression)
/// - Metadata: None (too small, overhead dominates)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Lz4,   // Fast compression for hot data (L0, WAL)
    Zstd1, // Balanced for warm data (L1-L3) or filters
    Zstd3, // Better ratio for mid-tier
    Zstd5, // Good ratio for cold data (L4+)
    Zstd9, // Maximum density for archival
}

/// A compressor implementation used for SST block compression.
pub trait Compressor {
    /// Compress input bytes, returning a new Vec<u8> or an error.
    fn compress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>>;

    /// Decompress input bytes, returning the original data.
    fn decompress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>>;
}

// Implementations consolidated below so callers can import from `crate::codec`.

/// No-op compressor: returns bytes unchanged. Useful for CompressionType::None.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCodec;

impl NoopCodec {
    pub fn new() -> Self {
        NoopCodec
    }
}

impl Compressor for NoopCodec {
    #[inline(always)]
    fn compress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    #[inline(always)]
    fn decompress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(input.to_vec())
    }
}

/// LZ4 codec wrapper using the `lz4_flex` crate
#[derive(Debug, Default, Clone, Copy)]
pub struct Lz4Codec;

impl Lz4Codec {
    pub fn new() -> Self {
        Lz4Codec
    }
}

impl Compressor for Lz4Codec {
    #[inline]
    fn compress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        let out = lz4_flex::compress_prepend_size(input);
        Ok(out)
    }

    #[inline]
    fn decompress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        let out = lz4_flex::decompress_size_prepended(input)?;
        Ok(out)
    }
}

/// Zstd codec wrapper using the `zstd` crate with configurable compression level
#[derive(Debug, Clone, Copy)]
pub struct ZstdCodec {
    level: i32,
}

impl ZstdCodec {
    pub fn new(level: i32) -> Self {
        ZstdCodec { level }
    }

    /// Create a Zstd codec with level 1 (fast, for L1-L3 or filters)
    pub fn level_1() -> Self {
        Self::new(1)
    }

    /// Create a Zstd codec with level 3 (balanced for mid-tier)
    pub fn level_3() -> Self {
        Self::new(3)
    }

    /// Create a Zstd codec with level 5 (good ratio for L4+)
    pub fn level_5() -> Self {
        Self::new(5)
    }

    /// Create a Zstd codec with level 9 (max density for archival)
    pub fn level_9() -> Self {
        Self::new(9)
    }
}

impl Default for ZstdCodec {
    fn default() -> Self {
        Self::new(3) // Default to balanced level
    }
}

impl Compressor for ZstdCodec {
    #[inline]
    fn compress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        let out = zstd::encode_all(input, self.level)?;
        Ok(out)
    }

    #[inline]
    fn decompress(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        let out = zstd::decode_all(input)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use rand::rngs::StdRng;
    use rand::RngCore;
    use rand::{Rng, SeedableRng};

    // ---------------------- behavior tests ----------------------
    fn roundtrip<C: Compressor>(codec: C, input: &[u8]) -> Result<()> {
        let compressed = codec.compress(input)?;
        let decompressed = codec.decompress(&compressed)?;
        assert_eq!(decompressed.as_slice(), input);
        Ok(())
    }

    macro_rules! behavior_tests_for {
        ($modname:ident, $codec_ty:ty, $ctor:expr) => {
            mod $modname {
                use super::*;

                #[test]
                fn should_roundtrip_given_small_input() -> Result<()> {
                    // Arrange
                    let codec = $ctor();
                    let input = b"hello world";

                    // Act
                    // Assert
                    roundtrip(codec, input)
                }

                #[test]
                fn should_roundtrip_given_empty_input() -> Result<()> {
                    // Arrange
                    let codec = $ctor();
                    let input: &[u8] = &[];

                    // Act
                    // Assert
                    roundtrip(codec, input)
                }

                #[test]
                fn should_roundtrip_given_binary_input() -> Result<()> {
                    // Arrange
                    let codec = $ctor();
                    let input = &[0u8, 1, 2, 3, 4, 5, 0xff, 0x00, 0x7f];

                    // Act
                    // Assert
                    roundtrip(codec, input)
                }
            }
        };
    }

    behavior_tests_for!(noop_codec, NoopCodec, NoopCodec::new);
    behavior_tests_for!(lz4_codec, Lz4Codec, Lz4Codec::new);
    behavior_tests_for!(zstd_codec_level1, ZstdCodec, ZstdCodec::level_1);
    behavior_tests_for!(zstd_codec_level3, ZstdCodec, ZstdCodec::level_3);
    behavior_tests_for!(zstd_codec_level5, ZstdCodec, ZstdCodec::level_5);
    behavior_tests_for!(zstd_codec_level9, ZstdCodec, ZstdCodec::level_9);

    // ---------------------- edgecase tests ----------------------
    // Helper to assert that decompression fails for corrupted payloads for non-noop codecs.
    fn assert_corruption_detected<C: Compressor>(codec: C) {
        let input = b"some deterministic payload to compress and then corrupt";
        let compressed = codec.compress(input).expect("compress ok");
        let mut corrupted = compressed.clone();
        if !corrupted.is_empty() {
            let mid = corrupted.len() / 2;
            corrupted[0] ^= 0xff;
            if corrupted.len() > 4 {
                corrupted[mid] ^= 0x55;
            }
        }
        if let Ok(out) = codec.decompress(&corrupted) {
            assert_ne!(
                out.as_slice(),
                input,
                "decompressed equals original despite corruption"
            );
        }
    }

    #[test]
    fn should_detect_corruption_for_lz4() {
        assert_corruption_detected(Lz4Codec::new());
    }

    #[test]
    fn should_detect_corruption_for_zstd_level1() {
        assert_corruption_detected(ZstdCodec::level_1());
    }

    #[test]
    fn should_detect_corruption_for_zstd_level3() {
        assert_corruption_detected(ZstdCodec::level_3());
    }

    #[test]
    fn should_detect_corruption_for_zstd_level5() {
        assert_corruption_detected(ZstdCodec::level_5());
    }

    #[test]
    fn should_detect_corruption_for_zstd_level9() {
        assert_corruption_detected(ZstdCodec::level_9());
    }

    #[test]
    fn should_tolerate_arbitrary_bytes_for_noop_codec() -> Result<()> {
        // Arrange
        let codec = NoopCodec::new();
        let input = b"foo";
        let compressed = codec.compress(input)?;
        let mut corrupted = compressed.clone();
        if !corrupted.is_empty() {
            corrupted[0] ^= 0xaa;
        }

        // Act
        let out = codec.decompress(&corrupted)?;

        // Assert
        assert_eq!(out.as_slice(), corrupted.as_slice());
        Ok(())
    }

    #[test]
    fn should_roundtrip_random_vectors_given_all_codecs() -> Result<()> {
        // Arrange
        let mut rng = StdRng::seed_from_u64(0x1234abcd);

        // Act
        // Assert
        for _ in 0..200 {
            let len = (rng.next_u32() as usize) % 128usize;
            let mut v = vec![0u8; len];
            rng.fill(&mut v[..]);
            let n = NoopCodec::new();
            let l = Lz4Codec::new();
            let z1 = ZstdCodec::level_1();
            let z3 = ZstdCodec::level_3();
            let z5 = ZstdCodec::level_5();
            let z9 = ZstdCodec::level_9();

            let c_n = n.compress(&v)?;
            assert_eq!(n.decompress(&c_n)?, v);
            let c_l = l.compress(&v)?;
            assert_eq!(l.decompress(&c_l)?, v);
            let c_z1 = z1.compress(&v)?;
            assert_eq!(z1.decompress(&c_z1)?, v);
            let c_z3 = z3.compress(&v)?;
            assert_eq!(z3.decompress(&c_z3)?, v);
            let c_z5 = z5.compress(&v)?;
            assert_eq!(z5.decompress(&c_z5)?, v);
            let c_z9 = z9.compress(&v)?;
            assert_eq!(z9.decompress(&c_z9)?, v);
        }
        Ok(())
    }

    #[test]
    fn should_roundtrip_large_input_given_compression_codecs() -> Result<()> {
        // Arrange
        let input = vec![0u8; 4 * 1024 * 1024];
        let l = Lz4Codec::new();
        let z1 = ZstdCodec::level_1();
        let z3 = ZstdCodec::level_3();
        let z5 = ZstdCodec::level_5();
        let z9 = ZstdCodec::level_9();

        // Act
        let cl = l.compress(&input)?;
        let cz1 = z1.compress(&input)?;
        let cz3 = z3.compress(&input)?;
        let cz5 = z5.compress(&input)?;
        let cz9 = z9.compress(&input)?;

        // Assert
        assert_eq!(l.decompress(&cl)?, input);
        assert_eq!(z1.decompress(&cz1)?, input);
        assert_eq!(z3.decompress(&cz3)?, input);
        assert_eq!(z5.decompress(&cz5)?, input);
        assert_eq!(z9.decompress(&cz9)?, input);
        Ok(())
    }

    // ---------------------- noop-specific tests ----------------------
    fn assert_noop_roundtrip(name: &str, input: Vec<u8>) {
        let codec = NoopCodec::new();
        let compressed = codec.compress(&input).expect("compress failed");
        let decompressed = codec.decompress(&compressed).expect("decompress failed");
        assert_eq!(
            compressed, input,
            "case: {} - compress should be no-op",
            name
        );
        assert_eq!(
            decompressed, input,
            "case: {} - decompress should be no-op",
            name
        );
    }

    #[test]
    fn should_roundtrip_empty_given_noop_codec() {
        // Arrange
        let input = vec![];

        // Act
        // Assert
        assert_noop_roundtrip("empty", input);
    }

    #[test]
    fn should_roundtrip_small_given_noop_codec() {
        // Arrange
        let input = b"hello".to_vec();

        // Act
        // Assert
        assert_noop_roundtrip("small", input);
    }

    #[test]
    fn should_roundtrip_binary_given_noop_codec() {
        // Arrange
        let input = vec![0, 1, 2, 3, 255];

        // Act
        // Assert
        assert_noop_roundtrip("binary", input);
    }

    #[test]
    fn should_roundtrip_long_given_noop_codec() {
        // Arrange
        let input = (0..1024).map(|b| (b % 256) as u8).collect();

        // Act
        // Assert
        assert_noop_roundtrip("long", input);
    }
}
