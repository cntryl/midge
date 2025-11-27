//! Tests for compression functionality in SST files.
//!
//! These tests verify that compression/decompression works correctly
//! for various compression types (None, LZ4, Zstd) at different levels.

use cntryl_midge::common::codec::{CompressionType, Compressor, Lz4Codec, NoopCodec, ZstdCodec};
use cntryl_midge::config::StorageMode;
use cntryl_midge::{MidgeEngine, MidgeOptions};
use tempfile::TempDir;

mod common;

// =============================================================================
// CODEC UNIT TESTS
// =============================================================================

#[test]
fn should_roundtrip_data_given_no_compression_when_using_noop_codec() {
    // Arrange
    let codec = NoopCodec::new();
    let original = b"This is some test data that should remain unchanged";

    // Act
    let compressed = codec.compress(original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
    assert_eq!(compressed.as_slice(), original.as_slice()); // Noop doesn't change data
}

#[test]
fn should_roundtrip_data_given_lz4_compression_when_compressing_text() {
    // Arrange
    let codec = Lz4Codec::new();
    let original = b"This is some test data with repetition. Repetition helps compression. Compression is good for storage.";

    // Act
    let compressed = codec.compress(original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
    // LZ4 should achieve some compression on repetitive data
    assert!(
        compressed.len() <= original.len() + 10,
        "LZ4 output should not be much larger"
    );
}

#[test]
fn should_roundtrip_data_given_zstd_level1_when_compressing_text() {
    // Arrange
    let codec = ZstdCodec::level_1();
    let original = b"Zstd level 1 is fast. Fast compression is useful. Useful for warm data layers.";

    // Act
    let compressed = codec.compress(original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

#[test]
fn should_roundtrip_data_given_zstd_level3_when_compressing_text() {
    // Arrange
    let codec = ZstdCodec::level_3();
    let original = b"Zstd level 3 provides better compression ratio while remaining reasonably fast for mid-tier data.";

    // Act
    let compressed = codec.compress(original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

#[test]
fn should_roundtrip_data_given_zstd_level5_when_compressing_text() {
    // Arrange
    let codec = ZstdCodec::level_5();
    let original = b"Zstd level 5 is intended for cold data where compression ratio matters more than speed.";

    // Act
    let compressed = codec.compress(original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

#[test]
fn should_achieve_better_ratio_given_higher_zstd_level_when_comparing_levels() {
    // Arrange
    let level1 = ZstdCodec::level_1();
    let level5 = ZstdCodec::level_5();
    // Highly repetitive data to make compression differences more visible
    let original: Vec<u8> = (0..1000)
        .flat_map(|_| b"The quick brown fox jumps over the lazy dog. ".to_vec())
        .collect();

    // Act
    let compressed_l1 = level1.compress(&original).expect("compress level 1");
    let compressed_l5 = level5.compress(&original).expect("compress level 5");

    // Assert - higher level should achieve better (or equal) compression
    assert!(
        compressed_l5.len() <= compressed_l1.len(),
        "level 5 ({}) should compress at least as well as level 1 ({})",
        compressed_l5.len(),
        compressed_l1.len()
    );
}

#[test]
fn should_compress_empty_data_given_empty_input_when_using_any_codec() {
    // Arrange
    let noop = NoopCodec::new();
    let lz4 = Lz4Codec::new();
    let zstd = ZstdCodec::level_1();
    let original: &[u8] = &[];

    // Act
    let noop_compressed = noop.compress(original).expect("noop compress");
    let noop_decompressed = noop.decompress(&noop_compressed).expect("noop decompress");
    let lz4_compressed = lz4.compress(original).expect("lz4 compress");
    let lz4_decompressed = lz4.decompress(&lz4_compressed).expect("lz4 decompress");
    let zstd_compressed = zstd.compress(original).expect("zstd compress");
    let zstd_decompressed = zstd.decompress(&zstd_compressed).expect("zstd decompress");

    // Assert
    assert_eq!(noop_decompressed.as_slice(), original);
    assert_eq!(lz4_decompressed.as_slice(), original);
    assert_eq!(zstd_decompressed.as_slice(), original);
}

#[test]
fn should_handle_large_data_given_multi_mb_input_when_using_lz4() {
    // Arrange
    let codec = Lz4Codec::new();
    // 2 MB of somewhat compressible data
    let original: Vec<u8> = (0..2 * 1024 * 1024)
        .map(|i| (i % 256) as u8)
        .collect();

    // Act
    let compressed = codec.compress(&original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

#[test]
fn should_handle_random_data_given_incompressible_input_when_using_lz4() {
    // Arrange
    let codec = Lz4Codec::new();
    // Random-ish data (not truly random, but low redundancy)
    let original: Vec<u8> = (0..10000)
        .map(|i| ((i * 31337 + 12345) % 256) as u8)
        .collect();

    // Act
    let compressed = codec.compress(&original).expect("compress");
    let decompressed = codec.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
    // Random data may not compress well, but should not fail
}

// =============================================================================
// ENGINE INTEGRATION TESTS
// =============================================================================

#[test]
fn should_read_compressed_data_given_lz4_compression_when_flushed_to_sst() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        compression: CompressionType::Lz4,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open engine");
    let cf = engine.default_column_family();

    // Write enough data to trigger flush
    for i in 0..5000 {
        let key = format!("key_{:06}", i);
        let value = format!("value_for_key_{:06}_with_some_extra_padding_to_make_it_longer", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).expect("put");
    }
    engine.flush().expect("flush");

    // Act - read back all data
    let mut read_count = 0;
    for i in 0..5000 {
        let key = format!("key_{:06}", i);
        let expected = format!("value_for_key_{:06}_with_some_extra_padding_to_make_it_longer", i);

        // Assert
        let value = engine.get(&cf, key.as_bytes()).expect("get").expect("value exists");
        assert_eq!(value.as_ref(), expected.as_bytes());
        read_count += 1;
    }
    assert_eq!(read_count, 5000);
}

#[test]
fn should_persist_compression_setting_given_reopen_when_using_same_options() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");

    // Write data with compression
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: temp_dir.path().to_path_buf(),
            },
            compression: CompressionType::Lz4,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        for i in 0..1000 {
            let key = format!("persist_key_{:04}", i);
            let value = format!("persist_value_{:04}", i);
            engine.put(&cf, key.as_bytes(), value.as_bytes()).expect("put");
        }
        engine.flush().expect("flush");
    }

    // Act - reopen with same options
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        compression: CompressionType::Lz4,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    // Assert - data should be readable
    for i in 0..1000 {
        let key = format!("persist_key_{:04}", i);
        let expected = format!("persist_value_{:04}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get").expect("exists");
        assert_eq!(value.as_ref(), expected.as_bytes());
    }
}

// =============================================================================
// COMPRESSION TYPE SELECTION TESTS
// =============================================================================

#[test]
fn should_support_all_compression_types_given_valid_enum_when_configuring() {
    // Arrange
    let types = vec![
        CompressionType::None,
        CompressionType::Lz4,
        CompressionType::Zstd1,
        CompressionType::Zstd3,
        CompressionType::Zstd5,
        CompressionType::Zstd9,
    ];

    // Act
    let distinct_count = types.len();

    // Assert - all should be distinct
    assert_eq!(distinct_count, 6, "should have 6 compression types");
    for (i, t1) in types.iter().enumerate() {
        for (j, t2) in types.iter().enumerate() {
            if i != j {
                assert_ne!(t1, t2, "compression types should be distinct");
            }
        }
    }
}

#[test]
fn should_default_to_reasonable_compression_given_default_options_when_not_specified() {
    // Arrange - nothing to set up

    // Act
    let opts = MidgeOptions::default();

    // Assert - default should be something reasonable (either None or Lz4)
    assert!(
        opts.compression == CompressionType::None || opts.compression == CompressionType::Lz4,
        "default compression should be None or Lz4"
    );
}

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

#[test]
fn should_handle_single_byte_data_given_minimal_input_when_compressing() {
    // Arrange
    let lz4 = Lz4Codec::new();
    let original = &[42u8];

    // Act
    let compressed = lz4.compress(original).expect("compress");
    let decompressed = lz4.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original);
}

#[test]
fn should_handle_all_zeros_given_maximally_compressible_data_when_using_lz4() {
    // Arrange
    let lz4 = Lz4Codec::new();
    let original = vec![0u8; 100_000];

    // Act
    let compressed = lz4.compress(&original).expect("compress");
    let decompressed = lz4.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
    // All zeros should compress extremely well
    assert!(
        compressed.len() < original.len() / 10,
        "all zeros should compress to <10% of original size, got {}",
        compressed.len()
    );
}

#[test]
fn should_handle_all_0xff_given_uniform_data_when_compressing() {
    // Arrange
    let lz4 = Lz4Codec::new();
    let original = vec![0xFFu8; 50_000];

    // Act
    let compressed = lz4.compress(&original).expect("compress");
    let decompressed = lz4.decompress(&compressed).expect("decompress");

    // Assert
    assert_eq!(decompressed.as_slice(), original.as_slice());
}
