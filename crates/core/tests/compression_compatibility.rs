use cntryl_midge_core::codec::{
    compress_block_with_trailer, decompress_block_with_trailer, CompressionAlgo, CompressionPolicy,
    BLOCK_TRAILER_SIZE,
};
use xxhash_rust::xxh3::xxh3_64;

fn structured_block(size: usize) -> Vec<u8> {
    let pattern = b"account=0042|region=east|status=active|segment=business|";
    pattern.iter().copied().cycle().take(size).collect()
}
#[test]
fn should_preserve_exact_sst_codec_codes() {
    // Arrange
    let expected = [
        (CompressionAlgo::None, 0),
        (CompressionAlgo::Lz4, 1),
        (CompressionAlgo::Zstd3, 2),
        (CompressionAlgo::Zstd9, 3),
    ];

    // Act
    for (algorithm, code) in expected {
        // Assert
        assert_eq!(algorithm.to_u8(), code);
        assert_eq!(CompressionAlgo::from_u8(code), Some(algorithm));
    }
    assert_eq!(CompressionAlgo::from_u8(4), None);
    assert_eq!(CompressionAlgo::from_u8(u8::MAX), None);
}

#[test]
fn should_preserve_five_byte_trailer_layout_with_crc_coverage() {
    // Arrange
    let data = b"trailer-format-fixture";

    // Act
    let block = compress_block_with_trailer(data, &CompressionPolicy::None)
        .expect("build checksummed raw block");
    let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
    let crc_offset = block.len() - size_of::<u32>();
    let stored_crc = u32::from_le_bytes(
        block[crc_offset..]
            .try_into()
            .expect("four-byte CRC32C trailer"),
    );

    // Assert
    assert_eq!(BLOCK_TRAILER_SIZE, 5);
    assert_eq!(&block[..algorithm_offset], data);
    assert_eq!(block[algorithm_offset], CompressionAlgo::None.to_u8());
    assert_eq!(stored_crc, crc32c::crc32c(&block[..crc_offset]));
}

#[test]
fn should_preserve_baseline_compressed_block_fixture() {
    // Arrange
    let data = structured_block(16 * 1024);
    let cases = [
        (
            CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            CompressionAlgo::Lz4,
            0xf8ab_776d_208c_bd15_u64,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
            CompressionAlgo::Zstd3,
            0x4e7b_d7fc_d9a0_d5c5_u64,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
            CompressionAlgo::Zstd9,
            0xe2b7_653b_ded1_b28e_u64,
        ),
    ];

    // Act
    for (policy, expected_algorithm, expected_digest) in cases {
        let block =
            compress_block_with_trailer(&data, &policy).expect("compress baseline block fixture");
        let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
        let actual_digest = xxh3_64(&block);
        eprintln!(
            "{expected_algorithm:?}: len={}, xxh3_64={actual_digest:016x}",
            block.len()
        );

        // Assert
        assert_eq!(block[algorithm_offset], expected_algorithm.to_u8());
        assert_eq!(actual_digest, expected_digest);
    }
}

#[test]
fn should_roundtrip_every_emitted_sst_block_deterministically() {
    // Arrange
    let data = structured_block(16 * 1024);
    let policies = [
        CompressionPolicy::None,
        CompressionPolicy::Fixed(CompressionAlgo::None),
        CompressionPolicy::Fixed(CompressionAlgo::Lz4),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 0.95,
            check_algorithms: vec![
                CompressionAlgo::None,
                CompressionAlgo::Lz4,
                CompressionAlgo::Zstd3,
            ],
        },
    ];

    // Act
    for policy in policies {
        let first = compress_block_with_trailer(&data, &policy).expect("first compression");
        let second = compress_block_with_trailer(&data, &policy).expect("second compression");
        let decoded = decompress_block_with_trailer(&first).expect("roundtrip block");

        // Assert
        assert_eq!(first, second, "policy must be deterministic: {policy:?}");
        assert_eq!(decoded.as_ref(), data.as_slice());
    }
}

#[test]
fn should_reject_invalid_sst_block_trailers() {
    // Arrange
    let data = structured_block(1024);
    let valid =
        compress_block_with_trailer(&data, &CompressionPolicy::None).expect("build valid block");
    let mut corrupt = valid.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x01;

    let mut unknown = data.clone();
    unknown.push(u8::MAX);
    let crc = crc32c::crc32c(&unknown);
    unknown.extend_from_slice(&crc.to_le_bytes());

    // Act
    let corrupt_error = decompress_block_with_trailer(&corrupt).expect_err("corrupt CRC must fail");
    let truncated_error =
        decompress_block_with_trailer(&valid[..4]).expect_err("short trailer must fail");
    let unknown_error =
        decompress_block_with_trailer(&unknown).expect_err("unknown codec must fail");

    // Assert
    assert!(corrupt_error.to_string().contains("CRC32C mismatch"));
    assert!(truncated_error
        .to_string()
        .contains("too small for trailer"));
    assert!(unknown_error
        .to_string()
        .contains("unknown compression algorithm code"));
}

#[test]
fn should_reject_nonshipping_codec_codes_without_fallback() {
    // Arrange
    let encoded = [4_u8, 5, u8::MAX].map(|code| {
        let mut block = b"payload".to_vec();
        block.push(code);
        block.extend_from_slice(&crc32c::crc32c(&block).to_le_bytes());
        block
    });

    // Act
    let errors = encoded.map(|block| {
        decompress_block_with_trailer(&block).expect_err("unknown codec must fail closed")
    });

    // Assert
    for error in errors {
        assert!(matches!(
            error,
            cntryl_midge_core::MidgeError::Corruption(_)
        ));
        assert!(error
            .to_string()
            .contains("unknown compression algorithm code"));
    }
}

#[test]
fn should_reject_corrupt_compressed_payload_for_every_shipping_codec() {
    // Arrange
    let data = structured_block(16 * 1024);
    let cases = [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd3,
        CompressionAlgo::Zstd9,
    ];

    // Act
    for algorithm in cases {
        let mut block = compress_block_with_trailer(&data, &CompressionPolicy::Fixed(algorithm))
            .expect("compress shipping codec")
            .to_vec();
        match algorithm {
            CompressionAlgo::Lz4 => {
                block[..4].copy_from_slice(&(64 * 1024 * 1024_u32 + 1).to_le_bytes());
            }
            CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => block[0] ^= 0xff,
            CompressionAlgo::None => unreachable!(),
        }
        let crc_offset = block.len() - size_of::<u32>();
        let crc = crc32c::crc32c(&block[..crc_offset]);
        block[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        let error = decompress_block_with_trailer(&block)
            .expect_err("codec payload corruption must not fall back to raw bytes");

        // Assert
        assert!(
            matches!(error, cntryl_midge_core::MidgeError::Corruption(_)),
            "{algorithm:?} returned {error}"
        );
    }
}
