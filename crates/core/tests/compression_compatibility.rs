use cntryl_midge_core::codec::{
    compress_block_with_trailer, decompress_block_with_trailer, CompressionAlgo, CompressionPolicy,
    BLOCK_TRAILER_SIZE,
};
use cntryl_midge_core::MidgeError;
use std::mem::size_of;

fn structured_block(size: usize) -> Vec<u8> {
    let pattern = b"account=0042|region=east|status=active|segment=business|";
    pattern.iter().copied().cycle().take(size).collect()
}

#[test]
fn should_preserve_exact_sst_codec_codes() {
    let expected = [
        (CompressionAlgo::None, 0),
        (CompressionAlgo::Lz4, 1),
        (CompressionAlgo::Zstd3, 2),
        (CompressionAlgo::Zstd9, 3),
    ];
    for (algorithm, code) in expected {
        assert_eq!(algorithm.to_u8(), code);
        assert_eq!(CompressionAlgo::from_u8(code), Some(algorithm));
    }
    assert_eq!(CompressionAlgo::from_u8(4), None);
    assert_eq!(CompressionAlgo::from_u8(u8::MAX), None);
}

#[test]
fn should_preserve_five_byte_trailer_layout_with_crc_coverage() {
    let data = b"trailer-format-fixture";
    let block = compress_block_with_trailer(data, &CompressionPolicy::None).unwrap();
    let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
    let crc_offset = block.len() - size_of::<u32>();
    let stored_crc = u32::from_le_bytes(block[crc_offset..].try_into().unwrap());
    assert_eq!(BLOCK_TRAILER_SIZE, 5);
    assert_eq!(&block[..algorithm_offset], data);
    assert_eq!(block[algorithm_offset], CompressionAlgo::None.to_u8());
    assert_eq!(stored_crc, crc32c::crc32c(&block[..crc_offset]));
}

#[test]
fn should_preserve_baseline_compressed_block_fixture() {
    let data = structured_block(16 * 1024);
    for (policy, expected) in [
        (
            CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            CompressionAlgo::Lz4,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
            CompressionAlgo::Zstd3,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
            CompressionAlgo::Zstd9,
        ),
    ] {
        let block = compress_block_with_trailer(&data, &policy).unwrap();
        assert_eq!(block[block.len() - BLOCK_TRAILER_SIZE], expected.to_u8());
    }
}

#[test]
fn should_roundtrip_every_emitted_sst_block_deterministically() {
    let data = structured_block(16 * 1024);
    let policies = [
        CompressionPolicy::None,
        CompressionPolicy::Fixed(CompressionAlgo::None),
        CompressionPolicy::Fixed(CompressionAlgo::Lz4),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
    ];
    for policy in policies {
        let first = compress_block_with_trailer(&data, &policy).unwrap();
        let second = compress_block_with_trailer(&data, &policy).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            decompress_block_with_trailer(&first).unwrap().as_ref(),
            data
        );
    }
}

#[test]
fn should_reject_invalid_sst_block_trailers() {
    let data = structured_block(1024);
    let valid = compress_block_with_trailer(&data, &CompressionPolicy::None).unwrap();
    let mut corrupt = valid.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    let mut unknown = data;
    unknown.push(u8::MAX);
    unknown.extend_from_slice(&crc32c::crc32c(&unknown).to_le_bytes());
    assert!(decompress_block_with_trailer(&corrupt)
        .unwrap_err()
        .to_string()
        .contains("CRC32C"));
    assert!(decompress_block_with_trailer(&valid[..4]).is_err());
    assert!(decompress_block_with_trailer(&unknown)
        .unwrap_err()
        .to_string()
        .contains("unknown compression"));
}

#[test]
fn should_reject_nonshipping_codec_codes_without_fallback() {
    for code in [4_u8, 5, u8::MAX] {
        let mut block = b"payload".to_vec();
        block.push(code);
        block.extend_from_slice(&crc32c::crc32c(&block).to_le_bytes());
        assert!(matches!(
            decompress_block_with_trailer(&block),
            Err(MidgeError::Corruption(_))
        ));
    }
}

#[test]
fn should_reject_corrupt_compressed_payload_for_every_shipping_codec() {
    let data = structured_block(16 * 1024);
    for algorithm in [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd3,
        CompressionAlgo::Zstd9,
    ] {
        let mut block = compress_block_with_trailer(&data, &CompressionPolicy::Fixed(algorithm))
            .unwrap()
            .to_vec();
        match algorithm {
            CompressionAlgo::Lz4 => {
                block[..4].copy_from_slice(&(64 * 1024 * 1024_u32 + 1).to_le_bytes())
            }
            CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => block[0] ^= 0xff,
            CompressionAlgo::None => unreachable!(),
        }
        let crc_offset = block.len() - size_of::<u32>();
        let crc = crc32c::crc32c(&block[..crc_offset]);
        block[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decompress_block_with_trailer(&block),
            Err(MidgeError::Corruption(_))
        ));
    }
}
