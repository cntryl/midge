//! Shared bounded binary metadata frames for local and cloud authority.
//!
//! MessagePack maps preserve explicit field names and optional fields while
//! encoding integers and key bytes compactly. CRC32C covers the header, kind,
//! length and exact payload, so a frame cannot be reinterpreted as another kind.

use crate::common::{MidgeError, MidgeResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

const MAGIC: &[u8; 8] = b"MDGEMD09";
const HEADER_BYTES: usize = 13;
const CHECKSUM_BYTES: usize = 4;
const MAX_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
mod framing;
pub(crate) const MANIFEST: u8 = 1;
pub(crate) const JOURNAL: u8 = 2;
pub(crate) const INTENT: u8 = 3;

pub(crate) fn encode<T: Serialize + ?Sized>(kind: u8, value: &T) -> MidgeResult<Vec<u8>> {
    let payload = rmp_serde::to_vec_named(value)
        .map_err(|error| MidgeError::Internal(format!("encode binary metadata: {error}")))?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(MidgeError::ResourceLimit(
            "metadata frame exceeds 256 MiB".into(),
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| MidgeError::ResourceLimit("metadata frame length overflow".into()))?;
    let mut bytes = Vec::with_capacity(HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.push(kind);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&payload);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

/// Complete frame extent, or None for a genuine partial EOF. Invalid headers
/// and oversized announcements are corruption even when their body is absent.
pub(crate) fn frame_len(bytes: &[u8]) -> MidgeResult<Option<usize>> {
    if !MAGIC.starts_with(&bytes[..bytes.len().min(MAGIC.len())]) {
        return Err(MidgeError::CompatibilityError(
            "unsupported metadata encoding; format 9 binary metadata is required".into(),
        ));
    }
    if bytes.len() < HEADER_BYTES {
        return Ok(None);
    }
    let mut length = [0_u8; 4];
    length.copy_from_slice(&bytes[9..13]);
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_PAYLOAD_BYTES {
        return Err(MidgeError::Corruption(
            "metadata frame exceeds decoder limit".into(),
        ));
    }
    let extent = HEADER_BYTES + length + CHECKSUM_BYTES;
    Ok((bytes.len() >= extent).then_some(extent))
}

pub(crate) fn decode<T: DeserializeOwned>(kind: u8, bytes: &[u8]) -> MidgeResult<T> {
    let extent = frame_len(bytes)?
        .ok_or_else(|| MidgeError::Corruption("truncated metadata frame".into()))?;
    if extent != bytes.len() || bytes[8] != kind {
        return Err(MidgeError::Corruption(
            "metadata kind or frame extent mismatch".into(),
        ));
    }
    let payload_end = extent - CHECKSUM_BYTES;
    let mut checksum = [0_u8; 4];
    checksum.copy_from_slice(&bytes[payload_end..]);
    if crc32c::crc32c(&bytes[..payload_end]) != u32::from_le_bytes(checksum) {
        return Err(MidgeError::Corruption("metadata CRC32C mismatch".into()));
    }
    let payload = &bytes[HEADER_BYTES..payload_end];
    framing::validate(payload)?;
    // ReadRefReader checks str/bin extents against its remaining slice before
    // exposing borrowed bytes, without allocating an announced-length buffer.
    let mut decoder = rmp_serde::Deserializer::from_read_ref(payload);
    decoder.set_max_depth(64);
    let value = T::deserialize(&mut decoder)
        .map_err(|error| MidgeError::Corruption(format!("decode binary metadata: {error}")))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn should_round_trip_binary_manifest_when_bounds_and_sequences_vary(
            key in proptest::collection::vec(any::<u8>(), 0..4096),
            sequence in any::<u64>(),
        ) {
            // Arrange
            let mut manifest = crate::metadata::Manifest::default();
            manifest.files.push(crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, 1),
                smallest_key: Some(key.clone()),
                largest_key: Some(key.clone()),
                largest_seq: Some(sequence),
                ..Default::default()
            });

            // Act
            let encoded = encode(MANIFEST, &manifest).expect("encode manifest");
            let decoded: crate::metadata::Manifest = decode(MANIFEST, &encoded).expect("decode manifest");

            // Assert
            prop_assert_eq!(&decoded.files[0].smallest_key, &Some(key));
            prop_assert_eq!(decoded.files[0].largest_seq, Some(sequence));
        }
    }

    #[test]
    fn should_reject_corrupt_kind_length_and_payload_when_decoding_metadata() {
        // Arrange
        let encoded = encode(MANIFEST, &crate::metadata::Manifest::default()).expect("encode");

        // Act / Assert
        for offset in [0, 8, 9, HEADER_BYTES, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[offset] ^= 1;
            assert!(decode::<crate::metadata::Manifest>(MANIFEST, &corrupt).is_err());
        }
        for length in 0..encoded.len() {
            assert!(decode::<crate::metadata::Manifest>(MANIFEST, &encoded[..length]).is_err());
        }
        assert!(decode::<crate::metadata::Manifest>(JOURNAL, &encoded).is_err());
    }

    #[test]
    fn should_reject_announced_lengths_and_trailing_values_before_metadata_allocation() {
        // Arrange: these small, correctly checksummed frames announce 4 GiB
        // strings, byte buffers, arrays or maps, or contain two root values.
        let payloads: &[&[u8]] = &[
            &[0xdb, 0xff, 0xff, 0xff, 0xff],
            &[0xc6, 0xff, 0xff, 0xff, 0xff],
            &[0xdd, 0xff, 0xff, 0xff, 0xff],
            &[0xdf, 0xff, 0xff, 0xff, 0xff],
            &[0xc0, 0xc0],
        ];

        // Act / Assert
        for payload in payloads {
            let mut encoded = MAGIC.to_vec();
            encoded.push(MANIFEST);
            encoded.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
            encoded.extend_from_slice(payload);
            let checksum = crc32c::crc32c(&encoded);
            encoded.extend_from_slice(&checksum.to_le_bytes());
            assert!(matches!(
                decode::<serde_json::Value>(MANIFEST, &encoded),
                Err(MidgeError::Corruption(_))
            ));
        }
    }

    #[test]
    fn should_reduce_manifest_bytes_when_encoding_twenty_thousand_bounded_files() {
        // Arrange
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files = (0..20_000)
            .map(|ordinal| crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(0, 1, ordinal),
                level: 1,
                size_bytes: 32 * 1024 * 1024,
                smallest_key: Some(format!("key/{ordinal:020}/a").into_bytes()),
                largest_key: Some(format!("key/{ordinal:020}/z").into_bytes()),
                smallest_seq: Some(ordinal),
                largest_seq: Some(ordinal + 10),
                key_bounds_complete: true,
                ..Default::default()
            })
            .collect();

        // Act
        let json = serde_json::to_vec(&manifest).expect("encode prior JSON representation");
        let binary = encode(MANIFEST, &manifest).expect("encode binary manifest");
        let decoded: crate::metadata::Manifest =
            decode(MANIFEST, &binary).expect("decode manifest");

        // Assert: record exact footprint evidence when qualification executes.
        assert_eq!(decoded.files.len(), 20_000);
        assert!(binary.len() < json.len());
        println!(
            "manifest_20000_files json_bytes={} binary_bytes={}",
            json.len(),
            binary.len()
        );
    }
}
