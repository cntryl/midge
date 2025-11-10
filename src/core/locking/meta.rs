//! Lock metadata serialization and validation.

use crate::common::{timestamp, tlv};
use crate::error::MidgeResult;

/// Lock metadata stored in lock file or cloud blob
#[derive(Debug, Clone)]
pub struct LockMeta {
    /// Lock format version (always 1 for now)
    pub version: u8,

    /// Process ID of lock holder
    pub pid: u64,

    /// Hostname of lock holder
    pub host: String,

    /// Unique session ID (UUID bytes)
    pub uuid: [u8; 16],

    /// When lock was initially acquired (unix millis)
    pub acquired_at: u64,

    /// Last renewal timestamp (unix millis)
    pub renewed_at: u64,

    /// Time-to-live in milliseconds
    pub ttl_ms: u32,

    /// Flags bitfield (bit 0: released, bit 1: readonly)
    pub flags: u8,
}

// TLV field type IDs for lock metadata
// Format: (wire_type << 4) | field_id
// Wire types: U8=0, U16=1, U32=2, U64=3, Varint=4, Bytes=5
const TLV_VERSION: u8 = 0x01; // U8, field 1
const TLV_PID: u8 = 0x32; // U64, field 2
const TLV_HOST: u8 = 0x53; // Bytes, field 3
const TLV_UUID: u8 = 0x54; // Bytes, field 4
const TLV_ACQUIRED_AT: u8 = 0x35; // U64, field 5
const TLV_RENEWED_AT: u8 = 0x36; // U64, field 6
const TLV_TTL_MS: u8 = 0x27; // U32, field 7
const TLV_FLAGS: u8 = 0x08; // U8, field 8

// Flag bits
const FLAG_RELEASED: u8 = 0x01;

impl LockMeta {
    /// Create new lock metadata with current timestamp
    pub fn new(ttl_ms: u32) -> Self {
        let now = timestamp::now_millis();

        let pid = std::process::id() as u64;

        let host = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let uuid = uuid::Uuid::new_v4().into_bytes();

        Self {
            version: 1,
            pid,
            host,
            uuid,
            acquired_at: now,
            renewed_at: now,
            ttl_ms,
            flags: 0,
        }
    }

    /// Encode to TLV bytes
    pub fn encode(&self) -> MidgeResult<Vec<u8>> {
        let mut writer = tlv::TlvWriter::with_capacity(128);

        writer.write_u8(TLV_VERSION, self.version);
        writer.write_u64(TLV_PID, self.pid);
        writer.write_bytes(TLV_HOST, self.host.as_bytes());
        writer.write_bytes(TLV_UUID, &self.uuid);
        writer.write_u64(TLV_ACQUIRED_AT, self.acquired_at);
        writer.write_u64(TLV_RENEWED_AT, self.renewed_at);
        writer.write_u32(TLV_TTL_MS, self.ttl_ms);
        writer.write_u8(TLV_FLAGS, self.flags);

        Ok(writer.finish())
    }

    /// Decode from TLV bytes
    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        let reader = tlv::TlvReader::new(data);

        let mut meta = LockMeta {
            version: 0,
            pid: 0,
            host: String::new(),
            uuid: [0u8; 16],
            acquired_at: 0,
            renewed_at: 0,
            ttl_ms: 0,
            flags: 0,
        };

        for (tag, value) in reader {
            match tag {
                TLV_VERSION => meta.version = tlv::parse_u8(value)?,
                TLV_PID => meta.pid = tlv::parse_u64(value)?,
                TLV_HOST => {
                    meta.host = String::from_utf8_lossy(value).to_string();
                }
                TLV_UUID => {
                    if value.len() >= 16 {
                        meta.uuid.copy_from_slice(&value[..16]);
                    }
                }
                TLV_ACQUIRED_AT => meta.acquired_at = tlv::parse_u64(value)?,
                TLV_RENEWED_AT => meta.renewed_at = tlv::parse_u64(value)?,
                TLV_TTL_MS => meta.ttl_ms = tlv::parse_u32(value)?,
                TLV_FLAGS => meta.flags = tlv::parse_u8(value)?,
                _ => {
                    // Unknown field, skip
                }
            }
        }

        Ok(meta)
    }

    /// Check if lock has expired
    pub fn expired(&self) -> bool {
        let now = timestamp::now_millis();

        now > self.renewed_at + self.ttl_ms as u64
    }

    /// Check if lock is marked as released
    pub fn is_released(&self) -> bool {
        self.flags & FLAG_RELEASED != 0
    }

    /// Mark lock as released
    pub fn mark_released(&mut self) {
        self.flags |= FLAG_RELEASED;
    }

    /// Update renewal timestamp
    pub fn renew(&mut self) {
        self.renewed_at = timestamp::now_millis();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_lock_meta_encoding() {
        // Arrange
        let meta = LockMeta::new(5000);

        // Act
        let encoded = meta.encode().unwrap();
        let decoded = LockMeta::decode(&encoded).unwrap();

        // Assert
        assert_eq!(meta.version, decoded.version);
        assert_eq!(meta.pid, decoded.pid);
        assert_eq!(meta.host, decoded.host);
        assert_eq!(meta.uuid, decoded.uuid);
        assert_eq!(meta.ttl_ms, decoded.ttl_ms);
    }

    #[test]
    fn should_detect_expiration_given_elapsed_ttl() {
        // Arrange
        let mut meta = LockMeta::new(100); // 100ms TTL
        assert!(!meta.expired());
        meta.renewed_at = crate::common::timestamp::now_millis() - 200;

        // Act
        let is_expired = meta.expired();

        // Assert
        assert!(is_expired);
    }
}
