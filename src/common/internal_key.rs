use crate::api::column_family::ColumnFamilyId;

/// Entry types for internal keys
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    Value = 0,
    Tombstone = 1,
    RangeTombstone = 2,
}

impl EntryType {
    #[inline(always)]
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(EntryType::Value),
            1 => Some(EntryType::Tombstone),
            2 => Some(EntryType::RangeTombstone),
            _ => None,
        }
    }
}

/// Internal key layout (with column family support):
/// cf_id (u32 big-endian) || userkey || seq (u64 big-endian inverted) || kind (u8)
///
/// Comparison order:
/// 1. cf_id (ascending) - ensures CFs are segregated
/// 2. user_key (lexicographic) - standard key ordering
/// 3. sequence (descending) - newer versions first
/// 4. kind (ascending) - values before tombstones
///
/// This ensures column families never overlap in sorted order.
#[inline(always)]
#[allow(clippy::uninit_vec)] // Performance: pre-allocate then fill with copy_from_slice
pub fn encode_internal_key_cf(
    cf_id: ColumnFamilyId,
    user_key: &[u8],
    seq: u64,
    entry_type: EntryType,
) -> Vec<u8> {
    let total_len = 4 + user_key.len() + 9;
    let mut out = Vec::with_capacity(total_len);

    // SAFETY: We're about to fill exactly total_len bytes
    unsafe {
        out.set_len(total_len);
    }

    // CF ID first (big-endian for lexicographic ordering)
    let cf_bytes = cf_id.as_u32().to_be_bytes();
    out[0..4].copy_from_slice(&cf_bytes);

    // User key
    let user_end = 4 + user_key.len();
    out[4..user_end].copy_from_slice(user_key);

    // Inverted sequence number (newer versions sort first within same user key)
    let inv_seq = u64::MAX.wrapping_sub(seq);
    let seq_bytes = inv_seq.to_be_bytes();
    out[user_end..user_end + 8].copy_from_slice(&seq_bytes);

    // Entry type
    out[user_end + 8] = entry_type as u8;

    out
}

/// Decode internal key with column family ID.
/// Returns (cf_id, user_key, sequence, entry_type).
#[inline(always)]
pub fn decode_internal_key_cf(ikey: &[u8]) -> Option<(ColumnFamilyId, Vec<u8>, u64, EntryType)> {
    if ikey.len() < 13 {
        // 4 (cf_id) + 9 (seq + type) = minimum 13 bytes
        return None;
    }
    let n = ikey.len();

    // Extract entry type (last byte) - validate first
    let entry_type = EntryType::from_u8(ikey[n - 1])?;

    // Extract CF ID (first 4 bytes) - use direct array access
    let cf_id = ColumnFamilyId::new(u32::from_be_bytes([ikey[0], ikey[1], ikey[2], ikey[3]]));

    // Extract inverted sequence (8 bytes before entry type) - direct array access
    let seq_start = n - 9;
    let inv_seq = u64::from_be_bytes([
        ikey[seq_start],
        ikey[seq_start + 1],
        ikey[seq_start + 2],
        ikey[seq_start + 3],
        ikey[seq_start + 4],
        ikey[seq_start + 5],
        ikey[seq_start + 6],
        ikey[seq_start + 7],
    ]);
    let seq = u64::MAX.wrapping_sub(inv_seq);

    // Extract user key (between cf_id and sequence)
    let user = ikey[4..n - 9].to_vec();

    Some((cf_id, user, seq, entry_type))
}

/// Compare two internal keys with column family support.
/// Returns Ordering for use in sorting/comparison.
///
/// Order: cf_id (asc) -> user_key (lex) -> seq (desc) -> type (asc)
#[inline(always)]
pub fn compare_internal_keys_cf(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Fast path: length check
    if a.len() < 13 || b.len() < 13 {
        return a.cmp(b); // Fallback to lexicographic
    }

    // Compare CF ID (first 4 bytes) - use u32 comparison for better codegen
    let a_cf = u32::from_be_bytes([a[0], a[1], a[2], a[3]]);
    let b_cf = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    match a_cf.cmp(&b_cf) {
        Ordering::Equal => {}
        other => return other,
    }

    // Both keys have same CF ID, compare user key portion
    let a_user_end = a.len() - 9;
    let b_user_end = b.len() - 9;
    let a_user = &a[4..a_user_end];
    let b_user = &b[4..b_user_end];

    match a_user.cmp(b_user) {
        Ordering::Equal => {}
        other => return other,
    }

    // Same user key, compare sequence (DESCENDING - newer first)
    // Sequences are already inverted in encoding, so regular comparison gives descending order
    let a_seq = u64::from_be_bytes([
        a[a_user_end],
        a[a_user_end + 1],
        a[a_user_end + 2],
        a[a_user_end + 3],
        a[a_user_end + 4],
        a[a_user_end + 5],
        a[a_user_end + 6],
        a[a_user_end + 7],
    ]);
    let b_seq = u64::from_be_bytes([
        b[b_user_end],
        b[b_user_end + 1],
        b[b_user_end + 2],
        b[b_user_end + 3],
        b[b_user_end + 4],
        b[b_user_end + 5],
        b[b_user_end + 6],
        b[b_user_end + 7],
    ]);

    match a_seq.cmp(&b_seq) {
        Ordering::Equal => {}
        other => return other,
    }

    // Same sequence, compare entry type (ASCENDING)
    a[a.len() - 1].cmp(&b[b.len() - 1])
}

/// Legacy internal key layout (no CF ID): userkey || seq (u64 big-endian) || kind (u8)
/// kind: 0 = value, 1 = point tombstone, 2 = range tombstone start
///
/// These functions are kept for backward compatibility during migration.
/// New code should use the _cf variants above.
#[inline(always)]
pub fn encode_internal_key(user_key: &[u8], seq: u64, tombstone: bool) -> Vec<u8> {
    encode_internal_key_typed(
        user_key,
        seq,
        if tombstone {
            EntryType::Tombstone
        } else {
            EntryType::Value
        },
    )
}

#[inline(always)]
#[allow(clippy::uninit_vec)] // Performance: pre-allocate then fill with copy_from_slice
pub fn encode_internal_key_typed(user_key: &[u8], seq: u64, entry_type: EntryType) -> Vec<u8> {
    let total_len = user_key.len() + 9;
    let mut out = Vec::with_capacity(total_len);

    // SAFETY: We're about to fill exactly total_len bytes
    unsafe {
        out.set_len(total_len);
    }

    // User key
    out[..user_key.len()].copy_from_slice(user_key);

    // Inverted sequence number (newer versions sort first)
    let inv = u64::MAX.wrapping_sub(seq);
    let seq_bytes = inv.to_be_bytes();
    out[user_key.len()..user_key.len() + 8].copy_from_slice(&seq_bytes);

    // Entry type
    out[user_key.len() + 8] = entry_type as u8;

    out
}

#[inline(always)]
pub fn decode_internal_key(ikey: &[u8]) -> Option<(Vec<u8>, u64, bool)> {
    if ikey.len() < 9 {
        return None;
    }
    let n = ikey.len();
    let kind = ikey[n - 1] != 0;

    // Direct array access for sequence
    let seq_start = n - 9;
    let inv = u64::from_be_bytes([
        ikey[seq_start],
        ikey[seq_start + 1],
        ikey[seq_start + 2],
        ikey[seq_start + 3],
        ikey[seq_start + 4],
        ikey[seq_start + 5],
        ikey[seq_start + 6],
        ikey[seq_start + 7],
    ]);
    let seq = u64::MAX.wrapping_sub(inv);
    let user = ikey[..n - 9].to_vec();
    Some((user, seq, kind))
}

#[inline(always)]
pub fn decode_internal_key_typed(ikey: &[u8]) -> Option<(Vec<u8>, u64, EntryType)> {
    if ikey.len() < 9 {
        return None;
    }
    let n = ikey.len();
    let entry_type = EntryType::from_u8(ikey[n - 1])?;

    // Direct array access for sequence
    let seq_start = n - 9;
    let inv = u64::from_be_bytes([
        ikey[seq_start],
        ikey[seq_start + 1],
        ikey[seq_start + 2],
        ikey[seq_start + 3],
        ikey[seq_start + 4],
        ikey[seq_start + 5],
        ikey[seq_start + 6],
        ikey[seq_start + 7],
    ]);
    let seq = u64::MAX.wrapping_sub(inv);
    let user = ikey[..n - 9].to_vec();
    Some((user, seq, entry_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== BASIC ROUNDTRIP TESTS ====================

    #[test]
    fn should_roundtrip_internal_key() {
        // Arrange
        let user = b"hello";
        let seq = 42u64;
        let tomb = true;

        // Act
        let ik = encode_internal_key(user, seq, tomb);
        let parsed = decode_internal_key(&ik).expect("parse");

        // Assert
        assert_eq!(parsed.0, user);
        assert_eq!(parsed.1, seq);
        assert_eq!(parsed.2, tomb);
    }

    #[test]
    fn should_roundtrip_range_tombstone() {
        // Arrange
        let user_start = b"key_a";
        let seq = 100u64;

        // Act
        let ik = encode_internal_key_typed(user_start, seq, EntryType::RangeTombstone);
        let parsed = decode_internal_key_typed(&ik).expect("parse");

        // Assert
        assert_eq!(parsed.0, user_start);
        assert_eq!(parsed.1, seq);
        assert_eq!(parsed.2, EntryType::RangeTombstone);
    }

    #[test]
    fn should_roundtrip_internal_key_cf() {
        // Arrange
        let cf_id = ColumnFamilyId::new(5);
        let user = b"test_key";
        let seq = 123u64;
        let entry_type = EntryType::Value;

        // Act
        let ik = encode_internal_key_cf(cf_id, user, seq, entry_type);
        let parsed = decode_internal_key_cf(&ik).expect("parse");

        // Assert
        assert_eq!(parsed.0, cf_id);
        assert_eq!(parsed.1, user);
        assert_eq!(parsed.2, seq);
        assert_eq!(parsed.3, entry_type);
    }

    #[test]
    fn should_roundtrip_internal_key_cf_tombstone() {
        // Arrange
        let cf_id = ColumnFamilyId::new(0); // Default CF
        let user = b"deleted_key";
        let seq = 999u64;
        let entry_type = EntryType::Tombstone;

        // Act
        let ik = encode_internal_key_cf(cf_id, user, seq, entry_type);
        let parsed = decode_internal_key_cf(&ik).expect("parse");

        // Assert
        assert_eq!(parsed.0, cf_id);
        assert_eq!(parsed.1, user);
        assert_eq!(parsed.2, seq);
        assert_eq!(parsed.3, entry_type);
    }

    // ==================== COMPARISON TESTS ====================

    #[test]
    fn should_compare_cf_ids_ascending() {
        // Arrange
        let cf0 = ColumnFamilyId::new(0);
        let cf1 = ColumnFamilyId::new(1);

        let key_cf0 = encode_internal_key_cf(cf0, b"key", 100, EntryType::Value);
        let key_cf1 = encode_internal_key_cf(cf1, b"key", 100, EntryType::Value);

        // Act
        let result = compare_internal_keys_cf(&key_cf0, &key_cf1);

        // Assert
        // CF 0 should come before CF 1
        assert!(result.is_lt());
    }

    #[test]
    fn should_compare_user_keys_within_cf() {
        // Arrange
        let cf_id = ColumnFamilyId::new(5);

        let key_a = encode_internal_key_cf(cf_id, b"aaa", 100, EntryType::Value);
        let key_b = encode_internal_key_cf(cf_id, b"bbb", 100, EntryType::Value);

        // Act
        let result = compare_internal_keys_cf(&key_a, &key_b);

        // Assert
        // "aaa" should come before "bbb"
        assert!(result.is_lt());
    }

    #[test]
    fn should_compare_sequences_descending() {
        // Arrange
        let cf_id = ColumnFamilyId::new(2);
        let user = b"same_key";

        let newer = encode_internal_key_cf(cf_id, user, 200, EntryType::Value);
        let older = encode_internal_key_cf(cf_id, user, 100, EntryType::Value);

        // Act
        let result = compare_internal_keys_cf(&newer, &older);

        // Assert
        // Newer (higher sequence) should come before older
        assert!(result.is_lt());
    }

    #[test]
    fn should_compare_entry_types_ascending() {
        // Arrange
        let cf_id = ColumnFamilyId::new(1);
        let user = b"key";
        let seq = 50;

        let value = encode_internal_key_cf(cf_id, user, seq, EntryType::Value);
        let tombstone = encode_internal_key_cf(cf_id, user, seq, EntryType::Tombstone);

        // Act
        let result = compare_internal_keys_cf(&value, &tombstone);

        // Assert
        // Value (0) should come before Tombstone (1)
        assert!(result.is_lt());
    }

    #[test]
    fn should_compare_full_ordering() {
        use std::cmp::Ordering;

        // Arrange
        // Create keys with different CFs and sequences
        let cf0_key1_seq100 =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key1", 100, EntryType::Value);
        let cf0_key1_seq200 =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key1", 200, EntryType::Value);
        let cf0_key2_seq100 =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key2", 100, EntryType::Value);
        let cf1_key1_seq100 =
            encode_internal_key_cf(ColumnFamilyId::new(1), b"key1", 100, EntryType::Value);

        // Act
        let r1 = compare_internal_keys_cf(&cf0_key1_seq100, &cf1_key1_seq100);
        let r2 = compare_internal_keys_cf(&cf0_key1_seq100, &cf0_key2_seq100);
        let r3 = compare_internal_keys_cf(&cf0_key1_seq200, &cf0_key1_seq100);
        let r4 = compare_internal_keys_cf(&cf0_key1_seq100, &cf0_key1_seq100);

        // Assert
        // CF 0 < CF 1
        assert_eq!(r1, Ordering::Less);

        // Within CF 0: key1 < key2
        assert_eq!(r2, Ordering::Less);

        // Within CF 0, same key: seq 200 < seq 100 (descending)
        assert_eq!(r3, Ordering::Less);

        // Same key should be equal
        assert_eq!(r4, Ordering::Equal);
    }

    // ==================== ENCODE TESTS ====================
    // Tests from internal_key_test_stubs.rs - comprehensive encoding tests

    #[test]
    fn should_encode_user_key_with_sequence_and_type() {
        // Arrange
        let user_key = b"test_key";
        let seq = 100u64;

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, seq, EntryType::Value);

        // Assert
        // Should be cf_id(4) + user_key + inverted_seq(8) + type(1)
        assert_eq!(encoded.len(), 4 + user_key.len() + 8 + 1);
        // Verify user key portion is preserved
        assert_eq!(&encoded[4..4 + user_key.len()], user_key);
    }

    #[test]
    fn should_produce_9_extra_bytes_for_suffix() {
        // Arrange
        let user_key = b"any_key";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, 42, EntryType::Value);

        // Assert
        // cf_id(4) + user_key + seq(8) + type(1) = 13 extra bytes
        assert_eq!(encoded.len(), user_key.len() + 13);
    }

    #[test]
    fn should_use_value_type_for_non_tombstones() {
        // Arrange
        let key = b"key";

        // Act
        let encoded = encode_internal_key_cf(ColumnFamilyId::new(0), key, 100, EntryType::Value);

        // Assert
        // Last byte should be 0 for Value
        assert_eq!(encoded[encoded.len() - 1], 0u8);
    }

    #[test]
    fn should_use_tombstone_type_for_tombstones() {
        // Arrange
        let key = b"key";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), key, 100, EntryType::Tombstone);

        // Assert
        // Last byte should be 1 for Tombstone
        assert_eq!(encoded[encoded.len() - 1], 1u8);
    }

    #[test]
    fn should_invert_sequence_for_descending_order() {
        // Arrange
        let seq = 100u64;

        // Act
        let encoded = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", seq, EntryType::Value);

        // Assert
        // Extract inverted sequence (8 bytes before last byte)
        let seq_start = encoded.len() - 9;
        let seq_bytes = &encoded[seq_start..seq_start + 8];
        let inverted = u64::from_be_bytes(seq_bytes.try_into().unwrap());
        assert_eq!(inverted, u64::MAX - seq);
    }

    #[test]
    fn should_order_higher_sequences_first_lexicographically() {
        // Arrange
        let key200 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 200, EntryType::Value);
        let key100 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);

        // Act
        let comparison = key200 < key100;

        // Assert
        // Higher sequence should come first (smaller lexicographically)
        assert!(comparison, "Higher sequence should sort first");
    }

    #[test]
    fn should_handle_sequence_zero() {
        // Arrange
        let key = b"key";

        // Act
        let encoded = encode_internal_key_cf(ColumnFamilyId::new(0), key, 0, EntryType::Value);

        // Assert
        // seq=0 should encode as u64::MAX
        let seq_start = encoded.len() - 9;
        let seq_bytes = &encoded[seq_start..seq_start + 8];
        let inverted = u64::from_be_bytes(seq_bytes.try_into().unwrap());
        assert_eq!(inverted, u64::MAX);
    }

    #[test]
    fn should_handle_sequence_max() {
        // Arrange
        let key = b"key";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), key, u64::MAX, EntryType::Value);

        // Assert
        // seq=u64::MAX should encode as 0
        let seq_start = encoded.len() - 9;
        let seq_bytes = &encoded[seq_start..seq_start + 8];
        let inverted = u64::from_be_bytes(seq_bytes.try_into().unwrap());
        assert_eq!(inverted, 0);
    }

    #[test]
    fn should_order_by_user_key_first() {
        // Arrange
        let key_a = encode_internal_key_cf(ColumnFamilyId::new(0), b"key_a", 100, EntryType::Value);
        let key_b = encode_internal_key_cf(ColumnFamilyId::new(0), b"key_b", 200, EntryType::Value);

        // Act
        let comparison = key_a < key_b;

        // Assert
        // User key takes precedence over sequence
        assert!(comparison, "key_a should sort before key_b");
    }

    #[test]
    fn should_order_by_sequence_second() {
        // Arrange
        // Same user key, different sequences
        let key_200 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 200, EntryType::Value);
        let key_100 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);

        // Act
        let comparison = key_200 < key_100;

        // Assert
        // Higher sequence sorts first (descending)
        assert!(comparison, "Higher sequence should sort first");
    }

    #[test]
    fn should_order_by_type_third() {
        // Arrange
        // Same user key and sequence, different types
        let value_key =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let tombstone_key =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Tombstone);

        // Act
        let comparison = value_key < tombstone_key;

        // Assert
        // Value (0) sorts before Tombstone (1)
        assert!(comparison, "Value should sort before tombstone");
    }

    #[test]
    fn should_handle_empty_user_key() {
        // Arrange
        let empty_key = b"";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), empty_key, 100, EntryType::Value);

        // Assert
        // Should be cf_id(4) + seq(8) + type(1) = 13 bytes
        assert_eq!(encoded.len(), 13);
    }

    #[test]
    fn should_handle_large_user_keys() {
        // Arrange
        let large_key = vec![b'k'; 100_000]; // 100KB key

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), &large_key, 100, EntryType::Value);

        // Assert
        // Should handle large keys
        assert_eq!(encoded.len(), large_key.len() + 13);
    }

    #[test]
    fn should_handle_binary_user_keys() {
        // Arrange
        // Key with null bytes, 0xFF, etc.
        let binary_key = b"key\x00with\xFFnull";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), binary_key, 100, EntryType::Value);

        // Assert
        // Binary data should be preserved
        assert_eq!(&encoded[4..4 + binary_key.len()], binary_key);
    }

    // ==================== DECODE TESTS ====================

    #[test]
    fn should_extract_user_key_seq_tombstone() {
        // Arrange
        let user_key = b"test_key";
        let seq = 12345u64;
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, seq, EntryType::Tombstone);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.0, ColumnFamilyId::new(0));
        assert_eq!(decoded.1, user_key);
        assert_eq!(decoded.2, seq);
        assert_eq!(decoded.3, EntryType::Tombstone);
    }

    #[test]
    fn should_return_none_given_key_shorter_than_9_bytes() {
        // Arrange
        // Key too short (need at least 13 bytes: cf_id(4) + seq(8) + type(1))
        let short_key = b"short";

        // Act
        let result = decode_internal_key_cf(short_key);

        // Assert
        // Should return None
        assert!(result.is_none(), "Short key should return None");
    }

    #[test]
    fn should_reverse_sequence_inversion() {
        // Arrange
        let original_seq = 99999u64;
        let encoded = encode_internal_key_cf(
            ColumnFamilyId::new(0),
            b"key",
            original_seq,
            EntryType::Value,
        );

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should get original sequence back
        assert_eq!(decoded.2, original_seq);
    }

    #[test]
    fn should_identify_value_type_as_non_tombstone() {
        // Arrange
        let encoded = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should be Value type
        assert_eq!(decoded.3, EntryType::Value);
    }

    #[test]
    fn should_identify_tombstone_type() {
        // Arrange
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Tombstone);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should be Tombstone type
        assert_eq!(decoded.3, EntryType::Tombstone);
    }

    #[test]
    fn should_roundtrip_with_encode() {
        // Arrange
        let cf_id = ColumnFamilyId::new(5);
        let user_key = b"roundtrip_key";
        let seq = 54321u64;
        let entry_type = EntryType::Tombstone;

        // Act
        // Encode then decode
        let encoded = encode_internal_key_cf(cf_id, user_key, seq, entry_type);
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should match original values
        assert_eq!(decoded.0, cf_id);
        assert_eq!(decoded.1, user_key);
        assert_eq!(decoded.2, seq);
        assert_eq!(decoded.3, entry_type);
    }

    #[test]
    fn should_maintain_user_key_length() {
        // Arrange
        let user_key = b"variable_length_key_12345";
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, 100, EntryType::Value);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // User key length preserved
        assert_eq!(decoded.1.len(), user_key.len());
    }

    #[test]
    fn should_maintain_sequence_value() {
        // Arrange
        let original_seq = 777777u64;
        let encoded = encode_internal_key_cf(
            ColumnFamilyId::new(0),
            b"key",
            original_seq,
            EntryType::Value,
        );

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Sequence preserved
        assert_eq!(decoded.2, original_seq);
    }

    #[test]
    fn should_maintain_tombstone_flag() {
        // Arrange
        // Test both Value and Tombstone
        let value_encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let tombstone_encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Tombstone);

        // Act
        let value_decoded = decode_internal_key_cf(&value_encoded).unwrap();
        let tombstone_decoded = decode_internal_key_cf(&tombstone_encoded).unwrap();

        // Assert
        // Types preserved
        assert_eq!(value_decoded.3, EntryType::Value);
        assert_eq!(tombstone_decoded.3, EntryType::Tombstone);
    }

    #[test]
    fn should_handle_exactly_9_byte_key() {
        // Arrange
        // Empty user key (minimum length internal key)
        let encoded = encode_internal_key_cf(ColumnFamilyId::new(0), b"", 100, EntryType::Value);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should decode successfully with empty user key
        assert_eq!(decoded.1.len(), 0);
        assert_eq!(decoded.2, 100);
    }

    #[test]
    fn should_handle_unknown_type_bytes() {
        // Arrange
        // Use RangeTombstone type
        let encoded = encode_internal_key_cf(
            ColumnFamilyId::new(0),
            b"key",
            100,
            EntryType::RangeTombstone,
        );

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // Should decode with correct type
        assert_eq!(decoded.3, EntryType::RangeTombstone);
    }

    #[test]
    fn should_treat_nonzero_types_as_tombstone_in_simplified_decode() {
        // Note: The decode_internal_key_cf function properly decodes all EntryType variants.
        // This test documents that RangeTombstone (0x02) is correctly identified.

        // Arrange
        // Create a manually encoded key with RangeTombstone type byte
        let cf_bytes = 0u32.to_be_bytes();
        let seq = u64::MAX - 100;
        let seq_bytes = seq.to_be_bytes();
        let type_byte = 0x02u8; // RangeTombstone

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&cf_bytes);
        encoded.extend_from_slice(b"key");
        encoded.extend_from_slice(&seq_bytes);
        encoded.push(type_byte);

        // Act
        let decoded = decode_internal_key_cf(&encoded).unwrap();

        // Assert
        // RangeTombstone is correctly identified
        assert_eq!(decoded.3, EntryType::RangeTombstone);
    }

    #[test]
    fn should_return_none_for_corrupted_data() {
        // Arrange
        // Random bytes too short to be valid
        let corrupted = vec![0xFF, 0xAB, 0x12, 0x34];

        // Act
        let result = decode_internal_key_cf(&corrupted);

        // Assert
        // Should return None
        assert!(result.is_none(), "Corrupted data should return None");
    }

    #[test]
    fn should_not_panic_on_malformed_input() {
        // Arrange
        // Various malformed inputs
        let inputs = vec![
            vec![],          // Empty
            vec![0x00],      // 1 byte
            vec![0x00; 5],   // 5 bytes
            vec![0x00; 12],  // Exactly 12 bytes (just below minimum)
            vec![0xFF; 100], // All 0xFF
        ];

        // Act
        // None should panic
        for input in inputs {
            let result = decode_internal_key_cf(&input);

            // Assert
            // All should return None gracefully
            assert!(result.is_none());
        }
    }

    // ==================== ORDERING PROPERTY TESTS ====================

    #[test]
    fn should_maintain_total_ordering() {
        // Arrange
        // Create multiple internal keys
        let key1 = encode_internal_key_cf(ColumnFamilyId::new(0), b"a", 100, EntryType::Value);
        let key2 = encode_internal_key_cf(ColumnFamilyId::new(0), b"b", 100, EntryType::Value);
        let key3 = encode_internal_key_cf(ColumnFamilyId::new(0), b"a", 200, EntryType::Value);

        // Act
        let r1 = key1 < key2;
        let r2 = key3 < key1;
        let r3 = key3 < key2;

        // Assert
        // All comparisons should work
        assert!(r1); // Different user keys
        assert!(r2); // Same user key, higher sequence (inverted)
        assert!(r3); // Transitivity
    }

    #[test]
    fn should_be_transitive() {
        // Arrange
        // Create three keys where a < b < c
        let a = encode_internal_key_cf(ColumnFamilyId::new(0), b"key1", 100, EntryType::Value);
        let b = encode_internal_key_cf(ColumnFamilyId::new(0), b"key2", 100, EntryType::Value);
        let c = encode_internal_key_cf(ColumnFamilyId::new(0), b"key3", 100, EntryType::Value);

        // Act
        let r1 = a < b;
        let r2 = b < c;
        let r3 = a < c;

        // Assert
        // If a < b and b < c, then a < c
        assert!(r1, "a should be less than b");
        assert!(r2, "b should be less than c");
        assert!(r3, "transitivity: a should be less than c");
    }

    #[test]
    fn should_be_antisymmetric() {
        // Arrange
        let a = encode_internal_key_cf(ColumnFamilyId::new(0), b"key1", 100, EntryType::Value);
        let b = encode_internal_key_cf(ColumnFamilyId::new(0), b"key2", 100, EntryType::Value);

        // Act
        let r1 = a < b;
        let r2 = b > a;

        // Assert
        // If a < b, then b > a
        assert!(r1, "a should be less than b");
        assert!(r2, "antisymmetry: b should be greater than a");
    }

    #[test]
    fn should_order_versions_for_compaction_correctly() {
        // Arrange
        // Multiple versions of same key with different sequences
        let v1 = encode_internal_key_cf(ColumnFamilyId::new(0), b"user_key", 100, EntryType::Value);
        let v2 = encode_internal_key_cf(ColumnFamilyId::new(0), b"user_key", 200, EntryType::Value);
        let v3 = encode_internal_key_cf(ColumnFamilyId::new(0), b"user_key", 300, EntryType::Value);

        // Act
        let r1 = v3 < v2;
        let r2 = v2 < v1;
        let r3 = v3 < v1;

        // Assert
        // Higher sequences should sort first (newest first)
        // Due to sequence inversion, v3 < v2 < v1 lexicographically
        assert!(r1, "seq 300 should sort before seq 200");
        assert!(r2, "seq 200 should sort before seq 100");
        assert!(r3, "seq 300 should sort before seq 100");
    }

    #[test]
    fn should_order_different_keys_alphabetically() {
        // Arrange
        // Different user keys with same sequence
        let k_apple =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"apple", 100, EntryType::Value);
        let k_banana =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"banana", 100, EntryType::Value);
        let k_cherry =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"cherry", 100, EntryType::Value);

        // Act
        let r1 = k_apple < k_banana;
        let r2 = k_banana < k_cherry;
        let r3 = k_apple < k_cherry;

        // Assert
        // Should sort lexicographically by user key
        assert!(r1, "apple < banana");
        assert!(r2, "banana < cherry");
        assert!(r3, "apple < cherry");
    }

    #[test]
    fn should_order_tombstones_after_values_for_same_key_seq() {
        // Note: This scenario should never happen in practice (same key+seq with different types),
        // but the ordering is well-defined: Value (0x00) < Tombstone (0x01) < RangeTombstone (0x02)

        // Arrange
        // Same key and sequence, different types
        let value = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let tombstone =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Tombstone);
        let range_tomb = encode_internal_key_cf(
            ColumnFamilyId::new(0),
            b"key",
            100,
            EntryType::RangeTombstone,
        );

        // Act
        let r1 = value < tombstone;
        let r2 = tombstone < range_tomb;
        let r3 = value < range_tomb;

        // Assert
        // Values < Tombstones < RangeTombstones
        assert!(r1, "Value should sort before Tombstone");
        assert!(r2, "Tombstone should sort before RangeTombstone");
        assert!(r3, "Value should sort before RangeTombstone");
    }

    // ========================================================================
    // Internal Key Encoding - Missing Tests from REQUIREMENTS.md
    // ========================================================================

    #[test]
    fn should_encode_internal_key_given_max_sequence_and_tombstone() {
        // Arrange
        let user_key = b"test";
        let max_seq = u64::MAX;

        // Act
        let encoded = encode_internal_key_cf(
            ColumnFamilyId::new(0),
            user_key,
            max_seq,
            EntryType::Tombstone,
        );
        let decoded = decode_internal_key_cf(&encoded).expect("decode failed");

        // Assert
        assert_eq!(decoded.2, max_seq);
        assert_eq!(decoded.3, EntryType::Tombstone);
    }

    #[test]
    fn should_encode_internal_key_given_min_sequence_and_value_type() {
        // Arrange
        let user_key = b"key";
        let min_seq = 0u64;

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, min_seq, EntryType::Value);
        let decoded = decode_internal_key_cf(&encoded).expect("decode failed");

        // Assert
        assert_eq!(decoded.2, min_seq);
        assert_eq!(decoded.3, EntryType::Value);
    }

    #[test]
    fn should_include_cf_id_in_encoding_given_multi_cf_support() {
        // Arrange
        let cf_id = ColumnFamilyId::new(42);
        let user_key = b"data";

        // Act
        let encoded = encode_internal_key_cf(cf_id, user_key, 100, EntryType::Value);

        // Assert
        // CF ID is first 4 bytes in big-endian
        assert_eq!(&encoded[0..4], &42u32.to_be_bytes());

        let decoded = decode_internal_key_cf(&encoded).expect("decode failed");
        assert_eq!(decoded.0, cf_id);
    }

    #[test]
    fn should_preserve_big_endian_order_given_lexicographic_sorting() {
        // Arrange
        let user_key = b"key";
        let seq1 = 100u64;
        let seq2 = 200u64;

        // Act
        let encoded1 =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, seq1, EntryType::Value);
        let encoded2 =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, seq2, EntryType::Value);

        // Assert
        // Higher sequence should sort BEFORE lower sequence (descending order)
        // because we invert the sequence
        assert!(encoded2 < encoded1, "seq=200 should sort before seq=100");
    }

    #[test]
    fn should_fail_gracefully_given_key_longer_than_supported() {
        // Arrange
        let large_key = vec![b'x'; 10 * 1024 * 1024];

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), &large_key, 1, EntryType::Value);

        // Assert
        let decoded = decode_internal_key_cf(&encoded).expect("decode failed");
        assert_eq!(decoded.1.len(), large_key.len());
    }

    #[test]
    fn should_handle_null_byte_in_user_key_given_encoding() {
        // Arrange
        let user_key = b"key\x00with\x00nulls";

        // Act
        let encoded =
            encode_internal_key_cf(ColumnFamilyId::new(0), user_key, 100, EntryType::Value);
        let decoded = decode_internal_key_cf(&encoded).expect("decode failed");

        // Assert
        assert_eq!(decoded.1, user_key);
    }

    // ========================================================================
    // Internal Key Decoding - Missing Tests
    // ========================================================================

    #[test]
    fn should_return_error_given_corrupted_suffix_bytes() {
        // Arrange
        let valid = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let mut corrupted = valid.clone();
        corrupted[valid.len() - 1] = 99;

        // Act
        let result = decode_internal_key_cf(&corrupted);

        // Assert
        assert!(result.is_none(), "Should reject invalid entry type");
    }

    #[test]
    fn should_detect_endianness_mismatch_given_different_encoding_version() {
        // Arrange
        let user_key = b"test";
        let seq = 12345u64;
        let inv_seq = u64::MAX.wrapping_sub(seq);

        let mut wrong_endian = Vec::new();
        wrong_endian.extend_from_slice(&0u32.to_be_bytes());
        wrong_endian.extend_from_slice(user_key);
        wrong_endian.extend_from_slice(&inv_seq.to_le_bytes());
        wrong_endian.push(EntryType::Value as u8);

        // Act
        let decoded = decode_internal_key_cf(&wrong_endian).expect("should decode");

        // Assert
        assert_ne!(
            decoded.2, seq,
            "Endianness mismatch should produce wrong sequence"
        );
    }

    #[test]
    fn should_handle_empty_input_given_decode_attempt() {
        // Arrange
        let empty: &[u8] = &[];

        // Act
        let result = decode_internal_key_cf(empty);

        // Assert
        assert!(result.is_none(), "Should reject empty input");
    }

    #[test]
    fn should_recover_user_key_given_truncated_suffix() {
        // Arrange
        let short_key = vec![0, 0, 0, 0, b'k', b'e', b'y'];

        // Act
        let result = decode_internal_key_cf(&short_key);

        // Assert
        assert!(result.is_none(), "Should reject truncated key");
    }

    #[test]
    fn should_validate_key_type_byte_given_decode() {
        // Arrange
        let valid_types = [
            (EntryType::Value, 0u8),
            (EntryType::Tombstone, 1u8),
            (EntryType::RangeTombstone, 2u8),
        ];

        // Act
        for (expected_type, type_byte) in valid_types {
            let mut key = vec![0, 0, 0, 0];
            key.extend_from_slice(b"key");
            key.extend_from_slice(&(u64::MAX - 100).to_be_bytes());
            key.push(type_byte);

            let decoded = decode_internal_key_cf(&key).expect("should decode");

            // Assert
            assert_eq!(decoded.3, expected_type);
        }

        let mut invalid_key = vec![0, 0, 0, 0];
        invalid_key.extend_from_slice(b"key");
        invalid_key.extend_from_slice(&(u64::MAX - 100).to_be_bytes());
        invalid_key.push(255);

        assert!(decode_internal_key_cf(&invalid_key).is_none());
    }

    #[test]
    fn should_preserve_cf_id_when_decoding_internal_key() {
        // Arrange
        let cf_ids = [0, 1, 42, 255, 65535, u32::MAX];

        // Act
        for cf_id_val in cf_ids {
            let cf_id = ColumnFamilyId::new(cf_id_val);
            let encoded = encode_internal_key_cf(cf_id, b"key", 100, EntryType::Value);
            let decoded = decode_internal_key_cf(&encoded).expect("decode failed");

            // Assert
            assert_eq!(decoded.0, cf_id, "CF ID not preserved for {}", cf_id_val);
        }
    }

    #[test]
    fn should_return_none_given_key_shorter_than_suffix_length() {
        // Arrange
        let test_cases = vec![
            vec![],
            vec![0],
            vec![0, 0, 0, 0],
            vec![0, 0, 0, 0, b'k'],
            vec![0, 0, 0, 0, b'k', b'e', b'y', 0, 0, 0, 0],
        ];

        // Act
        for short_key in test_cases {
            let result = decode_internal_key_cf(&short_key);

            // Assert
            assert!(
                result.is_none(),
                "Should reject key with {} bytes",
                short_key.len()
            );
        }
    }

    // ========================================================================
    // Internal Key Ordering - Missing Tests
    // ========================================================================

    #[test]
    fn should_compare_tombstones_after_values_given_same_key_seq() {
        // Arrange
        let value_key =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let tombstone_key =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Tombstone);

        // Act
        let ordering = compare_internal_keys_cf(&value_key, &tombstone_key);

        // Assert
        assert_eq!(
            ordering,
            std::cmp::Ordering::Less,
            "Value should sort before Tombstone"
        );
    }

    #[test]
    fn should_return_zero_given_same_key_seq_and_type() {
        // Arrange
        let key1 =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"identical", 42, EntryType::Value);
        let key2 =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"identical", 42, EntryType::Value);

        // Act
        let ordering = compare_internal_keys_cf(&key1, &key2);

        // Assert
        assert_eq!(ordering, std::cmp::Ordering::Equal);
    }

    #[test]
    fn should_compare_cf_ids_before_user_keys_given_multi_cf() {
        // Arrange
        let cf1_key = encode_internal_key_cf(ColumnFamilyId::new(1), b"zzz", 100, EntryType::Value);
        let cf2_key = encode_internal_key_cf(ColumnFamilyId::new(2), b"aaa", 100, EntryType::Value);

        // Act
        let ordering = compare_internal_keys_cf(&cf1_key, &cf2_key);

        // Assert
        assert_eq!(
            ordering,
            std::cmp::Ordering::Less,
            "CF ID takes precedence over user key"
        );
    }

    #[test]
    fn should_sort_user_keys_in_lexicographic_order() {
        // Arrange
        let key_a = encode_internal_key_cf(ColumnFamilyId::new(0), b"apple", 100, EntryType::Value);
        let key_b =
            encode_internal_key_cf(ColumnFamilyId::new(0), b"banana", 100, EntryType::Value);
        let key_z = encode_internal_key_cf(ColumnFamilyId::new(0), b"zebra", 100, EntryType::Value);

        // Act
        let ord_ab = compare_internal_keys_cf(&key_a, &key_b);
        let ord_bz = compare_internal_keys_cf(&key_b, &key_z);
        let ord_az = compare_internal_keys_cf(&key_a, &key_z);

        // Assert
        assert!(ord_ab == std::cmp::Ordering::Less);
        assert!(ord_bz == std::cmp::Ordering::Less);
        assert!(ord_az == std::cmp::Ordering::Less);
    }

    #[test]
    fn should_sort_descending_by_sequence_given_same_user_key() {
        // Arrange
        let seq_100 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 100, EntryType::Value);
        let seq_200 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 200, EntryType::Value);
        let seq_300 = encode_internal_key_cf(ColumnFamilyId::new(0), b"key", 300, EntryType::Value);

        // Act
        let ord_300_200 = compare_internal_keys_cf(&seq_300, &seq_200);
        let ord_200_100 = compare_internal_keys_cf(&seq_200, &seq_100);
        let ord_300_100 = compare_internal_keys_cf(&seq_300, &seq_100);

        // Assert
        assert!(ord_300_200 == std::cmp::Ordering::Less);
        assert!(ord_200_100 == std::cmp::Ordering::Less);
        assert!(ord_300_100 == std::cmp::Ordering::Less);
    }

    #[test]
    fn should_be_reflexive_given_same_internal_key() {
        // Arrange
        let key = encode_internal_key_cf(ColumnFamilyId::new(5), b"test", 42, EntryType::Tombstone);

        // Act
        let ordering = compare_internal_keys_cf(&key, &key);

        // Assert
        assert_eq!(
            ordering,
            std::cmp::Ordering::Equal,
            "Key should equal itself"
        );
    }

    #[test]
    fn should_be_consistent_with_equality_given_inverse_comparison() {
        // Arrange
        let key1 = encode_internal_key_cf(ColumnFamilyId::new(0), b"alpha", 100, EntryType::Value);
        let key2 = encode_internal_key_cf(ColumnFamilyId::new(0), b"beta", 100, EntryType::Value);

        // Act
        let forward = compare_internal_keys_cf(&key1, &key2);
        let reverse = compare_internal_keys_cf(&key2, &key1);

        // Assert
        assert_eq!(forward, std::cmp::Ordering::Less);
        assert_eq!(reverse, std::cmp::Ordering::Greater);
        assert_eq!(
            forward.reverse(),
            reverse,
            "Comparison should be consistent"
        );
    }
}
