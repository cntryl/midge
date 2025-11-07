use crate::error::MidgeResult;
use crate::sst::traits::RangeTombstone;
use bytes::{BufMut, Bytes, BytesMut};

pub fn encode_range_tombstones(tombstones: &[RangeTombstone]) -> MidgeResult<Bytes> {
    let mut buf = BytesMut::new();
    buf.put_u32_le(tombstones.len() as u32);

    for rt in tombstones {
        buf.put_u32_le(rt.start.len() as u32);
        buf.put_slice(&rt.start);
        buf.put_u32_le(rt.end.len() as u32);
        buf.put_slice(&rt.end);
        buf.put_slice(&rt.seq.to_be_bytes());
    }

    Ok(buf.freeze())
}

pub fn decode_range_tombstones(data: &[u8]) -> MidgeResult<Vec<RangeTombstone>> {
    if data.len() < 4 {
        return Ok(Vec::new());
    }
    let mut cur = 0usize;
    let count =
        u32::from_le_bytes([data[cur], data[cur + 1], data[cur + 2], data[cur + 3]]) as usize;
    cur += 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if cur + 4 > data.len() {
            break;
        }
        let slen =
            u32::from_le_bytes([data[cur], data[cur + 1], data[cur + 2], data[cur + 3]]) as usize;
        cur += 4;
        if cur + slen > data.len() {
            break;
        }
        let start = data[cur..cur + slen].to_vec();
        cur += slen;
        if cur + 4 > data.len() {
            break;
        }
        let elen =
            u32::from_le_bytes([data[cur], data[cur + 1], data[cur + 2], data[cur + 3]]) as usize;
        cur += 4;
        if cur + elen > data.len() {
            break;
        }
        let end = data[cur..cur + elen].to_vec();
        cur += elen;
        if cur + 8 > data.len() {
            break;
        }
        let seq = u64::from_be_bytes([
            data[cur],
            data[cur + 1],
            data[cur + 2],
            data[cur + 3],
            data[cur + 4],
            data[cur + 5],
            data[cur + 6],
            data[cur + 7],
        ]);
        cur += 8;
        out.push(RangeTombstone { start, end, seq });
    }
    Ok(out)
}

pub fn is_covered_by_range_tombstone(ts: &[RangeTombstone], key: &[u8], snapshot_seq: u64) -> bool {
    ts.iter()
        .any(|t| t.seq <= snapshot_seq && key >= t.start.as_slice() && key < t.end.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tombstone(start: &[u8], end: &[u8], seq: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq,
        }
    }

    // --- encode_range_tombstones tests ---

    #[test]
    fn should_encode_empty_tombstone_list() {
        // Arrange
        let tombstones: Vec<RangeTombstone> = vec![];

        // Act
        let result = encode_range_tombstones(&tombstones).unwrap();

        // Assert
        assert_eq!(result.len(), 4); // Just the count field
        assert_eq!(&result[0..4], &[0, 0, 0, 0]); // count = 0
    }

    #[test]
    fn should_encode_single_tombstone() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 100)];

        // Act
        let result = encode_range_tombstones(&tombstones).unwrap();

        // Assert
        assert!(result.len() > 4);
        // First 4 bytes: count = 1
        assert_eq!(
            u32::from_le_bytes([result[0], result[1], result[2], result[3]]),
            1
        );
    }

    #[test]
    fn should_encode_multiple_tombstones() {
        // Arrange
        let tombstones = vec![
            create_tombstone(b"a", b"m", 100),
            create_tombstone(b"n", b"z", 200),
        ];

        // Act
        let result = encode_range_tombstones(&tombstones).unwrap();

        // Assert
        let count = u32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(count, 2);
    }

    #[test]
    fn should_encode_tombstone_with_empty_start_key() {
        // Arrange
        let tombstones = vec![create_tombstone(b"", b"end", 50)];

        // Act
        let result = encode_range_tombstones(&tombstones);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_encode_tombstone_with_binary_keys() {
        // Arrange
        let tombstones = vec![create_tombstone(&[0x00, 0xFF, 0xAB], &[0xCD, 0xEF], 42)];

        // Act
        let result = encode_range_tombstones(&tombstones);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_encode_tombstone_with_large_sequence() {
        // Arrange
        let tombstones = vec![create_tombstone(b"start", b"end", u64::MAX)];

        // Act
        let result = encode_range_tombstones(&tombstones);

        // Assert
        assert!(result.is_ok());
    }

    // --- decode_range_tombstones tests ---

    #[test]
    fn should_decode_empty_data() {
        // Arrange
        let data: &[u8] = &[];

        // Act
        let result = decode_range_tombstones(data).unwrap();

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_decode_data_with_zero_count() {
        // Arrange
        let data = vec![0, 0, 0, 0]; // count = 0

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_decode_single_tombstone() {
        // Arrange
        let original = vec![create_tombstone(b"key1", b"key9", 123)];
        let encoded = encode_range_tombstones(&original).unwrap();

        // Act
        let decoded = decode_range_tombstones(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].start, b"key1");
        assert_eq!(decoded[0].end, b"key9");
        assert_eq!(decoded[0].seq, 123);
    }

    #[test]
    fn should_decode_multiple_tombstones() {
        // Arrange
        let original = vec![
            create_tombstone(b"a", b"b", 10),
            create_tombstone(b"c", b"d", 20),
            create_tombstone(b"e", b"f", 30),
        ];
        let encoded = encode_range_tombstones(&original).unwrap();

        // Act
        let decoded = decode_range_tombstones(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].start, b"a");
        assert_eq!(decoded[1].start, b"c");
        assert_eq!(decoded[2].start, b"e");
        assert_eq!(decoded[0].seq, 10);
        assert_eq!(decoded[1].seq, 20);
        assert_eq!(decoded[2].seq, 30);
    }

    #[test]
    fn should_handle_truncated_count_field() {
        // Arrange
        let data = vec![0, 0, 0]; // Incomplete count

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_handle_truncated_start_length() {
        // Arrange
        let mut data = vec![1, 0, 0, 0]; // count = 1
        data.extend_from_slice(&[10, 0]); // Incomplete start length

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0); // Should skip incomplete entry
    }

    #[test]
    fn should_handle_truncated_start_data() {
        // Arrange
        let mut data = vec![1, 0, 0, 0]; // count = 1
        data.extend_from_slice(&[5, 0, 0, 0]); // start_len = 5
        data.extend_from_slice(b"abc"); // Only 3 bytes instead of 5

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0); // Should skip incomplete entry
    }

    #[test]
    fn should_handle_truncated_end_length() {
        // Arrange
        let mut data = vec![1, 0, 0, 0]; // count = 1
        data.extend_from_slice(&[3, 0, 0, 0]); // start_len = 3
        data.extend_from_slice(b"abc"); // start data
        data.extend_from_slice(&[5, 0]); // Incomplete end length

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0); // Should skip incomplete entry
    }

    #[test]
    fn should_handle_truncated_sequence() {
        // Arrange
        let mut data = vec![1, 0, 0, 0]; // count = 1
        data.extend_from_slice(&[3, 0, 0, 0]); // start_len = 3
        data.extend_from_slice(b"abc"); // start data
        data.extend_from_slice(&[3, 0, 0, 0]); // end_len = 3
        data.extend_from_slice(b"xyz"); // end data
        data.extend_from_slice(&[0, 0, 0, 0]); // Only 4 bytes of sequence

        // Act
        let result = decode_range_tombstones(&data).unwrap();

        // Assert
        assert_eq!(result.len(), 0); // Should skip incomplete entry
    }

    #[test]
    fn should_preserve_binary_data_in_roundtrip() {
        // Arrange
        let original = vec![create_tombstone(
            &[0x00, 0xFF, 0xAB],
            &[0xCD, 0xEF, 0x12],
            999,
        )];
        let encoded = encode_range_tombstones(&original).unwrap();

        // Act
        let decoded = decode_range_tombstones(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].start, &[0x00, 0xFF, 0xAB]);
        assert_eq!(decoded[0].end, &[0xCD, 0xEF, 0x12]);
        assert_eq!(decoded[0].seq, 999);
    }

    // --- is_covered_by_range_tombstone tests ---

    #[test]
    fn should_return_false_when_tombstone_list_empty() {
        // Arrange
        let tombstones: Vec<RangeTombstone> = vec![];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"key", 100);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_return_true_when_key_in_range_and_seq_valid() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"m", 100);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_return_false_when_key_before_range() {
        // Arrange
        let tombstones = vec![create_tombstone(b"m", b"z", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"a", 100);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_return_false_when_key_after_range() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"m", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"z", 100);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_return_true_when_key_equals_start() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"a", 100);

        // Assert
        assert!(result); // start is inclusive
    }

    #[test]
    fn should_return_false_when_key_equals_end() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"z", 100);

        // Assert
        assert!(!result); // end is exclusive
    }

    #[test]
    fn should_return_false_when_snapshot_before_tombstone_seq() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 100)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"m", 50);

        // Assert
        assert!(!result); // tombstone not visible at snapshot
    }

    #[test]
    fn should_return_true_when_snapshot_equals_tombstone_seq() {
        // Arrange
        let tombstones = vec![create_tombstone(b"a", b"z", 100)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"m", 100);

        // Assert
        assert!(result); // tombstone visible at exact sequence
    }

    #[test]
    fn should_check_all_tombstones_in_list() {
        // Arrange
        let tombstones = vec![
            create_tombstone(b"a", b"f", 50),
            create_tombstone(b"m", b"s", 60),
            create_tombstone(b"t", b"z", 70),
        ];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"p", 100);

        // Assert
        assert!(result); // covered by second tombstone
    }

    #[test]
    fn should_return_true_when_any_tombstone_covers_key() {
        // Arrange
        let tombstones = vec![
            create_tombstone(b"a", b"b", 50),
            create_tombstone(b"k", b"m", 60), // This one covers
            create_tombstone(b"x", b"z", 70),
        ];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"l", 100);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_handle_overlapping_tombstones() {
        // Arrange
        let tombstones = vec![
            create_tombstone(b"a", b"m", 50),
            create_tombstone(b"h", b"z", 60),
        ];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"j", 100);

        // Assert
        assert!(result); // covered by both
    }

    #[test]
    fn should_handle_empty_key() {
        // Arrange
        let tombstones = vec![create_tombstone(b"", b"z", 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, b"", 100);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_handle_binary_key_comparison() {
        // Arrange
        let tombstones = vec![create_tombstone(&[0x00], &[0xFF], 50)];

        // Act
        let result = is_covered_by_range_tombstone(&tombstones, &[0x80], 100);

        // Assert
        assert!(result);
    }

    // --- Integration/roundtrip tests ---

    #[test]
    fn should_roundtrip_preserve_all_fields() {
        // Arrange
        let original = vec![
            create_tombstone(b"key1", b"key5", 100),
            create_tombstone(b"key6", b"key9", 200),
        ];

        // Act
        let encoded = encode_range_tombstones(&original).unwrap();
        let decoded = decode_range_tombstones(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), original.len());
        for (i, tombstone) in decoded.iter().enumerate() {
            assert_eq!(tombstone.start, original[i].start);
            assert_eq!(tombstone.end, original[i].end);
            assert_eq!(tombstone.seq, original[i].seq);
        }
    }

    #[test]
    fn should_roundtrip_large_tombstone_list() {
        // Arrange
        let mut original = Vec::new();
        for i in 0..100 {
            original.push(create_tombstone(
                format!("key{:03}_start", i).as_bytes(),
                format!("key{:03}_end", i).as_bytes(),
                i as u64 * 10,
            ));
        }

        // Act
        let encoded = encode_range_tombstones(&original).unwrap();
        let decoded = decode_range_tombstones(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 100);
        assert_eq!(decoded[0].seq, 0);
        assert_eq!(decoded[99].seq, 990);
    }
}
