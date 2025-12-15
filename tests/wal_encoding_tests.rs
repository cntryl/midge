use cntryl_midge::wal::encoding::{self as encoding, tags};
use cntryl_midge::wal::types::WalOpKind;

#[test]
fn should_use_last_occurrence_when_duplicate_tags() {
    // Build a minimal payload with duplicate SEQ tags (last wins)
    use bytes::{BytesMut, BufMut};

    let mut payload = BytesMut::new();
    payload.extend_from_slice(&[b'M', b'W']);
    payload.put_u8(1u8); // version

    // OP (Put) TLV
    payload.put_u8(tags::OP);
    payload.put_u32_le(1);
    payload.put_u8(WalOpKind::Put.to_wire_format());

    // CF_ID TLV (0)
    payload.put_u8(tags::CF_ID);
    payload.put_u32_le(4);
    payload.put_u32_le(0);

    // Duplicate SEQ TLVs
    payload.put_u8(tags::SEQ);
    payload.put_u32_le(8);
    payload.put_u64_le(100);

    payload.put_u8(tags::SEQ);
    payload.put_u32_le(8);
    payload.put_u64_le(200);

    // KEY TLV
    payload.put_u8(tags::KEY);
    payload.put_u32_le(1);
    payload.extend_from_slice(b"k");

    let decoded = encoding::decode(&payload.freeze()[..]).unwrap();
    assert_eq!(decoded.seq, 200);
}
