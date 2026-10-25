//! Unchecked FORMAT9 metadata fixtures for recovery and rejection invariants.
//! These helpers encode hostile semantic state without production validation.

use serde::{Serialize, Serializer};

fn frame<T: Serialize + ?Sized>(kind: u8, value: &T) -> Vec<u8> {
    let payload = rmp_serde::to_vec_named(value).expect("encode binary metadata fixture");
    let mut bytes = b"MDGEMD09".to_vec();
    bytes.push(kind);
    bytes.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("fixture length fits")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&payload);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

pub fn manifest<T: Serialize + ?Sized>(manifest: &T) -> Vec<u8> {
    frame(1, manifest)
}

pub fn intents<T: Serialize + ?Sized>(entries: &T) -> Vec<u8> {
    #[derive(Serialize)]
    struct InitialIntentState<'a, T: Serialize + ?Sized> {
        sequence: u64,
        previous_checksum: u32,
        keep: usize,
        append: &'a T,
    }
    frame(
        3,
        &InitialIntentState {
            sequence: 1,
            previous_checksum: 0,
            keep: 0,
            append: entries,
        },
    )
}

// Serde's serialize_with contract passes an optional field by reference.
#[allow(clippy::ref_option)]
pub fn serialize_key_bound<S: Serializer>(
    key: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    struct Binary<'a>(&'a [u8]);
    impl Serialize for Binary<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }
    match key {
        Some(key) => serializer.serialize_some(&Binary(key)),
        None => serializer.serialize_none(),
    }
}
