//! Compact manifest encoding for SST key bounds (#546).
//!
//! Key bounds used to serialize as JSON arrays, one number per byte. They now
//! serialize as a hex string. Decoding still accepts the array form, so
//! manifests and journals written before format version 4 load unchanged.

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserializer, Serializer};

// Serde's `with` contract passes the field by reference.
#[allow(clippy::ref_option)]
pub(crate) fn serialize<S: Serializer>(
    key: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match key {
        Some(bytes) => serializer.serialize_some(&hex::encode(bytes)),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    deserializer.deserialize_option(OptionalKey)
}

struct OptionalKey;

impl<'de> Visitor<'de> for OptionalKey {
    type Value = Option<Vec<u8>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a hex key string, a byte array, or null")
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(Key).map(Some)
    }
}

struct Key;

impl<'de> Visitor<'de> for Key {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a hex key string or a byte array")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        hex::decode(value).map_err(|error| E::custom(format!("invalid hex key bound: {error}")))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::FileMeta;

    fn bounded_file() -> FileMeta {
        FileMeta {
            name: "000001_00_00000000000000000001.sst".to_string(),
            smallest_key: Some(b"apple\x00\xff".to_vec()),
            largest_key: Some(Vec::new()),
            ..FileMeta::default()
        }
    }

    #[test]
    fn should_round_trip_key_bounds_when_encoded_compactly() {
        // Arrange
        let file = bounded_file();

        // Act
        let json = serde_json::to_string(&file).expect("serialize");
        let decoded: FileMeta = serde_json::from_str(&json).expect("deserialize");

        // Assert
        assert!(
            json.contains(r#""smallest_key":"6170706c6500ff""#),
            "{json}"
        );
        assert_eq!(decoded.smallest_key, file.smallest_key);
        assert_eq!(decoded.largest_key, Some(Vec::new()));
    }

    #[test]
    fn should_decode_legacy_array_key_bounds() {
        // Arrange: the form every manifest and journal used before format 4.
        let json = r#"{"name":"000001_00_00000000000000000001.sst","level":0,"size_bytes":1,"smallest_key":[97,0,255],"largest_key":null}"#;

        // Act
        let decoded: FileMeta = serde_json::from_str(json).expect("deserialize legacy");

        // Assert
        assert_eq!(decoded.smallest_key, Some(vec![97, 0, 255]));
        assert_eq!(decoded.largest_key, None);
    }

    #[test]
    fn should_treat_missing_key_bounds_as_absent() {
        // Arrange
        let json = r#"{"name":"000001_00_00000000000000000001.sst","level":0,"size_bytes":1}"#;

        // Act
        let decoded: FileMeta = serde_json::from_str(json).expect("deserialize");

        // Assert
        assert_eq!(decoded.smallest_key, None);
        assert_eq!(decoded.largest_key, None);
    }
}
