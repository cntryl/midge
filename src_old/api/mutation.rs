use crate::api::column_family::ColumnFamilyId;
use bytes::Bytes;

/// Mutation operation kinds used for batched writes/transactions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MutationOp {
    Put,
    Insert,
    Delete,
    DeleteRange,
    CompareAndSwap,
    Merge,
}

/// Represents a single mutation against the key/value store.
/// Keys and values are owned `Bytes` for cheap cloning and serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Mutation {
    pub op: MutationOp,
    pub cf_id: ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    /// Time-to-live duration for this mutation.
    ///
    /// When set, the key will automatically expire after the specified duration.
    /// The expiration is enforced during compaction via TTL filters.
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::api::mutation::Mutation;
    /// # use bytes::Bytes;
    /// # use std::time::Duration;
    /// // Create a put with 60 second TTL
    /// let mut_with_ttl = Mutation::put(
    ///     Bytes::from("session:abc"),
    ///     Bytes::from("data"),
    ///     Some(Duration::from_secs(60))
    /// );
    /// ```
    pub ttl: Option<std::time::Duration>,
    pub range_end: Option<Bytes>,
}

impl Mutation {
    #[inline]
    pub fn put(key: Bytes, value: Bytes, ttl: Option<std::time::Duration>) -> Self {
        Self::put_cf(crate::api::DEFAULT_CF_ID, key, value, ttl)
    }

    #[inline]
    pub fn put_cf(
        cf_id: ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Self {
        Self {
            op: MutationOp::Put,
            cf_id,
            key,
            value: Some(value),
            ttl,
            range_end: None,
        }
    }

    #[inline]
    pub fn insert(key: Bytes, value: Bytes, ttl: Option<std::time::Duration>) -> Self {
        Self::insert_cf(crate::api::DEFAULT_CF_ID, key, value, ttl)
    }

    #[inline]
    pub fn insert_cf(
        cf_id: ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Self {
        Self {
            op: MutationOp::Insert,
            cf_id,
            key,
            value: Some(value),
            ttl,
            range_end: None,
        }
    }

    #[inline]
    pub fn delete(key: Bytes) -> Self {
        Self::delete_cf(crate::api::DEFAULT_CF_ID, key)
    }

    #[inline]
    pub fn delete_cf(cf_id: ColumnFamilyId, key: Bytes) -> Self {
        Self {
            op: MutationOp::Delete,
            cf_id,
            key,
            value: None,
            ttl: None,
            range_end: None,
        }
    }

    #[inline]
    pub fn delete_range(start: Bytes, end: Bytes) -> Self {
        Self::delete_range_cf(crate::api::DEFAULT_CF_ID, start, end)
    }

    #[inline]
    pub fn delete_range_cf(cf_id: ColumnFamilyId, start: Bytes, end: Bytes) -> Self {
        Self {
            op: MutationOp::DeleteRange,
            cf_id,
            key: start,
            value: None,
            ttl: None,
            range_end: Some(end),
        }
    }

    #[inline]
    pub fn compare_and_swap(key: Bytes, expected: Option<Bytes>, new_value: Bytes) -> Self {
        Self::compare_and_swap_cf(crate::api::DEFAULT_CF_ID, key, expected, new_value)
    }

    #[inline]
    pub fn compare_and_swap_cf(
        cf_id: ColumnFamilyId,
        key: Bytes,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Self {
        Self {
            op: MutationOp::CompareAndSwap,
            cf_id,
            key,
            value: Some(new_value),
            ttl: None,
            range_end: expected,
        }
    }

    #[inline]
    pub fn merge(key: Bytes, value: Bytes) -> Self {
        Self::merge_cf(crate::api::DEFAULT_CF_ID, key, value)
    }

    #[inline]
    pub fn merge_cf(cf_id: ColumnFamilyId, key: Bytes, value: Bytes) -> Self {
        Self {
            op: MutationOp::Merge,
            cf_id,
            key,
            value: Some(value),
            ttl: None,
            range_end: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn should_create_put_mutation_given_key_and_value() {
        // Arrange
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // Act
        let mutation = Mutation::put(key.clone(), value.clone(), None);

        // Assert
        assert_eq!(mutation.op, MutationOp::Put);
        assert_eq!(mutation.key, key);
        assert_eq!(mutation.value, Some(value));
        assert!(mutation.ttl.is_none());
        assert!(mutation.range_end.is_none());
    }

    #[test]
    fn should_create_put_mutation_with_ttl_when_provided() {
        // Arrange
        let key = Bytes::from("session:abc");
        let value = Bytes::from("data");
        let ttl = Some(Duration::from_secs(60));

        // Act
        let mutation = Mutation::put(key.clone(), value.clone(), ttl);

        // Assert
        assert_eq!(mutation.op, MutationOp::Put);
        assert_eq!(mutation.ttl, ttl);
    }

    #[test]
    fn should_create_insert_mutation_given_key_and_value() {
        // Arrange
        let key = Bytes::from("new_key");
        let value = Bytes::from("new_value");

        // Act
        let mutation = Mutation::insert(key.clone(), value.clone(), None);

        // Assert
        assert_eq!(mutation.op, MutationOp::Insert);
        assert_eq!(mutation.key, key);
        assert_eq!(mutation.value, Some(value));
        assert!(mutation.ttl.is_none());
        assert!(mutation.range_end.is_none());
    }

    #[test]
    fn should_create_insert_mutation_with_ttl_when_provided() {
        // Arrange
        let key = Bytes::from("temp_key");
        let value = Bytes::from("temp_value");
        let ttl = Some(Duration::from_millis(500));

        // Act
        let mutation = Mutation::insert(key.clone(), value.clone(), ttl);

        // Assert
        assert_eq!(mutation.op, MutationOp::Insert);
        assert_eq!(mutation.ttl, ttl);
    }

    #[test]
    fn should_create_delete_mutation_given_key() {
        // Arrange
        let key = Bytes::from("to_delete");

        // Act
        let mutation = Mutation::delete(key.clone());

        // Assert
        assert_eq!(mutation.op, MutationOp::Delete);
        assert_eq!(mutation.key, key);
        assert!(mutation.value.is_none());
        assert!(mutation.ttl.is_none());
        assert!(mutation.range_end.is_none());
    }

    #[test]
    fn should_create_delete_range_mutation_given_start_and_end() {
        // Arrange
        let start = Bytes::from("key_a");
        let end = Bytes::from("key_z");

        // Act
        let mutation = Mutation::delete_range(start.clone(), end.clone());

        // Assert
        assert_eq!(mutation.op, MutationOp::DeleteRange);
        assert_eq!(mutation.key, start);
        assert_eq!(mutation.range_end, Some(end));
        assert!(mutation.value.is_none());
        assert!(mutation.ttl.is_none());
    }

    #[test]
    fn should_serialize_put_mutation() {
        // Arrange
        let mutation = Mutation::put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            Some(Duration::from_secs(10)),
        );

        // Act
        let result = serde_json::to_string(&mutation);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_deserialize_put_mutation() {
        // Arrange
        let original = Mutation::put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            Some(Duration::from_secs(10)),
        );
        let json = serde_json::to_string(&original).expect("serialize failed");

        // Act
        let deserialized: Mutation = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized.op, original.op);
        assert_eq!(deserialized.key, original.key);
        assert_eq!(deserialized.value, original.value);
        assert_eq!(deserialized.ttl, original.ttl);
    }

    #[test]
    fn should_serialize_delete_range_mutation() {
        // Arrange
        let mutation = Mutation::delete_range(Bytes::from("start"), Bytes::from("end"));

        // Act
        let result = serde_json::to_string(&mutation);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_deserialize_delete_range_mutation() {
        // Arrange
        let original = Mutation::delete_range(Bytes::from("start"), Bytes::from("end"));
        let json = serde_json::to_string(&original).expect("serialize failed");

        // Act
        let deserialized: Mutation = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized.op, original.op);
        assert_eq!(deserialized.key, original.key);
        assert_eq!(deserialized.range_end, original.range_end);
    }

    #[test]
    fn should_clone_mutation_preserving_all_fields() {
        // Arrange
        let original = Mutation::put(
            Bytes::from("key"),
            Bytes::from("value"),
            Some(Duration::from_secs(30)),
        );

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.op, original.op);
        assert_eq!(cloned.key, original.key);
        assert_eq!(cloned.value, original.value);
        assert_eq!(cloned.ttl, original.ttl);
        assert_eq!(cloned.range_end, original.range_end);
    }

    #[test]
    fn should_compare_mutation_ops_for_equality() {
        // Arrange
        let put = MutationOp::Put;
        let insert = MutationOp::Insert;
        let delete = MutationOp::Delete;
        let delete_range = MutationOp::DeleteRange;

        // Act
        let put_eq = put == MutationOp::Put;
        let put_ne_insert = put != insert;
        let delete_ne_range = delete != delete_range;

        // Assert
        assert!(put_eq);
        assert!(put_ne_insert);
        assert!(delete_ne_range);
    }
}
