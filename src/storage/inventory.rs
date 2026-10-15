//! Read-only questions startup asks an object store before it trusts what
//! the store holds: how large an object is, and which keys share a prefix.

use super::cloud::{BlockingCloud, CloudStorage};
use super::StorageBackend;
use crate::common::{DeadlineScope, MidgeResult, OperationDeadline};

/// Startup inventory of the object store that holds WAL segments and SSTs.
pub(crate) trait ObjectInventory: Send + Sync {
    /// Size of `key`. `None` only when the store reports the object
    /// definitively absent; an error means nothing is known about it.
    fn object_size(&self, key: &str, scope: Option<&DeadlineScope>) -> MidgeResult<Option<u64>>;

    /// Every key that starts with `prefix`, as callers name them.
    fn keys_under(&self, prefix: &str, scope: Option<&DeadlineScope>) -> MidgeResult<Vec<String>>;
}

/// An object store the engine both runs on and inventories at startup.
pub(crate) trait ObjectStore: StorageBackend + ObjectInventory {}

impl<T: StorageBackend + ObjectInventory> ObjectStore for T {}

fn deadline(scope: Option<&DeadlineScope>) -> OperationDeadline {
    scope.map_or_else(OperationDeadline::unbounded, DeadlineScope::deadline)
}

impl ObjectInventory for CloudStorage {
    fn object_size(&self, key: &str, scope: Option<&DeadlineScope>) -> MidgeResult<Option<u64>> {
        let deadline = deadline(scope);
        Ok(BlockingCloud::new(self, &deadline)
            .head_optional(key)?
            .map(|metadata| metadata.size))
    }

    fn keys_under(&self, prefix: &str, scope: Option<&DeadlineScope>) -> MidgeResult<Vec<String>> {
        let deadline = deadline(scope);
        Ok(BlockingCloud::new(self, &deadline)
            .list(prefix)?
            .into_iter()
            .map(|key| self.strip_namespace(&key).to_string())
            .collect())
    }
}
