//! Engine opening and initialization
use crate::common::MidgeResult;
use crate::storage::StorageBackend;
use super::MidgeEngine;
use std::sync::Arc;

/// Open a Midge database instance
pub fn open_engine(storage: Arc<dyn StorageBackend>) -> MidgeResult<MidgeEngine> {
    MidgeEngine::with_storage(storage)
}
