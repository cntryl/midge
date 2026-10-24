//! Canonical cloud manifest version names shared by authority and recovery.

use crate::common::{MidgeError, MidgeResult};

pub(crate) const CHAIN_FILE_NAME: &str = "manifest-chain";
pub(crate) const MAX_CHAIN_OBJECTS: usize = 16;
pub(crate) const MAX_CHAIN_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn validate_key(key: &str, ordinal: u64) -> MidgeResult<()> {
    let prefix = format!("manifest/{ordinal:020}/");
    let operation = key
        .strip_prefix(&prefix)
        .ok_or_else(|| MidgeError::Corruption("invalid cloud manifest version key".into()))?;
    if !uuid::Uuid::parse_str(operation).is_ok_and(|id| id.to_string() == operation) {
        return Err(MidgeError::Corruption(
            "invalid cloud manifest operation identity".into(),
        ));
    }
    Ok(())
}
