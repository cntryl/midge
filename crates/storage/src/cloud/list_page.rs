//! A single, non-retrying inventory page with bounded response ownership.

use super::{CloudError, CloudOutcome};

pub const MAX_LIST_PAGE_KEYS: usize = 32;
pub const MAX_LIST_PAGE_BYTES: usize = 64 * 1024;
const MAX_TOKEN_BYTES: usize = 4096;

#[derive(Clone, Debug)]
pub struct CloudListPageRequest {
    pub continuation: Option<String>,
    pub max_keys: usize,
}

#[derive(Clone, Debug)]
pub struct CloudListPage {
    pub keys: Vec<String>,
    pub continuation: Option<String>,
}

pub type CloudListPageCallback = std::sync::mpsc::Sender<CloudOutcome<Option<CloudListPage>>>;

impl CloudListPageRequest {
    pub(crate) fn validate(&self, prefix: &str) -> Result<(), CloudError> {
        if self.max_keys == 0
            || self.max_keys > MAX_LIST_PAGE_KEYS
            || prefix.len() > MAX_TOKEN_BYTES
            || self
                .continuation
                .as_ref()
                .is_some_and(|token| token.is_empty() || token.len() > MAX_TOKEN_BYTES)
        {
            return Err(CloudError::Protocol(
                "invalid bounded LIST page request".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn accept(
        &self,
        prefix: &str,
        keys: Vec<String>,
        continuation: Option<String>,
    ) -> Result<CloudListPage, CloudError> {
        self.validate(prefix)?;
        let bytes = keys
            .iter()
            .try_fold(0usize, |total, key| total.checked_add(key.len()));
        if keys.len() > self.max_keys
            || bytes.is_none_or(|bytes| bytes > MAX_LIST_PAGE_BYTES)
            || keys.iter().any(|key| !key.starts_with(prefix))
            || continuation.as_ref().is_some_and(|token| {
                token.is_empty()
                    || token.len() > MAX_TOKEN_BYTES
                    || self.continuation.as_ref() == Some(token)
            })
        {
            return Err(CloudError::Protocol(
                "bounded LIST page exceeded its identity or response bounds".into(),
            ));
        }
        Ok(CloudListPage { keys, continuation })
    }
}
