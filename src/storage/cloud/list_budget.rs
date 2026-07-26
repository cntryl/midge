use crate::common::{MidgeError, MidgeResult};
use std::collections::HashSet;

const MAX_CLOUD_LIST_PAGES: usize = 10_000;
const MAX_CLOUD_LIST_ITEMS: usize = 1_000_000;
const MAX_CLOUD_LIST_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy)]
struct ListBudgetLimits {
    pages: usize,
    items: usize,
    bytes: usize,
}

impl Default for ListBudgetLimits {
    fn default() -> Self {
        Self {
            pages: MAX_CLOUD_LIST_PAGES,
            items: MAX_CLOUD_LIST_ITEMS,
            bytes: MAX_CLOUD_LIST_BYTES,
        }
    }
}

/// Bounds cumulative state retained while following cloud LIST pagination.
#[derive(Default)]
pub(crate) struct CloudListBudget {
    limits: ListBudgetLimits,
    pages: usize,
    items: usize,
    bytes: usize,
    continuation_tokens: HashSet<String>,
}

impl CloudListBudget {
    #[cfg(test)]
    fn with_limits(pages: usize, items: usize, bytes: usize) -> Self {
        Self {
            limits: ListBudgetLimits {
                pages,
                items,
                bytes,
            },
            pages: 0,
            items: 0,
            bytes: 0,
            continuation_tokens: HashSet::new(),
        }
    }

    pub(crate) fn record_page(
        &mut self,
        page_items: &[String],
        continuation_token: Option<&str>,
    ) -> MidgeResult<()> {
        let pages = checked_add(self.pages, 1, "page count")?;
        enforce_limit("page count", pages, self.limits.pages)?;

        let items = checked_add(self.items, page_items.len(), "item count")?;
        enforce_limit("item count", items, self.limits.items)?;

        if let Some(token) = continuation_token {
            if self.continuation_tokens.contains(token) {
                return Err(MidgeError::Internal(
                    "cloud LIST repeated continuation token".to_string(),
                ));
            }
        }

        let page_bytes = page_items.iter().try_fold(0_usize, |total, item| {
            checked_add(total, item.len(), "aggregate byte count")
        })?;
        let page_bytes = match continuation_token {
            Some(token) => checked_add(page_bytes, token.len(), "aggregate byte count")?,
            None => page_bytes,
        };
        let bytes = checked_add(self.bytes, page_bytes, "aggregate byte count")?;
        enforce_limit("aggregate byte count", bytes, self.limits.bytes)?;

        self.pages = pages;
        self.items = items;
        self.bytes = bytes;
        if let Some(token) = continuation_token {
            self.continuation_tokens.insert(token.to_string());
        }
        Ok(())
    }
}

fn checked_add(current: usize, additional: usize, resource: &str) -> MidgeResult<usize> {
    current.checked_add(additional).ok_or_else(|| {
        MidgeError::Internal(format!(
            "cloud LIST {resource} overflowed its safety counter"
        ))
    })
}

fn enforce_limit(resource: &str, value: usize, limit: usize) -> MidgeResult<()> {
    if value > limit {
        return Err(MidgeError::Internal(format!(
            "cloud LIST {resource} exceeded limit of {limit}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_cloud_list_when_page_limit_exceeded() {
        // Arrange
        let mut budget = CloudListBudget::with_limits(1, usize::MAX, usize::MAX);
        budget.record_page(&[], None).unwrap();

        // Act
        let error = budget.record_page(&[], None).unwrap_err();

        // Assert
        assert!(error.to_string().contains("page count exceeded"));
    }

    #[test]
    fn should_reject_cloud_list_when_item_limit_exceeded() {
        // Arrange
        let mut budget = CloudListBudget::with_limits(usize::MAX, 1, usize::MAX);
        let items = vec!["a".to_string(), "b".to_string()];

        // Act
        let error = budget.record_page(&items, None).unwrap_err();

        // Assert
        assert!(error.to_string().contains("item count exceeded"));
    }

    #[test]
    fn should_reject_cloud_list_when_aggregate_byte_limit_exceeded() {
        // Arrange
        let mut budget = CloudListBudget::with_limits(usize::MAX, usize::MAX, 5);
        budget.record_page(&["abc".to_string()], Some("d")).unwrap();

        // Act
        let error = budget.record_page(&["ef".to_string()], None).unwrap_err();

        // Assert
        assert!(error.to_string().contains("aggregate byte count exceeded"));
    }

    #[test]
    fn should_reject_cloud_list_when_continuation_token_repeats() {
        // Arrange
        let mut budget = CloudListBudget::default();
        budget.record_page(&[], Some("repeat")).unwrap();

        // Act
        let error = budget.record_page(&[], Some("repeat")).unwrap_err();

        // Assert
        assert!(error.to_string().contains("repeated continuation token"));
    }
}
