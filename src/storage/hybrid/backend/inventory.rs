//! Inventory capabilities retained beside the class-specific object backends.

use super::HybridStorage;
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::storage::cloud::{CloudListPage, CloudListPageRequest};
use crate::storage::inventory::ObjectInventory;
use std::sync::Arc;

pub(super) struct ObjectInventories {
    wal: Arc<dyn ObjectInventory>,
    sst: Arc<dyn ObjectInventory>,
}

impl HybridStorage {
    pub(crate) fn configure_object_inventories(
        &self,
        wal: Arc<dyn ObjectInventory>,
        sst: Arc<dyn ObjectInventory>,
    ) -> MidgeResult<()> {
        self.object_inventories
            .set(ObjectInventories { wal, sst })
            .map_err(|_| MidgeError::Internal("object inventories were already configured".into()))
    }

    pub(crate) fn has_object_inventory(&self) -> bool {
        self.object_inventories.get().is_some()
    }

    pub(crate) fn list_remote_page_within(
        &self,
        prefix: &str,
        page: CloudListPageRequest,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Option<CloudListPage>> {
        let Some(inventories) = self.object_inventories.get() else {
            return Ok(None);
        };
        let source = if prefix.starts_with("sst/") {
            &inventories.sst
        } else if prefix.starts_with("wal/") {
            &inventories.wal
        } else {
            return Err(MidgeError::InvalidArgument(
                "unknown inventory object class".into(),
            ));
        };
        let scope = DeadlineScope::new(*deadline);
        source
            .keys_page(prefix, page.clone(), Some(&scope))?
            .map(|result| {
                page.accept(prefix, result.keys, result.continuation)
                    .map_err(|error| MidgeError::Corruption(error.to_string()))
            })
            .transpose()
    }
}
