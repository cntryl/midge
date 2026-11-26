//! Inventory capabilities retained beside the class-specific object backends.

use super::HybridStorage;
use crate::cloud::{CloudListPage, CloudListPageRequest};
use crate::inventory::ObjectInventory;
use cntryl_midge_core::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use std::sync::Arc;

pub(super) struct ObjectInventories {
    wal: Arc<dyn ObjectInventory>,
    sst: Arc<dyn ObjectInventory>,
}

impl HybridStorage {
    pub fn configure_object_inventories(
        &self,
        wal: Arc<dyn ObjectInventory>,
        sst: Arc<dyn ObjectInventory>,
    ) -> MidgeResult<()> {
        self.object_inventories
            .set(ObjectInventories { wal, sst })
            .map_err(|_| MidgeError::Internal("object inventories were already configured".into()))
    }

    pub fn has_object_inventory(&self) -> bool {
        self.object_inventories.get().is_some()
    }

    pub fn list_remote_page_within(
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
        let expected = page.clone();
        source
            .keys_page(prefix, page, Some(&scope))?
            .map(|result| {
                expected
                    .accept(prefix, result.keys, result.continuation)
                    .map_err(|error| MidgeError::Corruption(error.to_string()))
            })
            .transpose()
    }
}
