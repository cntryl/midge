//! Bind submitted writer work to its exact lease authority.

use crate::common::{MidgeError, MidgeResult};
use crate::lease::{
    ActiveWriterEffect, LeaderStore, LeaseValidity, WriterEffectGroup, WriterEffectGroups,
};
use crate::wal::writer_authority::{WriterAuthority, WriterGroupAuthority};
use std::sync::Arc;
use std::time::Duration;

pub(super) struct LeaseWriterAuthority {
    pub(super) validity: Option<Arc<LeaseValidity>>,
    pub(super) leader_store: Option<Arc<dyn LeaderStore>>,
    pub(super) holder_id: String,
    pub(super) epoch: u64,
    pub(super) timeout: Duration,
}

impl LeaseWriterAuthority {
    fn native_groups(&self) -> MidgeResult<&dyn WriterEffectGroups> {
        self.leader_store
            .as_ref()
            .and_then(|store| store.writer_effect_groups())
            .ok_or_else(|| MidgeError::Internal("WAL authority has no native writer groups".into()))
    }
    fn validate_validity(&self) -> MidgeResult<()> {
        if let Some(validity) = &self.validity {
            validity.remaining(self.epoch).map_err(|error| {
                error.into_validation_error("monotonic writer lease validity lost")
            })?;
        }
        Ok(())
    }
}

impl WriterAuthority for LeaseWriterAuthority {
    fn run(&self, operation: &mut dyn FnMut()) -> MidgeResult<()> {
        crate::wal::writer_effect::with_writer_effect(
            |run| {
                if let Some(store) = &self.leader_store {
                    store
                        .with_writer_epoch(&self.holder_id, self.epoch, self.timeout, run)
                        .map_err(|error| {
                            error.into_writer_effect_error("WAL writer storage effect")
                        })
                } else {
                    run();
                    Ok(())
                }
            },
            || {
                self.validate_validity()?;
                operation();
                Ok(())
            },
        )
        .map_err(|error| error.error)?
    }
    fn groups(&self) -> Option<&dyn WriterGroupAuthority> {
        self.leader_store
            .as_ref()?
            .writer_effect_groups()
            .map(|_| self as &dyn WriterGroupAuthority)
    }
}

impl WriterGroupAuthority for LeaseWriterAuthority {
    fn begin_group(&self, timeout: Duration) -> MidgeResult<WriterEffectGroup> {
        self.validate_validity()?;
        self.native_groups()?
            .begin_group(&self.holder_id, self.epoch, timeout)
            .map_err(|error| error.into_validation_error("WAL group admission"))
    }
    fn run_active(
        &self,
        active: &ActiveWriterEffect,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> MidgeResult<()> {
        crate::wal::writer_effect::with_writer_effect(
            |run| {
                self.native_groups()?
                    .run_active(&self.holder_id, self.epoch, active, timeout, run)
                    .map_err(|error| error.into_writer_effect_error("WAL group storage effect"))
            },
            || {
                self.validate_validity()?;
                operation();
                Ok(())
            },
        )
        .map_err(|error| error.error)?
    }
}
