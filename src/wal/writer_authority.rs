//! Authority captured by each submitted WAL request.

use crate::common::{MidgeError, MidgeResult};
use crate::lease::{ActiveWriterEffect, WriterEffectGroup, WriterEffectTicket};
use crate::wal::traits::WriteAuthority;
use crate::wal::writer_effect::{with_writer_effect, WriterEffectError};
use std::sync::Arc;
use std::time::Duration;

#[doc(hidden)]
pub trait WriterGroupAuthority: Send + Sync {
    /// Admit an operation under this exact authority.
    ///
    /// # Errors
    /// Returns an ownership or admission deadline failure.
    fn begin_group(&self, timeout: Duration) -> MidgeResult<WriterEffectGroup>;
    /// Execute a physical substep of a previously activated child.
    ///
    /// # Errors
    /// Returns an identity, validity, scope or filesystem fence failure.
    fn run_active(
        &self,
        active: &ActiveWriterEffect,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> MidgeResult<()>;
}

#[doc(hidden)]
pub trait WriterAuthority: Send + Sync {
    /// Execute the legacy per-effect authority boundary.
    ///
    /// # Errors
    /// Returns the authority failure before or after the callback.
    fn run(&self, operation: &mut dyn FnMut()) -> MidgeResult<()>;
    fn groups(&self) -> Option<&dyn WriterGroupAuthority> {
        None
    }
}

impl<F> WriterAuthority for F
where
    F: Fn(&mut dyn FnMut()) -> MidgeResult<()> + Send + Sync,
{
    fn run(&self, operation: &mut dyn FnMut()) -> MidgeResult<()> {
        self(operation)
    }
}

pub(crate) struct PendingWriterEffect {
    authority: Option<WriteAuthority>,
    ticket: Option<WriterEffectTicket>,
    timeout: Duration,
}

pub(crate) struct ActiveRequestEffect {
    authority: Option<WriteAuthority>,
    active: Option<ActiveWriterEffect>,
    timeout: Duration,
}

impl PendingWriterEffect {
    pub(crate) fn admit(
        authority: Option<WriteAuthority>,
        explicit_group: Option<&WriterEffectGroup>,
        timeout: Duration,
    ) -> MidgeResult<(Self, Option<WriterEffectGroup>)> {
        let groups = authority.as_ref().and_then(|authority| authority.groups());
        let root = match (groups, explicit_group) {
            (Some(groups), None) => Some(groups.begin_group(timeout)?),
            (None, Some(_)) => {
                return Err(MidgeError::Internal(
                    "WAL authority has no native writer groups".into(),
                ))
            }
            _ => None,
        };
        let ticket = explicit_group
            .or(root.as_ref())
            .map(|group| group.reserve_ticket(timeout))
            .transpose()
            .map_err(|error| error.into_writer_effect_error("WAL request admission"))?;
        Ok((
            Self {
                authority,
                ticket,
                timeout,
            },
            root,
        ))
    }

    pub(crate) fn same_authority(&self, other: &Self) -> bool {
        match (&self.authority, &other.authority) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }

    pub(crate) fn activate(self) -> MidgeResult<ActiveRequestEffect> {
        let active = self
            .ticket
            .map(WriterEffectTicket::activate)
            .transpose()
            .map_err(|error| error.into_writer_effect_error("WAL request activation"))?;
        Ok(ActiveRequestEffect {
            authority: self.authority,
            active,
            timeout: self.timeout,
        })
    }
}

impl ActiveRequestEffect {
    pub(crate) fn validate(&self) -> Result<(), WriterEffectError> {
        if self.active.is_some() {
            self.run(|| ()).map_err(|error| WriterEffectError {
                error: error.error,
                started: false,
            })
        } else {
            Ok(())
        }
    }
    pub(crate) fn run<T>(&self, operation: impl FnOnce() -> T) -> Result<T, WriterEffectError> {
        with_writer_effect(
            |run| match (&self.authority, &self.active) {
                (Some(authority), Some(active)) => authority
                    .groups()
                    .ok_or_else(|| {
                        MidgeError::Internal(
                            "native WAL request lost its authority capability".into(),
                        )
                    })?
                    .run_active(active, self.timeout, run),
                (Some(authority), None) => authority.run(run),
                (None, None) => {
                    run();
                    Ok(())
                }
                (None, Some(_)) => Err(MidgeError::Internal(
                    "native WAL request has no authority".into(),
                )),
            },
            operation,
        )
    }
}
