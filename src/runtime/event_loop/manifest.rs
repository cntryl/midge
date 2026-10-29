use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::FileMeta;
use crate::runtime::{RuntimeMsg, RuntimeResponse};
use crossbeam::channel::Receiver;

pub(super) struct ManifestCoordinator;

impl ManifestCoordinator {
    fn reject_uncertain_local_ddl(event_loop: &EventLoop, request_id: u64) -> bool {
        if event_loop.local_ddl_requires_restart() {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Fenced(
                        "local DDL journal outcome is unknown; reopen before further changes"
                            .into(),
                    ),
                },
            );
            true
        } else {
            false
        }
    }

    fn ddl_authority(
        event_loop: &EventLoop,
    ) -> crate::common::MidgeResult<Option<crate::runtime::ddl::DdlLeaseAuthority>> {
        if !event_loop.actors.cloud_coordinator.provider_ddl_fencing {
            return Ok(None);
        }
        Ok(Some(crate::runtime::ddl::DdlLeaseAuthority {
            store: event_loop
                .authority
                .fencing
                .leader_store
                .clone()
                .ok_or_else(|| {
                    crate::common::MidgeError::Fenced(
                        "provider cloud DDL requires a leader store".into(),
                    )
                })?,
            holder_id: event_loop
                .authority
                .fencing
                .leader_holder_id
                .clone()
                .ok_or_else(|| {
                    crate::common::MidgeError::Fenced(
                        "provider cloud DDL requires a lease holder".into(),
                    )
                })?,
            writer_epoch: event_loop.authority.fencing.writer_epoch,
        }))
    }

    #[cfg(test)]
    pub(super) fn add_sst(
        event_loop: &mut EventLoop,
        request_id: u64,
        file_meta: &FileMeta,
    ) -> HandleOutcome {
        let deadline = event_loop.registered_request_deadline(request_id);
        let result = event_loop
            .actors
            .manifest_actor
            .add_sst(&mut event_loop.state, file_meta)
            .and_then(|()| {
                event_loop.mirror_metadata_after_local_commit_within("manifest add sst", &deadline)
            });
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn compaction_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        removed: &[String],
        added: &[FileMeta],
    ) -> HandleOutcome {
        let deadline = event_loop.registered_request_deadline(request_id);
        let result = event_loop
            .actors
            .manifest_actor
            .compaction_complete(&mut event_loop.state, removed, added)
            .and_then(|()| {
                event_loop.mirror_metadata_after_local_commit_within(
                    "manifest compaction complete",
                    &deadline,
                )
            });
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
    }

    pub(super) fn persist(
        event_loop: &mut EventLoop,
        request_id: u64,
        origin: crate::metadata::accounting::Origin,
    ) -> HandleOutcome {
        if Self::reject_uncertain_local_ddl(event_loop, request_id) {
            return HandleOutcome::Continue;
        }
        let deadline = event_loop.registered_request_deadline(request_id);
        let result =
            crate::runtime::actors::ManifestActor::persist_for(&mut event_loop.state, origin)
                .and_then(|()| {
                    event_loop.start_metadata_mirror(
                        super::metadata_mirror::MirrorContinuation::Request { request_id },
                        deadline,
                    )
                });
        if matches!(result, Ok(true)) {
            return HandleOutcome::Continue;
        }
        let result =
            event_loop.accept_metadata_mirror_result(result.map(|_| ()), "manifest persist");
        if let Err(error) = &result {
            // Callers checkpoint after a change already committed through the
            // journal, so this failure cannot undo it; record it for health.
            event_loop.state.mark_persistence_anomaly();
            tracing::warn!(%error, "manifest checkpoint failed after a committed change");
        }
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
    }

    pub(super) fn create_column_family(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        name: &str,
    ) -> HandleOutcome {
        let deadline = event_loop.registered_request_deadline(request_id);
        if Self::reject_uncertain_local_ddl(event_loop, request_id) {
            return HandleOutcome::Continue;
        }
        let authority = match Self::ddl_authority(event_loop) {
            Ok(authority) => authority,
            Err(error) => {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                return HandleOutcome::Continue;
            }
        };
        if let Err(error) = crate::runtime::ddl::validate_column_family_name(name) {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }
        if let Some(authority) = &authority {
            if let Err(error) = authority.validate(&deadline) {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                return HandleOutcome::Continue;
            }
        }
        if event_loop.authority.fencing.ddl_authority_ambiguous {
            match crate::runtime::ddl::reconcile_prepared_within(
                &mut event_loop.state,
                event_loop.actors.cloud_coordinator.hybrid_storage.as_ref(),
                authority.as_ref(),
                &deadline,
            ) {
                Ok(()) => {
                    event_loop.authority.fencing.ddl_authority_ambiguous = false;
                    event_loop.publish_snapshot();
                    event_loop.publish_family_catalog();
                }
                Err(error) => {
                    event_loop.publish_snapshot();
                    event_loop.publish_family_catalog();
                    event_loop.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Fenced(format!(
                                "DDL authority remains ambiguous: {error}"
                            )),
                        },
                    );
                    return HandleOutcome::Continue;
                }
            }
        }

        let result = event_loop.force_wal_sync(msg_rx).and_then(|()| {
            let cf_id = if let Some(existing) = event_loop
                .state
                .manifest
                .get_column_family_by_name(name)
            {
                existing.id
            } else {
                let edit = crate::runtime::ddl::create_edit(&event_loop.state, name)?;
                let cf_id = match &edit {
                    crate::metadata::ManifestEdit::CreateColumnFamily { id, .. } => *id,
                    _ => unreachable!("create_edit returned a non-create edit"),
                };
                crate::runtime::ddl::execute_within(
                    &mut event_loop.state,
                    event_loop.actors.cloud_coordinator.hybrid_storage.as_ref(),
                    &edit,
                    authority.as_ref(),
                    &deadline,
                )?;
                event_loop.authority.fencing.ddl_authority_ambiguous = false;
                cf_id
            };

            // Local journal application (and remote DDL CAS in cloud mode) is
            // the authority switch. A later auxiliary metadata mirror error
            // must not report the create as rejected and strand Engine's
            // public handle registry behind the runtime state.
            event_loop.publish_snapshot();
            event_loop.publish_family_catalog();
            if let Err(error) = Self::mirror_committed_create(event_loop, &deadline) {
                event_loop.state.mark_persistence_anomaly();
                tracing::warn!(%error, cf_id, "column-family create committed but metadata mirror remains degraded");
            }
            Ok(cf_id)
        });
        if result.is_err() {
            Self::record_ddl_authority_ambiguity(event_loop);
        }
        let resp = result.map_or_else(
            |error| RuntimeResponse::Error { request_id, error },
            |cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id },
        );
        event_loop.publish_snapshot();
        event_loop.publish_family_catalog();
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    fn mirror_committed_create(
        event_loop: &mut EventLoop,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::ddl::after_create_local_commit_before_metadata_mirror",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: create metadata mirror failed after local commit".to_string()
            ))
        );
        event_loop.mirror_metadata_after_local_commit_within("create column family", deadline)
    }

    pub(super) fn drop_column_family(
        event_loop: &mut EventLoop,
        _msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        discard_unflushed: bool,
    ) -> HandleOutcome {
        let deadline = event_loop.registered_request_deadline(request_id);
        if Self::reject_uncertain_local_ddl(event_loop, request_id) {
            return HandleOutcome::Continue;
        }
        let authority = match Self::ddl_authority(event_loop) {
            Ok(authority) => authority,
            Err(error) => {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                return HandleOutcome::Continue;
            }
        };
        if let Some(authority) = &authority {
            if let Err(error) = authority.validate(&deadline) {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                return HandleOutcome::Continue;
            }
        }
        if event_loop.authority.fencing.ddl_authority_ambiguous {
            match crate::runtime::ddl::reconcile_prepared_within(
                &mut event_loop.state,
                event_loop.actors.cloud_coordinator.hybrid_storage.as_ref(),
                authority.as_ref(),
                &deadline,
            ) {
                Ok(()) => {
                    event_loop.authority.fencing.ddl_authority_ambiguous = false;
                    event_loop.publish_snapshot();
                    event_loop.publish_family_catalog();
                    let drop_committed = event_loop
                        .state
                        .manifest
                        .column_families
                        .iter()
                        .any(|cf| cf.id == cf_id && cf.deleted_at.is_some());
                    if drop_committed {
                        Self::finish_committed_drop(event_loop, request_id, cf_id, &deadline);
                        return HandleOutcome::Continue;
                    }
                }
                Err(error) => {
                    event_loop.publish_snapshot();
                    event_loop.publish_family_catalog();
                    event_loop.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Fenced(format!(
                                "DDL authority remains ambiguous: {error}"
                            )),
                        },
                    );
                    return HandleOutcome::Continue;
                }
            }
        }

        // Validate before I/O so Busy/InvalidArgument remains the deterministic
        // result even when the WAL device is unhealthy. The event loop is
        // serialized, so the validated state cannot change before execution.
        let result = crate::runtime::ddl::drop_edit(&event_loop.state, cf_id, discard_unflushed)
            .and_then(|edit| {
                event_loop.sync_current_wal()?;
                crate::runtime::ddl::execute_within(
                    &mut event_loop.state,
                    event_loop.actors.cloud_coordinator.hybrid_storage.as_ref(),
                    &edit,
                    authority.as_ref(),
                    &deadline,
                )?;
                event_loop.authority.fencing.ddl_authority_ambiguous = false;
                Ok(())
            });
        if let Err(error) = result {
            Self::record_ddl_authority_ambiguity(event_loop);
            event_loop.publish_snapshot();
            event_loop.publish_family_catalog();
            Self::respond_result(event_loop, request_id, Err(error));
        } else {
            Self::finish_committed_drop(event_loop, request_id, cf_id, &deadline);
        }
        HandleOutcome::Continue
    }

    fn finish_committed_drop(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        deadline: &crate::common::OperationDeadline,
    ) {
        Self::cancel_column_family_pending_work(event_loop, cf_id);
        event_loop.publish_snapshot();
        event_loop.publish_family_catalog();
        event_loop.wake_write_stall_waiters();
        let _ = event_loop.retry_gc_within(deadline);

        // The DDL registry/local journal authority switch has already
        // committed. An auxiliary manifest mirror failure must not report the
        // drop as rejected and leave Engine's handle registry split from
        // runtime state. Surface it through degraded health instead.
        // Reclamation above can already own a captured cut containing this
        // committed drop. No control-file edit follows that capture here.
        let mirrored_by_reclamation = event_loop
            .actors
            .cloud_coordinator
            .metadata_mirror
            .is_running();
        if let Err(error) = if mirrored_by_reclamation {
            Ok(())
        } else {
            Self::mirror_committed_drop(event_loop, deadline)
        } {
            event_loop.state.mark_persistence_anomaly();
            event_loop.publish_snapshot();
            tracing::warn!(%error, cf_id, "column-family drop committed but metadata mirror remains degraded");
        }
        Self::respond_result(event_loop, request_id, Ok(()));
    }

    /// Promote a DDL ambiguity recorded by the DDL path into runtime fencing.
    ///
    /// The DDL path marks the state when it cannot resolve a remote authority
    /// switch. Reading that flag keeps the decision independent of how the
    /// error is worded or wrapped on its way here.
    fn record_ddl_authority_ambiguity(event_loop: &mut EventLoop) {
        if event_loop.state.take_ddl_authority_ambiguous() {
            event_loop.authority.fencing.ddl_authority_ambiguous = true;
            event_loop.state.mark_persistence_anomaly();
            event_loop.publish_snapshot();
        }
    }

    fn mirror_committed_drop(
        event_loop: &mut EventLoop,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::ddl::after_drop_local_commit_before_metadata_mirror",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: drop metadata mirror failed after local commit".to_string()
            ))
        );
        event_loop.mirror_metadata_after_local_commit_within("drop column family", deadline)
    }

    fn cancel_column_family_pending_work(
        event_loop: &mut EventLoop,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        let error_message = format!("column family {cf_id} was dropped");
        if let Some(waiters) = event_loop.publication.flush_barrier_waiters.remove(&cf_id) {
            for waiter in waiters {
                event_loop.respond(
                    waiter.request_id,
                    RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: crate::common::MidgeError::InvalidArgument(error_message.clone()),
                    },
                );
            }
        }

        for request_id in event_loop
            .pipeline
            .write_stall_waiters
            .take_column_family(cf_id)
        {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(error_message.clone()),
                },
            );
        }
    }

    fn respond_result(
        event_loop: &mut EventLoop,
        request_id: u64,
        result: crate::common::MidgeResult<()>,
    ) {
        let resp = result.map_or_else(
            |error| RuntimeResponse::Error { request_id, error },
            |()| RuntimeResponse::Ok { request_id },
        );
        event_loop.respond(request_id, resp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::event_loop::tests::create_test_local_event_loop;

    #[test]
    fn should_fence_ddl_authority_from_recorded_state_not_error_text(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: the DDL path records the ambiguity it could not resolve.
        // Rewording or wrapping its error must not change what happens here.
        let mut event_loop = create_test_local_event_loop()?;
        event_loop.state.mark_ddl_authority_ambiguous();

        // Act
        ManifestCoordinator::record_ddl_authority_ambiguity(&mut event_loop);

        // Assert
        assert!(event_loop.authority.fencing.ddl_authority_ambiguous);
        assert!(event_loop.state.persistence_anomaly_detected());
        Ok(())
    }

    #[test]
    fn should_not_fence_ddl_authority_when_nothing_recorded_the_ambiguity(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let mut event_loop = create_test_local_event_loop()?;

        // Act
        ManifestCoordinator::record_ddl_authority_ambiguity(&mut event_loop);

        // Assert
        assert!(!event_loop.authority.fencing.ddl_authority_ambiguous);
        assert!(!event_loop.state.persistence_anomaly_detected());
        Ok(())
    }
}
