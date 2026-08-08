use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::FileMeta;
use crate::runtime::{RuntimeMsg, RuntimeResponse};
use crossbeam::channel::Receiver;

pub(super) struct ManifestCoordinator;

impl ManifestCoordinator {
    #[cfg(test)]
    pub(super) fn add_sst(
        event_loop: &mut EventLoop,
        request_id: u64,
        file_meta: FileMeta,
    ) -> HandleOutcome {
        let result = event_loop
            .manifest_actor
            .add_sst(&mut event_loop.state, file_meta)
            .and_then(|()| event_loop.mirror_metadata_after_local_commit("manifest add sst"));
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
        let result = event_loop
            .manifest_actor
            .compaction_complete(&mut event_loop.state, removed, added)
            .and_then(|()| {
                event_loop.mirror_metadata_after_local_commit("manifest compaction complete")
            });
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
    }

    pub(super) fn persist(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        let result = crate::runtime::actors::ManifestActor::persist(&event_loop.state)
            .and_then(|()| event_loop.mirror_metadata_after_local_commit("manifest persist"));
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
    }

    pub(super) fn create_column_family(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        name: &str,
    ) -> HandleOutcome {
        if let Err(error) = crate::runtime::ddl::validate_column_family_name(name) {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }
        if event_loop
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tracing::error!("ingest: attempted DDL (create CF) during ingest mode");
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(
                        "ingest: DDL forbidden during ingest mode".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let result = event_loop.force_wal_sync(msg_rx).and_then(|()| {
            if let Some(existing) = event_loop.state.manifest.get_column_family_by_name(name) {
                let existing_id = existing.id;
                return event_loop
                    .mirror_metadata_after_local_commit("idempotent create column family")
                    .map(|()| existing_id);
            }
            let edit = crate::runtime::ddl::create_edit(&event_loop.state, name)?;
            let cf_id = match &edit {
                crate::metadata::ManifestEdit::CreateColumnFamily { id, .. } => *id,
                _ => unreachable!("create_edit returned a non-create edit"),
            };
            crate::runtime::ddl::execute(
                &mut event_loop.state,
                event_loop.hybrid_storage.as_ref(),
                &edit,
            )?;
            event_loop.ddl_authority_ambiguous = false;
            event_loop.mirror_metadata_after_local_commit("create column family")?;
            Ok(cf_id)
        });
        if let Err(error) = &result {
            Self::record_ddl_authority_ambiguity(event_loop, error);
        }
        let should_publish = result.is_ok();
        let resp = result.map_or_else(
            |error| RuntimeResponse::Error { request_id, error },
            |cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id },
        );
        if should_publish {
            event_loop.publish_snapshot();
        }
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn drop_column_family(
        event_loop: &mut EventLoop,
        _msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        discard_unflushed: bool,
    ) -> HandleOutcome {
        if event_loop
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tracing::error!("ingest: attempted DDL (drop CF) during ingest mode");
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(
                        "ingest: DDL forbidden during ingest mode".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        if event_loop.ddl_authority_ambiguous {
            match crate::runtime::ddl::reconcile_prepared(
                &mut event_loop.state,
                event_loop.hybrid_storage.as_ref(),
            ) {
                Ok(()) => {
                    event_loop.ddl_authority_ambiguous = false;
                    let drop_committed = event_loop
                        .state
                        .manifest
                        .column_families
                        .iter()
                        .any(|cf| cf.id == cf_id && cf.deleted_at.is_some());
                    if drop_committed {
                        Self::finish_committed_drop(event_loop, request_id, cf_id);
                        return HandleOutcome::Continue;
                    }
                }
                Err(error) => {
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
                crate::runtime::ddl::execute(
                    &mut event_loop.state,
                    event_loop.hybrid_storage.as_ref(),
                    &edit,
                )?;
                event_loop.ddl_authority_ambiguous = false;
                Ok(())
            });
        if let Err(error) = result {
            Self::record_ddl_authority_ambiguity(event_loop, &error);
            Self::respond_result(event_loop, request_id, Err(error));
        } else {
            Self::finish_committed_drop(event_loop, request_id, cf_id);
        }
        HandleOutcome::Continue
    }

    fn finish_committed_drop(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        Self::cancel_column_family_pending_work(event_loop, cf_id);
        event_loop.publish_snapshot();
        let _ = event_loop.retry_gc();

        // The DDL registry/local journal authority switch has already
        // committed. An auxiliary manifest mirror failure must not report the
        // drop as rejected and leave Engine's handle registry split from
        // runtime state. Surface it through degraded health instead.
        if let Err(error) = Self::mirror_committed_drop(event_loop) {
            event_loop.state.mark_persistence_anomaly();
            event_loop.publish_snapshot();
            tracing::warn!(%error, cf_id, "column-family drop committed but metadata mirror remains degraded");
        }
        Self::respond_result(event_loop, request_id, Ok(()));
    }

    fn record_ddl_authority_ambiguity(
        event_loop: &mut EventLoop,
        error: &crate::common::MidgeError,
    ) {
        if matches!(
            error,
            crate::common::MidgeError::Fenced(message)
                if message.contains("DDL authority is ambiguous")
        ) {
            event_loop.ddl_authority_ambiguous = true;
            event_loop.state.mark_persistence_anomaly();
            event_loop.publish_snapshot();
        }
    }

    fn mirror_committed_drop(event_loop: &mut EventLoop) -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::ddl::after_drop_local_commit_before_metadata_mirror",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: drop metadata mirror failed after local commit".to_string()
            ))
        );
        event_loop.mirror_metadata_after_local_commit("drop column family")
    }

    fn cancel_column_family_pending_work(
        event_loop: &mut EventLoop,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        let error_message = format!("column family {cf_id} was dropped");
        if let Some(waiters) = event_loop.flush_barrier_waiters.remove(&cf_id) {
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

        event_loop.write_stall_waiter_queues.remove(&cf_id);
        let stalled_request_ids = event_loop
            .write_stall_waiters
            .iter()
            .filter_map(|(request_id, waiter_cf)| (*waiter_cf == cf_id).then_some(*request_id))
            .collect::<Vec<_>>();
        for request_id in stalled_request_ids {
            event_loop.write_stall_waiters.remove(&request_id);
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
