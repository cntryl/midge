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
                    error: crate::common::MidgeError::Internal(
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
            )
            .and_then(|()| event_loop.mirror_metadata_after_local_commit("create column family"))
            .map(|()| cf_id)
        });
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
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
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
                    error: crate::common::MidgeError::Internal(
                        "ingest: DDL forbidden during ingest mode".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let result = event_loop.force_wal_sync(msg_rx).and_then(|()| {
            let edit = crate::runtime::ddl::drop_edit(&event_loop.state, cf_id)?;
            crate::runtime::ddl::execute(
                &mut event_loop.state,
                event_loop.hybrid_storage.as_ref(),
                &edit,
            )
            .and_then(|()| event_loop.mirror_metadata_after_local_commit("drop column family"))
        });
        if result.is_ok() {
            event_loop.publish_snapshot();
            let _ = event_loop.retry_gc();
        }
        Self::respond_result(event_loop, request_id, result);
        HandleOutcome::Continue
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
