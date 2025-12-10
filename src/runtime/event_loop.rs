//! Event loop — central message dispatcher
//!
//! Receives messages from RuntimeHandle and routes them to the correct actor.
//!
//! Copilot note:
//! - Per-request routing is done exclusively via `router.complete()`.
//! - EventLoop never touches pending_responses directly.
//! - All read paths are local (memtables → SST later).
//! - All actor responses flow through `respond()`.

use crossbeam::channel::Receiver;
use std::sync::Arc;

use super::actors::{
    CloudActor, CompactionActor, EvictionActor, FlushActor, GcActor, ManifestActor, WalActor,
};
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::sst::Memtable;

/// Main synchronous event loop for the runtime.
///
/// Owns all actors and is responsible for routing inbound messages.
pub struct EventLoop {
    state: RuntimeState,

    // Actors
    flush_actor: FlushActor,
    compaction_actor: CompactionActor,
    wal_actor: WalActor,
    cloud_actor: CloudActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    eviction_actor: Option<EvictionActor>,

    hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    trace_enabled: bool,

    /// Per-request router (oneshot channels)
    router: Arc<ResponseRouter>,
}

impl EventLoop {
    pub(crate) fn new(
        state: RuntimeState,
        trace_enabled: bool,
        router: Arc<ResponseRouter>,
    ) -> crate::common::MidgeResult<Self> {
        let wal_dir = state.wal_dir.clone();
        let sst_dir = state.sst_dir.clone();

        let sst_factory = Arc::new(
            super::super::sst::FsSstFactory::new(&sst_dir, 64 * 1024), // 64KB block size
        );

        Ok(Self {
            state,
            flush_actor: FlushActor::new(&sst_dir)?,
            compaction_actor: CompactionActor::new(sst_factory),
            wal_actor: WalActor::new(wal_dir, crate::wal::DurabilityPolicy::Batched)?,
            cloud_actor: CloudActor::new(),
            gc_actor: GcActor::new(),
            manifest_actor: ManifestActor::new(),
            eviction_actor: None,
            hybrid_storage: None,
            trace_enabled,
            router,
        })
    }

    pub fn set_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.eviction_actor = Some(EvictionActor::new(storage.clone()));
        self.hybrid_storage = Some(storage);
    }

    /// Helper: deliver a RuntimeResponse to the requester via the router.
    #[inline]
    fn respond(&self, request_id: u64, resp: RuntimeResponse) {
        self.router.complete(resp);

        // Optional trace
        if self.trace_enabled {
            tracing::trace!(request_id, "response routed");
        }
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: Receiver<RuntimeMsg>) {
        while let Ok(msg) = msg_rx.recv() {
            if self.trace_enabled {
                tracing::trace!(?msg, "runtime received message");
            }

            match msg {
                RuntimeMsg::Shutdown => {
                    tracing::info!("Runtime shutting down");
                    break;
                }

                RuntimeMsg::Noop { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // =============================================================
                // Flush
                // =============================================================
                RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                    let resp = self
                        .flush_actor
                        .handle_flush(&mut self.state, cf_id, self.hybrid_storage.as_ref())
                        .map(|sst_name| RuntimeResponse::FlushComplete {
                            request_id,
                            sst_name,
                        })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });

                    self.respond(request_id, resp);
                }

                RuntimeMsg::FlushComplete {
                    request_id,
                    cf_id,
                    sst_name,
                    sequence,
                } => {
                    self.flush_actor.handle_flush_complete(
                        &mut self.state,
                        cf_id,
                        &sst_name,
                        sequence,
                    );
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // =============================================================
                // Compaction
                // =============================================================
                RuntimeMsg::CheckCompaction { request_id } => {
                    if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        let resp = self
                            .compaction_actor
                            .run_compaction(&mut self.state, plan, self.hybrid_storage.as_ref())
                            .map(|output_ssts| RuntimeResponse::CompactionComplete {
                                request_id,
                                output_ssts,
                            })
                            .unwrap_or_else(|e| RuntimeResponse::Error {
                                request_id,
                                message: e.to_string(),
                            });

                        self.respond(request_id, resp);
                    } else {
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                    }
                }

                RuntimeMsg::RunCompaction { request_id, plan } => {
                    let cplan = crate::compaction::CompactionPlan {
                        input_files: plan.input_files,
                        output_files: Vec::new(),
                        source_level: plan.source_level,
                        target_level: plan.target_level,
                        cf_id: plan.cf_id,
                    };

                    let resp = self
                        .compaction_actor
                        .run_compaction(&mut self.state, cplan, self.hybrid_storage.as_ref())
                        .map(|output_ssts| RuntimeResponse::CompactionComplete {
                            request_id,
                            output_ssts,
                        })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });

                    self.respond(request_id, resp);
                }

                RuntimeMsg::CompactionComplete {
                    request_id,
                    input_ssts,
                    output_ssts,
                } => {
                    self.compaction_actor
                        .handle_complete(&mut self.state, input_ssts, output_ssts);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // WAL
                RuntimeMsg::WalAppend {
                    request_id,
                    cf_id,
                    key,
                    value,
                    sequence,
                } => {
                    let result = self.wal_actor.append(
                        &mut self.state,
                        cf_id,
                        bytes::Bytes::from(key),
                        value.map(bytes::Bytes::from),
                        sequence,
                    );
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalSync { request_id } => {
                    let result = self.wal_actor.sync(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalRotate { request_id } => {
                    let result = self.wal_actor.rotate(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalSyncComplete {
                    request_id,
                    segment_id,
                } => {
                    self.wal_actor
                        .handle_sync_complete(&mut self.state, segment_id);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // Cloud
                RuntimeMsg::CloudUploadSst {
                    request_id,
                    sst_name,
                } => {
                    let result = self.cloud_actor.upload_sst(&mut self.state, &sst_name);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::CloudUploadWal {
                    request_id,
                    segment_id,
                } => {
                    let result = self.cloud_actor.upload_wal(&mut self.state, segment_id);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::CloudUploadComplete {
                    request_id,
                    resource,
                } => {
                    self.cloud_actor
                        .handle_upload_complete(&mut self.state, &resource);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // GC
                RuntimeMsg::CheckGc { request_id } => {
                    self.gc_actor.check(&self.state);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::DeleteObsoleteSsts {
                    request_id,
                    sst_names,
                } => {
                    let result = self.gc_actor.delete_ssts(&mut self.state, &sst_names);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                // Manifest
                RuntimeMsg::ManifestAddSst {
                    request_id,
                    file_meta,
                } => {
                    let result = self.manifest_actor.add_sst(&mut self.state, file_meta);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestCompactionComplete {
                    request_id,
                    removed,
                    added,
                } => {
                    let result =
                        self.manifest_actor
                            .compaction_complete(&mut self.state, removed, added);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestPersist { request_id } => {
                    let result = self.manifest_actor.persist(&self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => {
                    let result = self
                        .manifest_actor
                        .create_column_family(&mut self.state, name.clone());
                    let resp = result
                        .map(|cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestDropColumnFamily { request_id, cf_id } => {
                    let result = self
                        .manifest_actor
                        .drop_column_family(&mut self.state, cf_id);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                // Read path: local memtable lookup
                RuntimeMsg::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                } => {
                    let value = self.handle_read(cf_id, &key, sequence);
                    self.respond(request_id, RuntimeResponse::ReadValue { request_id, value });
                }
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }

    /// Local read path: memtable → immutable memtables → [SST TODO]
    fn handle_read(&self, cf_id: u32, key: &[u8], _seq: u64) -> Option<Vec<u8>> {
        let cf_state = self.state.column_families.get(&cf_id)?;

        // Active memtable
        if let Ok(Some(v)) = cf_state.memtable.get(key) {
            return Some(v);
        }

        // Immutable memtables (newest → oldest)
        for imm in cf_state.immutable_memtables.iter().rev() {
            if let Ok(Some(v)) = imm.get(key) {
                return Some(v);
            }
        }

        // SST lookup temporarily disabled
        None
    }
}
