//! Event loop - main message processing loop
//!
//! Receives messages from the RuntimeHandle and dispatches to actors.

use crossbeam::channel::{Receiver, Sender};

use super::actors::{CloudActor, CompactionActor, EvictionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::state::RuntimeState;
use super::{RuntimeMsg, RuntimeResponse};
use crate::sst::Memtable;

/// The main event loop that processes runtime messages
pub struct EventLoop {
    /// Centralized state owned by the runtime
    state: RuntimeState,
    /// Actors for handling different message types
    flush_actor: FlushActor,
    compaction_actor: CompactionActor,
    wal_actor: WalActor,
    cloud_actor: CloudActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    eviction_actor: Option<EvictionActor>,
    /// Hybrid storage with budget management
    hybrid_storage: Option<std::sync::Arc<crate::storage::HybridStorage>>,
    /// Whether to trace messages
    trace_enabled: bool,
}

impl EventLoop {
    /// Create a new event loop with the given state
    pub fn new(state: RuntimeState, trace_enabled: bool) -> crate::common::MidgeResult<Self> {
        let wal_dir = state.wal_dir.clone();
        let sst_dir = state.sst_dir.clone();

        let sst_factory = std::sync::Arc::new(
            super::super::sst::FsSstFactory::new(&sst_dir, 64 * 1024), // 64KB block size
        );

        Ok(Self {
            state,
            flush_actor: FlushActor::new(&sst_dir)?,
            compaction_actor: CompactionActor::new(sst_factory),
            wal_actor: WalActor::new(wal_dir)?,
            cloud_actor: CloudActor::new(),
            gc_actor: GcActor::new(),
            manifest_actor: ManifestActor::new(),
            eviction_actor: None,
            hybrid_storage: None,
            trace_enabled,
        })
    }

    /// Set the hybrid storage reference for SBA integration and initialize eviction actor
    pub fn set_hybrid_storage(&mut self, storage: std::sync::Arc<crate::storage::HybridStorage>) {
        // Initialize eviction actor with hybrid storage reference
        self.eviction_actor = Some(EvictionActor::new(std::sync::Arc::clone(&storage)));
        self.hybrid_storage = Some(storage);
    }

    /// Run the event loop until shutdown
    pub fn run(&mut self, msg_rx: Receiver<RuntimeMsg>, response_tx: Sender<RuntimeResponse>) {
        loop {
            match msg_rx.recv() {
                Ok(msg) => {
                    if self.trace_enabled {
                        tracing::trace!(?msg, "runtime received message");
                    }

                    match msg {
                        RuntimeMsg::Shutdown => {
                            tracing::info!("Runtime shutting down");
                            break;
                        }
                        RuntimeMsg::Noop => {
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }

                        // === Flush Actor ===
                        RuntimeMsg::FlushMemtable { cf_id } => {
                            let result = self.flush_actor.handle_flush(
                                &mut self.state,
                                cf_id,
                                self.hybrid_storage.as_ref(),
                            );
                            let _ = response_tx.send(match result {
                                Ok(sst_name) => RuntimeResponse::FlushComplete { sst_name },
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::FlushComplete {
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
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }

                        // === Compaction Actor ===
                        RuntimeMsg::CheckCompaction => {
                            // Try to pick a compaction
                            if let Some(plan) = self.compaction_actor.check_compaction(&self.state)
                            {
                                tracing::info!(
                                    input_count = plan.input_files.len(),
                                    source_level = plan.source_level,
                                    target_level = plan.target_level,
                                    "Triggering compaction"
                                );
                                let result = self.compaction_actor.run_compaction(
                                    &mut self.state,
                                    plan,
                                    self.hybrid_storage.as_ref(),
                                );
                                let _ = response_tx.send(match result {
                                    Ok(output_ssts) => {
                                        RuntimeResponse::CompactionComplete { output_ssts }
                                    }
                                    Err(e) => RuntimeResponse::Error(e.to_string()),
                                });
                            } else {
                                let _ = response_tx.send(RuntimeResponse::Ok);
                            }
                        }
                        RuntimeMsg::RunCompaction { plan } => {
                            // Convert runtime::CompactionPlan to compaction::CompactionPlan
                            let compaction_plan = crate::compaction::CompactionPlan {
                                input_files: plan.input_files,
                                output_files: Vec::new(),
                                source_level: plan.source_level,
                                target_level: plan.target_level,
                                cf_id: plan.cf_id,
                            };
                            let result = self.compaction_actor.run_compaction(
                                &mut self.state,
                                compaction_plan,
                                self.hybrid_storage.as_ref(),
                            );
                            let _ = response_tx.send(match result {
                                Ok(output_ssts) => {
                                    RuntimeResponse::CompactionComplete { output_ssts }
                                }
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::CompactionComplete {
                            input_ssts,
                            output_ssts,
                        } => {
                            self.compaction_actor.handle_complete(
                                &mut self.state,
                                input_ssts,
                                output_ssts,
                            );
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }

                        // === WAL Actor ===
                        RuntimeMsg::WalAppend {
                            cf_id,
                            key,
                            value,
                            sequence,
                        } => {
                            let key_bytes = bytes::Bytes::from(key);
                            let value_bytes = value.map(bytes::Bytes::from);
                            let result = self.wal_actor.append(
                                &mut self.state,
                                cf_id,
                                key_bytes,
                                value_bytes,
                                sequence,
                            );
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::WalSync => {
                            let result = self.wal_actor.sync(&mut self.state);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::WalRotate => {
                            let result = self.wal_actor.rotate(&mut self.state);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::WalSyncComplete { segment_id } => {
                            self.wal_actor
                                .handle_sync_complete(&mut self.state, segment_id);
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }

                        // === Cloud Actor ===
                        RuntimeMsg::CloudUploadSst { sst_name } => {
                            let result = self.cloud_actor.upload_sst(&mut self.state, &sst_name);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::CloudUploadWal { segment_id } => {
                            let result = self.cloud_actor.upload_wal(&mut self.state, segment_id);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::CloudUploadComplete { resource } => {
                            self.cloud_actor
                                .handle_upload_complete(&mut self.state, &resource);
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }

                        // === GC Actor ===
                        RuntimeMsg::CheckGc => {
                            self.gc_actor.check(&self.state);
                            let _ = response_tx.send(RuntimeResponse::Ok);
                        }
                        RuntimeMsg::DeleteObsoleteSsts { sst_names } => {
                            let result = self.gc_actor.delete_ssts(&mut self.state, &sst_names);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }

                        // === Manifest Actor ===
                        RuntimeMsg::ManifestAddSst { file_meta } => {
                            let result = self.manifest_actor.add_sst(&mut self.state, file_meta);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::ManifestCompactionComplete { removed, added } => {
                            let result = self.manifest_actor.compaction_complete(
                                &mut self.state,
                                removed,
                                added,
                            );
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::ManifestPersist => {
                            let result = self.manifest_actor.persist(&self.state);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }

                        // === Column Family Lifecycle ===
                        RuntimeMsg::ManifestCreateColumnFamily { name } => {
                            let result = self
                                .manifest_actor
                                .create_column_family(&mut self.state, name.clone());
                            let _ = response_tx.send(match result {
                                Ok(cf_id) => RuntimeResponse::ColumnFamilyCreated { cf_id },
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }
                        RuntimeMsg::ManifestDropColumnFamily { cf_id } => {
                            let result = self
                                .manifest_actor
                                .drop_column_family(&mut self.state, cf_id);
                            let _ = response_tx.send(match result {
                                Ok(()) => RuntimeResponse::Ok,
                                Err(e) => RuntimeResponse::Error(e.to_string()),
                            });
                        }

                        // === Read Path ===
                        RuntimeMsg::Read {
                            cf_id,
                            key,
                            sequence,
                        } => {
                            let value = self.handle_read(cf_id, &key, sequence);
                            let _ = response_tx.send(RuntimeResponse::ReadValue(value));
                        }
                    }
                }
                Err(_) => {
                    // Channel closed, exit
                    tracing::debug!("Runtime message channel closed");
                    break;
                }
            }
        }
    }

    /// Handle read operation by checking memtables and SST files
    fn handle_read(&self, cf_id: u32, key: &[u8], _sequence: u64) -> Option<Vec<u8>> {
        // Get column family state
        let cf_state = match self.state.column_families.get(&cf_id) {
            Some(cf) => cf,
            None => return None,
        };

        // Check active memtable first
        if let Ok(Some(value)) = cf_state.memtable.get(key) {
            return Some(value);
        }

        // Check immutable memtables (in reverse order - newest first)
        for immutable in cf_state.immutable_memtables.iter().rev() {
            if let Ok(Some(value)) = immutable.get(key) {
                return Some(value);
            }
        }

        // TODO: Check SST files via manifest
        // Temporarily disabled due to Windows file locking issues
        // SST file reading will be re-enabled after architectural changes
        None
    }
}
