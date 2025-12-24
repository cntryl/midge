use crate::io::{Durability, Fs, FsPath, FsResult};
use std::sync::Arc;
use parking_lot::{Mutex, Condvar};
use std::time::Instant;

#[derive(Default)]
pub struct SyncState {
    pub pending_flushes: u64,
    pub pending_fsyncs: u64,
    pub completed_flushes: u64,
    pub completed_fsyncs: u64,
}

/// Configuration struct to reduce constructor arguments
pub struct WriterConfig {
    pub fs: Arc<dyn Fs>,
    pub path: FsPath,
    pub queue: Arc<Mutex<Vec<Vec<u8>>>>,
    pub queue_cond: Arc<Condvar>,
    pub buf_pool: Arc<Mutex<Vec<Vec<u8>>>>,
    pub sync_state: Arc<Mutex<SyncState>>,
    pub sync_cond: Arc<Condvar>,
    pub current_pos: Arc<std::sync::atomic::AtomicU64>,
    pub shutdown: Arc<std::sync::atomic::AtomicBool>,
}

pub struct WriterRunner {
    config: WriterConfig,
}

impl WriterRunner {
    pub fn new(config: WriterConfig) -> Self {
        Self { config }
    }

    fn open_file_handle(&self) -> FsResult<Box<dyn crate::io::File + '_>> {
        // Prefer persistent handle if available
        match self.config.fs.open_persistent_handle(
            &self.config.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        ) {
            Ok(f) => Ok(f),
            Err(_) => self.config.fs.open(
                &self.config.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            ),
        }
    }

    pub fn run(self) {
        // Attempt to open persistent handle once and reuse it where possible
        let mut file_opt = self.open_file_handle().ok();

        loop {
            // Wait for work (queue data or sync request or shutdown)
            let mut batch: Vec<Vec<u8>> = Vec::new();
            let mut sync_requested = false;
            {
                let mut q = self.config.queue.lock();
                // Wait while: queue empty AND no pending syncs AND not shutting down
                while q.is_empty() 
                    && !self.config.shutdown.load(std::sync::atomic::Ordering::SeqCst)
                {
                    // Check if there's a pending sync request even with empty queue
                    {
                        let s = self.config.sync_state.lock();
                        if s.pending_fsyncs > s.completed_fsyncs {
                            sync_requested = true;
                            break;
                        }
                    }
                    self.config.queue_cond.wait(&mut q);
                }
                if self.config.shutdown.load(std::sync::atomic::Ordering::SeqCst) && q.is_empty() {
                    // Before exiting, handle any final pending syncs
                    let s = self.config.sync_state.lock();
                    if s.pending_fsyncs > s.completed_fsyncs {
                        sync_requested = true;
                    }
                    if !sync_requested {
                        break;
                    }
                }
                // Drain queue
                batch.append(&mut *q);
            }

            // Process any queued data
            if !batch.is_empty() {
                // Coalesce
                let total: usize = batch.iter().map(|b| b.len()).sum();
                let mut big = Vec::with_capacity(total);
                for mut small in batch.drain(..) {
                    big.append(&mut small);
                    // return small to pool
                    let mut pool = self.config.buf_pool.lock();
                    pool.push(std::mem::take(&mut small));
                }

                // Write
                let write_start = Instant::now();
                let write_result = if let Some(ref mut file) = file_opt {
                    match file.append(bytes::Bytes::from(std::mem::take(&mut big))) {
                        Ok(pos) => Some(pos),
                        Err(_) => {
                            // attempt re-open and try once
                            file_opt = self.open_file_handle().ok();
                            if let Some(ref mut f) = file_opt {
                                f.append(bytes::Bytes::from(std::mem::take(&mut big))).ok()
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    // open-on-demand
                    match self.open_file_handle() {
                        Ok(mut f) => {
                            let pos = f.append(bytes::Bytes::from(std::mem::take(&mut big))).ok();
                            file_opt = Some(f);
                            pos
                        }
                        Err(_) => None,
                    }
                };

                if let Some(start_pos) = write_result {
                    let write_elapsed = write_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_wal_write_syscall(write_elapsed.as_nanos() as u64);
                    }
                    self.config.current_pos.store(start_pos, std::sync::atomic::Ordering::SeqCst);
                }

                // Mark completed flushes (a write makes queued data visible to readers)
                {
                    let mut s = self.config.sync_state.lock();
                    s.completed_flushes = s.pending_flushes;
                    self.config.sync_cond.notify_all();
                }
            }

            // Handle pending fsyncs: perform single sync that completes all pending fsync requests
            let need_sync = {
                let s = self.config.sync_state.lock();
                s.pending_fsyncs > s.completed_fsyncs
            };

            if need_sync {
                if let Some(ref mut file) = file_opt {
                    let sync_start = Instant::now();
                    let _ = file.sync(Durability::Durable);
                    let sync_elapsed = sync_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_wal_fsync_ns(sync_elapsed.as_nanos() as u64);
                        t.metrics().record_wal_fsync_count();
                    }
                }

                let mut s = self.config.sync_state.lock();
                s.completed_fsyncs = s.pending_fsyncs;
                self.config.sync_cond.notify_all();
            }

            // Check for shutdown after processing
            if self.config.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
    }
}
