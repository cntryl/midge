use crate::io::{Durability, Fs, FsPath, FsResult};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
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
            // Wait for work (queue data, sync request, or shutdown)
            let batch: Vec<Vec<u8>>;
            {
                let mut q = self.config.queue.lock();
                // Wait until: queue has data OR a sync/flush is requested OR shutdown requested.
                //
                // IMPORTANT: `WalWriter::sync()` must work even when the queue is empty. The writer
                // thread performs I/O asynchronously, so an fsync request may arrive with no queued
                // buffers. If we only wake on queued data, callers can deadlock forever.
                while q.is_empty()
                    && !self
                        .config
                        .shutdown
                        .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let has_pending_sync = {
                        let s = self.config.sync_state.lock();
                        (s.pending_fsyncs > s.completed_fsyncs)
                            || (s.pending_flushes > s.completed_flushes)
                    };

                    if has_pending_sync {
                        break;
                    }

                    // Periodically wake to re-check shutdown/sync flags.
                    self.config
                        .queue_cond
                        .wait_for(&mut q, std::time::Duration::from_millis(10));
                }

                // Check for shutdown (but still allow pending fsync/flush requests to complete)
                if self
                    .config
                    .shutdown
                    .load(std::sync::atomic::Ordering::SeqCst)
                    && q.is_empty()
                {
                    let has_pending_sync = {
                        let s = self.config.sync_state.lock();
                        (s.pending_fsyncs > s.completed_fsyncs)
                            || (s.pending_flushes > s.completed_flushes)
                    };
                    if !has_pending_sync {
                        break;
                    }
                }

                // Drain queue
                batch = std::mem::take(&mut *q);
            }

            // Process any queued data
            if !batch.is_empty() {
                // Coalesce
                let total: usize = batch.iter().map(|b| b.len()).sum();
                let mut big = Vec::with_capacity(total);
                for mut small in batch.into_iter() {
                    big.extend_from_slice(&small);
                    small.clear();
                    // return small to pool
                    let mut pool = self.config.buf_pool.lock();
                    pool.push(small);
                }

                // Write
                let big_bytes = bytes::Bytes::from(big);
                let write_start = Instant::now();
                let mut write_result: Option<u64> = None;

                // Ensure a handle exists if possible
                if file_opt.is_none() {
                    file_opt = self.open_file_handle().ok();
                }

                // Attempt append; if it fails, reopen and retry once.
                if let Some(ref mut file) = file_opt {
                    match file.append(big_bytes.clone()) {
                        Ok(pos) => write_result = Some(pos),
                        Err(e1) => {
                            tracing::warn!(error = ?e1, "wal writer append failed; reopening and retrying");
                            file_opt = self.open_file_handle().ok();
                            if let Some(ref mut file2) = file_opt {
                                match file2.append(big_bytes.clone()) {
                                    Ok(pos) => write_result = Some(pos),
                                    Err(e2) => {
                                        tracing::error!(error = ?e2, "wal writer append failed after retry; re-queueing buffer");
                                        // Best-effort: requeue to avoid silent WAL loss.
                                        let mut q = self.config.queue.lock();
                                        q.push(big_bytes.to_vec());
                                        self.config.queue_cond.notify_one();
                                    }
                                }
                            } else {
                                tracing::error!("wal writer could not reopen file handle after append failure; re-queueing buffer");
                                let mut q = self.config.queue.lock();
                                q.push(big_bytes.to_vec());
                                self.config.queue_cond.notify_one();
                            }
                        }
                    }
                } else {
                    tracing::error!("wal writer has no file handle; re-queueing buffer");
                    let mut q = self.config.queue.lock();
                    q.push(big_bytes.to_vec());
                    self.config.queue_cond.notify_one();
                }

                if let Some(start_pos) = write_result {
                    let write_elapsed = write_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics()
                            .record_wal_write_syscall(write_elapsed.as_nanos() as u64);
                    }
                    // `append()` returns the starting offset; expose end offset as "current_pos".
                    let end_pos = start_pos.saturating_add(big_bytes.len() as u64);
                    self.config
                        .current_pos
                        .store(end_pos, std::sync::atomic::Ordering::SeqCst);

                    // Mark completed flushes (barrier for "writes before flush() have been written")
                    {
                        let mut s = self.config.sync_state.lock();
                        s.completed_flushes = s.pending_flushes;
                        self.config.sync_cond.notify_all();
                    }
                }
            }

            // If a flush was requested but there was no data batch, still complete it.
            // This makes `WalWriter::flush()` a reliable barrier even when the queue was
            // empty at the time of the call.
            let need_flush = {
                let s = self.config.sync_state.lock();
                s.pending_flushes > s.completed_flushes
            };
            if need_flush {
                let mut s = self.config.sync_state.lock();
                s.completed_flushes = s.pending_flushes;
                self.config.sync_cond.notify_all();
            }

            // Handle pending fsyncs
            let need_sync = {
                let s = self.config.sync_state.lock();
                s.pending_fsyncs > s.completed_fsyncs
            };

            if need_sync {
                if file_opt.is_none() {
                    file_opt = self.open_file_handle().ok();
                }
                if let Some(ref mut file) = file_opt {
                    let sync_start = Instant::now();
                    let _ = file.sync(Durability::Durable);
                    let sync_elapsed = sync_start.elapsed();
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics()
                            .record_wal_fsync_ns(sync_elapsed.as_nanos() as u64);
                        t.metrics().record_wal_fsync_count();
                    }
                }

                let mut s = self.config.sync_state.lock();
                s.completed_fsyncs = s.pending_fsyncs;
                self.config.sync_cond.notify_all();
            }

            // Check for shutdown after processing
            if self
                .config
                .shutdown
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
        }
    }
}
