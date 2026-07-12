// Responsibilities for this WAL actor slice stay within the actor namespace.
#[cfg(test)]
use super::super::cloud_write_queue::PendingCloudWrite;
use super::WalActor;
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsError, FsPath};
use crate::runtime::state::RuntimeState;
use crate::wal::FsWalFactoryIo;
#[cfg(test)]
use bytes::Bytes;
use std::sync::Arc;
#[cfg(test)]
use std::time::Instant;

impl WalActor {
    /// Rotate to a new WAL segment
    pub fn rotate(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let old_segment = state.wal.current_segment_id;

        // Close the current writer before renaming
        let _ = self.writer.take();

        if let Some(fs) = self.wal_fs.as_ref().map(Arc::clone) {
            // Rename the mutable active WAL to its immutable sealed segment name.
            let old_path = FsPath::new(crate::wal::ACTIVE_FILE_NAME);
            let new_path = FsPath::new(crate::wal::segment_file_name(old_segment));

            match fs.rename_atomic(&old_path, &new_path) {
                Ok(()) => {}
                Err(FsError::NotFound(_)) if self.can_ignore_missing_active_segment() => {
                    tracing::debug!(
                        old_segment,
                        "WAL rotate ignored missing empty active segment"
                    );
                }
                Err(error) => {
                    self.restore_active_writer_after_failed_rotate(&fs);
                    tracing::error!(
                        old_segment,
                        error = ?error,
                        "WAL rotate failed while sealing active segment"
                    );
                    return Err(MidgeError::from(error));
                }
            }

            // Create new writer for the next segment
            let factory =
                FsWalFactoryIo::new(Arc::clone(&fs)).with_io_timeout(self.storage_io_timeout);
            self.writer = Some(factory.create_writer(crate::wal::ACTIVE_FILE_NAME)?);
        }

        state.wal.current_segment_id += 1;
        self.segment_max_sequence = 0;

        tracing::info!(
            old_segment,
            new_segment = state.wal.current_segment_id,
            "WAL rotate"
        );

        Ok(())
    }

    fn can_ignore_missing_active_segment(&self) -> bool {
        self.segment_max_sequence == 0 && !self.has_pending_data()
    }

    fn restore_active_writer_after_failed_rotate(&mut self, fs: &Arc<dyn Fs>) {
        let factory = FsWalFactoryIo::new(Arc::clone(fs)).with_io_timeout(self.storage_io_timeout);
        match factory.create_writer(crate::wal::ACTIVE_FILE_NAME) {
            Ok(writer) => self.writer = Some(writer),
            Err(error) => {
                tracing::error!(
                    error = ?error,
                    "failed to reopen active WAL writer after rotate failure"
                );
            }
        }
    }

    /// Handle cloud upload completion (`CloudAsync` durability)
    ///
    /// Updates `cloud_durable_seq` and completes any pending writes
    /// by applying them to memtable.
    #[cfg(test)]
    fn apply_pending_cloud_write(
        state: &mut RuntimeState,
        pending: PendingCloudWrite,
    ) -> MidgeResult<()> {
        match pending {
            PendingCloudWrite::Single {
                cf_id,
                key,
                value,
                sequence,
                expiration,
                enqueued_at,
            } => {
                let wait_time = Instant::now().duration_since(enqueued_at);
                tracing::debug!(
                    sequence,
                    wait_ms = wait_time.as_millis(),
                    "Applying pending single write after cloud durability"
                );
                let key_bytes = Bytes::from(key);
                let value_bytes = value.map(Bytes::from);
                Self::apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key_bytes,
                    value_bytes,
                    expiration,
                )?;
            }
            PendingCloudWrite::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
                enqueued_at,
            } => {
                let wait_time = Instant::now().duration_since(enqueued_at);
                tracing::debug!(
                    cf_id,
                    sequence,
                    wait_ms = wait_time.as_millis(),
                    start_len = start_key.len(),
                    end_len = end_key.len(),
                    "Applying pending delete_range after cloud durability"
                );
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, &start_key, &end_key)?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
        max_seq_in_segment: u64,
    ) -> MidgeResult<()> {
        // Update cloud durability frontier
        state.wal.cloud_durable_seq = state.wal.cloud_durable_seq.max(max_seq_in_segment);

        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );

        // Apply pending writes to memtable.
        #[cfg(test)]
        {
            let drained_writes = self
                .cloud_write_queue
                .drain_until(state.wal.cloud_durable_seq);

            for pending in drained_writes {
                Self::apply_pending_cloud_write(state, pending)?;
            }
        }

        Ok(())
    }

    #[cfg(not(test))]
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
        max_seq_in_segment: u64,
    ) {
        state.wal.cloud_durable_seq = state.wal.cloud_durable_seq.max(max_seq_in_segment);
        self.cloud_pending_writes = false;

        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );
    }

    pub fn handle_cloud_upload_failed(&mut self, segment_id: u64, error: &str) {
        tracing::error!(segment_id, error, "Cloud upload failed");

        #[cfg(test)]
        self.cloud_write_queue.clear();
        #[cfg(not(test))]
        {
            self.cloud_pending_writes = false;
        }
    }
}
