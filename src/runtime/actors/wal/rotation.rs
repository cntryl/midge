// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::WalActor;
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsError, FsPath};
use crate::runtime::state::RuntimeState;
use crate::wal::FsWalFactoryIo;
use std::sync::Arc;

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
}
