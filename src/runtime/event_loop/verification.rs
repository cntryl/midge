use super::EventLoop;
use crate::runtime::{RuntimeMsg, RuntimeResponse};

impl EventLoop {
    pub(super) fn gate_message_for_flush_publication(
        &mut self,
        msg: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if !self.manifest_publication_active {
            return Some(msg);
        }
        let mutating = matches!(
            &msg,
            RuntimeMsg::ManifestPersist { .. }
                | RuntimeMsg::ManifestCreateColumnFamily { .. }
                | RuntimeMsg::ManifestDropColumnFamily { .. }
                | RuntimeMsg::CompactionComplete { .. }
                | RuntimeMsg::CompactAll { .. }
                | RuntimeMsg::RetryGc
        );
        #[cfg(test)]
        let mutating = mutating
            || matches!(
                &msg,
                RuntimeMsg::ManifestAddSst { .. }
                    | RuntimeMsg::ManifestCompactionComplete { .. }
                    | RuntimeMsg::RunCompaction { .. }
            );
        if mutating {
            self.publication_deferred_messages.push_back(msg);
            None
        } else {
            Some(msg)
        }
    }

    pub(super) fn gate_message_for_storage_verification(
        &mut self,
        msg: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if self.verification_barrier_token.is_none() {
            return Some(msg);
        }

        match msg {
            RuntimeMsg::ApplyTransaction {
                request_id,
                response_tx,
                ..
            }
            | RuntimeMsg::ApplySpilledTransaction {
                request_id,
                response_tx,
                ..
            } => {
                let response = RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Busy(
                        "storage verification barrier is active".to_string(),
                    ),
                };
                if let Some(response_tx) = response_tx {
                    let _ = response_tx.send(response);
                } else {
                    self.respond(request_id, response);
                }
                None
            }
            message @ RuntimeMsg::CompactionComplete { .. } => {
                self.defer_verification_message(message);
                None
            }
            message @ RuntimeMsg::RetryGc => {
                self.defer_verification_message(message);
                None
            }
            #[cfg(test)]
            message @ (RuntimeMsg::FlushComplete { .. }
            | RuntimeMsg::WalSyncComplete { .. }
            | RuntimeMsg::CloudUploadComplete { .. }
            | RuntimeMsg::DeleteObsoleteSsts { .. }
            | RuntimeMsg::ManifestAddSst { .. }
            | RuntimeMsg::ManifestCompactionComplete { .. }) => {
                self.defer_verification_message(message);
                None
            }
            message @ (RuntimeMsg::FlushMemtable { .. }
            | RuntimeMsg::WalSync { .. }
            | RuntimeMsg::SealWalForCloud { .. }
            | RuntimeMsg::ManifestPersist { .. }
            | RuntimeMsg::ManifestCreateColumnFamily { .. }
            | RuntimeMsg::ManifestDropColumnFamily { .. }
            | RuntimeMsg::SetRuntimeConfig { .. }
            | RuntimeMsg::CompactAll { .. }) => {
                let request_id = message
                    .request_id()
                    .expect("barrier-blocked runtime message has request id");
                self.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Busy(
                            "storage verification barrier is active".to_string(),
                        ),
                    },
                );
                None
            }
            #[cfg(test)]
            message @ (RuntimeMsg::WalAppend { .. }
            | RuntimeMsg::WalAppendDeleteRange { .. }
            | RuntimeMsg::WalRotate { .. }
            | RuntimeMsg::CloudUploadSst { .. }
            | RuntimeMsg::CloudUploadWal { .. }
            | RuntimeMsg::CheckGc { .. }
            | RuntimeMsg::RunCompaction { .. }
            | RuntimeMsg::BeginIngest { .. }
            | RuntimeMsg::EndIngest { .. }) => {
                let request_id = message
                    .request_id()
                    .expect("barrier-blocked test message has request id");
                self.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Busy(
                            "storage verification barrier is active".to_string(),
                        ),
                    },
                );
                None
            }
            message => Some(message),
        }
    }
}
