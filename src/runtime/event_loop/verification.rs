use super::{EventLoop, HandleOutcome};
use crate::runtime::{RuntimeMsg, RuntimeResponse, VerificationBarrierAction};

impl EventLoop {
    pub(super) fn gate_message_for_flush_publication(
        &mut self,
        msg: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if !self.publication_gate.is_active() {
            return Some(msg);
        }
        if msg.defers_under_publication_gate() {
            self.publication_gate.defer(msg);
            None
        } else {
            Some(msg)
        }
    }

    pub(super) fn gate_message_for_storage_verification(
        &mut self,
        msg: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if self.verification_barrier.token.is_none() {
            return Some(msg);
        }

        match msg.verification_barrier_action() {
            VerificationBarrierAction::Allow => Some(msg),
            VerificationBarrierAction::Defer => {
                self.defer_verification_message(msg);
                None
            }
            VerificationBarrierAction::Reject { request_id } => {
                self.reject_under_verification_barrier(msg, request_id);
                None
            }
        }
    }

    /// Fail a barrier-blocked message fast with `Busy`.
    ///
    /// Transaction applies may carry their own response channel, so the reply
    /// has to follow the channel the caller supplied rather than the router.
    ///
    /// `request_id` comes from [`VerificationBarrierAction::Reject`], so a
    /// message without one cannot reach here: it could not have been classified
    /// as `Reject` in the first place.
    fn reject_under_verification_barrier(&mut self, msg: RuntimeMsg, request_id: u64) {
        let error =
            crate::common::MidgeError::Busy("storage verification barrier is active".to_string());
        let response = RuntimeResponse::Error { request_id, error };
        match msg {
            RuntimeMsg::ApplyTransaction {
                response_tx: Some(response_tx),
                ..
            }
            | RuntimeMsg::ApplySpilledTransaction {
                response_tx: Some(response_tx),
                ..
            } => {
                let _ = response_tx.send(response);
            }
            _ => self.respond(request_id, response),
        }
    }
}

impl EventLoop {
    pub(super) fn defer_verification_message(&mut self, message: RuntimeMsg) {
        let is_duplicate_maintenance = matches!(message, RuntimeMsg::RetryGc)
            && self
                .verification_barrier
                .deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::RetryGc));
        let is_duplicate_drop_shutdown = matches!(message, RuntimeMsg::Shutdown)
            && self
                .verification_barrier
                .deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::Shutdown));
        if !is_duplicate_maintenance && !is_duplicate_drop_shutdown {
            self.verification_barrier
                .deferred_messages
                .push_back(message);
        }
    }

    pub(super) fn begin_storage_verification(&mut self, request_id: u64) -> HandleOutcome {
        self.begin_layout_barrier(request_id, false)
    }

    pub(super) fn begin_backup_capture(&mut self, request_id: u64) -> HandleOutcome {
        if self.cloud_coordinator.cloud_wal_prune_worker.is_some()
            || self
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| storage.pending_upload_count() > 0)
        {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Busy(
                        "cloud storage work is active during backup capture".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }
        self.begin_layout_barrier(request_id, true)
    }

    pub(super) fn begin_layout_barrier(
        &mut self,
        request_id: u64,
        sync_wal: bool,
    ) -> HandleOutcome {
        let active_compactions = self
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::Acquire);
        let layout_is_changing = active_compactions > 0
            || !self.state.compaction.compacting_ssts.is_empty()
            || self.flush_actor.is_inflight()
            || self.publication_gate.is_active();
        if self.verification_barrier.token.is_some() || layout_is_changing {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Busy(
                        "storage layout is busy or already being verified".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        if sync_wal {
            if let Err(error) = self.sync_current_wal() {
                self.respond(request_id, RuntimeResponse::Error { request_id, error });
                return HandleOutcome::Continue;
            }
        }

        let activated = self.verification_barrier.activate(request_id);
        debug_assert!(activated);
        crate::failpoints::fail_point!("midge::verification::before_barrier_response");
        self.respond(
            request_id,
            RuntimeResponse::StorageVerificationBarrier {
                request_id,
                token: request_id,
                health: self.state.health(),
                sequence: self.state.sequence,
            },
        );
        HandleOutcome::Continue
    }

    pub(super) fn end_storage_verification(
        &mut self,
        request_id: u64,
        token: u64,
    ) -> HandleOutcome {
        if self.verification_barrier.token != Some(token) {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(
                        "storage verification barrier token does not match".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let deferred = self.verification_barrier.release(token);
        let retirements = self.verification_barrier.take_deferred_wal_retirements();
        if !retirements.is_empty() {
            self.retire_acked_local_wal_segments(&retirements);
        }
        for event in self.verification_barrier.take_deferred_storage_events() {
            self.handle_storage_event(event);
        }
        if self.pending_msg.is_none() {
            self.pending_msg = deferred;
        }
        self.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    pub(super) fn restore_verification_deferred_message(&mut self) {
        if !self.shutting_down
            && self.verification_barrier.token.is_none()
            && self.pending_msg.is_none()
        {
            self.pending_msg = self.verification_barrier.deferred_messages.pop_front();
        }
    }
}
