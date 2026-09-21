use super::EventLoop;
use crate::runtime::{RuntimeMsg, RuntimeResponse, VerificationBarrierAction};

impl EventLoop {
    pub(super) fn gate_message_for_flush_publication(
        &mut self,
        msg: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if !self.publication_gate.active {
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
