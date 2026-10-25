use super::{
    compaction, fencing, Arc, AuthorityReadResources, Duration, EventLoopDiagnostics, HashMap,
    ReadResources, RefCell, RequestRouting, ResponseRouter, RuntimeResponse, Sender,
};

impl AuthorityReadResources {
    pub(super) fn new(
        read_resources: Option<Arc<ReadResources>>,
        config: &super::super::RuntimeConfig,
    ) -> Self {
        AuthorityReadResources {
            snapshot_cache: None,
            read_resources,
            fencing: fencing::RuntimeFence {
                lease_validity: config.lease_validity.clone(),
                lease_healthy: config.lease_healthy.clone(),
                ddl_authority_ambiguous: false,
                writer_epoch: config.writer_epoch,
                leader_store: config.leader_store.clone(),
                leader_holder_id: config.leader_holder_id.clone(),
            },
            compaction_fence: compaction::CompactionPublicationFence::default(),
        }
    }
}

impl RequestRouting {
    pub(super) fn new(router: Arc<ResponseRouter>, runtime_response_timeout: Duration) -> Self {
        Self {
            router,
            inline_responses: RefCell::new(HashMap::new()),
            pending_msg: None,
            runtime_response_timeout,
        }
    }

    pub(super) fn request_deadline(&self, request_id: u64) -> crate::common::OperationDeadline {
        self.router
            .request_deadline(request_id, self.runtime_response_timeout)
            .unwrap_or_else(|| crate::common::OperationDeadline::from_budget(Duration::ZERO))
    }

    pub(super) fn register_inline_response(
        &self,
        request_id: u64,
        response_tx: Sender<RuntimeResponse>,
    ) {
        self.inline_responses
            .borrow_mut()
            .insert(request_id, response_tx);
    }

    pub(super) fn respond(&self, request_id: u64, resp: RuntimeResponse) {
        let inline_tx = self.inline_responses.borrow_mut().remove(&request_id);
        if let Some(response_tx) = inline_tx {
            // A failed send means the inline caller already timed out and
            // dropped its receiver. That is the late-response signal for this
            // route, which the router's pending table never sees.
            if response_tx.send(resp).is_err() {
                self.router.record_late_response();
            }
        } else {
            self.router.complete(resp);
        }
    }
}

impl EventLoopDiagnostics {
    pub(super) fn record_wake_batch(&mut self, batch: usize) {
        if self.loop_debug {
            const LOOP_DEBUG_EVERY: u64 = 256;
            self.loop_debug_wakes += 1;
            self.loop_debug_batch_total += batch as u64;

            if self.loop_debug_wakes.is_multiple_of(LOOP_DEBUG_EVERY) {
                let avg_batch = self
                    .loop_debug_batch_total
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0)
                    / self
                        .loop_debug_wakes
                        .to_string()
                        .parse::<f64>()
                        .unwrap_or(1.0);
                eprintln!(
                    "[midge] loop_stats wakes={} avg_batch={:.2}",
                    self.loop_debug_wakes, avg_batch
                );
            }
        }
    }

    pub(super) fn new(trace_enabled: bool) -> Self {
        Self {
            trace_enabled,
            loop_debug: std::env::var_os("MIDGE_LOOP_DEBUG").is_some(),
            loop_debug_wakes: 0,
            loop_debug_batch_total: 0,
        }
    }
}
