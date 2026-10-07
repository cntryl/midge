//! Genuine compute receipts retain correlation without owning caller routes.

use super::*;
use crate::common::{MidgeError, MidgeResult};
use crate::lease::PrimaryLease as _;
use crate::types::KeyState;

const WAIT: Duration = Duration::from_secs(3);
const MANUAL_ID: u64 = 96_001;

struct NotificationFixture {
    el: EventLoop,
    worker: crossbeam::channel::Receiver<RuntimeMsg>,
    input: String,
    input_bytes: Vec<u8>,
    cloud: bool,
    lease: Option<Arc<crate::lease::CloudStorageLease>>,
}

struct Receipt {
    message: RuntimeMsg,
    notification_id: u64,
    outputs: Vec<String>,
    succeeded: bool,
}

impl NotificationFixture {
    fn new(cloud: bool) -> MidgeResult<Self> {
        let (mut el, worker) = if cloud {
            let (el, worker, _) = cloud_debt(1)?;
            (el, worker)
        } else {
            local_debt()?
        };
        el.state
            .manifest
            .test_mut()
            .files
            .retain(|file| file.cf_id == 0);
        el.state.set_compaction_enabled(false);
        let input = el.state.manifest.files[0].name.clone();
        let input_bytes = std::fs::read(if cloud {
            remote_sst_path_for_test(&el, &input)
        } else {
            el.state.sst_dir.join(&input)
        })?;
        Ok(Self {
            el,
            worker,
            input,
            input_bytes,
            cloud,
            lease: None,
        })
    }

    fn launch(&mut self) -> MidgeResult<()> {
        let plan = self
            .el
            .compaction_actor
            .check_manual_compaction(&self.el.state)?
            .expect("actual seeded SST needs compaction");
        self.el.launch_compaction(plan)
    }

    fn receive(&self) -> Receipt {
        let message = self
            .worker
            .recv_timeout(WAIT)
            .expect("actual compute receipt");
        assert_eq!(message.request_id(), None);
        let RuntimeMsg::CompactionComplete {
            request_id,
            output_ssts,
            succeeded,
            ..
        } = &message
        else {
            panic!("unexpected worker message: {message:?}")
        };
        assert!(
            self.el.router.registered_at(*request_id).is_none(),
            "the actor-generated notification never registered a caller"
        );
        Receipt {
            notification_id: *request_id,
            outputs: output_ssts.clone(),
            succeeded: *succeeded,
            message,
        }
    }

    fn dispatch(&mut self, receipt: Receipt) {
        let (_, messages) = crossbeam::channel::unbounded();
        self.el.handle_runtime_msg(receipt.message, &messages);
    }

    fn register_manual(&mut self) -> crossbeam::channel::Receiver<RuntimeResponse> {
        let response = self.el.router.register(MANUAL_ID, "CompactAll");
        self.el.cloud_coordinator.cloud_maintenance.next =
            crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::Compaction;
        CompactionCoordinator::compact_all(&mut self.el, MANUAL_ID);
        response
    }

    fn assert_input_retained(&self) -> MidgeResult<()> {
        assert!(self.el.state.manifest_has_file(&self.input));
        assert_eq!(
            std::fs::read(if self.cloud {
                remote_sst_path_for_test(&self.el, &self.input)
            } else {
                self.el.state.sst_dir.join(&self.input)
            })?,
            self.input_bytes
        );
        Ok(())
    }

    fn assert_output_row(&self, outputs: &[String]) -> MidgeResult<()> {
        assert_eq!(outputs.len(), 1, "one genuine row produces one output");
        let name = &outputs[0];
        assert!(self.el.state.manifest_has_file(name));
        let path = if self.cloud {
            Path::new("sst").join(name)
        } else {
            PathBuf::from(name)
        };
        let reader = self.el.compaction_actor.open_sst_reader(&path)?;
        let rows = reader.scan_range_raw_state(None, None)?;
        assert_eq!(rows.len(), 1);
        let expected_key: &[u8] = if self.cloud {
            b"prune-candidate"
        } else {
            b"local-debt"
        };
        assert_eq!(rows[0].0.as_ref(), expected_key);
        assert!(
            matches!(&rows[0].1, KeyState::Value(value, 81, None, EntryType::Put)
            if value.as_ref() == b"value")
        );
        Ok(())
    }

    fn assert_settled(&mut self) {
        assert_eq!(self.el.state.active_compactions.load(Ordering::Acquire), 0);
        assert!(self.el.state.pending_compaction_waits.is_empty());
        assert!(self.el.compaction_publication.get().is_none());
        if self.el.publication_gate.is_active() {
            assert!(self.el.publication_gate.is_owned_by(
                &crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune
            ));
            assert!(
                self.el.cloud_coordinator.cloud_wal_prune_worker.is_some()
                    || !self
                        .el
                        .cloud_coordinator
                        .cloud_wal
                        .prune_inflight
                        .is_empty(),
                "the remaining gate must belong to actual accepted WAL prune work"
            );
            drain_prune_completion_for_test(&mut self.el);
        }
        assert!(!self.el.publication_gate.is_active());
        if let Some(storage) = &self.el.cloud_coordinator.hybrid_storage {
            assert_eq!(storage.budget_snapshot().usage.reservations, 0);
        }
    }
}

impl Drop for NotificationFixture {
    fn drop(&mut self) {
        let _ = self.el.compaction_publish_actor.shutdown_and_join();
        let storage = self
            .el
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .map(|storage| {
                Arc::clone(storage)
                    as Arc<dyn crate::runtime::actors::compaction::CompactionStorage>
            });
        self.el
            .compaction_actor
            .cancel_and_join_worker(&mut self.el.state, storage.as_ref());
        self.el.gc_actor.shutdown_workers();
        if let Some(lease) = self.lease.take() {
            let _ = lease.release();
        }
    }
}

fn assert_one_manual_response(
    response: &crossbeam::channel::Receiver<RuntimeResponse>,
) -> RuntimeResponse {
    let result = response
        .recv_timeout(WAIT)
        .expect("registered manual caller response");
    assert_eq!(result.request_id(), MANUAL_ID);
    assert!(
        response.try_recv().is_err(),
        "a caller receives exactly one response"
    );
    result
}

#[test]
fn should_publish_background_compaction_without_routing_its_notification() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let mut fixture = NotificationFixture::new(true)?;
        fixture.launch()?;
        let receipt = fixture.receive();
        assert!(receipt.succeeded);
        let outputs = receipt.outputs.clone();
        let notification_id = receipt.notification_id;

        // Act
        fixture.dispatch(receipt);
        fixture.el.gc_actor.shutdown_workers();

        // Assert: real immutable publication and GC precede the routing check.
        fixture.assert_output_row(&outputs)?;
        assert!(!fixture.el.state.manifest_has_file(&fixture.input));
        assert!(!remote_sst_path_for_test(&fixture.el, &fixture.input).exists());
        fixture.assert_settled();
        assert!(!fixture.el.compaction_fence.is_degraded());
        assert!(fixture.el.router.registered_at(notification_id).is_none());
        assert_eq!(fixture.el.router.pending_len(), 0);
        assert_eq!(fixture.el.router.late_responses_total(), 0);
        Ok(())
    })
}

#[test]
fn should_complete_registered_manual_caller_once_after_real_compaction() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let mut fixture = NotificationFixture::new(false)?;
        let response = fixture.register_manual();
        let receipt = fixture.receive();
        assert!(receipt.succeeded);
        let outputs = receipt.outputs.clone();

        // Act
        fixture.dispatch(receipt);

        // Assert
        fixture.assert_output_row(&outputs)?;
        assert!(matches!(
            assert_one_manual_response(&response),
            RuntimeResponse::Ok { .. }
        ));
        fixture.assert_settled();
        assert_eq!(fixture.el.router.pending_len(), 0);
        assert_eq!(fixture.el.router.late_responses_total(), 0);
        Ok(())
    })
}

#[test]
fn should_fail_manual_caller_once_when_real_compute_detects_corrupt_input() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: damage the real block CRC, retaining every input byte.
        let mut fixture = NotificationFixture::new(false)?;
        let payload = u32::from_le_bytes(fixture.input_bytes[..4].try_into().unwrap());
        let trailer = 4 + usize::try_from(payload).unwrap() - 1;
        assert!(payload >= 4 && trailer < fixture.input_bytes.len());
        fixture.input_bytes[trailer] ^= 0xff;
        std::fs::write(
            fixture.el.state.sst_dir.join(&fixture.input),
            &fixture.input_bytes,
        )?;
        fixture.el.state.manifest.test_mut().files[0].content_crc32c =
            Some(crc32c::crc32c(&fixture.input_bytes));
        let response = fixture.register_manual();
        let receipt = fixture.receive();
        assert!(!receipt.succeeded);
        assert_eq!(receipt.outputs, [] as [String; 0]);

        // Act
        fixture.dispatch(receipt);

        // Assert
        fixture.assert_input_retained()?;
        assert!(matches!(
            assert_one_manual_response(&response),
            RuntimeResponse::Error {
                error: MidgeError::Corruption(_),
                ..
            }
        ));
        assert!(fixture.el.state.intent_log.is_empty());
        fixture.assert_settled();
        assert_eq!(fixture.el.router.late_responses_total(), 0);
        Ok(())
    })
}

#[test]
fn should_retain_inputs_without_routing_notification_when_publication_validation_fails(
) -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: actual compute finishes before its captured identity changes.
        let mut fixture = NotificationFixture::new(true)?;
        fixture.launch()?;
        let receipt = fixture.receive();
        assert!(receipt.succeeded && !receipt.outputs.is_empty());
        let outputs = receipt.outputs.clone();
        fixture.el.state.manifest.test_mut().files[0].size_bytes += 1;
        let response = fixture.register_manual();

        // Act
        fixture.dispatch(receipt);
        fixture.el.gc_actor.shutdown_workers();

        // Assert
        fixture.assert_input_retained()?;
        assert!(outputs
            .iter()
            .all(|name| !fixture.el.state.manifest_has_file(name)));
        assert!(
            matches!(assert_one_manual_response(&response), RuntimeResponse::Error {
            error: MidgeError::Fenced(message), .. }
            if message.contains("input metadata changed"))
        );
        assert!(fixture.el.state.intent_log.is_empty());
        fixture.assert_settled();
        assert_eq!(fixture.el.router.late_responses_total(), 0);
        Ok(())
    })
}

fn mirror_failure_case(fail_first: bool) -> MidgeResult<()> {
    let mut fixture = NotificationFixture::new(true)?;
    let backend = Arc::new(FailThirdIntentPutBackend::new(Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    )));
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend.clone(),
        "notification-mirror".into(),
    ));
    let lease =
        attach_provider_metadata_lease(&mut fixture.el, cloud.clone(), "notification-mirror");
    fixture.lease = Some(lease.clone());
    if fail_first {
        backend.intent_puts.store(2, Ordering::Release);
    }
    let response = fixture.register_manual();
    let receipt = fixture.receive();
    assert!(receipt.succeeded);
    let outputs = receipt.outputs.clone();

    fixture.dispatch(receipt);
    fixture.el.gc_actor.shutdown_workers();
    let result = assert_one_manual_response(&response);
    assert!(
        matches!(
            &result,
            RuntimeResponse::Error {
                error: MidgeError::Internal(_),
                ..
            }
        ),
        "actual metadata worker failure: {result:?}"
    );
    assert_eq!(backend.intent_puts.load(Ordering::Acquire), 3);
    if fail_first {
        fixture.assert_input_retained()?;
        assert!(outputs
            .iter()
            .all(|name| !fixture.el.state.manifest_has_file(name)));
        assert!(fixture
            .el
            .state
            .has_compaction_publication_intent(std::slice::from_ref(&fixture.input), &outputs));
        assert!(outputs
            .iter()
            .all(|name| remote_sst_path_for_test(&fixture.el, name).exists()));
    } else {
        fixture.assert_output_row(&outputs)?;
        assert!(!fixture.el.state.manifest_has_file(&fixture.input));
        assert!(!remote_sst_path_for_test(&fixture.el, &fixture.input).exists());
        assert!(fixture.el.state.intent_log.is_empty());
        let committed = get_committed_cloud_metadata_for_test(
            &cloud,
            &lease,
            crate::metadata::files::INTENT_LOG,
        );
        assert!(
            String::from_utf8(committed).unwrap().contains(&outputs[0]),
            "the failed clear retains the previously committed remote intent"
        );
        assert!(matches!(&result, RuntimeResponse::Error { error, .. }
            if error.to_string().contains("failed to mirror cleared compaction publication intent")));
    }
    fixture.assert_settled();
    assert!(fixture.el.compaction_fence.is_degraded());
    lease.release()?;
    fixture.lease = None;
    assert_eq!(fixture.el.router.pending_len(), 0);
    assert_eq!(fixture.el.router.late_responses_total(), 0);
    Ok(())
}

#[test]
fn should_retain_real_publication_failure_without_routing_worker_notification() -> MidgeResult<()> {
    // Arrange: use the real metadata backend with its first intent upload rejected.
    // Act: dispatch the genuine compute notification and run publication.
    // Assert: retain the input/intent/reservation state and fail the manual route once.
    crate::failpoints::with_read_gate(|| mirror_failure_case(true))
}

#[test]
fn should_settle_real_intent_clear_failure_without_routing_worker_notification() -> MidgeResult<()>
{
    // Arrange: reject the third actual metadata intent upload.
    // Act: publish the real output, install the manifest, then fail its clear mirror.
    // Assert: retain settled authority and fail the manual route once.
    crate::failpoints::with_read_gate(|| mirror_failure_case(false))
}

#[test]
fn should_fail_real_shutdown_caller_without_routing_deferred_compute_notification(
) -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let mut fixture = NotificationFixture::new(false)?;
        let response = fixture.register_manual();
        let receipt = fixture.receive();
        assert!(receipt.succeeded && !receipt.outputs.is_empty());
        let id = receipt.notification_id;
        fixture.el.publication_gate.defer(receipt.message);
        assert_eq!(fixture.el.publication_gate.deferred_messages_len(), 1);

        // Act
        fixture.el.fail_shutdown_held_work();
        fixture
            .el
            .compaction_actor
            .cancel_and_join_worker(&mut fixture.el.state, None);

        // Assert
        fixture.assert_input_retained()?;
        assert!(
            matches!(assert_one_manual_response(&response), RuntimeResponse::Error {
            error: MidgeError::Busy(message), .. } if message == "runtime is shutting down")
        );
        assert!(fixture.el.publication_gate.deferred_messages_is_empty());
        fixture.assert_settled();
        assert!(fixture.el.router.registered_at(id).is_none());
        assert_eq!(fixture.el.router.pending_len(), 0);
        assert_eq!(fixture.el.router.late_responses_total(), 0);
        Ok(())
    })
}

#[test]
fn should_count_real_late_response_when_metrics_caller_has_abandoned_its_route() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let mut fixture = NotificationFixture::new(false)?;
        let id = 96_002;
        let response = fixture.el.router.register(id, "GetRuntimeMetrics");
        let message = RuntimeMsg::GetRuntimeMetrics { request_id: id };
        assert_eq!(message.request_id(), Some(id));
        assert!(fixture.el.router.abandon(id, Duration::ZERO));
        assert!(response.try_recv().is_err());
        let (_, messages) = crossbeam::channel::unbounded();

        // Act
        fixture.el.handle_runtime_msg(message, &messages);

        // Assert
        fixture.assert_input_retained()?;
        assert_eq!(fixture.el.router.pending_len(), 0);
        assert_eq!(fixture.el.router.late_responses_total(), 1);
        Ok(())
    })
}
