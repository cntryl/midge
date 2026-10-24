//! Production Background owners sharing one real conditional metadata provider.

use super::*;
use crate::lease::PrimaryLease as _;
use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

struct HeldPut {
    entered: crossbeam::channel::Sender<Instant>,
    release: crossbeam::channel::Receiver<()>,
}

#[derive(Clone, Copy)]
enum HoldKind {
    ImmutablePut,
    PointerDuplicate,
    PointerLost,
}

struct HeldPointer {
    gate: HeldPut,
    delivery: HoldKind,
}

struct MetadataProvider {
    inner: MockCloudBackend,
    next_manifest: Mutex<Option<HeldPut>>,
    next_pointer: Mutex<Option<HeldPointer>>,
    held: AtomicBool,
}

impl crate::storage::cloud::CloudReadCapability for MetadataProvider {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}
impl crate::storage::cloud::CloudWriteCapability for MetadataProvider {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let hold = key
            .starts_with("manifest/")
            .then(|| self.next_manifest.lock().unwrap().take())
            .flatten();
        if let Some(hold) = hold {
            self.held.store(true, Ordering::Release);
            hold.entered.send(Instant::now()).unwrap();
            // The backend retains this actual immutable PUT and its callback;
            // unrelated writes use the same inner provider concurrently.
            let _ = hold.release.recv_timeout(Duration::from_secs(5));
            self.held.store(false, Ordering::Release);
        }
        let pointer = key
            .ends_with("metadata/committed-pointer.v1")
            .then(|| self.next_pointer.lock().unwrap().take())
            .flatten();
        if let Some(pointer) = pointer {
            let (sent, received) = std::sync::mpsc::channel();
            self.inner.submit_put(key, bytes, headers, sent);
            let actual = received.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(matches!(
                actual,
                crate::storage::cloud::CloudEvent::Put { result: Ok(()), .. }
            ));
            self.held.store(true, Ordering::Release);
            pointer.gate.entered.send(Instant::now()).unwrap();
            // The real conditional mutation has succeeded. Only its genuine
            // callback is delayed/duplicated/lost, never its selected effect.
            let _ = pointer.gate.release.recv_timeout(Duration::from_secs(5));
            self.held.store(false, Ordering::Release);
            if matches!(pointer.delivery, HoldKind::PointerDuplicate) {
                let _ = callback.send(actual.clone());
                let _ = callback.send(actual);
            }
            return;
        }
        self.inner.submit_put(key, bytes, headers, callback);
    }
}
impl crate::storage::cloud::CloudDeleteCapability for MetadataProvider {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl crate::storage::cloud::CloudListCapability for MetadataProvider {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl crate::storage::cloud::CloudHeadCapability for MetadataProvider {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}
impl CloudBackend for MetadataProvider {}

struct PutRelease(Option<crossbeam::channel::Sender<()>>);
impl PutRelease {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
impl Drop for PutRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct Fixture {
    el: EventLoop,
    workers: crossbeam::channel::Receiver<RuntimeMsg>,
    messages: crossbeam::channel::Receiver<RuntimeMsg>,
    provider: Arc<MetadataProvider>,
    metadata: Arc<CloudStorage>,
    lease: Arc<crate::lease::CloudStorageLease>,
    _guard: crate::lease::LeaseGuard,
}

impl Fixture {
    #[cfg(feature = "failpoints")]
    fn fail_flush_manifest_after_actual_upload(&mut self) -> MidgeResult<u64> {
        self.acknowledge_row()?;
        let flush_id = self.el.freeze_active_memtable(0)?.unwrap();
        self.el.schedule_next_flush_worker_during_shutdown();
        let built = self
            .el
            .pipeline
            .flush_worker_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("real flush build completes");
        assert!(matches!(
            built,
            crate::runtime::actors::flush::FlushWorkerResult::Build(_)
        ));
        self.el.handle_flush_worker_result(built);
        let published = self
            .el
            .pipeline
            .flush_worker_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("real SST upload completes");
        assert!(
            matches!(&published, crate::runtime::actors::flush::FlushWorkerResult::Publish(completion)
            if completion.identity.flush_id == flush_id && completion.result.is_ok())
        );
        fail::cfg(
            "midge::manifest::inject_no_space_on_append_edit_batch",
            "return",
        )
        .unwrap();
        self.el.handle_flush_worker_result(published);
        fail::remove("midge::manifest::inject_no_space_on_append_edit_batch");
        self.el.state.retry_metadata_reload()?;
        let intents = crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
            &self.el.state.fs,
            crate::config::RecoveryPolicy::Strict,
        )?;
        assert!(intents.iter().any(|intent| matches!(
            intent,
            crate::runtime::IntentLogEntry::FlushPublish {
                phase: crate::runtime::PublicationPhase::OutputDurable,
                ..
            }
        )));
        assert!(self.el.state.immutable_flush_by_id(flush_id).is_some());
        Ok(flush_id)
    }

    fn new() -> MidgeResult<Self> {
        let (mut original, _, _) = cloud_debt(4)?;
        original
            .state
            .manifest
            .test_mut()
            .files
            .retain(|file| file.cf_id == 0);
        original.state.sequence = test_flush_frontier(&original.state.manifest, 0);
        original.state.applied_sequence = original.state.sequence;
        crate::metadata::ManifestPersistence::save_with_fs(
            &original.state.fs,
            &original.state.manifest,
        )
        .map_err(MidgeError::Internal)?;
        let hybrid = original
            .actors
            .cloud_coordinator
            .hybrid_storage
            .clone()
            .unwrap();
        let provider = Arc::new(MetadataProvider {
            inner: MockCloudBackend::new(),
            next_manifest: Mutex::new(None),
            next_pointer: Mutex::new(None),
            held: AtomicBool::new(false),
        });
        let metadata = Arc::new(CloudStorage::new_with_timeout(
            provider.clone(),
            String::new(),
            Duration::from_secs(5),
        ));
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: "overlap".into(),
            },
            original.state.db_path.clone(),
            Arc::clone(&metadata),
        ));
        let guard = Arc::clone(&lease)
            .try_acquire()
            .map_err(|error| error.into_validation_error("overlap lease"))?;
        let state = std::mem::replace(
            &mut original.state,
            crate::runtime::event_loop::tests::create_test_state(),
        );
        drop(original);
        let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            Arc::clone(&state.fs),
            hybrid.remote_sst_backend(),
            Duration::from_secs(5),
        ));
        let (completed, workers) = crossbeam::channel::unbounded();
        let config = crate::runtime::RuntimeConfig {
            background_compaction: false,
            compaction_concurrency: 2,
            flush_concurrency: 2,
            writer_epoch: lease.epoch(),
            leader_store: lease.get_leader_store(),
            leader_holder_id: Some(lease.holder_id()),
            lease_validity: Some(lease.lease_validity()),
            hybrid_storage: Some(hybrid),
            cloud_metadata_storage: Some(Arc::clone(&metadata)),
            sst_read_fs: Some(reads),
            wal_durability_policy: DurabilityPolicy::CloudAsync,
            runtime_response_timeout: Duration::from_secs(5),
            ..crate::runtime::RuntimeConfig::default()
        };
        let mut el = EventLoop::new(
            state,
            false,
            Arc::new(ResponseRouter::new()),
            config,
            crate::runtime::event_loop::FlushWorkerMode::Background(completed),
        )?;
        el.state.set_compaction_enabled(false);
        let mut fixture = Self {
            el,
            workers,
            messages: crossbeam::channel::never(),
            provider,
            metadata,
            lease,
            _guard: guard,
        };
        fixture.el.mirror_metadata_to_authoritative_cloud()?;
        fixture.reserve_names()?;
        Ok(fixture)
    }

    fn reserve_names(&mut self) -> MidgeResult<()> {
        // Actual ahead-use reservation settles before the timed publication;
        // this regression measures contention between admitted control cuts.
        if let Err(error) = self.el.reserve_sst_name_durably(0, 1024) {
            if !matches!(error, MidgeError::Busy(_)) {
                return Err(error);
            }
        }
        let until = Instant::now() + Duration::from_secs(5);
        while self
            .el
            .state
            .sst_names
            .reserved_through
            .get(&0)
            .copied()
            .unwrap_or(0)
            <= 1024
        {
            self.progress();
            assert!(
                Instant::now() < until,
                "actual name reservation must commit"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    fn progress(&mut self) {
        while let Ok(message) = self.workers.try_recv() {
            self.el.process_one(message, &self.messages);
        }
        self.el.background_progress(Some(&self.messages));
    }

    fn acknowledge_row(&mut self) -> MidgeResult<()> {
        let response = self.el.routing.router.register(91_950, "ApplyTransaction");
        self.el.handle_runtime_msg(
            RuntimeMsg::ApplyTransaction {
                request_id: 91_950,
                ops: vec![crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(MANUAL_KEY),
                    value: Bytes::from_static(MANUAL_VALUE),
                    ttl_seconds: None,
                    insert_only: false,
                }],
                assertions: Vec::new(),
                durability_policy: Some(DurabilityPolicy::CloudAsync),
                start_sequence: None,
                conflict_policy: ConflictPolicy::LastWriteWins,
                response_tx: None,
            },
            &self.messages,
        );
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            self.progress();
            if let Ok(result) = response.try_recv() {
                return match result {
                    RuntimeResponse::TransactionApplied { op_count: 1, .. } => Ok(()),
                    actual => Err(MidgeError::Internal(format!(
                        "actual row acknowledgement: {actual:?}"
                    ))),
                };
            }
            assert!(
                Instant::now() < until,
                "actual transaction must acknowledge before the provider hold"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn hold_compaction_publication(
        &mut self,
        kind: HoldKind,
    ) -> MidgeResult<(PutRelease, Instant)> {
        let plan = self
            .el
            .actors
            .compaction_actor
            .check_manual_compaction(&self.el.state)?
            .expect("genuine L0 compaction plan");
        let (entered, arrived) = crossbeam::channel::bounded(1);
        let (release, resumed) = crossbeam::channel::bounded(1);
        let gate = HeldPut {
            entered,
            release: resumed,
        };
        match kind {
            HoldKind::ImmutablePut => *self.provider.next_manifest.lock().unwrap() = Some(gate),
            HoldKind::PointerDuplicate | HoldKind::PointerLost => {
                *self.provider.next_pointer.lock().unwrap() = Some(HeldPointer {
                    gate,
                    delivery: kind,
                });
            }
        }
        self.el.launch_compaction(plan)?;
        let until = Instant::now() + Duration::from_secs(3);
        let held_at = loop {
            self.progress();
            if let Ok(held_at) = arrived.try_recv() {
                break held_at;
            }
            assert!(
                Instant::now() < until,
                "real compaction immutable PUT reached provider"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        Ok((PutRelease(Some(release)), held_at))
    }

    fn assert_rows(&self) -> MidgeResult<()> {
        let snapshot = self
            .el
            .create_read_snapshot(0)
            .expect("readable default family");
        let expected = [
            (MANUAL_KEY.to_vec(), MANUAL_VALUE.to_vec()),
            (b"prune-candidate".to_vec(), b"value".to_vec()),
        ];
        for (key, value) in &expected {
            assert_eq!(snapshot.get(key, u64::MAX)?.as_ref(), Some(value));
        }
        assert_eq!(snapshot.range_scan(b"", b"", u64::MAX)?, expected);
        Ok(())
    }

    fn held_output_names(
        &self,
        pending_compaction: bool,
    ) -> MidgeResult<std::collections::BTreeSet<String>> {
        let names = if pending_compaction {
            crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
                &self.el.state.fs,
                crate::config::RecoveryPolicy::Strict,
            )?
            .into_iter()
            .filter_map(|intent| match intent {
                crate::runtime::IntentLogEntry::CompactionPublish {
                    phase: crate::runtime::PublicationPhase::OutputDurable,
                    added,
                    ..
                } => Some(added),
                _ => None,
            })
            .flatten()
            .map(|file| file.name)
            .collect::<std::collections::BTreeSet<_>>()
        } else {
            std::collections::BTreeSet::new()
        };
        if pending_compaction {
            assert!(
                !names.is_empty(),
                "held compaction outputs must have a durable intent"
            );
        }
        Ok(names)
    }

    fn assert_cloud_restart(&self, pending_compaction: bool) -> MidgeResult<()> {
        let held_outputs = self.held_output_names(pending_compaction)?;
        let cold = tempfile::tempdir()?;
        let fs = crate::io::real::test_root(cold.path());
        let store = self.lease.get_leader_store().unwrap();
        crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
            &self.metadata,
            store.as_ref(),
            &fs,
            cold.path(),
            crate::config::RecoveryPolicy::Strict,
        )?;
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .map_err(MidgeError::Internal)?;
        assert!(
            manifest
                .files
                .iter()
                .all(|file| !held_outputs.contains(&file.name)),
            "unselected compaction outputs cannot become authoritative during hydration"
        );
        let intents = crate::runtime::IntentPersistence::load_with_fs_and_policy(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .map_err(MidgeError::Internal)?;
        assert_eq!(
            intents.iter().any(|intent| matches!(
                intent,
                crate::runtime::IntentLogEntry::CompactionPublish { .. }
            )),
            pending_compaction
        );
        let mut sources = manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for intent in &intents {
            crate::runtime::hybrid_persistence::visit_intent_sources(intent, |name| {
                sources.insert(name.to_string());
            });
        }
        std::fs::create_dir_all(cold.path().join("sst"))?;
        for name in sources {
            let source = remote_sst_path_for_test(&self.el, &name);
            if source.exists() {
                std::fs::copy(source, cold.path().join("sst").join(&name))?;
            }
        }
        let mut reopened = crate::Engine::open(
            crate::OpenOptions::local(cold.path())
                .background_compaction(false)
                .build()?,
        )?;
        let recovered = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .map_err(MidgeError::Internal)?;
        assert!(
            recovered
                .files
                .iter()
                .all(|file| !held_outputs.contains(&file.name)),
            "restart cannot admit held compaction's unselected output names"
        );
        let transaction = reopened.begin_tx(0, crate::TransactionMode::ReadOnly)?;
        assert_eq!(transaction.get(MANUAL_KEY)?.as_deref(), Some(MANUAL_VALUE));
        assert_eq!(
            transaction.get(b"prune-candidate")?.as_deref(),
            Some(b"value".as_slice())
        );
        let scan = transaction
            .scan(&crate::Query::new())?
            .collect::<MidgeResult<Vec<_>>>()?;
        let actual = scan
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [
                (MANUAL_KEY.to_vec(), MANUAL_VALUE.to_vec()),
                (b"prune-candidate".to_vec(), b"value".to_vec())
            ]
        );
        drop(transaction);
        reopened.shutdown(Duration::from_secs(5))?;
        Ok(())
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn should_finish_flush_retry_when_earlier_capture_retains_its_failed_manifest_intent(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange: an actual upload succeeds, but its manifest journal append
    // fails. A different admitted metadata owner captures that durable intent.
    let mut fixture = Fixture::new()?;
    let flush_id = fixture.fail_flush_manifest_after_actual_upload()?;
    fixture
        .el
        .drain_metadata_mirror_within(&crate::common::OperationDeadline::from_budget(
            Duration::from_secs(5),
        ))?;
    let (entered, arrived) = crossbeam::channel::bounded(1);
    let (release, resumed) = crossbeam::channel::bounded(1);
    *fixture.provider.next_manifest.lock().unwrap() = Some(HeldPut {
        entered,
        release: resumed,
    });
    let mut held = PutRelease(Some(release));
    fixture.el.start_metadata_mirror(
        crate::runtime::event_loop::metadata_mirror::MirrorContinuation::Auxiliary {
            context: "captured failed flush publication".into(),
        },
        crate::common::OperationDeadline::from_budget(Duration::from_secs(5)),
    )?;
    arrived.recv_timeout(Duration::from_secs(2)).unwrap();

    // Act: the same accepted immutable retries through production build,
    // upload, journal and metadata workers while the older cut remains owned.
    let until = Instant::now() + Duration::from_secs(2);
    while fixture.el.state.immutable_flush_by_id(flush_id).is_some() && Instant::now() < until {
        fixture.el.schedule_next_flush_worker_during_shutdown();
        fixture.el.drain_flush_worker_results();
        fixture.el.retry_flush_mirrors_if_due();
        std::thread::sleep(Duration::from_millis(1));
    }

    // Assert: selected terminal authority settles the flush without dropping
    // the earlier source or weakening its exact intent-removal requirement.
    assert!(fixture.provider.held.load(Ordering::Acquire));
    if fixture.el.state.immutable_flush_by_id(flush_id).is_some() {
        let capture = fixture
            .el
            .publication
            .metadata_publication_lock
            .capture_control_cut(
                &fixture.el.state.fs,
                fixture.el.state.writer_epoch,
                &crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
            );
        if let Err(error) = capture {
            eprintln!("failed flush retry coverage: {error}");
        }
        let intents = crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
            &fixture.el.state.fs,
            crate::config::RecoveryPolicy::Strict,
        )?;
        assert!(
            !intents.iter().any(|intent| matches!(
                intent,
                crate::runtime::IntentLogEntry::FlushPublish { .. }
            )),
            "the retry actually cleared its original durable flush intent"
        );
    }
    assert!(
        fixture.el.state.immutable_flush_by_id(flush_id).is_none(),
        "real flush retry must finish while its earlier intent capture remains owned"
    );
    fixture.assert_rows()?;
    fixture.assert_cloud_restart(false)?;
    held.release();
    fixture
        .el
        .drain_metadata_mirror_within(&crate::common::OperationDeadline::from_budget(
            Duration::from_secs(5),
        ))?;
    fixture.assert_rows()?;
    Ok(())
}

#[test]
fn should_commit_later_flush_within_hundred_millis_while_compaction_provider_put_waits_two_seconds(
) -> MidgeResult<()> {
    verify_held_publication(HoldKind::ImmutablePut)
}

#[test]
fn should_preserve_later_flush_when_successful_earlier_pointer_cas_callback_is_late_and_duplicated(
) -> MidgeResult<()> {
    verify_held_publication(HoldKind::PointerDuplicate)
}

#[test]
fn should_preserve_later_flush_when_successful_earlier_pointer_cas_callback_is_lost(
) -> MidgeResult<()> {
    verify_held_publication(HoldKind::PointerLost)
}

fn verify_held_publication(kind: HoldKind) -> MidgeResult<()> {
    // Arrange: both flush and compaction use the real Background actors and
    // one conditional metadata provider; only its actual earlier PUT is held.
    let mut fixture = Fixture::new()?;
    fixture.acknowledge_row()?;
    let deferred_before = fixture.el.state.flush_metrics.publication_deferred_by_gate;
    let (mut release, held_at) = fixture.hold_compaction_publication(kind)?;
    let started = Instant::now();
    let response = submit_manual_flush(&mut fixture.el, 0, 91_951);
    let mut flush_result = None;
    let mut recovered_before_release = false;

    // Act: drive the same production owner dispatch while the earlier provider
    // effect remains outstanding for a full two seconds.
    while held_at.elapsed() < Duration::from_secs(2) {
        fixture.progress();
        if flush_result.is_none() {
            if let Ok(result) = response.try_recv() {
                flush_result = Some((started.elapsed(), result));
            }
        }
        if flush_result.is_some() && !recovered_before_release {
            fixture.assert_rows()?;
            // Recover the selected cut and all manifest/intent SST sources
            // into an empty cache while the earlier effect remains unresolved.
            fixture.assert_cloud_restart(true)?;
            assert!(fixture.provider.held.load(Ordering::Acquire));
            recovered_before_release = true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    release.release();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        fixture.progress();
        if fixture.el.state.active_compactions.load(Ordering::Acquire) == 0
            && !fixture.el.actors.flush_actor.is_inflight()
            && !fixture.el.publication.compaction_publication.is_active()
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "released real owners must finish without resetting flush build"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // Assert: stale work cannot install an earlier full snapshot, regress the
    // selected frontier, resurrect settled intents, or lose the later row.
    let (elapsed, result) = flush_result.expect("later flush commits before earlier PUT release");
    assert!(
        elapsed < Duration::from_millis(100),
        "later flush took {elapsed:?} while compaction provider was held"
    );
    assert!(matches!(result, RuntimeResponse::Ok { request_id: 91_951 }));
    assert_eq!(
        fixture.el.state.flush_metrics.publication_deferred_by_gate - deferred_before,
        0,
        "independent Background publication must not defer the flush behind provider work"
    );
    assert!(
        recovered_before_release,
        "selected flush must survive cold/cache-loss recovery before earlier PUT resolves"
    );
    assert_eq!(
        fixture
            .el
            .publication
            .metadata_publication_lock
            .pending_captures(),
        0
    );
    assert!(!crate::runtime::IntentPersistence::load_with_fs_and_policy(
        &fixture.el.state.fs,
        crate::config::RecoveryPolicy::Strict
    )
    .unwrap()
    .iter()
    .any(|intent| matches!(
        intent,
        crate::runtime::IntentLogEntry::FlushPublish { .. }
            | crate::runtime::IntentLogEntry::CompactionPublish { .. }
    )));
    fixture.assert_rows()?;
    fixture.assert_cloud_restart(false)?;
    Ok(())
}
