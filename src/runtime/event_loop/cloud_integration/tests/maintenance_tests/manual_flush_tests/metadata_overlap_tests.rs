//! Production Background owners sharing one real conditional metadata provider.

use super::*;
use cntryl_midge_durability::lease::PrimaryLease as _;
use cntryl_midge_storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

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

impl cntryl_midge_storage::cloud::CloudReadCapability for MetadataProvider {
    cntryl_midge_storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}
impl cntryl_midge_storage::cloud::CloudWriteCapability for MetadataProvider {
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
                cntryl_midge_storage::cloud::CloudEvent::Put { result: Ok(()), .. }
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
impl cntryl_midge_storage::cloud::CloudDeleteCapability for MetadataProvider {
    cntryl_midge_storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl cntryl_midge_storage::cloud::CloudListCapability for MetadataProvider {
    cntryl_midge_storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl cntryl_midge_storage::cloud::CloudHeadCapability for MetadataProvider {
    cntryl_midge_storage::cloud::forward_cloud_backend!(inner; submit_head);
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
    lease: Arc<cntryl_midge_durability::lease::CloudStorageLease>,
    _guard: cntryl_midge_durability::lease::LeaseGuard,
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
            cntryl_midge_durability::RecoveryPolicy::Strict,
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
        cntryl_midge_durability::metadata::ManifestPersistence::save_with_fs(
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
        let lease = Arc::new(cntryl_midge_durability::lease::CloudStorageLease::new_provider_backed(
            cntryl_midge_durability::lease::CloudLeaseConfig {
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
        Self::from_parts(state, hybrid, provider, metadata, lease, guard)
    }

    fn controlled() -> MidgeResult<Self> {
        // Seed identical encoded SST/control bytes before the latency clock.
        // Keep persistence enabled: MockFs replaces device latency, not the
        // real journal, intent, codec, upload, actor or conditional-CAS paths.
        let Self {
            el: mut original,
            provider,
            metadata,
            lease,
            _guard: guard,
            ..
        } = Self::new()?;
        let deadline = cntryl_midge_core::common::OperationDeadline::from_budget(Duration::from_secs(5));
        original.drain_metadata_mirror_within(&deadline)?;
        let root: Arc<dyn cntryl_midge_core::io::Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
        root.create_dir_all(&cntryl_midge_core::io::FsPath::new("sst"))
            .map_err(FsError::into_midge)?;
        for name in cntryl_midge_durability::metadata::files::CLOUD_MIRRORED {
            let path = cntryl_midge_core::io::FsPath::new(*name);
            match cntryl_midge_core::io::read_all(original.state.fs.as_ref(), &path) {
                Ok(bytes) => write_controlled_file(&root, &path, bytes)?,
                Err(FsError::NotFound(_)) => {}
                Err(error) => return Err(error.into_midge()),
            }
        }
        let data = Arc::new(CloudStorage::with_mock());
        for file in &original.state.manifest.files {
            let bytes = read_remote_sst(&original, &file.name)?.expect("seeded authoritative SST");
            write_controlled_file(
                &root,
                &cntryl_midge_table::sst::fs::sst_key(&file.name),
                bytes.clone().into(),
            )?;
            cntryl_midge_storage::cloud::BlockingCloud::new(&data, &deadline).put_with_headers(
                &format!("sst/{}", file.name),
                bytes,
                Vec::new(),
            )?;
        }
        let state = crate::runtime::RuntimeState::recover(
            std::path::Path::new(""),
            false,
            root,
            cntryl_midge_durability::RecoveryPolicy::Strict,
            crate::runtime::state::RecoveryWal::Replay {
                writer_epoch: lease.epoch(),
            },
            None,
            None,
        )?;
        assert!(
            !state.is_memory_mode(),
            "durable publication paths stay enabled"
        );
        drop(original);
        let hybrid = Arc::new(cntryl_midge_storage::HybridStorage::with_policy(
            data,
            cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        crate::runtime::hybrid_persistence::CloudPersistence::new(Arc::clone(&hybrid))
            .fence_cloud_wal_catalog(lease.epoch())?;
        Self::from_parts(state, hybrid, provider, metadata, lease, guard)
    }

    fn from_parts(
        state: crate::runtime::RuntimeState,
        hybrid: Arc<cntryl_midge_storage::HybridStorage>,
        provider: Arc<MetadataProvider>,
        metadata: Arc<CloudStorage>,
        lease: Arc<cntryl_midge_durability::lease::CloudStorageLease>,
        guard: cntryl_midge_durability::lease::LeaseGuard,
    ) -> MidgeResult<Self> {
        let reads = Arc::new(cntryl_midge_storage::remote_sst::RemoteSstFs::new(
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
        let mut families = vec![0];
        families.extend(
            self.el
                .state
                .manifest
                .column_families
                .iter()
                .filter_map(|family| family.deleted_at.is_none().then_some(family.id)),
        );
        families.sort_unstable();
        families.dedup();
        let deadline = cntryl_midge_core::common::OperationDeadline::from_budget(Duration::from_secs(5));
        for cf_id in families {
            while self
                .el
                .state
                .sst_names
                .reserved_through
                .get(&cf_id)
                .copied()
                .unwrap_or(0)
                <= 1024
            {
                cntryl_midge_core::common::DeadlineScope::new(deadline).check("fixture name coverage")?;
                if !self
                    .el
                    .actors
                    .cloud_coordinator
                    .metadata_mirror
                    .is_running()
                    && !self.el.publication.publication_gate.is_active()
                {
                    if let Err(error) = self
                        .el
                        .reserve_sst_name_durably_within(cf_id, 1024, &deadline)
                    {
                        if !matches!(error, MidgeError::Busy(_))
                            || !self
                                .el
                                .actors
                                .cloud_coordinator
                                .metadata_mirror
                                .is_running()
                        {
                            return Err(error);
                        }
                    }
                }
                self.progress();
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        self.el.drain_metadata_mirror_within(&deadline)?;
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
        self.held_output_names(true)?;
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
                cntryl_midge_durability::RecoveryPolicy::Strict,
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
        let fs = cntryl_midge_core::io::real::test_root(cold.path());
        let store = self.lease.get_leader_store().unwrap();
        crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
            &self.metadata,
            store.as_ref(),
            &fs,
            cold.path(),
            cntryl_midge_durability::RecoveryPolicy::Strict,
        )?;
        let manifest = cntryl_midge_durability::metadata::ManifestPersistence::load_with_fs_and_policy(
            &fs,
            cntryl_midge_durability::RecoveryPolicy::Strict,
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
            cntryl_midge_durability::RecoveryPolicy::Strict,
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
            if let Some(bytes) = read_remote_sst(&self.el, &name)? {
                std::fs::write(cold.path().join("sst").join(&name), bytes)?;
            }
        }
        let mut reopened = crate::Engine::open(
            crate::OpenOptions::local(cold.path())
                .background_compaction(false)
                .build()?,
        )?;
        let recovered = cntryl_midge_durability::metadata::ManifestPersistence::load_with_fs_and_policy(
            &fs,
            cntryl_midge_durability::RecoveryPolicy::Strict,
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

fn write_controlled_file(
    root: &Arc<dyn cntryl_midge_core::io::Fs>,
    path: &cntryl_midge_core::io::FsPath,
    bytes: Bytes,
) -> MidgeResult<()> {
    root.open(
        path,
        cntryl_midge_core::io::OpenOptions {
            mode: cntryl_midge_core::io::OpenMode::ReadWrite,
            create: true,
            create_new: true,
            truncate: false,
        },
    )
    .and_then(|mut file| file.append(bytes))
    .map(|_| ())
    .map_err(FsError::into_midge)
}

fn read_remote_sst(el: &EventLoop, name: &str) -> MidgeResult<Option<Vec<u8>>> {
    let backend = el
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .remote_sst_backend();
    let (sent, received) = std::sync::mpsc::channel();
    backend.submit_metadata_read_request(
        cntryl_midge_storage::StorageRequest::new(
            format!("sst/{name}"),
            cntryl_midge_core::common::OperationDeadline::from_budget(Duration::from_secs(5)),
            Duration::from_secs(5),
        ),
        sent,
    );
    match received.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok((bytes, _))) => Ok(Some(bytes)),
        Ok(Err(error)) if error.is_not_found() => Ok(None),
        actual => Err(MidgeError::Internal(format!(
            "read fixture SST {name}: {actual:?}"
        ))),
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn should_finish_flush_retry_when_earlier_capture_retains_its_failed_manifest_intent(
) -> MidgeResult<()> {
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange: an actual upload succeeds, but its manifest journal append
    // fails. A different admitted metadata owner captures that durable intent.
    let mut fixture = Fixture::new()?;
    let flush_id = fixture.fail_flush_manifest_after_actual_upload()?;
    fixture
        .el
        .drain_metadata_mirror_within(&cntryl_midge_core::common::OperationDeadline::from_budget(
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
        cntryl_midge_core::common::OperationDeadline::from_budget(Duration::from_secs(5)),
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
                &cntryl_midge_core::common::OperationDeadline::from_budget(Duration::from_secs(1)),
            );
        if let Err(error) = capture {
            eprintln!("failed flush retry coverage: {error}");
        }
        let intents = crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
            &fixture.el.state.fs,
            cntryl_midge_durability::RecoveryPolicy::Strict,
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
        .drain_metadata_mirror_within(&cntryl_midge_core::common::OperationDeadline::from_budget(
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

#[test]
fn should_recover_real_files_while_compaction_immutable_put_remains_held() -> MidgeResult<()> {
    verify_real_held_publication(HoldKind::ImmutablePut)
}

#[test]
fn should_recover_real_files_when_successful_compaction_pointer_callback_is_duplicated(
) -> MidgeResult<()> {
    verify_real_held_publication(HoldKind::PointerDuplicate)
}

#[test]
fn should_recover_real_files_when_successful_compaction_pointer_callback_is_lost() -> MidgeResult<()>
{
    verify_real_held_publication(HoldKind::PointerLost)
}

fn verify_held_publication(kind: HoldKind) -> MidgeResult<()> {
    // Setup/recovery I/O of another latency fixture is outside this clock.
    // This fixture's own compactor and flush remain concurrent.
    static LATENCY_FIXTURE: Mutex<()> = Mutex::new(());
    let _fixture = LATENCY_FIXTURE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cntryl_midge_core::failpoints::with_read_gate(|| {
        let evidence = exercise_held_publication(Fixture::controlled()?, kind)?;
        assert!(
            evidence.acknowledged < Duration::from_millis(100),
            "controlled-storage ACK took {:?}; progress observer saw {:?}",
            evidence.acknowledged,
            evidence.observed,
        );
        Ok(())
    })
}

fn verify_real_held_publication(kind: HoldKind) -> MidgeResult<()> {
    cntryl_midge_core::failpoints::with_read_gate(|| {
        exercise_held_publication(Fixture::new()?, kind).map(|_| ())
    })
}

struct PublicationEvidence {
    acknowledged: Duration,
    observed: Duration,
}

struct FlushReceipt {
    received_at: Instant,
    result: Result<RuntimeResponse, crossbeam::channel::RecvTimeoutError>,
}

struct ObservedFlush {
    receipt: FlushReceipt,
    observed_at: Instant,
    work: FlushWork,
}

struct FlushWork {
    build_ns: u64,
    publish_ns: u64,
    retries: u64,
    metadata: cntryl_midge_durability::metadata::accounting::Snapshot,
}

impl FlushWork {
    fn capture(fixture: &Fixture) -> Self {
        let metrics = &fixture.el.state.flush_metrics;
        Self {
            build_ns: metrics.build_ns_total,
            publish_ns: metrics.publish_ns_total,
            retries: metrics.retries_total,
            metadata: fixture.el.state.metadata_accounting().handle().snapshot(),
        }
    }

    fn log_delta(&self, after: &Self) {
        eprintln!(
            "flush worker build={:?}, upload={:?}, retries={}",
            Duration::from_nanos(after.build_ns - self.build_ns),
            Duration::from_nanos(after.publish_ns - self.publish_ns),
            after.retries - self.retries,
        );
        match after.metadata.delta(&self.metadata) {
            Ok(delta) => {
                let counters = &delta
                    .bucket(
                        cntryl_midge_durability::metadata::accounting::Origin::CloudFlush,
                        cntryl_midge_durability::metadata::accounting::Medium::Persistent,
                    )
                    .counters;
                eprintln!(
                    "flush metadata attempt={:?}, full publication={:?}, journal={:?}, checkpoint={:?}, attempts={}, journal appends={}, checkpoints={}",
                    Duration::from_nanos(counters.publication_attempt_elapsed_ns),
                    Duration::from_nanos(counters.flush_full_publication_elapsed_ns),
                    Duration::from_nanos(counters.journal_elapsed_ns),
                    Duration::from_nanos(counters.checkpoint_elapsed_ns),
                    counters.publication_attempts,
                    counters.journal_append_attempts,
                    counters.checkpoint_attempts,
                );
            }
            Err(error) => eprintln!("flush metadata diagnostics unavailable: {error}"),
        }
    }
}

fn exercise_held_publication(
    mut fixture: Fixture,
    kind: HoldKind,
) -> MidgeResult<PublicationEvidence> {
    // Arrange: both flush and compaction use the real Background actors and
    // one conditional metadata provider; only its actual earlier PUT is held.
    fixture.acknowledge_row()?;
    let deferred_before = fixture.el.state.flush_metrics.publication_deferred_by_gate;
    let (release, held_at) = fixture.hold_compaction_publication(kind)?;
    std::thread::scope(|scope| {
        // This owner drops before the scope joins a failed/timed-out receiver.
        let mut release = release;
        let response = fixture.el.routing.router.register(91_951, "FlushMemtable");
        let (ready, armed) = crossbeam::channel::bounded(1);
        let (completed, observed) = crossbeam::channel::bounded(1);
        let watcher = scope.spawn(move || {
            let _ = ready.send(());
            let result = response.recv_timeout(Duration::from_secs(5));
            let received_at = Instant::now();
            let _ = completed.send(FlushReceipt {
                received_at,
                result,
            });
        });
        armed.recv_timeout(Duration::from_secs(5)).unwrap();
        let before_work = FlushWork::capture(&fixture);
        let started = Instant::now();
        fixture.el.handle_runtime_msg(
            RuntimeMsg::FlushMemtable {
                request_id: 91_951,
                cf_id: 0,
            },
            &fixture.messages,
        );
        let flush_id = fixture
            .el
            .state
            .get_cf(0)
            .unwrap()
            .immutable_flushes
            .last()
            .unwrap()
            .flush_id;
        let (flush_result, recovered_before_release) =
            drive_held_publication(&mut fixture, held_at, &observed, flush_id)?;
        release.release();
        finish_released_publication(&mut fixture);
        watcher.join().unwrap();

        // Assert: sample the actual receiver, independently of post-ACK work
        // that the same production background pass may finish before returning.
        let observation = flush_result.expect("later flush commits before earlier PUT release");
        let elapsed = observation.receipt.received_at.duration_since(started);
        let observer_elapsed = observation.observed_at.duration_since(started);
        before_work.log_delta(&observation.work);
        assert!(
            matches!(
                &observation.receipt.result,
                Ok(RuntimeResponse::Ok { request_id: 91_951 })
            ),
            "actual flush response: {:?}",
            observation.receipt.result
        );
        eprintln!("flush ACK={elapsed:?}; progress observer={observer_elapsed:?}");
        assert_settled_publication(&fixture, deferred_before, recovered_before_release)?;
        Ok(PublicationEvidence {
            acknowledged: elapsed,
            observed: observer_elapsed,
        })
    })
}

fn drive_held_publication(
    fixture: &mut Fixture,
    held_at: Instant,
    observed: &crossbeam::channel::Receiver<FlushReceipt>,
    flush_id: u64,
) -> MidgeResult<(Option<ObservedFlush>, bool)> {
    let mut flush_result = None;
    let mut recovered_before_release = false;

    // Act: drive the same production owner dispatch while the earlier provider
    // effect remains outstanding for a full two seconds.
    while held_at.elapsed() < Duration::from_secs(2) {
        fixture.progress();
        if flush_result.is_none() {
            if let Ok(receipt) = observed.try_recv() {
                flush_result = Some(ObservedFlush {
                    receipt,
                    observed_at: Instant::now(),
                    work: FlushWork::capture(fixture),
                });
            }
        }
        if flush_result.is_some() && !recovered_before_release {
            assert!(fixture.el.state.immutable_flush_by_id(flush_id).is_none());
            assert!(!fixture.el.actors.flush_actor.has_pipeline(flush_id));
            fixture.assert_rows()?;
            // Recover the selected cut and all manifest/intent SST sources
            // into an empty cache while the earlier effect remains unresolved.
            fixture.assert_cloud_restart(true)?;
            assert!(fixture.provider.held.load(Ordering::Acquire));
            recovered_before_release = true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok((flush_result, recovered_before_release))
}

fn finish_released_publication(fixture: &mut Fixture) {
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
}

fn assert_settled_publication(
    fixture: &Fixture,
    deferred_before: u64,
    recovered_before_release: bool,
) -> MidgeResult<()> {
    // Assert: stale work cannot install an earlier full snapshot, regress the
    // selected frontier, resurrect settled intents, or lose the later row.
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
        cntryl_midge_durability::RecoveryPolicy::Strict
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
