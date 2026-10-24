//! Production Background owners sharing one real conditional metadata provider.

use super::*;
use crate::lease::PrimaryLease as _;
use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

struct HeldPut {
    entered: crossbeam::channel::Sender<Instant>,
    release: crossbeam::channel::Receiver<()>,
}

struct MetadataProvider {
    inner: MockCloudBackend,
    next_manifest: Mutex<Option<HeldPut>>,
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
            hold.entered.send(Instant::now()).unwrap();
            // The backend retains this actual immutable PUT and its callback;
            // unrelated writes use the same inner provider concurrently.
            let _ = hold.release.recv_timeout(Duration::from_secs(5));
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

    fn hold_compaction_put(&mut self) -> MidgeResult<(PutRelease, Instant)> {
        let plan = self
            .el
            .actors
            .compaction_actor
            .check_manual_compaction(&self.el.state)?
            .expect("genuine L0 compaction plan");
        let (entered, arrived) = crossbeam::channel::bounded(1);
        let (release, resumed) = crossbeam::channel::bounded(1);
        *self.provider.next_manifest.lock().unwrap() = Some(HeldPut {
            entered,
            release: resumed,
        });
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

    fn assert_cloud_restart(&self) -> MidgeResult<()> {
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
        std::fs::create_dir_all(cold.path().join("sst"))?;
        for file in &manifest.files {
            let source = remote_sst_path_for_test(&self.el, &file.name);
            std::fs::copy(source, cold.path().join("sst").join(&file.name))?;
        }
        let mut reopened = crate::Engine::open(
            crate::OpenOptions::local(cold.path())
                .background_compaction(false)
                .build()?,
        )?;
        let transaction = reopened.begin_tx(0, crate::TransactionMode::ReadOnly)?;
        assert_eq!(transaction.get(MANUAL_KEY)?.as_deref(), Some(MANUAL_VALUE));
        assert_eq!(
            transaction.get(b"prune-candidate")?.as_deref(),
            Some(b"value".as_slice())
        );
        let scan = transaction
            .scan(&crate::Query::new())?
            .collect::<MidgeResult<Vec<_>>>()?;
        assert_eq!(scan.len(), 2);
        drop(transaction);
        reopened.shutdown(Duration::from_secs(5))?;
        Ok(())
    }
}

#[test]
fn should_commit_later_flush_within_hundred_millis_while_compaction_provider_put_waits_two_seconds(
) -> MidgeResult<()> {
    // Arrange: both flush and compaction use the real Background actors and
    // one conditional metadata provider; only its actual earlier PUT is held.
    let mut fixture = Fixture::new()?;
    fixture.acknowledge_row()?;
    let (mut release, held_at) = fixture.hold_compaction_put()?;
    let started = Instant::now();
    let response = submit_manual_flush(&mut fixture.el, 0, 91_951);
    let mut flush_result = None;

    // Act: drive the same production owner dispatch while the earlier provider
    // effect remains outstanding for a full two seconds.
    while held_at.elapsed() < Duration::from_secs(2) {
        fixture.progress();
        if flush_result.is_none() {
            if let Ok(result) = response.try_recv() {
                flush_result = Some((started.elapsed(), result));
            }
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
    fixture.assert_cloud_restart()?;
    Ok(())
}
