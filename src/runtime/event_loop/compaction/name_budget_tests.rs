//! Actual name reservation through conditional provider metadata authority.
//!
//! The compatibility backend forwards real `MockCloudBackend` bytes and `ETags`
//! after a finite synchronous hold. These controls prove inherited admission
//! accounting; they do not establish native socket cancellation or public ACKs.

use super::EventLoop;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::{CloudMetadataHead, LeaseGuard, PrimaryLease as _};
use crate::runtime::event_loop::tests::create_test_cloud_event_loop;
use crate::runtime::RuntimeState;
use crate::sst::SstFactory as _;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use crate::types::{EntryType, KeyState};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ORIGINAL_BUDGET: Duration = Duration::from_secs(2);
const LATE_SUCCESS: Duration = Duration::from_millis(2500);
const PROVIDER_CAP: Duration = Duration::from_secs(4);
const SETUP_BUDGET: Duration = Duration::from_secs(8);
const RESERVED_GENERATION: u64 = 64;
const RESERVED_THROUGH: u64 = RESERVED_GENERATION + 16;
const KEY: &[u8] = b"name-budget-row";
const VALUE: &[u8] = b"unchanged-real-sst-value";

type DataEvidence = (Vec<u8>, Vec<(Bytes, KeyState)>);

#[derive(Clone, Debug)]
struct ReadEvidence {
    key: String,
    genuine_success: bool,
    provider_budget: Option<Duration>,
    delegated_at: Instant,
    forwarded_at: Instant,
    uploads_at_delegation: Vec<(String, u64)>,
}

#[derive(Clone, Debug)]
struct PutEvidence {
    key: String,
    submitted_at: Instant,
    completed_at: Instant,
    genuine_success: bool,
}

#[derive(Default)]
struct DelayedAuthorityBackend {
    inner: MockCloudBackend,
    hold_until: Mutex<Option<Instant>>,
    release: AtomicBool,
    reads: Mutex<Vec<ReadEvidence>>,
    puts: Mutex<Vec<PutEvidence>>,
}

impl DelayedAuthorityBackend {
    fn metadata_get(&self, key: &str, timeout: Option<Duration>, callback: &CloudCallback) {
        let (observed, result) = std::sync::mpsc::channel();
        self.inner.submit_get_with_metadata(key, observed);
        let event = result
            .recv_timeout(PROVIDER_CAP)
            .expect("receive actual delegated authority GET");
        let hold_until = self.hold_until.lock().unwrap().take();
        if let Some(until) = hold_until {
            let delegated_at = Instant::now();
            let uploads_at_delegation = self.inner.get_uploads();
            let genuine_success = matches!(&event,
                CloudEvent::GetWithMetadata { key: actual, result: Ok((bytes, metadata)) }
                if actual == key && !bytes.is_empty() && !metadata.etag.is_empty()
                    && metadata.size == u64::try_from(bytes.len()).unwrap());
            // The immutable finite target is the safety release even if the
            // observer never reaches its explicit cleanup path.
            while Instant::now() < until && !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.reads.lock().unwrap().push(ReadEvidence {
                key: key.to_string(),
                genuine_success,
                provider_budget: timeout,
                delegated_at,
                forwarded_at: Instant::now(),
                uploads_at_delegation,
            });
        }
        let _ = callback.send(event);
    }
}

impl CloudBackend for DelayedAuthorityBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.metadata_get(key, None, &callback);
    }

    fn submit_get_with_metadata_with_timeout(
        &self,
        key: &str,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        self.metadata_get(key, Some(timeout), &callback);
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let submitted_at = Instant::now();
        let (observed, result) = std::sync::mpsc::channel();
        self.inner.submit_put(key, bytes, headers, observed);
        let event = result
            .recv_timeout(PROVIDER_CAP)
            .expect("receive actual delegated conditional PUT");
        self.puts.lock().unwrap().push(PutEvidence {
            key: key.to_string(),
            submitted_at,
            completed_at: Instant::now(),
            genuine_success: matches!(&event,
                CloudEvent::Put { key: actual, result: crate::storage::cloud::CloudOutcome::Ok(()) }
                if actual == key),
        });
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

struct NameFixture {
    el: EventLoop,
    cloud: Arc<CloudStorage>,
    backend: Arc<DelayedAuthorityBackend>,
    lease: Arc<crate::lease::CloudStorageLease>,
    _guard: LeaseGuard,
    sst_name: String,
    sst_bytes: Vec<u8>,
    stopped: bool,
}

fn seed_actual_sst(el: &mut EventLoop) -> MidgeResult<(String, Vec<u8>)> {
    let name = crate::cloud_layout::file_name(0, 0, 1);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&el.state.fs), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(KEY, Some(VALUE), 7, EntryType::Put, None)?;
    let path = el.state.sst_dir.join(&name);
    writer.finish_to_path(&path)?;
    let bytes = std::fs::read(path)?;
    let manifest = el.state.manifest.test_mut();
    manifest.files.push(crate::metadata::FileMeta {
        name: name.clone(),
        level: 0,
        cf_id: 0,
        size_bytes: u64::try_from(bytes.len()).unwrap(),
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        smallest_key: Some(KEY.to_vec()),
        largest_key: Some(KEY.to_vec()),
        smallest_seq: Some(7),
        largest_seq: Some(7),
        key_bounds_complete: true,
        ..Default::default()
    });
    manifest.next_sst_seqs.insert(0, 2);
    manifest.last_persisted_sequence = 7;
    el.state.sequence = 7;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(MidgeError::Internal)?;
    Ok((name, bytes))
}

impl NameFixture {
    fn new() -> MidgeResult<Self> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        el.state.set_compaction_enabled(false);
        let (sst_name, sst_bytes) = seed_actual_sst(&mut el)?;
        let backend = Arc::new(DelayedAuthorityBackend::default());
        let cloud = Arc::new(CloudStorage::new_with_timeout(
            backend.clone(),
            String::new(),
            PROVIDER_CAP,
        ));
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: "name-budget".into(),
            },
            el.state.db_path.clone(),
            Arc::clone(&cloud),
        ));
        let guard = Arc::clone(&lease)
            .try_acquire()
            .map_err(|error| error.into_validation_error("name fixture lease acquisition"))?;
        el.fencing.writer_epoch = lease.epoch();
        el.fencing.leader_holder_id = Some(lease.holder_id());
        el.fencing.leader_store = lease.get_leader_store();
        el.fencing.lease_validity = Some(lease.lease_validity());
        el.fencing.lease_healthy = Some(Arc::new(AtomicBool::new(true)));
        el.cloud_coordinator.cloud_metadata_storage = Some(Arc::clone(&cloud));
        let fixture = Self {
            el,
            cloud,
            backend,
            lease,
            _guard: guard,
            sst_name,
            sst_bytes,
            stopped: false,
        };
        fixture.el.mirror_metadata_to_authoritative_cloud_within(
            &OperationDeadline::from_budget(SETUP_BUDGET),
        )?;
        Ok(fixture)
    }

    fn committed_head(&self) -> MidgeResult<CloudMetadataHead> {
        self.lease
            .get_leader_store()
            .expect("real metadata authority")
            .read_committed_metadata(PROVIDER_CAP)
            .map_err(|error| error.into_validation_error("name fixture committed pointer"))
    }

    fn committed_manifest(&self) -> MidgeResult<crate::metadata::Manifest> {
        let directory = tempfile::tempdir()?;
        let store = self
            .lease
            .get_leader_store()
            .expect("real metadata authority");
        crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
            &self.cloud,
            store.as_ref(),
            directory.path(),
            crate::config::RecoveryPolicy::Strict,
        )?;
        crate::metadata::ManifestPersistence::load(directory.path()).map_err(MidgeError::Internal)
    }

    fn exact_data(&self) -> MidgeResult<DataEvidence> {
        let bytes = std::fs::read(self.el.state.sst_dir.join(&self.sst_name))?;
        let reader = self
            .el
            .compaction_actor
            // The actor factory's Fs is rooted at sst_dir. Reader paths are
            // FsPath addresses; only the writer accepts a host destination.
            .open_sst_reader(std::path::Path::new(&self.sst_name))?;
        Ok((bytes, reader.scan_range_raw_state(None, None)?))
    }

    fn stop(&mut self) -> MidgeResult<()> {
        if self.stopped {
            return Ok(());
        }
        self.backend.release.store(true, Ordering::Release);
        self.el.compaction_publish_actor.shutdown_and_join()?;
        self.el.gc_actor.shutdown_workers();
        self.el.join_cloud_wal_prune_worker();
        self.stopped = true;
        self.lease
            .release()
            .map_err(|error| error.into_validation_error("name fixture conditional release"))
    }
}

impl Drop for NameFixture {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn assert_exact_fixture_data(actual: MidgeResult<DataEvidence>, expected: &[u8]) {
    let (bytes, rows) = actual.expect("read exact genuine SST data");
    assert_eq!(bytes, expected);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref(), KEY);
    assert!(
        matches!(&rows[0].1, KeyState::Value(value, 7, None, EntryType::Put)
        if value.as_ref() == VALUE)
    );
}

fn assert_late_genuine_read(reads: &[ReadEvidence], started: Instant) {
    assert_eq!(reads.len(), 1, "actual delegated hold: {reads:?}");
    let read = &reads[0];
    assert!(read.genuine_success && !read.key.is_empty(), "{read:?}");
    assert!(read
        .provider_budget
        .is_some_and(|budget| budget <= ORIGINAL_BUDGET));
    assert!(read.forwarded_at >= started + LATE_SUCCESS);
    assert!(read.forwarded_at.duration_since(read.delegated_at) < PROVIDER_CAP);
}

#[test]
fn should_keep_name_reservation_uncovered_when_actual_metadata_read_outlives_original_budget(
) -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: real FORMAT/manifest and conditional committed pointer precede time.
        let mut fixture = NameFixture::new()?;
        let before_head = fixture.committed_head()?;
        let before_manifest = fixture.committed_manifest()?;
        let before_uploads = fixture.backend.inner.get_uploads();
        let before_puts = fixture.backend.puts.lock().unwrap().len();
        let baseline_data = fixture.exact_data()?;
        let before_bound = fixture.el.state.sst_names.reserved_through.get(&0).copied();
        let started = Instant::now();
        let deadline = OperationDeadline::from_start(started, ORIGINAL_BUDGET);
        *fixture.backend.hold_until.lock().unwrap() = Some(started + LATE_SUCCESS);

        // Act: forward a genuine successful authority response after caller expiry.
        let result = fixture
            .el
            .reserve_sst_name_durably_within(0, RESERVED_GENERATION, &deadline);
        let elapsed = started.elapsed();
        fixture.backend.release.store(true, Ordering::Release);
        let reads = fixture.backend.reads.lock().unwrap().clone();
        let after_head = fixture.committed_head();
        let after_manifest = fixture.committed_manifest();
        let after_uploads = fixture.backend.inner.get_uploads();
        let puts = fixture.backend.puts.lock().unwrap()[before_puts..].to_vec();
        let bound = fixture.el.state.sst_names.reserved_through.get(&0).copied();
        let local = crate::metadata::ManifestPersistence::load(&fixture.el.state.db_path);
        let mut recovered = RuntimeState::try_new(
            fixture.el.state.db_path.clone(),
            false,
            crate::config::RecoveryPolicy::Strict,
        )?;
        let next_generation = recovered.next_compaction_output_generation();
        let lease_is_live = fixture.el.check_lease_health();
        let accepted_epoch = fixture.lease.epoch();
        let store = fixture.lease.get_leader_store().unwrap();
        let owner = store.read_current();
        let data = fixture.exact_data();
        let cleanup = fixture.stop();

        // Assert: local journal conserves the name, but it has no mirrored coverage.
        assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
        assert!(
            elapsed >= LATE_SUCCESS && elapsed < SETUP_BUDGET,
            "{elapsed:?}"
        );
        assert_late_genuine_read(&reads, started);
        assert_eq!(bound, before_bound);
        assert_eq!(
            fixture.el.state.manifest.next_sst_seqs[&0],
            RESERVED_THROUGH
        );
        assert_eq!(
            local
                .expect("replay actual journaled reservation")
                .next_sst_seqs[&0],
            RESERVED_THROUGH
        );
        assert!(
            next_generation.expect("recovered allocator retains reservation") > RESERVED_THROUGH
        );
        assert_eq!(
            after_head.expect("read unchanged authoritative pointer"),
            before_head
        );
        assert_eq!(
            after_manifest
                .expect("hydrate unchanged real committed manifest")
                .next_sst_seqs,
            before_manifest.next_sst_seqs
        );
        assert_journal_staging_before_expiry(
            &fixture,
            &before_uploads,
            &after_uploads,
            &puts,
            &reads[0],
            started,
        )?;
        let owner = owner
            .expect("read actual owner")
            .expect("lease remains present");
        assert_eq!(owner.holder_id, fixture.lease.holder_id());
        assert_eq!(owner.epoch, accepted_epoch);
        lease_is_live.expect("original lease remains healthy after caller timeout");
        assert_eq!(baseline_data.0, fixture.sst_bytes);
        assert_exact_fixture_data(data, &baseline_data.0);
        cleanup.expect("release actual owned lease after cleanup joins");
        Ok(())
    })
}

fn assert_journal_staging_before_expiry(
    fixture: &NameFixture,
    before_uploads: &[(String, u64)],
    after_uploads: &[(String, u64)],
    puts: &[PutEvidence],
    read: &ReadEvidence,
    started: Instant,
) -> MidgeResult<()> {
    assert_eq!(
        after_uploads,
        read.uploads_at_delegation.as_slice(),
        "late authority response must admit no further upload or lease CAS"
    );
    assert_eq!(&after_uploads[..before_uploads.len()], before_uploads);
    let staged = &after_uploads[before_uploads.len()..];
    assert_eq!(
        staged.len(),
        1,
        "only the changed journal is staged: {staged:?}"
    );
    assert!(staged[0].0.starts_with("metadata/generations/"));
    assert!(staged[0].0.ends_with("/manifest.journal"));
    assert_eq!(
        staged[0].1,
        std::fs::metadata(
            fixture
                .el
                .state
                .db_path
                .join(crate::metadata::files::JOURNAL)
        )?
        .len()
    );
    assert_eq!(
        puts.len(),
        1,
        "only one genuine pre-expiry journal PUT: {puts:?}"
    );
    assert_eq!(puts[0].key, staged[0].0);
    assert!(puts[0].genuine_success, "{puts:?}");
    assert!(puts[0].submitted_at >= started);
    assert!(puts[0].completed_at < started + ORIGINAL_BUDGET, "{puts:?}");
    Ok(())
}

#[test]
fn should_cover_reserved_name_when_real_metadata_mirror_fits_original_budget() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let mut fixture = NameFixture::new()?;
        let before = fixture.committed_head()?;
        let deadline = OperationDeadline::from_budget(ORIGINAL_BUDGET);

        // Act
        let result = fixture
            .el
            .reserve_sst_name_durably_within(0, RESERVED_GENERATION, &deadline);
        let covered = fixture.el.state.sst_names.reserved_through.get(&0).copied();
        let head = fixture.committed_head();
        let manifest = fixture.committed_manifest();
        let local = crate::metadata::ManifestPersistence::load(&fixture.el.state.db_path);
        let data = fixture.exact_data();
        let healthy = fixture.el.check_lease_health();
        let cleanup = fixture.stop();

        // Assert
        result.expect("actual conditional metadata mirror succeeds within one budget");
        assert_eq!(covered, Some(RESERVED_THROUGH));
        let head = head.expect("read genuine newly committed pointer");
        assert!(matches!(head, CloudMetadataHead::Committed(_)));
        assert_ne!(head, before);
        let manifest = manifest.expect("hydrate real newly committed manifest and journal");
        assert_eq!(manifest.next_sst_seqs[&0], RESERVED_THROUGH);
        assert_eq!(
            local.expect("read actual local reservation").next_sst_seqs,
            manifest.next_sst_seqs
        );
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].name, fixture.sst_name);
        assert_exact_fixture_data(data, &fixture.sst_bytes);
        healthy.expect("live provider-backed ownership survives healthy mirror");
        cleanup.expect("release owned lease after actual fixture workers join");
        Ok(())
    })
}
