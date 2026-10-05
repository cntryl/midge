//! Actual early staging and missing-proof fallback share the accepted origin.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

const IO_CAP: Duration = Duration::from_secs(5);

#[derive(Default)]
struct HeldHeadBackend {
    inner: MockCloudBackend,
    hold: parking_lot::Mutex<Option<(String, OperationDeadline)>>,
    held_missing: AtomicBool,
    held_heads: AtomicUsize,
}

impl HeldHeadBackend {
    fn arm(&self, key: String, deadline: OperationDeadline) {
        assert!(self.hold.lock().replace((key, deadline)).is_none());
    }
}

impl CloudBackend for HeldHeadBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get_with_metadata(key, callback);
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: crate::storage::StorageObjectMetadata,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        self.inner
            .submit_get_range_with_identity(key, start, end, expected, timeout, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let hold = {
            let mut hold = self.hold.lock();
            if hold.as_ref().is_some_and(|(held, _)| held == key) {
                hold.take()
            } else {
                None
            }
        };
        let Some((_, deadline)) = hold else {
            self.inner.submit_head(key, callback);
            return;
        };
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.submit_head(key, tx);
        let event = rx.recv_timeout(IO_CAP).expect("actual completed HEAD");
        self.held_missing.store(
            matches!(&event, CloudEvent::Head { key: actual, result: Err(error) }
                if actual == key && error.is_not_found()),
            Ordering::Release,
        );
        self.held_heads.fetch_add(1, Ordering::AcqRel);
        while !deadline.is_expired() {
            std::thread::sleep(Duration::from_millis(5));
        }
        // Forward the exact delegated reply. No error or success is invented.
        let _ = callback.send(event);
    }
}

struct PublicationFixture {
    _directory: tempfile::TempDir,
    directory: PathBuf,
    fs: Arc<dyn crate::io::Fs>,
    backend: Arc<HeldHeadBackend>,
    hybrid: Arc<crate::storage::HybridStorage>,
    outputs: Vec<PreparedCompactionOutput>,
    originals: Vec<Vec<u8>>,
}

impl PublicationFixture {
    fn new(count: u32) -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let backend = Arc::new(HeldHeadBackend::default());
        let cloud: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(CloudStorage::new(backend.clone(), String::new()));
        let (tx, _rx) = crossbeam::channel::unbounded();
        let hybrid = Arc::new(
            crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
                Arc::new(crate::storage::filesystem::FileSystem::new(
                    directory.path().join("local"),
                )?),
                cloud.clone(),
                cloud.clone(),
                cloud,
                tx,
                IO_CAP,
            ),
        );
        hybrid.enable_ephemeral_sst_cache(1024 * 1024);
        hybrid.retire_legacy_local_store();
        let budget = ResourceBudget::new(2 * 1024 * 1024);
        let mut outputs = Vec::new();
        let mut originals = Vec::new();
        for ordinal in 0..count {
            let name = crate::cloud_layout::compaction_file_name(0, 1, 3, ordinal);
            let path = directory.path().join(&name);
            let mut writer = factory.create()?;
            writer.add_with_meta(b"key", Some(b"exact value"), 9, EntryType::Put, None)?;
            crate::sst::fs::finish_writer_to_path(writer, &path)?;
            originals.push(std::fs::read(&path)?);
            outputs.push(stage_local_output_partition(
                &fs,
                0,
                1,
                &name,
                &path,
                CompactionOutputAdmission {
                    budget: &budget,
                    manual_deadline: None,
                },
            )?);
        }
        Ok(Self {
            directory: directory.path().to_path_buf(),
            _directory: directory,
            fs,
            backend,
            hybrid,
            outputs,
            originals,
        })
    }

    fn run_fallback(
        &self,
        manual_deadline: Option<OperationDeadline>,
    ) -> MidgeResult<publication::CompactionPublishCompletion> {
        crate::failpoints::with_read_gate(|| self.run_fallback_in_read_scope(manual_deadline))
    }

    fn run_fallback_in_read_scope(
        &self,
        manual_deadline: Option<OperationDeadline>,
    ) -> MidgeResult<publication::CompactionPublishCompletion> {
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(7, std::time::Instant::now() + Duration::from_secs(60))
            .map_err(|error| error.into_validation_error("fixture validity"))?;
        let (tx, rx) = crossbeam::channel::unbounded();
        let mut actor = publication::CompactionPublishActor::new(tx, false)?;
        actor.submit(publication::CompactionPublishTask {
            token: publication::CompactionPublicationToken {
                request_id: 51,
                writer_epoch: 7,
                cf_id: 0,
                target_level: 1,
                output_generation: 3,
                input_ssts: vec!["accepted-input.sst".into()],
                output_ssts: self
                    .outputs
                    .iter()
                    .map(|output| output.metadata.name.clone())
                    .collect(),
            },
            phase: publication::CompactionPublishPhase::OutputDurable,
            outputs: self.outputs.clone(),
            sst_dir: self.directory.clone(),
            fs: self.fs.clone(),
            hybrid_storage: Some(self.hybrid.clone()),
            cloud_metadata_storage: None,
            metadata_publication_lock: crate::runtime::MetadataPublicationLock::default(),
            lease_healthy: Some(Arc::new(AtomicBool::new(true))),
            lease_validity: Some(validity),
            leader_store: None,
            leader_holder_id: None,
            metadata_sequence: 1,
            publication_memory_limit: 2 * 1024 * 1024,
            runtime_response_timeout: IO_CAP,
            manual_deadline,
        })?;
        let completion = rx.recv_timeout(IO_CAP + IO_CAP);
        // Join the actual owner before asserting success/error/retained bytes.
        actor.shutdown_and_join()?;
        completion.map_err(|error| MidgeError::Internal(format!("fixture completion: {error}")))
    }

    fn assert_retained_bytes(&self) -> MidgeResult<()> {
        for (output, original) in self.outputs.iter().zip(&self.originals) {
            assert_eq!(
                std::fs::read(self.directory.join(&output.metadata.name))?,
                *original,
            );
            assert!(output.proof.is_none(), "genuine missing-proof fallback");
        }
        Ok(())
    }

    fn assert_actual_upload(&self, index: usize) {
        let key = crate::cloud_layout::object_key(&self.outputs[index].metadata.name);
        let (tx, rx) = std::sync::mpsc::channel();
        self.backend.inner.submit_get(&key, tx);
        let event = rx.recv_timeout(IO_CAP).expect("actual uploaded bytes");
        assert!(
            matches!(event, CloudEvent::Get { key: actual, result: Ok(bytes) }
            if actual == key && bytes == self.originals[index])
        );
    }
}

#[test]
fn should_stop_early_output_upload_when_original_deadline_expires_in_head() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: genuine finished SST, original deadline smaller than normal I/O cap.
        let fixture = PublicationFixture::new(1)?;
        let output = &fixture.outputs[0];
        let key = crate::cloud_layout::object_key(&output.metadata.name);
        let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
        fixture.backend.arm(key, deadline);
        let budget = ResourceBudget::new(2 * 1024 * 1024);
        let prepared = PreparedCompactionOutputs::default();

        // Act: actual early worker partition summarization and publication.
        let result = crate::failpoints::with_read_gate(|| {
            record_staged_output_partition(
                Some(fixture.hybrid.as_ref()),
                &fixture.fs,
                &prepared,
                0,
                1,
                &output.metadata.name,
                &fixture.directory.join(&output.metadata.name),
                CompactionOutputAdmission {
                    budget: &budget,
                    manual_deadline: Some(deadline),
                },
            )
        });

        // Assert: real completed missing HEAD, no later PUT or manufactured proof.
        fixture.assert_retained_bytes()?;
        assert!(fixture.backend.held_missing.load(Ordering::Acquire));
        assert_eq!(fixture.backend.held_heads.load(Ordering::Acquire), 1);
        assert!(
            matches!(result, Err(MidgeError::Timeout(_))),
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(
            fixture.backend.inner.get_uploads(),
            [] as [(String, u64); 0]
        );
        assert!(prepared.lock().is_empty());
        assert_eq!(budget.used(), 0);
        Ok(())
    })
}

#[test]
fn should_keep_original_deadline_when_missing_proof_fallback_stages_later_output() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: two actual encoded outputs, neither already carries a cloud proof.
        let fixture = PublicationFixture::new(2)?;
        let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
        fixture.backend.arm(
            crate::cloud_layout::object_key(&fixture.outputs[1].metadata.name),
            deadline,
        );

        // Act: the real publication worker stages first, then holds second actual HEAD.
        let completion = fixture.run_fallback(Some(deadline))?;

        // Assert: first real PUT/readback succeeds; original expiry forbids second PUT.
        fixture.assert_retained_bytes()?;
        fixture.assert_actual_upload(0);
        assert!(fixture.backend.held_missing.load(Ordering::Acquire));
        assert_eq!(fixture.backend.held_heads.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.inner.get_range_downloads().len(), 1);
        assert_eq!(fixture.backend.inner.get_uploads().len(), 1);
        assert!(
            matches!(completion.result, Err(MidgeError::Timeout(_))),
            "actual worker error: {:?}",
            completion.result.as_ref().err()
        );
        Ok(())
    })
}

#[test]
fn should_stage_all_missing_proofs_when_background_origin_has_no_caller_deadline() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: same real fallback fixture, with callerless accepted origin.
        let fixture = PublicationFixture::new(2)?;

        // Act: retain existing ordinary publication caps for background work.
        let completion = fixture.run_fallback(None)?;

        // Assert: actual provider bodies, pinned readbacks and unchanged local bytes.
        fixture.assert_retained_bytes()?;
        fixture.assert_actual_upload(0);
        fixture.assert_actual_upload(1);
        assert_eq!(fixture.backend.inner.get_uploads().len(), 2);
        assert_eq!(fixture.backend.inner.get_range_downloads().len(), 2);
        assert!(
            completion.result.is_ok(),
            "{:?}",
            completion.result.as_ref().err()
        );
        Ok(())
    })
}
