//! Corrupt streaming uploads must fail before compaction reaches manifest authority.

use super::EventLoop;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{ResponseRouter, RuntimeConfig, RuntimeState};
use crate::sst::{FsSstFactoryIo, SstFactory as _};
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudHeadCapability, CloudReadCapability,
    CloudStorage, CloudWriteCapability, MockCloudBackend,
};
use crate::storage::{StorageFileUpload, StorageWriteReceipt};
use crate::types::EntryType;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

const LARGE_VALUE_BYTES: usize = 32 * 1024;
const LARGE_ROW_COUNT: usize = 350;
const UPLOAD_WAIT: Duration = Duration::from_secs(10);
const SETTLE_WAIT: Duration = Duration::from_secs(30);

struct CorruptingMultipartBackend {
    inner: MockCloudBackend,
    corrupted_parts: AtomicUsize,
    uploaded: parking_lot::Mutex<Vec<(String, usize)>>,
}

impl CorruptingMultipartBackend {
    fn new() -> Self {
        Self {
            inner: MockCloudBackend::new(),
            corrupted_parts: AtomicUsize::new(0),
            uploaded: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl CloudReadCapability for CorruptingMultipartBackend {
    crate::storage::cloud::forward_cloud_backend!(inner;
        submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}

impl CloudWriteCapability for CorruptingMultipartBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);

    fn supports_file_upload(&self) -> bool {
        true
    }

    fn write_file_with_receipt(
        &self,
        key: &str,
        source: StorageFileUpload,
        options: crate::storage::cloud::CloudRequestOptions,
        callback: CloudCallback,
    ) {
        use crate::storage::cloud::CloudError;

        let Ok(capacity) = usize::try_from(source.size) else {
            let _ = callback.send(CloudEvent::PutReceipt {
                key: key.into(),
                result: Err(CloudError::Protocol("test upload exceeds usize".into())),
            });
            return;
        };
        let mut bytes = Vec::with_capacity(capacity);
        let mut offset = 0_u64;
        let mut part_ordinal = 0_usize;
        while offset < source.size {
            let end = offset
                .saturating_add(source.part_size as u64)
                .min(source.size);
            let mut part = match source.file.read_at(offset, end - offset) {
                Ok(part) if part.len() as u64 == end - offset => part.to_vec(),
                Ok(_) => {
                    let _ = callback.send(CloudEvent::PutReceipt {
                        key: key.into(),
                        result: Err(CloudError::Protocol("short multipart source read".into())),
                    });
                    return;
                }
                Err(error) => {
                    let _ = callback.send(CloudEvent::PutReceipt {
                        key: key.into(),
                        result: Err(CloudError::Protocol(error.to_string())),
                    });
                    return;
                }
            };
            if part_ordinal == 0 {
                part[0] ^= 1;
                self.corrupted_parts.fetch_add(1, Ordering::AcqRel);
            }
            bytes.extend_from_slice(&part);
            offset = end;
            part_ordinal += 1;
        }
        let checksum = crc32c::crc32c(&bytes);
        assert_ne!(
            checksum, source.content_crc32c,
            "the corrupted multipart body must not match the finalized local SST"
        );
        let uploaded_size = bytes.len();
        self.uploaded.lock().push((key.to_string(), uploaded_size));

        let (tx, rx) = mpsc::channel();
        self.inner.submit_put_with_reservation(
            key,
            bytes,
            options.headers_with_deadline(),
            options.reservation,
            tx,
        );
        if !matches!(
            rx.recv_timeout(UPLOAD_WAIT),
            Ok(CloudEvent::Put { result: Ok(()), .. })
        ) {
            let _ = callback.send(CloudEvent::PutReceipt {
                key: key.into(),
                result: Err(CloudError::Protocol("mock part upload failed".into())),
            });
            return;
        }

        let (tx, rx) = mpsc::channel();
        self.inner.submit_head(key, tx);
        let metadata = match rx.recv_timeout(UPLOAD_WAIT) {
            Ok(CloudEvent::Head {
                result: Ok(metadata),
                ..
            }) => metadata,
            event => {
                let _ = callback.send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: Err(CloudError::Protocol(format!(
                        "mock uploaded object has no identity: {event:?}"
                    ))),
                });
                return;
            }
        };
        let _ = callback.send(CloudEvent::PutReceipt {
            key: key.into(),
            result: Ok(Some(StorageWriteReceipt {
                metadata,
                content_crc32c: checksum,
            })),
        });
    }
}

impl crate::storage::cloud::CloudDeleteCapability for CorruptingMultipartBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}

impl crate::storage::cloud::CloudListCapability for CorruptingMultipartBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}

impl CloudHeadCapability for CorruptingMultipartBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}

impl CloudBackend for CorruptingMultipartBackend {}

fn add_l0_input(
    event_loop: &mut EventLoop,
    factory: &FsSstFactoryIo,
    generation: u64,
    start: usize,
    count: usize,
    value: &[u8],
) -> MidgeResult<crate::metadata::FileMeta> {
    let name = crate::cloud_layout::file_name(0, 0, generation);
    let mut writer = factory.create()?;
    for ordinal in start..start + count {
        let key = format!("part-{ordinal:04}");
        writer.add_sorted_with_meta(
            key.as_bytes(),
            Some(value),
            generation,
            EntryType::Put,
            None,
        )?;
    }
    let finalized = writer
        .finish_to_path_with_summary(Path::new(&name))?
        .expect("sorted SST finalization summary");
    let summary = finalized.summary.expect("SST key and sequence summary");
    let metadata = crate::metadata::FileMeta {
        name,
        cf_id: 0,
        level: 0,
        sst_seq: generation,
        size_bytes: finalized.size_bytes,
        content_crc32c: Some(finalized.content_crc32c),
        smallest_key: Some(summary.smallest_key),
        largest_key: Some(summary.largest_key),
        smallest_seq: Some(summary.smallest_seq),
        largest_seq: Some(summary.largest_seq),
        key_bounds_complete: true,
        ..Default::default()
    };
    event_loop
        .state
        .manifest
        .test_mut()
        .files
        .push(metadata.clone());
    Ok(metadata)
}

fn manifest_names(manifest: &crate::metadata::Manifest) -> Vec<String> {
    let mut names = manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn event_loop_with_corrupting_storage(
    path: &Path,
    backend: &Arc<CorruptingMultipartBackend>,
) -> MidgeResult<EventLoop> {
    let cloud_backend: Arc<dyn crate::storage::cloud::CloudBackend> = backend.clone();
    let cloud = Arc::new(CloudStorage::new(cloud_backend, String::new()));
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::new(128 * 1024 * 1024),
    ));
    let state = RuntimeState::new(path.to_path_buf(), false);
    crate::metadata::ensure_or_create_format_marker(&state.fs, path)?;
    crate::metadata::ManifestPersistence::save_with_fs(&state.fs, &state.manifest)
        .map_err(MidgeError::Internal)?;
    let (wake_tx, _wake_rx) = crossbeam::channel::unbounded();
    EventLoop::new(
        state,
        false,
        Arc::new(ResponseRouter::new()),
        RuntimeConfig {
            background_compaction: false,
            compaction_concurrency: 1,
            compaction_memory_limit: 128 * 1024 * 1024,
            target_sst_size: 64 * 1024 * 1024,
            hybrid_storage: Some(hybrid),
            ..RuntimeConfig::default()
        },
        crate::runtime::event_loop::FlushWorkerMode::Background(wake_tx),
    )
}

fn add_large_inputs_and_plan(
    event_loop: &mut EventLoop,
    hybrid: &crate::storage::HybridStorage,
) -> MidgeResult<(Vec<String>, crate::compaction::CompactionPlan)> {
    let factory = FsSstFactoryIo::new(
        crate::io::subdirectory(Arc::clone(&event_loop.state.fs), "sst"),
        64 * 1024,
    )
    .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
        crate::codec::CompressionAlgo::None,
    ));
    let value = vec![0x5a; LARGE_VALUE_BYTES];
    let first = add_l0_input(event_loop, &factory, 1, 0, LARGE_ROW_COUNT, &value)?;
    let duplicate = add_l0_input(event_loop, &factory, 2, 0, 1, &value)?;
    event_loop.state.sequence = 2;
    event_loop.state.applied_sequence = 2;
    let initial_names = manifest_names(&event_loop.state.manifest);
    crate::metadata::ManifestPersistence::save_with_fs(
        &event_loop.state.fs,
        &event_loop.state.manifest,
    )
    .map_err(MidgeError::Internal)?;

    let mut actor = crate::runtime::actors::CompactionActor::new_with_config(
        Arc::new(factory),
        crate::compaction::LeveledCompactionConfig::default(),
    );
    actor.set_l0_file_count_threshold(2);
    actor.set_execution_limits(64 * 1024 * 1024, 128 * 1024 * 1024);
    actor.configure_concurrency(1)?;
    actor.configure_work_target(Some(hybrid));
    event_loop.actors.compaction_actor = actor;
    let plan = event_loop
        .actors
        .compaction_actor
        .check_manual_compaction(&event_loop.state)?
        .expect("two L0 inputs create a manual compaction plan");
    assert_eq!(plan.input_files.len(), 2);
    assert!(plan.input_files.contains(&first.name));
    assert!(plan.input_files.contains(&duplicate.name));
    Ok((initial_names, plan))
}

fn wait_for_compaction_failure(event_loop: &mut EventLoop, failures_before: u64) {
    let deadline = Instant::now() + SETTLE_WAIT;
    while event_loop.state.diagnostics.counters().compaction_failures == failures_before {
        event_loop.background_progress(Some(&crossbeam::channel::never()));
        assert!(
            Instant::now() < deadline,
            "corrupt multipart publication must settle as a compaction failure"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn should_keep_compaction_manifest_unchanged_when_streaming_upload_corrupts_a_part(
) -> MidgeResult<()> {
    // Arrange: the merge output exceeds two multipart chunks, forcing the real
    // streaming SST publication path through the injected mock provider.
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(CorruptingMultipartBackend::new());
    let mut event_loop = event_loop_with_corrupting_storage(directory.path(), &backend)?;
    let hybrid = event_loop
        .actors
        .cloud_coordinator
        .hybrid_storage
        .clone()
        .expect("injected hybrid storage");
    let (initial_names, plan) = add_large_inputs_and_plan(&mut event_loop, hybrid.as_ref())?;

    // Act: corrupt an actual uploaded part and return the checksum of the
    // corrupted body. The storage receipt therefore disagrees with the
    // finalized local SST identity and cannot become an output proof.
    let failures_before = event_loop.state.diagnostics.counters().compaction_failures;
    event_loop.launch_compaction(plan)?;
    wait_for_compaction_failure(&mut event_loop, failures_before);

    // Assert: the failed output has no manifest authority, while original SSTs
    // remain the complete persisted authority and source of recovery.
    assert_eq!(backend.corrupted_parts.load(Ordering::Acquire), 1);
    let uploaded = backend.uploaded.lock();
    assert_eq!(uploaded.len(), 1);
    assert!(uploaded[0].1 > 2 * crate::storage::FILE_UPLOAD_PART_BYTES);
    drop(uploaded);
    assert_eq!(manifest_names(&event_loop.state.manifest), initial_names);
    assert!(event_loop.state.intent_log.is_empty());
    let persisted = crate::metadata::ManifestPersistence::load(&event_loop.state.db_path)
        .map_err(MidgeError::Internal)?;
    assert_eq!(manifest_names(&persisted), initial_names);
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .all(|file| initial_names.contains(&file.name)));
    assert_eq!(
        event_loop.state.diagnostics.counters().compaction_failures,
        failures_before + 1
    );
    Ok(())
}
