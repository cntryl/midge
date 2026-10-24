use super::MetadataPublicationLock;
use crate::common::resource_budget::ResourceBudget;
use crate::common::OperationDeadline;
use crate::io::{Fs, FsPath};
use crate::lease::fs_leader_store::FsLeaderStore;
use crate::lease::LeaderStore;
use crate::metadata::{files, Manifest, ManifestEdit, ManifestPersistence};
use crate::runtime::hybrid_persistence::{
    mirror_captured_control_metadata_within, CloudMetadataMirrorAuthority,
    CloudMetadataMirrorContext,
};
use crate::runtime::{IntentLogEntry, IntentPersistence, PublicationPhase};
use crate::storage::cloud::CloudStorage;
use std::sync::Arc;
use std::time::Duration;

/// This synchronous provider has no completion thread or retained transport
/// buffer. Its admitted reservation remains owned until the real mock PUT
/// completes, allowing the regression's pool to hold exactly one source.
struct InlineCloudBackend {
    inner: crate::storage::cloud::MockCloudBackend,
}

impl crate::storage::cloud::CloudReadCapability for InlineCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);

    fn read(
        &self,
        key: &str,
        options: crate::storage::cloud::CloudReadOptions,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        // This fixture only requests whole immutable bodies. Its real mock
        // GET sends the complete body synchronously, so no relay stack or
        // payload remains after the reservation-carrying options are dropped.
        self.inner.submit_get(key, callback);
        drop(options);
    }
}

impl crate::storage::cloud::CloudWriteCapability for InlineCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);

    fn submit_put_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
        drop(reservation);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for InlineCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl crate::storage::cloud::CloudListCapability for InlineCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl crate::storage::cloud::CloudHeadCapability for InlineCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}
impl crate::storage::cloud::CloudBackend for InlineCloudBackend {}

struct DroppedSource {
    _directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    lock: MetadataPublicationLock,
    memory: ResourceBudget,
    encoded: u64,
    witness_bytes: usize,
}

fn select_source_before_contended_drop() -> DroppedSource {
    let directory = tempfile::tempdir().unwrap();
    let fs = crate::io::real::test_root(directory.path());
    crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
    ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
    let file = crate::metadata::FileMeta {
        name: crate::cloud_layout::file_name(0, 0, 1),
        size_bytes: 3,
        content_crc32c: Some(crc32c::crc32c(b"sst")),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..crate::metadata::FileMeta::default()
    };
    crate::metadata::store::ManifestStore::new(Arc::clone(&fs))
        .append(&ManifestEdit::Batch(vec![
            ManifestEdit::AddSst(file.clone()),
            ManifestEdit::AdvanceFlushFrontier {
                cf_id: 0,
                freeze_sequence: 1,
            },
        ]))
        .unwrap();
    IntentPersistence::save_with_fs(
        &fs,
        &[IntentLogEntry::FlushPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id: 0,
            sequence: 1,
            file_meta: crate::runtime::FileMeta::from(&file),
        }],
    )
    .unwrap();
    let encoded = files::CLOUD_MIRRORED
        .iter()
        .map(|name| fs.metadata(&FsPath::new(*name)).unwrap().len)
        .sum::<u64>();
    let memory = ResourceBudget::new(usize::try_from(encoded).unwrap() * 16 + 4096);
    let lock = MetadataPublicationLock::default();
    lock.configure_capture_memory(memory.clone());
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let cut = lock.capture_control_cut(&fs, 1, &deadline).unwrap();
    let cloud = CloudStorage::new(
        Arc::new(InlineCloudBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
        }),
        "one-cut".into(),
    );
    let store = FsLeaderStore::new(Arc::clone(&fs), crate::lease::LeaseClock::default());
    store.acquire_leadership("writer").unwrap();
    mirror_captured_control_metadata_within(
        CloudMetadataMirrorContext {
            cloud: &cloud,
            fs: fs.as_ref(),
            publication_lock: &lock,
            lock_wait_budget: Duration::from_secs(5),
            deadline: &deadline,
            authority: CloudMetadataMirrorAuthority {
                store: &store,
                holder_id: "writer",
                writer_epoch: 1,
            },
        },
        &cut,
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(lock.pending_captures(), 0);
    let registry = lock.cuts.lock();
    assert_eq!(registry.published_intents.len(), 1);
    let witness_bytes = registry
        .selected_terminal
        .as_ref()
        .unwrap()
        .witnesses
        .as_ref()
        .unwrap()
        .memory
        .bytes();
    let source = Arc::downgrade(&cut);
    drop(cut);
    assert!(source.upgrade().is_none());
    assert_eq!(memory.used(), witness_bytes);
    assert!(
        witness_bytes < memory.limit(),
        "finished source workspace must be released even while registry cleanup is blocked"
    );
    drop(registry);
    DroppedSource {
        _directory: directory,
        fs,
        lock,
        memory,
        encoded,
        witness_bytes,
    }
}

#[test]
fn should_admit_next_capture_when_final_source_drop_could_not_prune_its_receipt() {
    // Arrange: a selected ManifestPublished receipt owns the entire one-cut
    // pool's selected compact witness after its final source drop lost the
    // registry mutex race. Full body workspace is already released.
    let fixture = select_source_before_contended_drop();

    // Act
    let next = fixture
        .lock
        .capture_control_cut(
            &fixture.fs,
            1,
            &OperationDeadline::from_budget(Duration::from_secs(1)),
        )
        .unwrap();

    // Assert: byte admission first discards dead receipt ownership, without
    // changing the configured pool or dropping the new capture's charge.
    assert_eq!(fixture.lock.cuts.lock().published_intents.len(), 0);
    assert_eq!(fixture.lock.pending_captures(), 1);
    assert_eq!(fixture.memory.used(), fixture.memory.limit());
    assert!(next.memory_reservation().bytes()
        >= usize::try_from(fixture.encoded).unwrap() * 8 + 4096,
        "shared witness credit must leave the full body and decoder allowance independently charged");
    let witness = next.terminal_witnesses.as_ref().unwrap();
    let owned_allocations = std::mem::size_of_val(witness.as_ref())
        + std::mem::size_of_val(witness.digests.as_ref())
        + witness.memory.allocation_bytes()
        + 4 * std::mem::size_of::<usize>()
        + super::RECEIPT_COPIES
            * std::mem::size_of::<super::ReceiptEntry>()
            * witness.digests.len();
    assert!(witness.memory.bytes() >= owned_allocations);
    assert!(witness.memory.bytes() <= usize::try_from(fixture.encoded).unwrap() * 4);
    assert_eq!(next.cut_ordinal(), 2);
}

#[test]
fn should_admit_selected_body_read_when_final_source_drop_could_not_prune_its_receipt() {
    // Arrange
    let fixture = select_source_before_contended_drop();

    // Act: cold selected-body reconstruction enters through the same byte
    // admission boundary as capture, after no further source drop can run.
    let selected = fixture
        .lock
        .reserve_metadata_body_memory(fixture.encoded)
        .unwrap();

    // Assert
    assert_eq!(fixture.lock.cuts.lock().published_intents.len(), 0);
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(fixture.memory.used(), fixture.memory.limit());
    drop(selected);
    assert_eq!(fixture.memory.used(), fixture.witness_bytes);
}
