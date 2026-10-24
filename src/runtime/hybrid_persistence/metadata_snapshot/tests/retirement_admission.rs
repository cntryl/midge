use super::super::{
    capture_local_metadata, read_committed_metadata_bodies_within, CloudMetadataMirrorAuthority,
    CloudMetadataMirrorContext, SharedMetadataSnapshot,
};
use super::Fixture;
use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, OperationDeadline};
use crate::lease::{CloudMetadataGeneration, LeaderStore, SharedGenerationId};
use crate::metadata::{files, Manifest};
use crate::runtime::hybrid_persistence::metadata_cuts::tests::HeldReadbackBackend;
use crate::storage::cloud::{CloudStorage, MockCloudBackend};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn publish_raw_snapshot(fixture: &Fixture, bytes: &[u8]) -> CloudMetadataGeneration {
    std::fs::write(
        fixture._directory.path().join(files::MANIFEST_SNAPSHOT),
        bytes,
    )
    .unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let context = CloudMetadataMirrorContext {
        cloud: &fixture.cloud,
        fs: fixture.fs.as_ref(),
        publication_lock: &fixture.lock,
        lock_wait_budget: Duration::from_secs(5),
        deadline: &deadline,
        authority: CloudMetadataMirrorAuthority {
            store: fixture.store.as_ref(),
            holder_id: "writer",
            writer_epoch: 1,
        },
    };
    let local = capture_local_metadata(&context).unwrap();
    let objects = crate::runtime::hybrid_persistence::metadata_chain::publish(
        &context,
        &local,
        None,
        None,
        None,
        SharedGenerationId::FIRST,
    )
    .unwrap();
    let generation = CloudMetadataGeneration {
        id: SharedGenerationId::FIRST,
        manifest_horizon: 0,
        objects,
    };
    fixture
        .store
        .publish_committed_metadata("writer", 1, None, generation.clone(), deadline.remaining())
        .unwrap();
    generation
}

fn snapshot(fixture: &Fixture, budget: ResourceBudget) -> SharedMetadataSnapshot {
    SharedMetadataSnapshot::new(
        Arc::clone(&fixture.cloud),
        budget,
        fixture.store.clone(),
        "writer".into(),
        1,
        fixture.lock.clone(),
    )
}

fn initial_charge(generation: &CloudMetadataGeneration) -> usize {
    generation
        .objects
        .iter()
        .map(|object| usize::try_from(object.len).unwrap())
        .sum::<usize>()
        * 16
        + 4096
}

fn assert_released(root: &ResourceBudget, child: &ResourceBudget) {
    let until = Instant::now() + Duration::from_secs(2);
    while root.used() != 0 && Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(child.used(), 0);
    assert_eq!(root.used(), 0);
}

#[test]
fn should_refuse_dense_selected_slots_before_decoding_retirement_authority() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: this real current-format positional shape decodes before
        // invalid SST names are checked, and its typed slots exceed 16E.
        let fixture = Fixture::new();
        let mut raw = serde_json::to_value(Manifest::default()).unwrap();
        raw["files"] = serde_json::json!(vec![serde_json::json!(["", 0, 0]); 8192]);
        let bytes = crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &raw).unwrap();
        let decoded: Manifest =
            crate::metadata::codec::decode(crate::metadata::codec::MANIFEST, &bytes).unwrap();
        let actual_slots =
            decoded.files.capacity() * std::mem::size_of::<crate::metadata::FileMeta>();
        drop(decoded);
        let generation = publish_raw_snapshot(&fixture, &bytes);
        // The compatibility backend separately owns a 64 KiB completion
        // stack while delivering a reserved GET. Admit that real transport
        // workspace so the refusal protects typed decode, not dispatch.
        let limit = initial_charge(&generation) + 64 * 1024;
        assert!(actual_slots > limit);
        let root = ResourceBudget::new(limit);
        let child = root.child(limit);
        let reader = snapshot(&fixture, child.clone());

        // Act
        let result = reader.committed_retirement_authority_within(&OperationDeadline::from_budget(
            Duration::from_secs(5),
        ));

        // Assert: no authority or typed allocation is admitted, and the exact
        // selected immutable body remains available for diagnosis/recovery.
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("dense selected decode must not grant retirement authority"),
        };
        assert!(
            matches!(&error, MidgeError::ResourceLimit(message)
            if message.contains("selected metadata typed workspace")),
            "{error:?}"
        );
        assert_released(&root, &child);
        let retained = read_committed_metadata_bodies_within(
            &fixture.cloud,
            &generation,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .unwrap();
        assert_eq!(retained[files::MANIFEST_SNAPSHOT], bytes);
    });
}

#[test]
fn should_retain_retirement_read_charge_until_the_actual_late_get_callback_finishes() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: fetch the actual selected immutable body, then hold its
        // physical callback beyond the original caller deadline.
        let (entered, arrived) = std::sync::mpsc::channel();
        let released = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let backend = Arc::new(HeldReadbackBackend::new(
            Arc::new(MockCloudBackend::new()),
            entered,
            Arc::clone(&released),
        ));
        let fixture = Fixture::with_cloud(Arc::new(CloudStorage::new(
            backend.clone(),
            "retirement-held-read".into(),
        )));
        fixture.mirror().unwrap();
        let root = ResourceBudget::new(512 * 1024);
        let child = root.child(256 * 1024);
        let reader = snapshot(&fixture, child.clone());
        backend.hold_next_read();
        let worker_gate = crate::failpoints::worker_gate();
        let worker = std::thread::spawn(move || {
            worker_gate.run(|| {
                reader.committed_retirement_authority_within(&OperationDeadline::from_budget(
                    Duration::from_millis(200),
                ))
            })
        });
        arrived.recv_timeout(Duration::from_secs(2)).unwrap();

        // Act: let the logical caller time out while the accepted GET still
        // owns the genuine body and callback, with no replacement read.
        let result = worker.join().unwrap();

        // Assert: logical timeout cannot return either parent's bytes early.
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert!(child.used() > 0, "held GET must retain its source charge");
        assert_eq!(root.used(), child.used());
        let (lock, changed) = &*released;
        *lock.lock().unwrap() = true;
        changed.notify_all();
        assert_released(&root, &child);
    });
}

#[test]
fn should_retain_typed_charge_for_the_full_retirement_manifest_lifetime() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: valid compact positional files need more conservative typed
        // workspace than the initial raw-body charge, within the same pool.
        let fixture = Fixture::new();
        let mut raw = serde_json::to_value(Manifest::default()).unwrap();
        raw["files"] = serde_json::Value::Array(
            (1..=64)
                .map(|sequence| {
                    serde_json::json!([crate::cloud_layout::file_name(0, 0, sequence), 0, 3,])
                })
                .collect(),
        );
        let bytes = crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &raw).unwrap();
        let generation = publish_raw_snapshot(&fixture, &bytes);
        let initial = initial_charge(&generation);
        let root = ResourceBudget::new(512 * 1024);
        let child = root.child(256 * 1024);
        let reader = snapshot(&fixture, child.clone());

        // Act
        let authority = reader
            .committed_retirement_authority_within(&OperationDeadline::from_budget(
                Duration::from_secs(5),
            ))
            .unwrap();

        // Assert: all decoded files remain owned by the returned authority;
        // both hierarchical charges outlive loading and release on its Drop.
        assert_eq!(authority.manifest.files.len(), 64);
        assert!(
            child.used() > initial,
            "typed workspace must remain independently charged"
        );
        assert_eq!(child.used(), root.used());
        assert!(child.used() <= child.limit());
        assert_eq!(
            authority.frontiers().get(0).unwrap(),
            crate::metadata::frontiers::FamilyFrontier::Live {
                through_sequence: 0
            }
        );
        drop(authority);
        assert_released(&root, &child);
    });
}
