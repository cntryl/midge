use super::*;
use crate::lease::cloud::{
    provider_activate_authority_sentinel, provider_create_authority_sentinel, provider_write_doc,
    BACKUP_EXPIRED_TIMESTAMP,
};
use crate::lease::{LeaderStore, LeaseClock, LeaseValidity};
use std::sync::Arc;
use std::time::Instant;

fn store(cloud: Arc<CloudStorage>, epoch: u64) -> ProviderLeaderStore {
    let validity = Arc::new(LeaseValidity::new());
    validity
        .activate(epoch, Instant::now() + Duration::from_mins(1))
        .unwrap();
    ProviderLeaderStore::new(
        cloud,
        Duration::from_mins(1),
        format!("owner-{epoch}"),
        validity,
        Duration::ZERO,
        LeaseClock::default(),
    )
}

fn document(epoch: u64) -> LeaseDocument {
    LeaseDocument {
        version: LeaseDocumentVersion::V4,
        epoch: Some(epoch),
        holder_id: format!("holder-{epoch}"),
        owner_token: Some(format!("owner-{epoch}")),
        acquired_at: chrono::Utc::now().to_rfc3339(),
        expires_at: (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
        committed_metadata: None,
    }
}

fn generation(id: u64) -> CloudMetadataGeneration {
    CloudMetadataGeneration {
        id: crate::lease::SharedGenerationId::new(id).unwrap(),
        manifest_horizon: id,
        objects: [
            crate::metadata::files::FORMAT,
            crate::metadata::files::MANIFEST_SNAPSHOT,
        ]
        .into_iter()
        .map(|name| crate::lease::CloudMetadataObject {
            file_name: name.into(),
            object_key: format!("metadata/generations/{}/{name}", uuid::Uuid::new_v4()),
            len: 1,
            crc32c: 1,
        })
        .collect(),
    }
}

#[test]
fn should_preserve_predecessor_history_when_successor_claims_metadata_pointer() {
    // Arrange
    let cloud = Arc::new(CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "midge".into(),
    ));
    let first = store(Arc::clone(&cloud), 7);
    provider_create_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    first
        .claim_metadata_pointer(&cloud, &document(7), Duration::from_secs(5))
        .unwrap();
    provider_activate_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    let committed = generation(1);
    first
        .publish_separate_metadata(
            "holder-7",
            7,
            None,
            committed.clone(),
            Duration::from_secs(5),
        )
        .unwrap();
    let before = read_pointer(&cloud, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let second = store(Arc::clone(&cloud), 8);

    // Act
    second
        .claim_metadata_pointer(&cloud, &document(8), Duration::from_secs(5))
        .unwrap();
    let current = read_pointer(&cloud, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let stale = first.publish_separate_metadata(
        "holder-7",
        7,
        Some(&committed),
        generation(2),
        Duration::from_secs(5),
    );

    // Assert: even a stale writer that learns the new ETag cannot use it.
    assert_eq!(current.0.writer_epoch, 8);
    assert_eq!(current.0.committed.as_ref(), Some(&committed));
    assert!(matches!(stale, Err(LeaseError::RenewalFailed(_))));
    assert!(!current.1.same_version(&before.1));
}

#[test]
fn should_reject_delayed_predecessor_cas_when_pointer_epoch_claim_has_committed() {
    // Arrange
    let cloud = Arc::new(CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "midge".into(),
    ));
    let first = store(Arc::clone(&cloud), 7);
    provider_create_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    first
        .claim_metadata_pointer(&cloud, &document(7), Duration::from_secs(5))
        .unwrap();
    provider_activate_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    let (mut delayed, old_identity) = read_pointer(&cloud, Duration::from_secs(5))
        .unwrap()
        .unwrap();
    delayed.committed = Some(generation(1));
    let second = store(Arc::clone(&cloud), 8);
    second
        .claim_metadata_pointer(&cloud, &document(8), Duration::from_secs(5))
        .unwrap();

    // Act
    let outcome = write_pointer(
        &cloud,
        &delayed,
        StoragePrecondition::IfMatch(old_identity),
        &crate::common::OperationDeadline::from_budget(Duration::from_secs(5)),
    );
    let current = read_pointer(&cloud, Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .0;

    // Assert
    assert!(matches!(outcome, Err(LeaseError::AcquisitionFailed(_))));
    assert_eq!(current.writer_epoch, 8);
    assert!(current.committed.is_none());
}

#[test]
fn should_resume_initialization_when_crash_precedes_pointer_claim_or_activation() {
    for pointer_was_claimed in [false, true] {
        // Arrange: the first acquisition durably wrote a version4 lease but
        // never activated validity or admitted user writes.
        let cloud = Arc::new(CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "midge".into(),
        ));
        provider_create_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
        let mut crashed = document(7);
        crashed.expires_at = BACKUP_EXPIRED_TIMESTAMP.into();
        provider_write_doc(&cloud, &crashed, vec![("If-None-Match".into(), "*".into())]).unwrap();
        if pointer_was_claimed {
            store(Arc::clone(&cloud), 7)
                .claim_metadata_pointer(&cloud, &crashed, Duration::from_secs(5))
                .unwrap();
        }
        let successor = ProviderLeaderStore::new(
            Arc::clone(&cloud),
            Duration::from_mins(1),
            "successor-owner".into(),
            Arc::new(LeaseValidity::new()),
            Duration::ZERO,
            LeaseClock::default(),
        );

        // Act: startup can reach takeover rather than misclassifying this
        // known-new initialization state as an incompatible legacy namespace.
        let before = successor
            .read_separate_metadata_head(Duration::from_secs(5))
            .unwrap();
        let acquired = successor.acquire_leadership("successor").unwrap();
        let after = successor
            .read_separate_metadata_head(Duration::from_secs(5))
            .unwrap();
        let claimed = read_pointer(&cloud, Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .0;

        // Assert
        assert_eq!(before, CloudMetadataHead::Uncommitted);
        assert_eq!(after, CloudMetadataHead::Uncommitted);
        assert_eq!(acquired.epoch, 8);
        assert_eq!(claimed.writer_epoch, 8);
        assert!(claimed.committed.is_none());
        assert_eq!(
            provider_read_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap(),
            AuthoritySentinelState::Active
        );
    }
}

#[test]
fn should_reject_empty_pointer_repair_when_initialized_authority_is_missing() {
    // Arrange: activation is durable, so missing pointer may mean lost committed
    // history and cannot be treated as a fresh namespace.
    let cloud = Arc::new(CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "midge".into(),
    ));
    provider_create_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    provider_activate_authority_sentinel(&cloud, Duration::from_secs(5)).unwrap();
    let successor = store(Arc::clone(&cloud), 8);

    // Act
    let outcome = successor.claim_metadata_pointer(&cloud, &document(8), Duration::from_secs(5));

    // Assert
    assert!(matches!(outcome, Err(LeaseError::Indeterminate(_))));
    assert!(read_pointer(&cloud, Duration::from_secs(5))
        .unwrap()
        .is_none());
}
