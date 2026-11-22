use super::*;
use crate::lease::PrimaryLease as _;
use std::sync::Arc;
use std::time::Duration;

struct MigrationFixture {
    backend: Arc<crate::storage::cloud::MockCloudBackend>,
    cloud: Arc<CloudStorage>,
    lease: Arc<crate::lease::CloudStorageLease>,
    _guard: crate::lease::LeaseGuard,
    store: Arc<dyn LeaderStore>,
    source: CloudMetadataGeneration,
    candidate: CloudMetadataGeneration,
    pending: CloudFormatMigrationPending,
}

impl MigrationFixture {
    fn new(stage_descriptor: bool) -> MidgeResult<Self> {
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), String::new()));
        let temp = tempfile::tempdir()?;
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            temp.path().to_path_buf(),
            Arc::clone(&cloud),
        ));
        let guard = Arc::clone(&lease).try_acquire()?;
        let store = lease
            .get_leader_store()
            .expect("provider lease exposes its leader store");
        let source_manifest = crate::metadata::codec::encode(
            crate::metadata::codec::MANIFEST,
            &crate::metadata::Manifest::default(),
        )?;
        let source_bodies = std::collections::BTreeMap::from([
            (
                crate::metadata::files::FORMAT.to_string(),
                crate::metadata::format::current_format_marker_bytes(),
            ),
            (
                crate::metadata::files::MANIFEST_SNAPSHOT.to_string(),
                source_manifest,
            ),
        ]);
        let source = generation(1, &source_bodies);
        upload_generation(&cloud, &source, &source_bodies)?;
        store.publish_committed_metadata(
            &lease.holder_id(),
            lease.epoch(),
            None,
            source.clone(),
            Duration::from_secs(2),
        )?;
        let source_bodies = read_committed_metadata_bodies_within(
            &cloud,
            &source,
            &OperationDeadline::from_budget(Duration::from_secs(2)),
        )?;
        let local_root: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
        crate::metadata::format::ensure_or_create_format_marker(
            &local_root,
            std::path::Path::new("."),
        )?;
        crate::metadata::local_authority::LocalMetadataAuthority::stage_upgrade_candidate(
            Arc::clone(&local_root),
            [7; 32],
            source_bodies
                .get(crate::metadata::files::MANIFEST_SNAPSHOT)
                .expect("source manifest"),
            0,
            b"[]",
            0,
            0,
        )?;
        let mut candidate_bodies = std::collections::BTreeMap::from([(
            crate::metadata::files::FORMAT.to_string(),
            FORMAT_10_MARKER.to_vec(),
        )]);
        for file_name in
            crate::metadata::local_authority::LocalMetadataAuthority::selected_relative_paths_from(
                local_root.as_ref(),
            )?
        {
            let bytes =
                crate::io::read_all(local_root.as_ref(), &crate::io::FsPath::new(&file_name))
                    .map_err(crate::io::FsError::into_midge)?;
            candidate_bodies.insert(file_name, bytes.to_vec());
        }
        crate::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(
            &candidate_bodies,
        )?;
        let candidate = generation(2, &candidate_bodies);
        upload_generation(&cloud, &candidate, &candidate_bodies)?;
        let migration_id = "81900000-0000-4000-8000-000000000001".to_string();
        let pending = CloudFormatMigrationPending {
            migration_id: migration_id.clone(),
            target_format: 10,
            source: CloudMetadataGenerationIdentity::from_generation(&source),
            candidate: crate::lease::CloudFormatMigrationCandidate {
                generation: CloudMetadataGenerationIdentity::from_generation(&candidate),
                descriptor_key: CloudFormatMigrationPending::candidate_descriptor_key(
                    &migration_id,
                ),
            },
            source_sha256: source_payload_sha256(&source_bodies),
        };
        stage_source_descriptor(
            &cloud,
            &pending,
            &source,
            &OperationDeadline::from_budget(Duration::from_secs(2)),
        )?;
        if stage_descriptor {
            stage_candidate_descriptor(
                &cloud,
                &pending,
                &candidate,
                &OperationDeadline::from_budget(Duration::from_secs(2)),
            )?;
        }
        store.install_format_10_pending(
            &lease.holder_id(),
            lease.epoch(),
            pending.clone(),
            Duration::from_secs(2),
        )?;
        backend.clear_history();
        Ok(Self {
            backend,
            cloud,
            lease,
            _guard: guard,
            store,
            source,
            candidate,
            pending,
        })
    }

    fn reconcile(&self) -> MidgeResult<()> {
        super::super::storage::reconcile_cloud_format_before_mutations(
            &self.cloud,
            self.store.as_ref(),
            &self.lease.holder_id(),
            self.lease.epoch(),
            Duration::from_secs(3),
            None,
        )
    }

    fn selected(&self) -> MidgeResult<Option<CloudMetadataGeneration>> {
        match self.store.read_committed_metadata(Duration::from_secs(2))? {
            CloudMetadataHead::Committed(generation) => Ok(Some(generation)),
            CloudMetadataHead::Uncommitted => Ok(None),
            CloudMetadataHead::MissingLease => Err(MidgeError::LeaseIndeterminate(
                "test lease disappeared".into(),
            )),
        }
    }

    fn gate(&self) -> MidgeResult<Option<CloudFormatMigrationGate>> {
        Ok(self.store.read_format_migration_gate(
            &self.lease.holder_id(),
            self.lease.epoch(),
            Duration::from_secs(2),
        )?)
    }

    fn assert_no_startup_catalog_mutations(&self) {
        let uploads = self.backend.get_uploads();
        assert!(!uploads.iter().any(|(key, _)| {
            key == crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY
                || key == crate::wal::cloud_catalog::OBJECT_KEY
                || key == crate::wal::cloud_catalog::MIRROR_OBJECT_KEY
        }));
    }
}

fn generation(
    ordinal: u64,
    bodies: &std::collections::BTreeMap<String, Vec<u8>>,
) -> CloudMetadataGeneration {
    let id = crate::lease::SharedGenerationId::new(ordinal).expect("positive generation ordinal");
    let uuid = format!("00000000-0000-4000-8000-{ordinal:012}");
    let objects = bodies
        .iter()
        .map(|(file_name, body)| crate::lease::CloudMetadataObject {
            file_name: file_name.clone(),
            object_key: format!("metadata/generations/{uuid}/{file_name}"),
            len: u64::try_from(body.len()).expect("test body length"),
            crc32c: crc32c::crc32c(body),
        })
        .collect();
    CloudMetadataGeneration {
        id,
        manifest_horizon: 0,
        objects,
    }
}

fn upload_generation(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    bodies: &std::collections::BTreeMap<String, Vec<u8>>,
) -> MidgeResult<()> {
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    for object in &generation.objects {
        let body = bodies
            .get(&object.file_name)
            .expect("generation object has a body")
            .clone();
        crate::storage::cloud::BlockingCloud::new(cloud, &deadline).put_with_precondition(
            &object.object_key,
            body,
            &StoragePrecondition::IfAbsent,
        )?;
    }
    Ok(())
}

#[test]
fn should_resume_pending_cloud_migration_from_exact_v9_predecessor() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(true)?;

    // Act: startup reconciliation selects the immutable candidate, activates
    // the permanent v5 gate, then rejects public FORMAT 10 runtime use.
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::NotSupported(_))));
    assert_eq!(fixture.selected()?.as_ref(), Some(&fixture.candidate));
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Active { target_format: 10 })
    );
    assert_eq!(
        read_source_descriptor(
            &fixture.cloud,
            &fixture.pending,
            &OperationDeadline::from_budget(Duration::from_secs(2)),
        )?,
        fixture.source,
        "the exact v9 source descriptor remains outside the v10 body namespace GC"
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[test]
fn should_finalize_pending_gate_when_exact_candidate_is_already_selected() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(true)?;
    fixture.store.publish_committed_metadata(
        &fixture.lease.holder_id(),
        fixture.lease.epoch(),
        Some(&fixture.source),
        fixture.candidate.clone(),
        Duration::from_secs(2),
    )?;
    fixture.backend.clear_history();

    // Act
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::NotSupported(_))));
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Active { target_format: 10 })
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[test]
fn should_keep_pending_gate_when_cloud_pointer_selects_unexpected_generation() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(true)?;
    force_unexpected_pointer_selection(&fixture.cloud, &fixture.candidate)?;
    fixture.backend.clear_history();

    // Act
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Pending(fixture.pending.clone()))
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[test]
fn should_reject_missing_candidate_descriptor_before_startup_catalog_mutation() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(false)?;

    // Act
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    assert_eq!(fixture.selected()?.as_ref(), Some(&fixture.source));
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Pending(fixture.pending.clone()))
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[test]
fn should_reject_changed_candidate_descriptor_before_startup_catalog_mutation() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(true)?;
    let mut changed = fixture.candidate.clone();
    changed.manifest_horizon += 1;
    let wire = serde_json::to_vec(&CandidateDescriptor::from(&changed))
        .expect("encode changed descriptor");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    BlockingCloud::new(&fixture.cloud, &deadline).put_with_headers(
        &fixture.pending.candidate.descriptor_key,
        wire,
        Vec::new(),
    )?;
    fixture.backend.clear_history();

    // Act
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(fixture.selected()?.as_ref(), Some(&fixture.source));
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Pending(fixture.pending.clone()))
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[test]
fn should_reject_changed_candidate_object_before_startup_catalog_mutation() -> MidgeResult<()> {
    // Arrange
    let fixture = MigrationFixture::new(true)?;
    let object = fixture
        .candidate
        .objects
        .iter()
        .find(|object| object.file_name == "metadata/generations/upgrade-candidate/manifest/base")
        .expect("candidate manifest base object");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    crate::storage::cloud::BlockingCloud::new(&fixture.cloud, &deadline).put_with_headers(
        &object.object_key,
        b"changed candidate".to_vec(),
        Vec::new(),
    )?;
    fixture.backend.clear_history();

    // Act
    let result = fixture.reconcile();

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(fixture.selected()?.as_ref(), Some(&fixture.source));
    assert_eq!(
        fixture.gate()?,
        Some(CloudFormatMigrationGate::Pending(fixture.pending.clone()))
    );
    fixture.assert_no_startup_catalog_mutations();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct TestPointer {
    writer_epoch: u64,
    holder_id: String,
    owner_token: String,
    committed: Option<CloudMetadataGeneration>,
}

fn force_unexpected_pointer_selection(
    cloud: &CloudStorage,
    candidate: &CloudMetadataGeneration,
) -> MidgeResult<()> {
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    let wire = BlockingCloud::new(cloud, &deadline)
        .get_optional("metadata/committed-pointer.v1")?
        .ok_or_else(|| MidgeError::NotFound)?;
    let mut pointer: TestPointer = crate::metadata::codec::decode(4, &wire)
        .map_err(|error| MidgeError::Corruption(error.to_string()))?;
    let mut unexpected = candidate.clone();
    unexpected.id = crate::lease::SharedGenerationId::new(3).expect("third generation");
    pointer.committed = Some(unexpected);
    let wire = crate::metadata::codec::encode(4, &pointer)
        .map_err(|error| MidgeError::Internal(error.to_string()))?;
    BlockingCloud::new(cloud, &deadline).put_with_headers(
        "metadata/committed-pointer.v1",
        wire,
        Vec::new(),
    )
}
