use super::*;
use crate::wal::cloud_catalog::{PublishedWalSegment, WalPublicationCatalog};
use bytes::Bytes;

fn framed_wal(sequence: u64, epoch: u64, value: &[u8]) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::copy_from_slice(value)),
        sequence,
        epoch,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("encode WAL frame");
    bytes
}

fn limits() -> StreamingReplayLimits {
    StreamingReplayLimits {
        max_frame_bytes: 128 * 1_024,
        max_pending_txn_bytes: 256 * 1_024,
        max_memtable_encoded_bytes: 256 * 1_024,
        target_memtable_encoded_bytes: 256 * 1_024,
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    cloud: Arc<dyn StorageBackend>,
    catalog: WalPublicationCatalog,
}

impl Fixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir_all(directory.path().join("local/wal"))?;
        let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("cloud"),
        )?);
        Ok(Self {
            directory,
            cloud,
            catalog: WalPublicationCatalog::empty(9).expect("catalog"),
        })
    }

    fn publish(&mut self, id: u64, sequence: u64, epoch: u64, bytes: &[u8]) -> MidgeResult<()> {
        let publication = PublishedWalSegment::from_validated_bytes(id, sequence, epoch, bytes);
        let path = self
            .directory
            .path()
            .join("cloud")
            .join(&publication.object_key);
        std::fs::create_dir_all(path.parent().expect("remote parent"))?;
        std::fs::write(path, bytes)?;
        self.catalog.segments.insert(id, publication);
        Ok(())
    }

    fn local(&self, name: &str, bytes: &[u8]) -> MidgeResult<PathBuf> {
        let path = self.directory.path().join("local/wal").join(name);
        std::fs::write(&path, bytes)?;
        Ok(path)
    }

    fn build(&self, policy: RecoveryPolicy) -> MidgeResult<StreamingCloudWalRecovery> {
        self.build_with_limits(policy, limits())
    }

    fn build_with_limits(
        &self,
        policy: RecoveryPolicy,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<StreamingCloudWalRecovery> {
        StreamingCloudWalRecovery::build(
            &self.directory.path().join("local"),
            &self.cloud,
            &self.catalog,
            policy,
            Duration::from_secs(5),
            127,
            limits,
        )
    }
}

#[test]
fn should_normalize_recovery_sources_without_copying_wal_bytes() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, 7, &vec![b'a'; 32 * 1_024]);
    let second = framed_wal(2, 7, &vec![b'b'; 32 * 1_024]);
    fixture.publish(1, 1, 7, &first)?;
    fixture.publish(2, 2, 7, &second)?;
    let legacy = fixture.local("1.wal", &first)?;
    #[cfg(unix)]
    let original_inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&legacy)?.ino()
    };
    std::fs::write(
        fixture.directory.path().join("cloud/unpublished.wal"),
        b"not authoritative",
    )?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    assert_eq!(recovered.plan.remote_segments.len(), 2);
    assert!(recovered.plan.local_segments.is_empty());
    assert!(!recovered.plan.replay_dir.exists());
    assert!(!legacy.exists());
    let canonical = fixture
        .directory
        .path()
        .join("local/wal")
        .join(crate::wal::segment_file_name(1));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(&canonical)?.ino(), original_inode);
    }
    assert_eq!(std::fs::metadata(&canonical)?.len(), first.len() as u64);
    assert_eq!(
        std::fs::read_dir(fixture.directory.path().join("local/wal"))?.count(),
        1
    );
    assert_eq!(recovered.fs.list_dir(&FsPath::new("wal"))?.len(), 2);
    let file = recovered
        .fs
        .open(&FsPath::new(crate::wal::segment_file_name(2)), READ_ONLY)?;
    assert_eq!(file.read_at(0, second.len() as u64)?.as_ref(), second);
    Ok(())
}

#[test]
fn should_reject_local_remote_wal_divergence_even_during_salvage() -> MidgeResult<()> {
    for policy in [RecoveryPolicy::Strict, RecoveryPolicy::Salvage] {
        // Arrange
        let mut fixture = Fixture::new()?;
        fixture.publish(1, 1, 7, &framed_wal(1, 7, b"remote"))?;
        let local = fixture.local(
            &crate::wal::segment_file_name(1),
            &framed_wal(1, 7, b"local!"),
        )?;

        // Act
        let result = fixture.build(policy);

        // Assert
        assert!(
            matches!(result, Err(MidgeError::RecoveryFailed(message)) if message.contains("diverge"))
        );
        assert!(local.exists());
    }
    Ok(())
}

#[test]
fn should_preserve_conflicting_local_aliases_when_salvaging_canonical_wal() -> MidgeResult<()> {
    for corrupt_canonical in [false, true] {
        // Arrange
        let fixture = Fixture::new()?;
        let valid = framed_wal(1, 7, b"selected");
        let canonical_bytes = if corrupt_canonical {
            b"bad".to_vec()
        } else {
            framed_wal(1, 7, b"canonical")
        };
        let canonical = fixture.local(&crate::wal::segment_file_name(1), &canonical_bytes)?;
        let legacy = fixture.local("1.wal", &valid)?;
        assert!(fixture.build(RecoveryPolicy::Strict).is_err());

        // Act
        let recovered = fixture.build(RecoveryPolicy::Salvage)?;

        // Assert
        assert!(recovered.plan.opened_in_salvage_mode);
        assert_eq!(recovered.plan.local_segments.len(), 1);
        let quarantined = if corrupt_canonical {
            canonical.with_file_name(format!(
                "{}.salvage-retained",
                crate::wal::segment_file_name(1)
            ))
        } else {
            legacy.with_file_name("1.wal.salvage-retained")
        };
        assert!(quarantined.exists());
        assert!(canonical.exists());
        assert!(!legacy.exists());
        assert_eq!(
            std::fs::read(&canonical)?,
            if corrupt_canonical {
                valid
            } else {
                canonical_bytes
            }
        );
    }
    Ok(())
}

#[test]
fn should_truncate_only_incomplete_active_wal_tail_before_virtual_replay() -> MidgeResult<()> {
    // Arrange
    let fixture = Fixture::new()?;
    let valid = framed_wal(4, 8, b"value");
    let mut torn = valid.clone();
    torn.extend_from_slice(&[0xFF; 3]);
    let path = fixture.local(crate::wal::ACTIVE_FILE_NAME, &torn)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    assert_eq!(std::fs::metadata(path)?.len(), valid.len() as u64);
    assert_eq!(
        recovered
            .plan
            .active_wal
            .expect("active metadata")
            .max_sequence,
        4
    );
    assert!(!recovered.plan.opened_in_salvage_mode);
    assert_eq!(
        recovered.fs.metadata(&FsPath::new("wal/wal.log"))?.len,
        valid.len() as u64
    );
    Ok(())
}

#[test]
fn should_reject_resource_limits_without_salvaging_or_truncating_wal_data() -> MidgeResult<()> {
    for active in [false, true] {
        // Arrange
        let mut fixture = Fixture::new()?;
        let bytes = framed_wal(1, 7, &[b'x'; 256]);
        let path = if active {
            fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?
        } else {
            fixture.publish(1, 1, 7, &bytes)?;
            fixture
                .directory
                .path()
                .join("cloud")
                .join(crate::wal::segment_object_key(1, 7))
        };
        let limits = StreamingReplayLimits {
            max_frame_bytes: 32,
            ..limits()
        };

        // Act
        let result = fixture.build_with_limits(RecoveryPolicy::Salvage, limits);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(std::fs::read(path)?, bytes);
    }
    Ok(())
}

#[test]
fn should_validate_catalog_authority_before_exposing_replay_sources() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, 8, b"newer epoch"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"stale epoch"))?;
    fixture.publish(3, 3, 9, &framed_wal(3, 9, b"invalid checksum"))?;
    fixture
        .catalog
        .segments
        .get_mut(&3)
        .expect("publication")
        .content_crc32c ^= 1;
    fixture.local(
        crate::wal::ACTIVE_FILE_NAME,
        &framed_wal(4, 7, b"stale active"),
    )?;
    assert!(fixture.build(RecoveryPolicy::Strict).is_err());

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert!(recovered.plan.opened_in_salvage_mode);
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(recovered.plan.active_wal.is_none());
    assert_eq!(recovered.fs.list_dir(&FsPath::new("wal"))?.len(), 1);
    assert!(!fixture.directory.path().join("local/wal/wal.log").exists());
    assert!(fixture
        .directory
        .path()
        .join("local/wal/wal.log.salvage-retained")
        .exists());
    assert_eq!(recovered.next_segment_id, 4);
    Ok(())
}

#[test]
fn should_preserve_skipped_wal_sources_for_safe_salvage() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(42, 1, 7, &framed_wal(1, 7, b"catalog source"))?;
    fixture
        .catalog
        .segments
        .get_mut(&42)
        .expect("publication")
        .content_crc32c ^= 1;
    fixture.local(&crate::wal::segment_file_name(77), b"invalid sealed WAL")?;
    let mut invalid_active = framed_wal(2, 7, b"invalid active source");
    invalid_active[8] ^= 1;
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &invalid_active)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(recovered.next_segment_id, 78);
    assert!(recovered.plan.remote_segments.is_empty());
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.plan.active_wal.is_none());
    assert!(!active.exists());
    assert_eq!(
        std::fs::read(active.with_file_name("wal.log.salvage-retained"))?,
        invalid_active
    );
    Ok(())
}
