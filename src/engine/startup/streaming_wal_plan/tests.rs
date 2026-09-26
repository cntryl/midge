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

    /// Plans recovery and renames set-aside local WAL, as startup does once
    /// the sequence floor is durable.
    fn build_with_limits(
        &self,
        policy: RecoveryPolicy,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<StreamingCloudWalRecovery> {
        let recovered = self.plan_only(policy, limits)?;
        recovered
            .plan
            .set_aside_local_wal(&self.directory.path().join("local"))?;
        Ok(recovered)
    }

    fn plan_only(
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
    assert!(!fixture
        .directory
        .path()
        .join("local/cloud_recovery")
        .exists());
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

#[test]
fn should_fail_open_without_truncating_active_wal_when_cloud_salvage_read_fails_transiently(
) -> MidgeResult<()> {
    // Arrange: three acknowledged records; reads past the first one fail.
    let fixture = Fixture::new()?;
    let first = framed_wal(1, 7, b"one");
    let mut bytes = first.clone();
    bytes.extend(framed_wal(2, 7, b"two"));
    bytes.extend(framed_wal(3, 7, b"three"));
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;
    let fs: Arc<dyn Fs> = Arc::new(crate::io::transient_read::TransientReadFs {
        inner: crate::io::RealFs::new(fixture.directory.path().join("local"))?,
        fail_from: first.len() as u64,
    });
    let mut plan = CloudWalRecoveryPlan {
        remote_segments: BTreeMap::new(),
        local_segments: BTreeMap::new(),
        active_wal: None,
        opened_in_salvage_mode: false,
        unreplayed_segments: Vec::new(),
        max_unreplayed_sequence: 0,
        set_aside_local_paths: Vec::new(),
    };

    // Act
    let result = active_local_source(&fs, &active, RecoveryPolicy::Salvage, limits(), &mut plan);

    // Assert
    assert!(result.is_err(), "a transient read error must fail the open");
    assert_eq!(
        std::fs::read(&active)?,
        bytes,
        "wal.log must keep every byte"
    );
    Ok(())
}

fn corrupt_publication(fixture: &mut Fixture, id: u64) {
    fixture
        .catalog
        .segments
        .get_mut(&id)
        .expect("publication")
        .content_crc32c ^= 1;
}

#[test]
fn should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one() -> MidgeResult<()>
{
    // Arrange: segment 2 is lost and no local copy can fill the hole.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"two"))?;
    fixture.publish(3, 3, 7, &framed_wal(3, 7, b"three"))?;
    corrupt_publication(&mut fixture, 2);
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &framed_wal(4, 7, b"four"))?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert: replay stops at the hole and history past it is set aside.
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
    assert_eq!(recovered.fs.list_dir(&FsPath::new("wal"))?.len(), 1);
    assert_eq!(
        recovered
            .plan
            .unreplayed_segments
            .iter()
            .map(|segment| segment.segment_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(recovered.plan.active_wal.is_none());
    assert!(!active.exists());
    assert!(active.with_file_name("wal.log.salvage-retained").exists());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 4);
    assert_eq!(recovered.next_segment_id, 4);
    Ok(())
}

#[test]
fn should_lift_sequence_floor_over_verified_prefix_of_corrupt_local_segment_set_aside(
) -> MidgeResult<()> {
    // Arrange: segment 2 is lost; local-only segment 3 holds sequences 7
    // and 8 followed by damage, so it is in neither the catalog nor the plan.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let mut local = framed_wal(7, 7, b"seven");
    local.extend_from_slice(&framed_wal(8, 7, b"eight"));
    let mut damaged = framed_wal(9, 7, b"nine");
    damaged[8] ^= 1;
    local.extend_from_slice(&damaged);
    let path = fixture.local(&crate::wal::segment_file_name(3), &local)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert!(!path.exists());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 8);
    Ok(())
}

#[test]
fn should_lift_floor_over_valid_suffix_in_corrupt_local_segment_set_aside() -> MidgeResult<()> {
    // Arrange: a missing catalog segment puts a later local-only segment
    // aside. Its final frame is valid even though a middle frame is damaged.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let mut local = framed_wal(7, 7, b"seven");
    let mut damaged = framed_wal(8, 7, b"eight");
    damaged[8] ^= 1;
    local.extend_from_slice(&damaged);
    local.extend_from_slice(&framed_wal(10, 7, b"ten"));
    let path = fixture.local(&crate::wal::segment_file_name(3), &local)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert!(!path.exists());
    assert!(recovered.plan.max_unreplayed_sequence >= 10);
    Ok(())
}

#[test]
fn should_lift_floor_over_valid_frames_after_corrupt_frame_in_retained_active_wal(
) -> MidgeResult<()> {
    // Arrange: salvage keeps the first frame and retains the original copy.
    // The third frame is still verifiable after the damaged second frame.
    let fixture = Fixture::new()?;
    let first = framed_wal(1, 7, b"one");
    let mut damaged = framed_wal(2, 7, b"two");
    damaged[8] ^= 1;
    let mut bytes = first.clone();
    bytes.extend_from_slice(&damaged);
    bytes.extend_from_slice(&framed_wal(3, 7, b"three"));
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(
        recovered
            .plan
            .active_wal
            .expect("salvaged prefix")
            .max_sequence,
        1
    );
    assert_eq!(std::fs::read(&active)?, first);
    assert_eq!(
        std::fs::read(active.with_file_name("wal.log.salvage-retained"))?,
        bytes
    );
    assert!(
        recovered.plan.max_unreplayed_sequence >= 3,
        "new writes must not reuse sequence 3 in the retained copy"
    );
    Ok(())
}

#[test]
fn should_not_rename_local_wal_aside_before_startup_persists_its_floor() -> MidgeResult<()> {
    // Arrange: a crash after planning must leave the hole visible, so the
    // next open recomputes the floor instead of losing the local files'
    // sequences.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let segment = fixture.local(
        &crate::wal::segment_file_name(3),
        &framed_wal(7, 7, b"seven"),
    )?;
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &framed_wal(8, 7, b"eight"))?;

    // Act
    let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;

    // Assert
    assert!(segment.exists());
    assert!(active.exists());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 8);
    assert_eq!(recovered.plan.set_aside_local_paths.len(), 2);
    Ok(())
}

#[test]
fn should_replay_every_segment_when_valid_local_copy_fills_cloud_hole() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let second = framed_wal(2, 7, b"two");
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"one"))?;
    fixture.publish(2, 2, 7, &second)?;
    fixture.publish(3, 3, 7, &framed_wal(3, 7, b"three"))?;
    corrupt_publication(&mut fixture, 2);
    fixture.local(&crate::wal::segment_file_name(2), &second)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(recovered.fs.list_dir(&FsPath::new("wal"))?.len(), 3);
    assert!(recovered.plan.unreplayed_segments.is_empty());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 0);
    Ok(())
}

#[test]
fn should_preserve_active_wal_copy_when_cloud_salvage_truncates_corrupt_suffix() -> MidgeResult<()>
{
    // Arrange: three records with a flipped byte inside the second.
    let fixture = Fixture::new()?;
    let first = framed_wal(1, 7, b"one");
    let mut bytes = first.clone();
    let second_start = bytes.len();
    bytes.extend(framed_wal(2, 7, b"two"));
    bytes.extend(framed_wal(3, 7, b"three"));
    bytes[second_start + 8] ^= 1;
    let active = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert!(recovered.plan.opened_in_salvage_mode);
    assert_eq!(std::fs::read(&active)?, first);
    assert_eq!(
        std::fs::read(active.with_file_name("wal.log.salvage-retained"))?,
        bytes,
        "salvage must keep a full copy of the original wal.log"
    );
    Ok(())
}

#[test]
fn should_replay_cataloged_segments_when_corrupt_local_segment_predates_catalog() -> MidgeResult<()>
{
    // Arrange: segment 1 was retired from the catalog (covered by SSTs), but
    // its local copy leaked and is now corrupt.
    let mut fixture = Fixture::new()?;
    fixture.publish(5, 5, 7, &framed_wal(5, 7, b"five"))?;
    fixture.publish(6, 6, 7, &framed_wal(6, 7, b"six"))?;
    let leaked = fixture.local(&crate::wal::segment_file_name(1), b"corrupt leftover")?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(recovered.fs.list_dir(&FsPath::new("wal"))?.len(), 2);
    assert!(recovered.plan.unreplayed_segments.is_empty());
    assert!(leaked.exists(), "the leftover stays where it was");
    Ok(())
}

#[test]
fn should_reject_strict_recovery_when_later_segment_has_lower_writer_epoch() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, 8, b"newer epoch"))?;
    let stale = fixture.local(
        &crate::wal::segment_file_name(2),
        &framed_wal(2, 7, b"stale epoch"),
    )?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("epoch regression")),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert!(stale.exists());
    Ok(())
}

#[test]
fn should_reject_strict_recovery_when_active_wal_has_lower_writer_epoch() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, 8, b"newer epoch"))?;
    let active = fixture.local(
        crate::wal::ACTIVE_FILE_NAME,
        &framed_wal(2, 7, b"stale active"),
    )?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("epoch regression")),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert!(active.exists());
    assert!(!fixture
        .directory
        .path()
        .join("local/wal/wal.log.salvage-retained")
        .exists());
    Ok(())
}

#[test]
fn should_accept_strict_recovery_when_active_wal_has_rising_writer_epochs() -> MidgeResult<()> {
    // Arrange: an active WAL reopened in place after failover appends
    // under a newer epoch; that is its normal shape, not corruption.
    let fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, 7, b"before failover");
    bytes.extend_from_slice(&framed_wal(2, 8, b"after failover"));
    let path = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    let active = recovered.plan.active_wal.expect("active metadata");
    assert_eq!(active.max_sequence, 2);
    assert_eq!(active.writer_epoch, 8);
    assert_eq!(active.record_count, 2);
    assert!(!recovered.plan.opened_in_salvage_mode);
    assert_eq!(std::fs::read(path)?, bytes);
    Ok(())
}

#[test]
fn should_accept_local_sealed_segment_when_fenced_writer_record_is_stale() -> MidgeResult<()> {
    // Arrange: a local segment spans a restart, then the old writer appends
    // late. Cloud objects still use the separate single-epoch contract.
    let fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, 7, b"old writer");
    bytes.extend_from_slice(&framed_wal(2, 8, b"new writer"));
    bytes.extend_from_slice(&framed_wal(3, 7, b"fenced writer"));
    bytes.extend_from_slice(&framed_wal(4, 8, b"new writer again"));
    let path = fixture.local(&crate::wal::segment_file_name(1), &bytes)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    let local = recovered
        .plan
        .local_segments
        .get(&1)
        .expect("local segment");
    assert_eq!(local.max_sequence, 4);
    assert_eq!(local.writer_epoch, 8);
    assert!(!recovered.plan.opened_in_salvage_mode);
    assert_eq!(std::fs::read(path)?, bytes);
    Ok(())
}

#[test]
fn should_recover_valid_active_wal_prefix_when_tail_is_zero_filled() -> MidgeResult<()> {
    // Arrange
    let fixture = Fixture::new()?;
    let valid = framed_wal(3, 7, b"value");
    let mut padded = valid.clone();
    padded.extend_from_slice(&[0; 64]);
    let path = fixture.local(crate::wal::ACTIVE_FILE_NAME, &padded)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    assert_eq!(
        recovered
            .plan
            .active_wal
            .expect("active metadata")
            .max_sequence,
        3
    );
    assert_eq!(std::fs::metadata(path)?.len(), valid.len() as u64);
    Ok(())
}

#[test]
fn should_fail_strict_recovery_without_truncating_when_corrupt_length_hides_valid_suffix(
) -> MidgeResult<()> {
    // Arrange
    let fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, 7, b"first");
    bytes.extend_from_slice(&framed_wal(2, 7, b"verified suffix"));
    let corrupt_length = u32::try_from(bytes.len()).expect("WAL length fits u32");
    bytes[..4].copy_from_slice(&corrupt_length.to_le_bytes());
    let path = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("hides a verified later frame")),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert_eq!(std::fs::read(path)?, bytes);
    Ok(())
}

#[test]
fn should_fail_strict_recovery_naming_object_when_cataloged_segment_is_missing() -> MidgeResult<()>
{
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, 7, b"present"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, 7, b"missing"))?;
    let key = fixture.catalog.segments[&2].object_key.clone();
    std::fs::remove_file(fixture.directory.path().join("cloud").join(&key))?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    let error = result.err().expect("missing publication fails Strict");
    assert!(
        error.to_string().contains(&key),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn should_keep_every_acknowledged_record_when_fenced_writer_interleaved_into_active_wal(
) -> MidgeResult<()> {
    for policy in [RecoveryPolicy::Strict, RecoveryPolicy::Salvage] {
        // Arrange: a paused, fenced writer (epoch 5) appended into the new
        // writer's file. Replay skips its record as stale, exactly as local
        // replay does, so inspection must not reject or truncate the file.
        let fixture = Fixture::new()?;
        let bytes = [
            framed_wal(1, 6, b"first"),
            framed_wal(2, 6, b"second"),
            framed_wal(3, 5, b"fenced"),
            framed_wal(4, 6, b"third"),
            framed_wal(5, 6, b"fourth"),
        ]
        .concat();
        let path = fixture.local(crate::wal::ACTIVE_FILE_NAME, &bytes)?;

        // Act
        let recovered = fixture.build(policy)?;

        // Assert
        let active = recovered.plan.active_wal.expect("active metadata");
        assert_eq!(active.valid_bytes, bytes.len(), "{policy:?}");
        assert_eq!(active.writer_epoch, 6);
        assert_eq!(active.max_sequence, 5);
        assert!(!recovered.plan.opened_in_salvage_mode);
        assert_eq!(std::fs::read(&path)?, bytes);
    }
    Ok(())
}
