use super::*;
use crate::io::{FsError, OpenOptions};
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::format::ChainLink;
use crate::wal::planner::HistoryAction;
use bytes::Bytes;
use std::collections::BTreeSet;

/// The frame of a put at `sequence`: a segment body the fixture wraps.
fn framed_wal(sequence: u64, value: &[u8]) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::copy_from_slice(value)),
        sequence,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("encode WAL frame");
    bytes
}

/// `body` behind the header of the segment file at `path`: what an open
/// segment the fixture wrote with `body` holds after a cut to `body`.
fn with_header(path: &std::path::Path, body: &[u8]) -> MidgeResult<Vec<u8>> {
    let mut bytes = std::fs::read(path)?;
    bytes.truncate(crate::wal::format::HEADER_LEN);
    bytes.extend_from_slice(body);
    Ok(bytes)
}

/// The last sequence replay reaches through the open segment `recovered`
/// planned, or `None` when it plans none.
fn open_tail_sequence(recovered: &StreamingCloudWalRecovery) -> Option<u64> {
    recovered.segments.open_segment()?;
    crate::wal::recovery::validate_wal_with_policy(
        recovered.fs.as_ref(),
        &recovered.segments,
        crate::wal::recovery::ReplayPolicy::Strict,
        None,
    )
    .expect("replay the plan")
    .max_sequence
}

fn limits() -> StreamingReplayLimits {
    StreamingReplayLimits {
        max_frame_bytes: 128 * 1_024,
        max_pending_txn_bytes: 256 * 1_024,
        max_memtable_encoded_bytes: 256 * 1_024,
        target_memtable_encoded_bytes: 256 * 1_024,
    }
}

/// The epoch a fixture's writer uses unless a test names another.
const FIXTURE_EPOCH: u64 = 7;

/// A cloud WAL with local cache. Segment bodies a test hands it become v2
/// segments: one published links the catalog's head, as a writer's
/// publication must, and one written locally links the newest sealed
/// segment below it, so a body published and written locally becomes
/// identical bytes in both places. The catalog lives in the cloud store,
/// both copies, as startup reads and commits it.
struct Fixture {
    directory: tempfile::TempDir,
    cloud: Arc<dyn StorageBackend>,
    /// Sealed ledger members the fixture wrote locally.
    sealed: BTreeSet<u64>,
    /// The open ledger member, once the fixture wrote it.
    open: Option<u64>,
    /// The link each sealed segment ends with, by id.
    links: BTreeMap<u64, ChainLink>,
    /// The epoch each published segment's header names.
    epochs: BTreeMap<u64, u64>,
}

impl Fixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir_all(directory.path().join("local/wal"))?;
        let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("cloud"),
        )?);
        let fixture = Self {
            directory,
            cloud,
            sealed: BTreeSet::new(),
            open: None,
            links: BTreeMap::new(),
            epochs: BTreeMap::new(),
        };
        fixture.save_catalog(&WalPublicationCatalog::empty(9).expect("catalog"))?;
        Ok(fixture)
    }

    /// The catalog as the cloud store holds it.
    fn catalog(&self) -> WalPublicationCatalog {
        WalPublicationCatalog::decode(
            &std::fs::read(self.catalog_path()).expect("read the fixture catalog"),
        )
        .expect("decode the fixture catalog")
    }

    fn catalog_path(&self) -> std::path::PathBuf {
        self.directory
            .path()
            .join("cloud")
            .join(crate::wal::cloud_catalog::OBJECT_KEY)
    }

    /// Store `catalog` as both catalog copies.
    fn save_catalog(&self, catalog: &WalPublicationCatalog) -> MidgeResult<()> {
        let bytes = catalog.encode().map_err(MidgeError::Internal)?;
        for key in [
            crate::wal::cloud_catalog::OBJECT_KEY,
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        ] {
            let path = self.directory.path().join("cloud").join(key);
            std::fs::create_dir_all(path.parent().expect("catalog parent"))?;
            std::fs::write(path, &bytes)?;
        }
        Ok(())
    }

    /// Apply `edit` to the stored catalog.
    fn edit_catalog(&self, edit: impl FnOnce(&mut WalPublicationCatalog)) {
        let mut catalog = self.catalog();
        edit(&mut catalog);
        self.save_catalog(&catalog)
            .expect("store the fixture catalog");
    }

    /// The newest epoch any published segment names.
    fn current_epoch(&self) -> u64 {
        self.epochs.values().copied().max().unwrap_or(FIXTURE_EPOCH)
    }

    /// The newest sealed link below `id`.
    fn link_below(&self, id: u64) -> ChainLink {
        self.links
            .range(..id)
            .next_back()
            .map_or(ChainLink::GENESIS, |(_, link)| *link)
    }

    /// Segment `id` holding `body`, written by `epoch` and linked to the
    /// newest sealed segment below it; sealed with a footer when `sealed`.
    /// A body that is already a segment stays as it is.
    fn wrap(&mut self, id: u64, epoch: u64, body: &[u8], sealed: bool) -> Vec<u8> {
        if crate::wal::format::SegmentHeader::decode(body).is_ok() {
            return body.to_vec();
        }
        let builder =
            crate::wal::format::segment::SegmentBuilder::new(id, epoch, self.link_below(id))
                .bytes(body);
        if sealed {
            let (bytes, link) = builder.seal();
            self.links.insert(id, link);
            bytes
        } else {
            builder.open()
        }
    }

    /// Publish segment `id` holding `body`, written by `epoch` and linked to
    /// the catalog's head, which must hold sequences through `sequence`.
    fn publish(&mut self, id: u64, sequence: u64, epoch: u64, body: &[u8]) -> MidgeResult<()> {
        let bytes = if crate::wal::format::SegmentHeader::decode(body).is_ok() {
            body.to_vec()
        } else {
            crate::wal::format::segment::SegmentBuilder::new(id, epoch, self.catalog().head())
                .bytes(body)
                .seal()
                .0
        };
        let entry = CatalogEntry::from_sealed_bytes(&bytes).map_err(MidgeError::Corruption)?;
        assert_eq!(
            entry.max_sequence(),
            sequence,
            "a fixture publication's last sequence"
        );
        self.publish_entry(entry, &bytes)
    }

    /// Publish `entry` naming the object `bytes`, as the catalog's next
    /// segment.
    fn publish_entry(&mut self, entry: CatalogEntry, bytes: &[u8]) -> MidgeResult<()> {
        self.links.insert(entry.id(), entry.link());
        self.epochs.insert(entry.id(), entry.epoch());
        let path = self.directory.path().join("cloud").join(entry.object_key());
        std::fs::create_dir_all(path.parent().expect("remote parent"))?;
        std::fs::write(path, bytes)?;
        let mut catalog = self.catalog();
        if entry.epoch() > catalog.fencing_epoch() {
            catalog
                .fence_to(entry.epoch())
                .expect("a later epoch fences the catalog");
        }
        catalog
            .append_run(catalog.fencing_epoch(), &[entry])
            .map_err(MidgeError::from)?;
        self.save_catalog(&catalog)
    }

    /// The host file of local segment `id`.
    fn segment(&self, id: u64) -> std::path::PathBuf {
        self.directory
            .path()
            .join("local/wal")
            .join(crate::wal::segment_file_name(id))
    }

    /// Write sealed ledger member `id` holding `body`, written by the epoch
    /// that published it, if any, else the newest one.
    fn local(&mut self, id: u64, body: &[u8]) -> MidgeResult<std::path::PathBuf> {
        let epoch = self
            .epochs
            .get(&id)
            .copied()
            .unwrap_or_else(|| self.current_epoch());
        self.local_in_epoch(id, epoch, body)
    }

    /// Write sealed ledger member `id` holding `body`, written by `epoch`.
    fn local_in_epoch(
        &mut self,
        id: u64,
        epoch: u64,
        body: &[u8],
    ) -> MidgeResult<std::path::PathBuf> {
        self.sealed.insert(id);
        let bytes = self.wrap(id, epoch, body, true);
        let path = self.segment(id);
        std::fs::write(&path, bytes)?;
        Ok(path)
    }

    /// Write the open ledger member holding `body`, written by the newest
    /// epoch; an empty body leaves it holding only its header.
    fn active(&mut self, body: &[u8]) -> MidgeResult<std::path::PathBuf> {
        let epoch = self.current_epoch();
        self.active_in_epoch(epoch, body)
    }

    /// Write the open ledger member holding `body`, written by `epoch`.
    fn active_in_epoch(&mut self, epoch: u64, body: &[u8]) -> MidgeResult<std::path::PathBuf> {
        let id = self.open_segment();
        self.open = Some(id);
        let bytes = self.wrap(id, epoch, body, false);
        let path = self.segment(id);
        std::fs::write(&path, bytes)?;
        Ok(path)
    }

    /// The open member: unless written, past every id the catalog handed
    /// out and every sealed one, as the writer that published them left it.
    fn open_segment(&self) -> u64 {
        self.open.unwrap_or_else(|| {
            let local = self.sealed.iter().next_back().map_or(1, |id| id + 1);
            self.catalog().next_segment_id().max(local)
        })
    }

    /// The WAL ledger as startup opens it: recorded from the fixture's
    /// members the first time, then as earlier opens left it, with files it
    /// does not name swept. An open member the fixture never wrote holds
    /// only its header, linked to the newest sealed segment, as a writer
    /// leaves it.
    fn history(&self) -> MidgeResult<crate::runtime::wal_history::WalHistory> {
        let wal: Arc<dyn Fs> = Arc::new(
            crate::io::RealFs::new(self.directory.path().join("local/wal"))
                .map_err(FsError::into_midge)?,
        );
        if !wal
            .exists(&FsPath::new(crate::wal::ledger_store::LEDGER_FILE))
            .map_err(FsError::into_midge)?
        {
            let open = self.open_segment();
            if self.open.is_none() && !self.segment(open).exists() {
                let header = crate::wal::format::segment::SegmentBuilder::new(
                    open,
                    self.current_epoch(),
                    self.link_below(open),
                )
                .open();
                std::fs::write(self.segment(open), header)?;
            }
            let sealed: Vec<u64> = self.sealed.iter().copied().collect();
            crate::runtime::wal_history::fixture::record(&wal, &sealed, open)?;
        }
        // Startup opens the ledger under its lease, at least the catalog's
        // fencing epoch and never below an epoch the members name.
        let epoch = self.catalog().fencing_epoch().max(self.current_epoch());
        crate::runtime::wal_history::WalHistory::open(wal, 1, epoch)
    }

    /// Retire every cataloged segment through `id`, as pruning does once
    /// SSTs cover them.
    fn retire_through(&mut self, id: u64) {
        self.edit_catalog(|catalog| {
            let retired: Vec<CatalogEntry> = catalog
                .entries()
                .iter()
                .filter(|entry| entry.id() <= id)
                .cloned()
                .collect();
            catalog
                .retire_prefix(
                    catalog.fencing_epoch(),
                    &retired,
                    crate::lease::SharedGenerationId::new(1).unwrap(),
                )
                .expect("retire the catalog's oldest segments");
        });
    }

    /// Roll the catalog back to its segments through `id`, as a stale copy
    /// would: the later ones vanish with no sequence floor covering them.
    fn roll_back_to(&mut self, id: u64) {
        self.edit_catalog(|catalog| {
            let lost: Vec<CatalogEntry> = catalog
                .entries()
                .iter()
                .filter(|entry| entry.id() > id)
                .cloned()
                .collect();
            catalog
                .set_aside_suffix(
                    catalog.fencing_epoch(),
                    lost.first().map(CatalogEntry::id),
                    &lost,
                    0,
                )
                .expect("drop the catalog's newest segments");
        });
    }

    /// Where salvage copied segment `id` aside, if it did.
    fn salvaged(&self, id: u64) -> Option<std::path::PathBuf> {
        std::fs::read_dir(self.directory.path().join("local/wal"))
            .ok()?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("salvaged-"))
            .map(|entry| entry.path().join(crate::wal::segment_file_name(id)))
            .find(|path| path.exists())
    }

    fn build(&self, policy: RecoveryPolicy) -> MidgeResult<StreamingCloudWalRecovery> {
        self.build_with_limits(policy, limits())
    }

    /// Plans recovery and makes the plan durable, as startup does.
    fn build_with_limits(
        &self,
        policy: RecoveryPolicy,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<StreamingCloudWalRecovery> {
        let history = self.history()?;
        let recovered = StreamingCloudWalRecovery::build(
            &crate::io::real::test_root(self.directory.path().join("local")),
            Some(history.ledger()),
            &self.cloud,
            &self.catalog(),
            policy,
            Duration::from_secs(5),
            127,
            limits,
        )?;
        drop(commit_as_startup(
            self,
            &recovered,
            crate::runtime::cloud_startup::CacheHistory::Existing(history),
        )?);
        Ok(recovered)
    }

    /// Plans recovery through `local`, a filesystem rooted at the database.
    fn plan_with_fs(
        &self,
        policy: RecoveryPolicy,
        local: &Arc<dyn Fs>,
    ) -> MidgeResult<StreamingCloudWalRecovery> {
        let history = self.history()?;
        StreamingCloudWalRecovery::build(
            local,
            Some(history.ledger()),
            &self.cloud,
            &self.catalog(),
            policy,
            Duration::from_secs(5),
            127,
            limits(),
        )
    }

    fn plan_only(
        &self,
        policy: RecoveryPolicy,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<StreamingCloudWalRecovery> {
        let history = self.history()?;
        StreamingCloudWalRecovery::build(
            &crate::io::real::test_root(self.directory.path().join("local")),
            Some(history.ledger()),
            &self.cloud,
            &self.catalog(),
            policy,
            Duration::from_secs(5),
            127,
            limits,
        )
    }
}

#[test]
fn should_serve_planned_wal_sources_without_staging_copies() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, &vec![b'a'; 32 * 1_024]);
    let second = framed_wal(2, &vec![b'b'; 32 * 1_024]);
    fixture.publish(1, 1, 7, &first)?;
    fixture.publish(2, 2, 7, &second)?;
    let local_first = fixture.local(1, &first)?;
    let local_second = fixture.local(2, &second)?;
    // The local copies are the sealed segments the fixture published.
    let (first, second) = (std::fs::read(&local_first)?, std::fs::read(&local_second)?);
    drop(fixture.history()?);
    #[cfg(unix)]
    let original_inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&local_first)?.ino()
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(&local_first)?.ino(), original_inode);
    }
    assert_eq!(std::fs::read(&local_first)?, first);
    assert_eq!(std::fs::read(&local_second)?, second);
    assert_eq!(
        recovered
            .fs
            .list_dir(&FsPath::new("wal"))
            .map_err(FsError::into_midge)?
            .len(),
        2
    );
    let file = recovered
        .fs
        .open_read(&FsPath::new(crate::wal::segment_file_name(2)))
        .map_err(FsError::into_midge)?;
    assert_eq!(
        file.read_at(0, second.len() as u64)
            .map_err(FsError::into_midge)?
            .as_ref(),
        second
    );
    Ok(())
}

#[test]
fn should_truncate_only_incomplete_active_wal_tail_before_virtual_replay() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let valid = framed_wal(4, b"value");
    let path = fixture.active(&valid)?;
    let history = fixture.history()?;
    drop(history);
    let mut active = std::fs::OpenOptions::new().append(true).open(&path)?;
    std::io::Write::write_all(&mut active, &[0xFF; 3])?;
    drop(active);

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    let cut = (crate::wal::format::HEADER_LEN + valid.len()) as u64;
    assert_eq!(std::fs::metadata(path)?.len(), cut);
    assert_eq!(open_tail_sequence(&recovered).expect("an open segment"), 4);
    assert!(!recovered.plan.opened_in_salvage_mode);
    assert_eq!(
        recovered
            .fs
            .metadata(&crate::wal::segment_key(1))
            .map_err(FsError::into_midge)?
            .len,
        cut
    );
    Ok(())
}

#[test]
fn should_reject_resource_limits_without_salvaging_or_truncating_wal_data() -> MidgeResult<()> {
    for active in [false, true] {
        // Arrange
        let mut fixture = Fixture::new()?;
        let bytes = framed_wal(1, &[b'x'; 256]);
        let path = if active {
            fixture.active(&bytes)?
        } else {
            fixture.publish(1, 1, 7, &bytes)?;
            fixture
                .directory
                .path()
                .join("cloud")
                .join(crate::wal::segment_object_key(1, 7))
        };
        let written = std::fs::read(&path)?;
        let limits = StreamingReplayLimits {
            max_frame_bytes: 32,
            ..limits()
        };

        // Act
        let result = fixture.build_with_limits(RecoveryPolicy::Salvage, limits);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(std::fs::read(path)?, written);
    }
    Ok(())
}

#[test]
fn should_validate_catalog_authority_before_exposing_replay_sources() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, b"first"))?;
    fixture.publish(2, 2, 8, &framed_wal(2, b"invalid checksum"))?;
    fixture.publish(3, 3, 9, &framed_wal(3, b"after the hole"))?;
    corrupt_publication(&mut fixture, 2);
    let active = fixture.active(&framed_wal(4, b"after the hole"))?;
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
    assert!(recovered.segments.open_segment().is_none());
    assert_eq!(
        recovered
            .fs
            .list_dir(&FsPath::new("wal"))
            .map_err(FsError::into_midge)?
            .len(),
        1
    );
    assert!(!active.exists());
    assert!(fixture.salvaged(4).is_some());
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase {
            open_at_least: 4,
            ..
        }
    ));
    assert_eq!(fixture.history()?.ledger().open_segment(), 5);
    Ok(())
}

#[test]
fn should_preserve_skipped_wal_sources_for_safe_salvage() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(42, 1, 7, &framed_wal(1, b"catalog source"))?;
    corrupt_publication(&mut fixture, 42);
    let sealed = fixture.local(77, b"invalid sealed WAL")?;
    let mut invalid_active = framed_wal(2, b"invalid active source");
    invalid_active[8] ^= 1;
    let active = fixture.active(&invalid_active)?;
    // Recording the ledger seals the invalid member with the fixture's
    // lenient footer; keep the bytes the ledger names.
    drop(fixture.history()?);
    let (sealed_bytes, active_bytes) = (std::fs::read(&sealed)?, std::fs::read(&active)?);

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert: the hole is the catalog's object 42, so every member is set
    // aside whole, the open one included, and nothing is cut.
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase {
            open_at_least: 43,
            ..
        }
    ));
    assert_eq!(fixture.history()?.ledger().open_segment(), 79);
    assert!(recovered.plan.remote_segments.is_empty());
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.segments.open_segment().is_none());
    assert!(!sealed.exists());
    assert!(!active.exists());
    assert_eq!(
        std::fs::read(fixture.salvaged(77).expect("sealed member set aside"))?,
        sealed_bytes
    );
    assert_eq!(
        std::fs::read(fixture.salvaged(78).expect("open member set aside"))?,
        active_bytes
    );
    Ok(())
}

#[test]
fn should_fail_open_without_truncating_active_wal_when_cloud_salvage_read_fails_transiently(
) -> MidgeResult<()> {
    // Arrange: three acknowledged records; reads past the first one fail.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    let mut bytes = first.clone();
    bytes.extend(framed_wal(2, b"two"));
    bytes.extend(framed_wal(3, b"three"));
    let active = fixture.active(&bytes)?;
    let written = std::fs::read(&active)?;
    let fs: Arc<dyn Fs> = Arc::new(crate::io::transient_read::TransientReadFs {
        inner: crate::io::RealFs::new(fixture.directory.path().join("local"))
            .map_err(FsError::into_midge)?,
        fail_from: (crate::wal::format::HEADER_LEN + first.len()) as u64,
    });

    // Act
    let result = fixture.plan_with_fs(RecoveryPolicy::Salvage, &fs);

    // Assert
    assert!(result.is_err(), "a transient read error must fail the open");
    assert_eq!(
        std::fs::read(&active)?,
        written,
        "the open segment must keep every byte"
    );
    Ok(())
}

fn corrupt_publication(fixture: &mut Fixture, id: u64) {
    // Damage a record inside the published object, so its bytes no longer
    // match the catalog's checksum.
    let key = fixture
        .catalog()
        .entry(id)
        .expect("publication")
        .object_key();
    let path = fixture.directory.path().join("cloud").join(key);
    let mut bytes = std::fs::read(&path).expect("read publication");
    bytes[crate::wal::format::HEADER_LEN + 8] ^= 1;
    std::fs::write(path, bytes).expect("damage publication");
}

#[test]
fn should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one() -> MidgeResult<()>
{
    // Arrange: segment 2 is lost and no local copy can fill the hole.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    fixture.publish(3, 3, 7, &framed_wal(3, b"three"))?;
    corrupt_publication(&mut fixture, 2);
    let active = fixture.active(&framed_wal(4, b"four"))?;

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
    assert_eq!(
        recovered
            .fs
            .list_dir(&FsPath::new("wal"))
            .map_err(FsError::into_midge)?
            .len(),
        1
    );
    assert_eq!(
        recovered
            .plan
            .set_aside_entries
            .iter()
            .map(CatalogEntry::id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(recovered.segments.open_segment().is_none());
    assert!(!active.exists());
    assert!(fixture.salvaged(4).is_some());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 4);
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase {
            open_at_least: 4,
            ..
        }
    ));
    // One catalog change set 2 and 3 aside behind a floor over all of it.
    let catalog = fixture.catalog();
    assert_eq!(
        catalog
            .entries()
            .iter()
            .map(CatalogEntry::id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(catalog.seq_floor(), 4);
    Ok(())
}

#[test]
fn should_lift_sequence_floor_over_verified_prefix_of_corrupt_local_segment_set_aside(
) -> MidgeResult<()> {
    // Arrange: segment 2 is lost; local-only segment 3 holds sequences 7
    // and 8 followed by damage, so it is in neither the catalog nor the plan.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let mut local = framed_wal(7, b"seven");
    local.extend_from_slice(&framed_wal(8, b"eight"));
    let mut damaged = framed_wal(9, b"nine");
    damaged[8] ^= 1;
    local.extend_from_slice(&damaged);
    let path = fixture.local(3, &local)?;

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
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let mut local = framed_wal(7, b"seven");
    let mut damaged = framed_wal(8, b"eight");
    damaged[8] ^= 1;
    local.extend_from_slice(&damaged);
    local.extend_from_slice(&framed_wal(10, b"ten"));
    let path = fixture.local(3, &local)?;

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
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    let mut damaged = framed_wal(2, b"two");
    damaged[8] ^= 1;
    let mut bytes = first.clone();
    bytes.extend_from_slice(&damaged);
    bytes.extend_from_slice(&framed_wal(3, b"three"));
    let active = fixture.active(&bytes)?;
    let written = std::fs::read(&active)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(open_tail_sequence(&recovered).expect("an open segment"), 1);
    assert_eq!(std::fs::read(&active)?, with_header(&active, &first)?);
    assert_eq!(
        std::fs::read(active.with_file_name(format!(
            "{}.salvage-retained",
            crate::wal::segment_file_name(1)
        )))?,
        written
    );
    assert!(
        recovered.plan.max_unreplayed_sequence >= 3,
        "new writes must not reuse sequence 3 in the retained copy"
    );
    Ok(())
}

#[test]
fn should_not_set_local_wal_aside_before_startup_persists_its_floor() -> MidgeResult<()> {
    // Arrange: a crash after planning must leave the hole visible, so the
    // next open recomputes the floor instead of losing the local files'
    // sequences.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    corrupt_publication(&mut fixture, 2);
    let segment = fixture.local(3, &framed_wal(7, b"seven"))?;
    let active = fixture.active(&framed_wal(8, b"eight"))?;

    // Act
    let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;

    // Assert
    assert!(segment.exists());
    assert!(active.exists());
    assert_eq!(recovered.plan.max_unreplayed_sequence, 8);
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase {
            open_at_least: 3,
            ..
        }
    ));
    assert_eq!(
        fixture.history()?.ledger().members().collect::<Vec<_>>(),
        vec![3, 4],
        "planning must leave every member in the ledger"
    );
    Ok(())
}

#[test]
fn should_replay_every_segment_when_valid_local_copy_fills_cloud_hole() -> MidgeResult<()> {
    // Arrange: the cache still holds segments 2 and 3, which it sealed.
    let mut fixture = Fixture::new()?;
    let second = framed_wal(2, b"two");
    let third = framed_wal(3, b"three");
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &second)?;
    fixture.publish(3, 3, 7, &third)?;
    corrupt_publication(&mut fixture, 2);
    fixture.local(2, &second)?;
    fixture.local(3, &third)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(
        recovered
            .fs
            .list_dir(&FsPath::new("wal"))
            .map_err(FsError::into_midge)?
            .len(),
        3
    );
    assert_eq!(recovered.plan.retire_catalog_from, None);
    assert_eq!(
        recovered
            .plan
            .local_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![2]
    );
    // New writes start past the catalog's head.
    assert_eq!(recovered.plan.max_unreplayed_sequence, 3);
    Ok(())
}

#[test]
fn should_preserve_active_wal_copy_when_cloud_salvage_truncates_corrupt_suffix() -> MidgeResult<()>
{
    // Arrange: three records with a flipped byte inside the second.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    let mut bytes = first.clone();
    let second_start = bytes.len();
    bytes.extend(framed_wal(2, b"two"));
    bytes.extend(framed_wal(3, b"three"));
    bytes[second_start + 8] ^= 1;
    let active = fixture.active(&bytes)?;
    let written = std::fs::read(&active)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert!(recovered.plan.opened_in_salvage_mode);
    assert_eq!(std::fs::read(&active)?, with_header(&active, &first)?);
    assert_eq!(
        std::fs::read(active.with_file_name(format!(
            "{}.salvage-retained",
            crate::wal::segment_file_name(1)
        )))?,
        written,
        "salvage must keep a full copy of the original open segment"
    );
    Ok(())
}

#[test]
fn should_replay_cataloged_segments_when_corrupt_local_segment_predates_catalog() -> MidgeResult<()>
{
    // Arrange: segment 1 was retired from the catalog (covered by SSTs) and
    // from the ledger, but its local copy leaked and is now corrupt.
    let mut fixture = Fixture::new()?;
    fixture.publish(5, 5, 7, &framed_wal(5, b"five"))?;
    fixture.publish(6, 6, 7, &framed_wal(6, b"six"))?;
    std::fs::write(fixture.segment(1), b"corrupt leftover")?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert
    assert_eq!(
        recovered
            .fs
            .list_dir(&FsPath::new("wal"))
            .map_err(FsError::into_midge)?
            .len(),
        2
    );
    assert_eq!(recovered.plan.retire_catalog_from, None);
    assert!(recovered.plan.local_segments.is_empty());
    assert!(!recovered.plan.opened_in_salvage_mode);
    Ok(())
}

#[test]
fn should_reject_strict_recovery_when_later_segment_has_lower_writer_epoch() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, b"newer epoch"))?;
    let stale = fixture.local_in_epoch(2, 7, &framed_wal(2, b"stale epoch"))?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("below epoch")),
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
    fixture.publish(1, 1, 8, &framed_wal(1, b"newer epoch"))?;
    let active = fixture.active_in_epoch(7, &framed_wal(2, b"stale active"))?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("below epoch")),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert!(active.exists());
    assert!(!active
        .with_file_name(format!(
            "{}.salvage-retained",
            crate::wal::segment_file_name(2)
        ))
        .exists());
    Ok(())
}

#[test]
fn should_recover_valid_active_wal_prefix_when_tail_is_zero_filled() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let valid = framed_wal(3, b"value");
    let path = fixture.active(&valid)?;
    let history = fixture.history()?;
    drop(history);
    let mut active = std::fs::OpenOptions::new().append(true).open(&path)?;
    std::io::Write::write_all(&mut active, &[0; 64])?;
    drop(active);

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    assert_eq!(open_tail_sequence(&recovered).expect("an open segment"), 3);
    assert_eq!(
        std::fs::metadata(path)?.len(),
        (crate::wal::format::HEADER_LEN + valid.len()) as u64
    );
    Ok(())
}

#[test]
fn should_reject_strict_recovery_when_incomplete_active_tail_crosses_durable_boundary(
) -> MidgeResult<()> {
    // Arrange: the ledger records the complete file length as synced.
    let mut fixture = Fixture::new()?;
    let valid = framed_wal(5, b"acknowledged");
    let mut torn = valid;
    torn.extend_from_slice(&[0xFF; 3]);
    let path = fixture.active(&torn)?;
    let before = std::fs::metadata(&path)?.len();

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert: synced bytes are never discarded as an interrupted tail.
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    assert_eq!(std::fs::metadata(path)?.len(), before);
    Ok(())
}

#[test]
fn should_fail_strict_recovery_without_truncating_when_corrupt_length_hides_valid_suffix(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, b"first");
    bytes.extend_from_slice(&framed_wal(2, b"verified suffix"));
    let corrupt_length = u32::try_from(bytes.len()).expect("WAL length fits u32");
    bytes[..4].copy_from_slice(&corrupt_length.to_le_bytes());
    let path = fixture.active(&bytes)?;
    let written = std::fs::read(&path)?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(message)) if message.contains("hides a verified later frame")),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert_eq!(std::fs::read(path)?, written);
    Ok(())
}

#[test]
fn should_fail_strict_recovery_naming_object_when_cataloged_segment_is_missing() -> MidgeResult<()>
{
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"present"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"missing"))?;
    let key = fixture
        .catalog()
        .entry(2)
        .expect("publication")
        .object_key();
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

/// A real filesystem whose file handles refuse to truncate, standing in for a
/// disk that fails the salvage cut.
struct TruncateFailingFs {
    inner: crate::io::RealFs,
}

struct TruncateFailingFile<'a> {
    inner: Box<dyn crate::io::File + 'a>,
}

impl crate::io::ReadFile for TruncateFailingFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> crate::io::FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }

    fn len(&self) -> crate::io::FsResult<u64> {
        self.inner.len()
    }
}

impl crate::io::File for TruncateFailingFile<'_> {
    fn write_at(&mut self, offset: u64, data: Bytes) -> crate::io::FsResult<()> {
        self.inner.write_at(offset, data)
    }

    fn truncate(&mut self, len: u64) -> crate::io::FsResult<()> {
        Err(FsError::NoSpace(format!(
            "injected truncate failure at {len} bytes"
        )))
    }

    fn append(&mut self, data: Bytes) -> crate::io::FsResult<u64> {
        self.inner.append(data)
    }

    fn sync(&mut self, dur: crate::io::Durability) -> crate::io::FsResult<()> {
        self.inner.sync(dur)
    }
}

impl ReadFs for TruncateFailingFs {
    fn open_read(&self, path: &FsPath) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> crate::io::FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> crate::io::FsResult<crate::io::traits::Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl Fs for TruncateFailingFs {
    fn open(
        &self,
        path: &FsPath,
        opts: OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        Ok(Box::new(TruncateFailingFile {
            inner: self.inner.open(path, opts)?,
        }))
    }

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        opts: OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
        self.inner.open_persistent_handle(path, opts)
    }

    fn remove_file(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_file(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, dur: crate::io::Durability) -> crate::io::FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> crate::io::FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

#[test]
fn should_preserve_wal_bytes_when_salvage_truncate_fails() -> MidgeResult<()> {
    // Arrange: three records with a flipped byte inside the second, so
    // salvage must cut the active WAL back to the first.
    let mut fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, b"one");
    let second_start = bytes.len();
    bytes.extend(framed_wal(2, b"two"));
    bytes.extend(framed_wal(3, b"three"));
    bytes[second_start + 8] ^= 1;
    let active = fixture.active(&bytes)?;
    let written = std::fs::read(&active)?;
    let local: Arc<dyn Fs> = Arc::new(TruncateFailingFs {
        inner: crate::io::RealFs::new(fixture.directory.path().join("local"))
            .map_err(FsError::into_midge)?,
    });

    // Act
    let recovered = fixture.plan_with_fs(RecoveryPolicy::Salvage, &local)?;
    assert_eq!(
        std::fs::read(&active)?,
        written,
        "planning must be read-only"
    );
    let history = crate::runtime::wal_history::WalHistory::load(
        crate::io::subdirectory(Arc::clone(&local), "wal"),
        fixture.current_epoch(),
    )?
    .expect("fixture history");
    let result = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(history),
    );

    // Assert
    assert!(
        result.is_err(),
        "a failed salvage truncation must fail recovery instead of replaying"
    );
    assert_eq!(
        std::fs::read(&active)?,
        written,
        "a failed truncation must leave the active WAL whole"
    );
    assert_eq!(
        std::fs::read(active.with_file_name(format!(
            "{}.salvage-retained",
            crate::wal::segment_file_name(1)
        )))?,
        written,
        "the retained copy must exist before the cut is attempted"
    );
    Ok(())
}

#[test]
fn should_stage_stale_active_wal_until_sequence_floor_is_durable() -> MidgeResult<()> {
    // Arrange
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 8, &framed_wal(1, b"one"))?;
    let active = fixture.active_in_epoch(7, &framed_wal(2, b"two"))?;

    // Act
    let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;

    // Assert
    assert_eq!(recovered.plan.max_unreplayed_sequence, 2);
    assert!(
        active.exists(),
        "planning must preserve the only durable sequence evidence"
    );
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase {
            open_at_least: 2,
            ..
        }
    ));
    assert!(recovered.segments.open_segment().is_none());
    Ok(())
}

#[cfg(feature = "failpoints")]
mod crashes;

#[derive(Default)]
struct PlannerEvidence {
    successful_heads: std::sync::atomic::AtomicUsize,
    deliveries_before_expiry: std::sync::atomic::AtomicUsize,
    deliveries_after_expiry: std::sync::atomic::AtomicUsize,
    mutations: std::sync::atomic::AtomicUsize,
}

struct DelayedPlannerBackend {
    inner: Arc<dyn StorageBackend>,
    evidence: Arc<PlannerEvidence>,
    deadline: OperationDeadline,
}

impl StorageBackend for DelayedPlannerBackend {
    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.submit_range_head_request(request, tx);
        let event = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("actual filesystem HEAD");
        assert!(matches!(
            &event,
            crate::storage::StorageEvent::HeadComplete {
                result: crate::storage::StorageOutcome::Ok(_),
                ..
            }
        ));
        let completed = self
            .evidence
            .successful_heads
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        if completed == 2 {
            // Admission of this second real HEAD proves the first segment's
            // checksum and inspection finished. Expire the original budget
            // here, rather than assuming two calibrated sleeps fit the runner.
            while !self.deadline.is_expired() {
                std::thread::sleep(self.deadline.remaining());
            }
        }
        let deliveries = if self.deadline.is_expired() {
            &self.evidence.deliveries_after_expiry
        } else {
            &self.evidence.deliveries_before_expiry
        };
        deliveries.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = callback.send(event);
    }

    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        self.inner
            .submit_range_read_request(request, range, callback);
    }
    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        self.inner.submit_metadata_read_request(request, callback);
    }
    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        self.inner.submit_head_request(request, callback);
    }
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        self.evidence
            .mutations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.inner.submit_delete_request(request, callback);
    }
    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        self.evidence
            .mutations
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.inner.submit_write_request(request, data, callback);
    }
}

fn run_planner_with_expiring_second_head(
    fixture: &Fixture,
    policy: RecoveryPolicy,
) -> (
    MidgeResult<StreamingCloudWalRecovery>,
    Arc<PlannerEvidence>,
    OperationDeadline,
) {
    crate::failpoints::with_read_gate(|| {
        let db_path = fixture.directory.path().join("local");
        let local: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(&db_path).unwrap());
        let history = fixture.history().unwrap();
        let evidence = Arc::new(PlannerEvidence::default());
        // Setup and failpoint-gate admission precede this real immutable clock.
        // The second completed HEAD forces expiry, independent of runner speed.
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let scope = crate::common::DeadlineScope::new(deadline);
        let remote: Arc<dyn StorageBackend> = Arc::new(DelayedPlannerBackend {
            inner: Arc::clone(&fixture.cloud),
            evidence: Arc::clone(&evidence),
            deadline,
        });
        let result = StreamingCloudWalRecovery::build_within(
            &local,
            Some(history.ledger()),
            &remote,
            &fixture.catalog(),
            policy,
            Duration::from_secs(5),
            127,
            limits(),
            &scope,
        );
        (result, evidence, deadline)
    })
}

/// The names in the fixture's local WAL directory, sorted.
fn wal_directory_listing(fixture: &Fixture) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(fixture.directory.path().join("local/wal"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn assert_planner_aggregate_timeout(policy: RecoveryPolicy) {
    // Arrange: a real catalog names four genuine sealed WAL objects. The
    // second successful HEAD is held until the same captured budget expires.
    let mut fixture = Fixture::new().expect("actual planner fixture");
    let mut objects = Vec::new();
    for id in 1..=4 {
        fixture
            .publish(id, id, 7, &framed_wal(id, b"acknowledged value"))
            .unwrap();
        let key = fixture
            .catalog()
            .entry(id)
            .expect("publication")
            .object_key();
        let bytes = std::fs::read(fixture.directory.path().join("cloud").join(&key)).unwrap();
        objects.push((key, bytes));
    }
    let local_path = fixture
        .local(4, &framed_wal(4, b"acknowledged value"))
        .unwrap();
    let local_bytes = std::fs::read(&local_path).unwrap();
    drop(fixture.history().unwrap());
    let local_before = wal_directory_listing(&fixture);
    let catalog_before = fixture.catalog().encode().unwrap();
    // Act: the real planner owns checksum/inspection and receives real HEADs;
    // a completed successful submission is not free aggregate time.
    let (result, evidence, deadline) = run_planner_with_expiring_second_head(&fixture, policy);

    // Assert: Timeout is typed under both policies and stops before later
    // segment admission, alias quarantine, remote deletion, or catalog changes.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert!(deadline.is_expired());
    assert_eq!(
        evidence
            .successful_heads
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert_eq!(
        evidence
            .deliveries_before_expiry
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        evidence
            .deliveries_after_expiry
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        evidence
            .mutations
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(fixture.catalog().encode().unwrap(), catalog_before);
    assert_eq!(std::fs::read(local_path).unwrap(), local_bytes);
    assert_eq!(wal_directory_listing(&fixture), local_before);
    for (key, bytes) in objects {
        assert_eq!(
            std::fs::read(fixture.directory.path().join("cloud").join(key)).unwrap(),
            bytes
        );
    }
    assert!(!fixture
        .directory
        .path()
        .join("local/cloud_recovery")
        .exists());

    // A fresh healthy compatibility attempt still verifies the exact history.
    let recovered = fixture
        .plan_only(policy, limits())
        .expect("healthy planner after timeout");
    assert_eq!(recovered.plan.remote_segments.len(), 4);
    assert!(!recovered.plan.opened_in_salvage_mode);
}

#[test]
fn should_preserve_recovery_inputs_when_strict_planner_exhausts_aggregate_head_budget() {
    assert_planner_aggregate_timeout(RecoveryPolicy::Strict);
}

#[test]
fn should_preserve_recovery_inputs_when_salvage_planner_exhausts_aggregate_head_budget() {
    assert_planner_aggregate_timeout(RecoveryPolicy::Salvage);
}

#[test]
fn should_preserve_sequence_floor_when_salvage_is_interrupted_before_authority_commit(
) -> MidgeResult<()> {
    // Arrange: a verifiable suffix proves sequence 3 was allocated.
    let mut fixture = Fixture::new()?;
    let mut bytes = framed_wal(1, b"one");
    let mut corrupt = framed_wal(2, b"two");
    corrupt[8] ^= 1;
    bytes.extend(corrupt);
    bytes.extend(framed_wal(3, b"three"));
    let active = fixture.active(&bytes)?;
    let original = std::fs::read(&active)?;

    // Act: the planner runs, but the process loses the response/state before
    // the authority commit. A later open must retain the allocation floor.
    let first = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;
    assert!(first.plan.max_unreplayed_sequence >= 3);
    assert_eq!(open_tail_sequence(&first), Some(1));
    assert_eq!(
        std::fs::read(&active)?,
        original,
        "planning destroyed durable sequence evidence"
    );
    drop(first);
    let reopened = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;

    // Assert
    assert!(
        reopened.plan.max_unreplayed_sequence >= 3,
        "interrupted salvage lost its sequence floor: {}",
        reopened.plan.max_unreplayed_sequence
    );
    Ok(())
}

#[test]
fn should_continue_past_successor_history_when_clean_cache_fails_back() -> MidgeResult<()> {
    // Arrange: this cache published segment 1 and shut down cleanly with an
    // empty open segment 2; a successor then published 2 and 3.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.publish(2, 2, 8, &framed_wal(2, b"two"))?;
    fixture.publish(3, 3, 8, &framed_wal(3, b"three"))?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert: replay is the catalog's history, and this cache's history
    // continues past it.
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(!recovered.plan.opened_in_salvage_mode);
    assert_eq!(fixture.history()?.ledger().open_segment(), 4);
    Ok(())
}

/// Make `recovered`'s plan durable as startup does under the catalog's
/// lease epoch, over `history`. Plans that salvage nothing never touch the
/// cloud.
fn commit_as_startup(
    fixture: &Fixture,
    recovered: &StreamingCloudWalRecovery,
    history: crate::runtime::cloud_startup::CacheHistory,
) -> MidgeResult<crate::runtime::wal_history::WalHistory> {
    let persistence = crate::runtime::hybrid_persistence::CloudPersistence::new(Arc::new(
        crate::storage::HybridStorage::with_policy(
            Arc::clone(&fixture.cloud),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ),
    ));
    let catalog = fixture.catalog();
    crate::runtime::cloud_startup::commit_recovery_plan_within(
        &recovered.plan,
        &persistence,
        catalog.fencing_epoch(),
        &catalog,
        history,
        None,
        &|| Ok(()),
        &crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded()),
    )
}

/// The link the footer of published segment `id` names.
fn published_link(fixture: &Fixture, id: u64) -> MidgeResult<ChainLink> {
    let key = fixture
        .catalog()
        .entry(id)
        .expect("publication")
        .object_key();
    let bytes = std::fs::read(fixture.directory.path().join("cloud").join(key))?;
    Ok(crate::wal::format::segment::footer_link(&bytes)?.expect("a published segment is sealed"))
}

/// Append a put at `sequence` to the fixture's local segment `id`, as the
/// writer does after the epoch starts.
fn append_local(fixture: &Fixture, id: u64, sequence: u64) -> MidgeResult<()> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(fixture.segment(id))?;
    file.write_all(&framed_wal(sequence, b"after"))?;
    file.sync_all()?;
    Ok(())
}

/// Replay `recovered`'s plan strictly, every segment, the open one included.
fn replay_strictly(recovered: &StreamingCloudWalRecovery) -> MidgeResult<()> {
    crate::wal::recovery::validate_wal_with_policy(
        recovered.fs.as_ref(),
        &recovered.segments,
        crate::wal::recovery::ReplayPolicy::Strict,
        None,
    )
    .map(drop)
}

#[test]
fn should_link_rejoined_cache_to_catalog_head_when_failing_back() -> MidgeResult<()> {
    // Arrange: this cache published segment 1 and shut down with an empty
    // open segment 2; a successor then published 2 and 3.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.publish(2, 2, 8, &framed_wal(2, b"two"))?;
    fixture.publish(3, 3, 8, &framed_wal(3, b"three"))?;
    let recovered = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    let head = published_link(&fixture, 3)?;

    // Act
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(fixture.history()?),
    )?;

    // Assert: the cache continues after the catalog head, and a strict
    // reopen replays the catalog into the rebased open segment.
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase { .. }
    ));
    assert_eq!(history.ledger().anchor(), head);
    assert_eq!(history.ledger().open_member().open.prev, head);
    assert_eq!(history.ledger().open_segment(), 4);
    drop(history);
    append_local(&fixture, 4, 4)?;
    let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    assert!(matches!(reopened.plan.history, HistoryAction::Keep { .. }));
    assert_eq!(reopened.segments.open_segment(), Some(4));
    replay_strictly(&reopened)
}

#[test]
fn should_link_reopened_cache_to_catalog_head_when_a_successor_takes_its_next_id() -> MidgeResult<()>
{
    // Arrange: this cache published segment 1 and left an empty open
    // segment 2; an idle restart reopened it as 3, still linked to 1. A
    // successor then published its own segment 2, linked to 1.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.active(b"")?;
    let mut history = fixture.history()?;
    history.start_epoch(8, 2)?;
    assert_eq!(history.ledger().open_segment(), 3);
    drop(history);
    fixture.publish(2, 2, 9, &framed_wal(2, b"successor"))?;
    let head = published_link(&fixture, 2)?;
    let recovered = fixture.plan_only(RecoveryPolicy::Strict, limits())?;

    // Act
    let mut history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(fixture.history()?),
    )?;

    // Assert: the successor took over, so the cache continues after its
    // segment rather than forking the chain at segment 1.
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase { .. }
    ));
    let open = history.ledger().open_member().open;
    assert_eq!(open.prev, head);
    // A write in the rebased segment publishes after the successor's, and
    // the published chain replays strictly.
    append_local(&fixture, open.id, head.last_seq + 1)?;
    crate::runtime::wal_history::fixture::rotate(&mut history)?;
    drop(history);
    let sealed = std::fs::read(fixture.segment(open.id))?;
    fixture.publish(open.id, head.last_seq + 1, 9, &sealed)?;
    let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    assert!(matches!(reopened.plan.history, HistoryAction::Keep { .. }));
    assert_eq!(
        reopened
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2, open.id]
    );
    replay_strictly(&reopened)
}

#[test]
fn should_rebase_cache_when_a_successor_published_its_unpublished_segment_id() -> MidgeResult<()> {
    // Arrange: this cache published segment 1, sealed segment 2 without
    // publishing it, and left an empty open segment 3. A successor then
    // published its own segment 2, linked to 1.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &framed_wal(2, b"unpublished"))?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    let mut header =
        crate::wal::format::segment::SegmentBuilder::new(2, 9, published_link(&fixture, 1)?)
            .header();
    header.holder = [0x42; 16];
    let (successor, head) = crate::wal::format::segment::SegmentBuilder::from_header(header)
        .bytes(&framed_wal(2, b"successor"))
        .seal();
    fixture.publish(2, 2, 9, &successor)?;

    // Act
    let recovered = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(fixture.history()?),
    )?;

    // Assert: the catalog's segment 2 is the successor's, so this cache's
    // segment 2 was never published; it rejoins after the successor.
    assert!(matches!(
        recovered.plan.history,
        HistoryAction::Rebase { .. }
    ));
    assert_eq!(history.ledger().open_member().open.prev, head);
    Ok(())
}

#[test]
fn should_set_aside_cache_tail_when_the_catalog_set_aside_a_successor_segment_at_its_id(
) -> MidgeResult<()> {
    // Arrange: as above, but a salvage later set the successor's segment 2
    // aside, so the catalog's head is segment 1 again while its next id has
    // passed 2: this cache's segment 2 is still not history.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &framed_wal(2, b"unpublished"))?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    let head = published_link(&fixture, 1)?;
    let mut header = crate::wal::format::segment::SegmentBuilder::new(2, 9, head).header();
    header.holder = [0x42; 16];
    let (successor, _) = crate::wal::format::segment::SegmentBuilder::from_header(header)
        .bytes(&framed_wal(2, b"successor"))
        .seal();
    fixture.publish(2, 2, 9, &successor)?;
    fixture.edit_catalog(|catalog| {
        let set_aside = catalog.entries()[1..].to_vec();
        catalog
            .set_aside_suffix(catalog.fencing_epoch(), Some(2), &set_aside, 2)
            .expect("set the successor's segment aside");
    });

    // Act
    let recovered = fixture.plan_only(RecoveryPolicy::Strict, limits())?;

    // Assert: this cache's segment 2 is neither replayed nor owed, and the
    // cache continues after the catalog at an id it never handed out.
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.segments.open_segment().is_none());
    assert_eq!(
        recovered.plan.history,
        HistoryAction::Rebase {
            anchor: head,
            open_at_least: 3
        }
    );
    Ok(())
}

#[test]
fn should_link_fresh_cache_first_segment_to_catalog_head() -> MidgeResult<()> {
    // Arrange: a new cache over a catalog holding segments 1 and 2.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    let local = crate::io::real::test_root(fixture.directory.path().join("local"));
    let recovered = StreamingCloudWalRecovery::build(
        &local,
        None,
        &fixture.cloud,
        &fixture.catalog(),
        RecoveryPolicy::Strict,
        Duration::from_secs(5),
        127,
        limits(),
    )?;
    let head = published_link(&fixture, 2)?;
    let wal = crate::io::subdirectory(Arc::clone(&local), crate::wal::WAL_DIR);

    // Act
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Fresh(wal),
    )?;

    // Assert: the cache's first segment follows the catalog head, and a
    // strict reopen replays the catalog into it.
    assert_eq!(history.ledger().anchor(), head);
    assert_eq!(history.ledger().open_member().open.prev, head);
    assert_eq!(history.ledger().open_segment(), 3);
    drop(history);
    append_local(&fixture, 3, 3)?;
    let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    assert_eq!(reopened.segments.open_segment(), Some(3));
    replay_strictly(&reopened)
}

#[test]
fn should_set_aside_cache_tail_when_successor_published_past_it() -> MidgeResult<()> {
    // Arrange: as above, but this cache's open segment holds a record the
    // catalog never received.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    let stale = std::fs::read(fixture.active(&framed_wal(2, b"stale"))?)?;
    drop(fixture.history()?);
    fixture.publish(2, 2, 8, &framed_wal(2, b"two"))?;
    fixture.publish(3, 3, 8, &framed_wal(3, b"three"))?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert: only the catalog replays; the stale tail is kept aside.
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.segments.open_segment().is_none());
    assert_eq!(
        std::fs::read(fixture.salvaged(2).expect("stale tail kept aside"))?,
        stale
    );
    assert_eq!(fixture.history()?.ledger().open_segment(), 4);
    Ok(())
}

#[test]
fn should_not_replay_unpublished_segment_below_catalog_when_successor_took_over() -> MidgeResult<()>
{
    // Arrange: this cache published 1 but never 2; a successor published 3
    // and 4, then retired 1 from the catalog.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &framed_wal(2, b"unpublished"))?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.publish(3, 3, 8, &framed_wal(3, b"three"))?;
    fixture.publish(4, 4, 8, &framed_wal(4, b"four"))?;
    fixture.retire_through(1);

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert: segment 2 is not history once another writer took over.
    assert!(recovered.plan.local_segments.is_empty());
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(fixture.salvaged(2).is_some());
    assert_eq!(fixture.history()?.ledger().open_segment(), 5);
    Ok(())
}

#[test]
fn should_rejoin_when_failed_back_cache_lost_its_open_segment_mid_set_aside() -> MidgeResult<()> {
    // Arrange: an earlier rejoin moved the open segment aside, then stopped
    // before the ledger recorded it.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.publish(2, 2, 8, &framed_wal(2, b"two"))?;
    std::fs::remove_file(fixture.segment(2))?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(fixture.history()?.ledger().open_segment(), 3);
    Ok(())
}

#[test]
fn should_stop_salvage_at_local_member_whose_file_is_missing() -> MidgeResult<()> {
    // Arrange: segment 2 never reached the catalog, and its file is gone.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &framed_wal(2, b"lost"))?;
    fixture.active(&framed_wal(3, b"after"))?;
    drop(fixture.history()?);
    std::fs::remove_file(fixture.segment(2))?;

    // Act
    let strict = fixture.build(RecoveryPolicy::Strict);
    let salvaged = fixture.build(RecoveryPolicy::Salvage)?;

    // Assert: strict refuses; salvage keeps 1, sets 2 onward aside and lifts
    // the floor over the records after the hole.
    assert!(
        matches!(strict, Err(MidgeError::RecoveryFailed(_))),
        "{:?}",
        strict.err()
    );
    assert!(salvaged.plan.opened_in_salvage_mode);
    assert_eq!(
        salvaged
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(salvaged.segments.open_segment().is_none());
    assert!(salvaged.plan.max_unreplayed_sequence >= 3);
    assert!(fixture.salvaged(3).is_some());
    Ok(())
}

#[test]
fn should_continue_past_retired_catalog_history_when_fresh_cache_takes_over() -> MidgeResult<()> {
    // Arrange: a writer published segments 1 and 2, then retired both once
    // SSTs covered them; a new cache takes over.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    let head = published_link(&fixture, 2)?;
    fixture.retire_through(2);
    let local = crate::io::real::test_root(fixture.directory.path().join("local"));
    let recovered = StreamingCloudWalRecovery::build(
        &local,
        None,
        &fixture.cloud,
        &fixture.catalog(),
        RecoveryPolicy::Strict,
        Duration::from_secs(5),
        127,
        limits(),
    )?;
    let wal = crate::io::subdirectory(Arc::clone(&local), crate::wal::WAL_DIR);

    // Act
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Fresh(wal),
    )?;

    // Assert: the new history continues after the retired segment 2, under
    // an id the catalog never handed out.
    assert_eq!(history.ledger().anchor(), head);
    assert_eq!(history.ledger().open_segment(), 3);
    Ok(())
}

#[test]
fn should_set_aside_cache_tail_when_successor_retired_its_published_history() -> MidgeResult<()> {
    // Arrange: this cache published segment 1 and wrote an unpublished
    // record into its open segment 2. A successor published its own
    // segment 2, then retired both segments from the catalog.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    let stale = std::fs::read(fixture.active(&framed_wal(2, b"stale"))?)?;
    drop(fixture.history()?);
    fixture.publish(2, 2, 8, &framed_wal(2, b"successor"))?;
    let head = published_link(&fixture, 2)?;
    fixture.retire_through(2);

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert: nothing local replays; the stale tail is kept aside and the
    // cache continues after the successor's segment.
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.segments.open_segment().is_none());
    assert_eq!(
        std::fs::read(fixture.salvaged(2).expect("stale tail kept aside"))?,
        stale
    );
    assert_eq!(fixture.history()?.ledger().open_member().open.prev, head);
    Ok(())
}

#[test]
fn should_set_aside_cache_tail_when_successor_published_into_a_reopened_id_gap() -> MidgeResult<()>
{
    // Arrange: this cache published segment 1 and left an empty open
    // segment 2; an idle restart reopened it as 3, which then took a write.
    // A successor with a new cache started at the catalog's next id, 2, and
    // published it.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.active(b"")?;
    let mut history = fixture.history()?;
    history.start_epoch(8, 2)?;
    let reopened = history.ledger().open_segment();
    drop(history);
    append_local(&fixture, reopened, 2)?;
    let stale = std::fs::read(fixture.segment(reopened))?;
    fixture.publish(2, 2, 9, &framed_wal(2, b"successor"))?;
    let head = published_link(&fixture, 2)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;

    // Assert: the successor's segment 2 replays and stays cataloged; this
    // cache's write after the reopen is kept aside, not replayed.
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(recovered.plan.local_segments.is_empty());
    assert!(recovered.segments.open_segment().is_none());
    assert_eq!(
        std::fs::read(fixture.salvaged(reopened).expect("stale tail kept aside"))?,
        stale
    );
    assert_eq!(fixture.history()?.ledger().open_member().open.prev, head);
    Ok(())
}

#[test]
fn should_not_owe_or_replay_member_the_catalog_already_retired() -> MidgeResult<()> {
    // Arrange: this cache published segments 1 and 2; the catalog then
    // retired 1, and the cache stopped before retiring its local copy.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    let second = framed_wal(2, b"two");
    fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &second)?;
    fixture.publish(2, 2, 7, &second)?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.retire_through(1);
    let recovered = fixture.plan_only(RecoveryPolicy::Strict, limits())?;

    // Act
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(fixture.history()?),
    )?;

    // Assert: segment 1 is neither replayed nor owed again. Durable ledger
    // retirement leaves its physical cleanup to GC or the next orphan sweep.
    assert!(recovered.plan.local_segments.is_empty());
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(history.ledger().log_start(), 2);
    assert!(fixture.segment(1).exists());
    drop(history);
    let reopened = fixture.history()?;
    assert_eq!(reopened.ledger().log_start(), 2);
    assert!(!fixture.segment(1).exists());
    Ok(())
}

#[test]
fn should_ignore_corrupt_local_copy_of_retired_prefix_when_later_history_remains() -> MidgeResult<()>
{
    // Arrange: segment 1 is retired in the catalog, but its local cache copy
    // is damaged while segment 2 remains authoritative history.
    let mut fixture = Fixture::new()?;
    let first = framed_wal(1, b"one");
    let second = framed_wal(2, b"two");
    let retired_local = fixture.local(1, &first)?;
    fixture.publish(1, 1, 7, &first)?;
    fixture.local(2, &second)?;
    fixture.publish(2, 2, 8, &second)?;
    fixture.active(b"")?;
    drop(fixture.history()?);
    fixture.retire_through(1);
    let mut damaged = std::fs::read(&retired_local)?;
    damaged[crate::wal::format::HEADER_LEN + 8] ^= 1;
    std::fs::write(&retired_local, damaged)?;

    // Act
    let recovered = fixture.build(RecoveryPolicy::Strict)?;
    let history = commit_as_startup(
        &fixture,
        &recovered,
        crate::runtime::cloud_startup::CacheHistory::Existing(fixture.history()?),
    )?;

    // Assert: retired local corruption cannot block later catalog history.
    assert_eq!(
        recovered
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert!(recovered.plan.local_segments.is_empty());
    assert!(history.ledger().log_start() >= 2);
    drop(history);
    assert!(!retired_local.exists());
    Ok(())
}

#[test]
fn should_fail_strict_open_when_the_catalog_lost_history_this_cache_retired() -> MidgeResult<()> {
    // Arrange: this cache published segments 1 and 2 and holds no copy of
    // them; it wrote local segment 3 and open segment 4 after them. The
    // catalog then lost segment 2, as a stale copy would, with no sequence
    // floor covering it.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    fixture.publish(2, 2, 7, &framed_wal(2, b"two"))?;
    let third = fixture.local(3, &framed_wal(3, b"three"))?;
    let active = fixture.active(&framed_wal(4, b"four"))?;
    drop(fixture.history()?);
    let (third_bytes, active_bytes) = (std::fs::read(&third)?, std::fs::read(&active)?);
    fixture.roll_back_to(1);

    // Act
    let result = fixture.plan_only(RecoveryPolicy::Strict, limits());

    // Assert: strict recovery refuses before touching the cache.
    assert!(
        matches!(&result, Err(MidgeError::Corruption(_))),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    assert_eq!(std::fs::read(&third)?, third_bytes);
    assert_eq!(std::fs::read(&active)?, active_bytes);
    assert_eq!(
        fixture.history()?.ledger().members().collect::<Vec<_>>(),
        vec![3, 4]
    );
    Ok(())
}

#[test]
fn should_set_aside_local_tail_when_its_cataloged_segment_differs() -> MidgeResult<()> {
    for policy in [RecoveryPolicy::Strict, RecoveryPolicy::Salvage] {
        // Arrange: the catalog's segment 1 holds other bytes than this
        // cache's segment 1, so another writer published it.
        let mut fixture = Fixture::new()?;
        fixture.publish(1, 1, 7, &framed_wal(1, b"remote"))?;
        let head = published_link(&fixture, 1)?;
        let local = fixture.local(1, &framed_wal(1, b"local!"))?;
        drop(fixture.history()?);
        let local_bytes = std::fs::read(&local)?;

        // Act
        let recovered = fixture.build(policy)?;

        // Assert: the catalog's history replays and the cache continues
        // after it, keeping its own segment aside.
        assert_eq!(
            recovered
                .plan
                .remote_segments
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(recovered.plan.local_segments.is_empty());
        assert!(!recovered.plan.opened_in_salvage_mode);
        assert_eq!(
            std::fs::read(fixture.salvaged(1).expect("local tail kept aside"))?,
            local_bytes
        );
        assert_eq!(fixture.history()?.ledger().open_member().open.prev, head);
    }
    Ok(())
}

#[test]
fn should_fail_strict_recovery_when_a_cataloged_object_does_not_link_its_entry() -> MidgeResult<()>
{
    // Arrange: the catalog's first object links a segment the catalog
    // never held.
    let mut fixture = Fixture::new()?;
    let stranger = ChainLink {
        id: 0,
        last_seq: 0,
        digest: [0x5a; 16],
    };
    let (bytes, _link) = crate::wal::format::segment::SegmentBuilder::new(1, 7, stranger)
        .bytes(&framed_wal(1, b"one"))
        .seal();
    let entry = CatalogEntry::describing(
        &crate::wal::cloud_segment::SegmentValidation {
            writer_epoch: 7,
            prev: ChainLink::GENESIS,
            facts: crate::wal::format::segment::sealed_footer(&bytes)
                .unwrap()
                .unwrap()
                .sealed_facts(),
        },
        &bytes,
    );
    fixture.publish_entry(entry, &bytes)?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::RecoveryFailed(_))),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    Ok(())
}

#[test]
fn should_reject_catalog_family_summary_when_payload_has_different_families() -> MidgeResult<()> {
    // Arrange: the link, digest, length and CRC still match. Only the
    // persisted family maximum is forged, which could authorize unsafe GC.
    let mut fixture = Fixture::new()?;
    fixture.publish(1, 1, 7, &framed_wal(1, b"one"))?;
    let catalog = fixture.catalog();
    let mut encoded = serde_json::to_value(&catalog).unwrap();
    encoded["segments"][0]["facts"]["max_seq_per_cf"] = serde_json::json!([[7, 1]]);
    let forged = WalPublicationCatalog::decode(&serde_json::to_vec(&encoded).unwrap())
        .map_err(|error| MidgeError::Corruption(error.to_string()))?;
    fixture.save_catalog(&forged)?;
    let before = std::fs::read(fixture.catalog_path())?;

    // Act
    let result = fixture.build(RecoveryPolicy::Strict);

    // Assert
    assert!(
        result.is_err(),
        "payload and catalog family maxima must agree before planning"
    );
    assert_eq!(std::fs::read(fixture.catalog_path())?, before);
    Ok(())
}
