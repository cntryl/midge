//! Cloud startup recovery authority: which cloud WAL segments and local WAL
//! files replay, which manifest SSTs survive, what salvage cuts or sets aside,
//! and which remote objects startup may delete.
//!
//! `engine::startup` wires these decisions into engine startup; the coverage
//! rule they rely on is the shared one in `runtime::hybrid_persistence`.

pub(crate) mod cloud_io;
mod cloud_recovery;
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub(crate) mod recovery_progress_fixture;
pub(crate) mod replay_coverage;
pub(crate) mod streaming_wal_fs;
pub(crate) mod streaming_wal_plan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloudSstRecoveryProof {
    pub(crate) name: String,
    pub(crate) expected_size_bytes: Option<u64>,
    pub(crate) expected_crc32c: Option<u32>,
}

impl CloudSstRecoveryProof {
    #[cfg(test)]
    pub(crate) fn name_only(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expected_size_bytes: None,
            expected_crc32c: None,
        }
    }

    pub(crate) fn from_manifest(file: &crate::metadata::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(crate) fn from_runtime(file: &crate::runtime::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(crate) fn merge_from(&mut self, other: &Self) {
        if self.expected_size_bytes.is_none() {
            self.expected_size_bytes = other.expected_size_bytes;
        }
        if self.expected_crc32c.is_none() {
            self.expected_crc32c = other.expected_crc32c;
        }
    }
}

pub(crate) struct CloudStartupRecovery;

pub(crate) struct CloudWalRecoveryPlan {
    pub(crate) remote_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(crate) local_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(crate) opened_in_salvage_mode: bool,
    /// Cataloged segments at and after the first hole that salvage stopped
    /// at. They are not replayed; startup retires them from the catalog and
    /// keeps their objects.
    pub(crate) unreplayed_segments: Vec<crate::wal::cloud_catalog::PublishedWalSegment>,
    /// Highest sequence held by WAL salvage set aside, now or on an earlier
    /// open (the catalog's persisted floor), so new writes never reuse one.
    /// Zero when nothing was ever set aside.
    pub(crate) max_unreplayed_sequence: u64,
    /// The first ledger member salvage drops; every later one goes too, the
    /// open member included. Dropped only after the floor covering them is
    /// durable.
    pub(crate) set_aside_from: Option<crate::wal::ledger::SegmentId>,
    /// Another writer published past this cache's open segment: every local
    /// member is kept aside, and the history continues from the catalog.
    pub(crate) failed_back: bool,
    /// The link of the newest catalog segment replay reads (#843): a history
    /// this cache starts or rebases continues from it.
    pub(crate) catalog_head_link: Option<crate::wal::format::ChainLink>,
}

/// The local WAL history a cloud cache holds when recovery plans.
pub(crate) enum CacheHistory {
    /// No ledger yet: a new cache, or one whose genesis did not finish. The
    /// WAL directory it will start in.
    Fresh(std::sync::Arc<dyn crate::io::Fs>),
    /// The ledger the cache already has.
    Existing(crate::runtime::wal_history::WalHistory),
}

impl CloudWalRecoveryPlan {
    /// A plan that has admitted nothing yet over a catalog whose sequence
    /// floor is `sequence_floor`.
    pub(crate) fn new(sequence_floor: u64, failed_back: bool) -> Self {
        Self {
            remote_segments: std::collections::BTreeMap::new(),
            local_segments: std::collections::BTreeMap::new(),
            opened_in_salvage_mode: false,
            unreplayed_segments: Vec::new(),
            max_unreplayed_sequence: sequence_floor,
            set_aside_from: None,
            failed_back,
            catalog_head_link: None,
        }
    }

    /// Drop ledger member `from` and every later one.
    pub(crate) fn set_aside(&mut self, from: crate::wal::ledger::SegmentId) {
        self.set_aside_from = Some(
            self.set_aside_from
                .map_or(from, |current| current.min(from)),
        );
    }

    /// Makes the local history this plan replays durable, salvage set-aside
    /// included, in the one order that survives a crash between any two
    /// steps, and returns the history the writer continues:
    ///
    /// 1. Persist the sequence floor. Until the ledger drops the local
    ///    members, the next open finds the same hole and recomputes it anyway.
    /// 2. Fix the ledger, opening a new segment past every cataloged one
    ///    (`next_remote_id`): a cache without a ledger starts one linked to
    ///    the catalog's head (`genesis`); one another writer published past,
    ///    or whose every member salvage drops, keeps its members aside and
    ///    continues from that head (`rebase`); otherwise salvage drops its
    ///    members from the hole (`set_aside`). After this only the persisted
    ///    floor remembers their sequences.
    /// 3. Retire the cataloged segments past the hole. Retiring before step
    ///    2 would let the next open replay local members past the hole.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_within(
        &self,
        persistence: &crate::runtime::hybrid_persistence::CloudPersistence,
        writer_epoch: u64,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        history: CacheHistory,
        next_remote_id: crate::wal::ledger::SegmentId,
        validate: &dyn Fn() -> crate::common::MidgeResult<()>,
        scope: &crate::common::DeadlineScope,
    ) -> crate::common::MidgeResult<crate::runtime::wal_history::WalHistory> {
        let deadline = scope.deadline();
        let validate = &|| {
            scope.check("cloud WAL salvage authority")?;
            validate()
        };
        validate()?;
        if self.max_unreplayed_sequence > catalog.sequence_floor {
            persistence.raise_wal_sequence_floor_with_authority_within(
                writer_epoch,
                self.max_unreplayed_sequence,
                validate,
                &deadline,
            )?;
        }
        crate::failpoints::fail_point!("midge::recovery::after_salvage_floor_before_quarantine");
        validate()?;
        scope.check("local WAL history")?;
        let head = self.catalog_head_link;
        let history = match history {
            CacheHistory::Fresh(wal) => crate::runtime::wal_history::WalHistory::genesis(
                wal,
                writer_epoch,
                head.unwrap_or(crate::wal::format::ChainLink::GENESIS),
                next_remote_id,
            )?,
            CacheHistory::Existing(mut history) => {
                let log_start = history.ledger().log_start();
                let drops_every_member = self.set_aside_from.is_some_and(|from| from <= log_start);
                if self.failed_back || drops_every_member {
                    let anchor = head.unwrap_or_else(|| history.ledger().anchor());
                    history.rebase(anchor, next_remote_id)?;
                } else if let Some(from) = self.set_aside_from {
                    history.set_aside(
                        crate::runtime::wal_history::SetAside::From(from),
                        next_remote_id,
                    )?;
                }
                history
            }
        };
        crate::failpoints::fail_point!("midge::recovery::before_salvage_catalog_retirement");
        validate()?;
        if !self.unreplayed_segments.is_empty() {
            persistence.retire_unreplayed_wal_segments_with_authority_within(
                writer_epoch,
                &self.unreplayed_segments,
                validate,
                &deadline,
            )?;
        }
        validate()?;
        Ok(history)
    }

    pub(crate) fn remote_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    pub(crate) fn remote_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }

    pub(crate) fn local_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    pub(crate) fn local_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }
}
