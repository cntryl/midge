//! Cloud startup recovery authority: which cloud WAL segments and local WAL
//! files replay, which manifest SSTs survive, what salvage cuts or sets aside,
//! and which remote objects startup may delete.
//!
//! `engine::startup` wires these decisions into engine startup. Persisted
//! family frontiers govern replay; committed shared generations govern retirement.

pub(crate) mod cloud_io;
mod cloud_recovery;
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub(crate) mod recovery_progress_fixture;
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

/// The local WAL history a cloud cache holds when recovery plans.
pub(crate) enum CacheHistory {
    /// No ledger yet: a new cache, or one whose genesis did not finish. The
    /// WAL directory it will start in.
    Fresh(std::sync::Arc<dyn crate::io::Fs>),
    /// The ledger the cache already has.
    Existing(crate::runtime::wal_history::WalHistory),
}

/// Makes a cloud recovery `plan` durable in the one order that survives a
/// crash between any two steps (#844), and returns the history the writer
/// continues:
///
/// 1. One catalog compare-and-swap sets aside the cataloged segments
///    salvage stops short of and raises the sequence floor over everything
///    set aside, when it does either. A crash after it leaves a ledger that
///    still names members the catalog no longer chains to: the next open
///    finds the same hole, or a catalog behind the ledger whose floor covers
///    it, and completes the set-aside either way.
/// 2. The ledger carries out the plan's history action: a new history after
///    the catalog's head, the catalog-retired members retired locally, the
///    members from the hole set aside, or every member set aside and the
///    history rebased onto the catalog.
///
/// `validate` is the writer's authority check, run before every step.
///
/// # Errors
///
/// The authority, catalog or ledger error; the plan may be partly durable,
/// and the next open re-plans from what landed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_recovery_plan_within(
    plan: &crate::wal::planner::RecoveryPlan,
    persistence: &crate::runtime::hybrid_persistence::CloudPersistence,
    writer_epoch: u64,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    mut history: CacheHistory,
    wal_fence: Option<crate::runtime::wal_history::WalHistoryFence>,
    validate: &dyn Fn() -> crate::common::MidgeResult<()>,
    scope: &crate::common::DeadlineScope,
) -> crate::common::MidgeResult<crate::runtime::wal_history::WalHistory> {
    use crate::wal::planner::HistoryAction;
    let deadline = scope.deadline();
    let validate = &|| {
        scope.check("cloud WAL salvage authority")?;
        validate()
    };
    validate()?;
    if plan.changes_catalog(catalog) {
        persistence.set_aside_catalog_suffix_with_authority_within(
            writer_epoch,
            plan.retire_catalog_from,
            &plan.set_aside_entries,
            plan.catalog_floor,
            validate,
            &deadline,
        )?;
    }
    crate::failpoints::fail_point!("midge::recovery::after_salvage_floor_before_quarantine");
    validate()?;
    if let Some(cut) = plan.open_cut {
        let CacheHistory::Existing(history) = &mut history else {
            return Err(crate::common::MidgeError::Internal(
                "open WAL cut without local history".into(),
            ));
        };
        let wal = history.segment_files().ok_or_else(|| {
            crate::common::MidgeError::Internal("open WAL cut without segment files".into())
        })?;
        let wal = crate::io::scope_fs(wal, scope.clone());
        history.prepare_open_cut(cut, plan.max_unreplayed_sequence)?;
        let truncate = || streaming_wal_plan::apply_open_cut(wal.as_ref(), cut);
        match &wal_fence {
            Some(fence) => fence.with_writer_effect(truncate)?,
            None => truncate()?,
        }
    }
    scope.check("local WAL history")?;
    let history = match (plan.history, history) {
        (
            HistoryAction::Genesis {
                anchor,
                open_at_least,
            },
            CacheHistory::Fresh(wal),
        ) => match wal_fence {
            Some(fence) => crate::runtime::wal_history::WalHistory::genesis_with_fence(
                wal,
                writer_epoch,
                anchor,
                open_at_least,
                fence,
            ),
            None => crate::runtime::wal_history::WalHistory::genesis(
                wal,
                writer_epoch,
                anchor,
                open_at_least,
            ),
        }?,
        (HistoryAction::Keep { retire_local_below }, CacheHistory::Existing(mut history)) => {
            if let Some(to) = retire_local_below {
                history.retire_below(to)?;
            }
            history
        }
        (
            HistoryAction::SetAside {
                from,
                open_at_least,
            },
            CacheHistory::Existing(mut history),
        ) => {
            history.set_aside(
                crate::runtime::wal_history::SetAside::From(from),
                open_at_least,
            )?;
            history
        }
        (
            HistoryAction::Rebase {
                anchor,
                open_at_least,
            },
            CacheHistory::Existing(mut history),
        ) => {
            history.rebase(anchor, open_at_least)?;
            history
        }
        (action, _) => {
            return Err(crate::common::MidgeError::Internal(format!(
                "the cloud WAL recovery plan's {action:?} does not fit the cache's history"
            )))
        }
    };
    validate()?;
    Ok(history)
}
