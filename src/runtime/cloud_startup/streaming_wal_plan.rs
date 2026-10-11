//! Catalog-authorized cloud WAL replay with no local segment staging.

use super::streaming_wal_fs::{validate_wal_source, wal_sources_equal, StreamingWalFs};
use super::{CloudStartupRecovery, CloudWalRecoveryPlan};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::config::RecoveryPolicy;
use crate::io::FsError;
use crate::io::{Fs, FsPath, OpenMode, OpenOptions};
use crate::storage::{StorageBackend, StorageEvent, StorageOutcome};
use crate::wal::ledger::{SegmentId, WalLedger};
use crate::wal::recovery::streaming::{
    inspect_local_sealed_wal_file, inspect_sealed_wal_file, inspect_wal_file,
    max_verified_suffix_sequence, StreamingReplayLimits,
};
use crate::wal::recovery::ReplaySegments;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests;

const READ_ONLY: OpenOptions = OpenOptions {
    mode: OpenMode::ReadOnly,
    create: false,
    create_new: false,
    truncate: false,
};

struct ReplaySource {
    fs: Arc<dyn Fs>,
    path: FsPath,
}

pub(crate) struct StreamingCloudWalRecovery {
    pub(crate) fs: Arc<dyn Fs>,
    /// The planned WAL files in `fs`, in replay order.
    pub(crate) segments: ReplaySegments,
    pub(crate) plan: CloudWalRecoveryPlan,
    /// The lowest id a new open segment may take: one past every cataloged
    /// segment.
    pub(crate) next_remote_id: SegmentId,
}

impl StreamingCloudWalRecovery {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        local: &Arc<dyn Fs>,
        ledger: &WalLedger,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<Self> {
        Self::build_within(
            local,
            ledger,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
            &DeadlineScope::new(OperationDeadline::unbounded()),
        )
    }

    /// Plan recovery over `local`, the database root, whose WAL members
    /// `ledger` names. Every local WAL mutation (retained copy, truncation)
    /// goes through `local`, so fault injection reaches them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_within(
        local: &Arc<dyn Fs>,
        ledger: &WalLedger,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("cloud WAL planning")?;
        let local = &crate::io::scope_fs(Arc::clone(local), scope.clone());
        let next_remote_id = next_segment_id(catalog.segments.keys().copied().max())?;
        let failed_back = ledger.open_segment() < next_remote_id;
        if failed_back {
            ensure_nothing_unpublished(local.as_ref(), ledger, catalog, next_remote_id)?;
        }
        let mut replay_fs = StreamingWalFs::new(read_window)?;
        let mut plan = CloudWalRecoveryPlan {
            remote_segments: BTreeMap::new(),
            local_segments: BTreeMap::new(),
            active_wal: None,
            opened_in_salvage_mode: false,
            unreplayed_segments: Vec::new(),
            max_unreplayed_sequence: catalog.sequence_floor,
            set_aside_from: None,
        };
        if failed_back {
            // History continues past the catalog, in a new segment.
            plan.set_aside(ledger.open_segment());
        }
        let mut sources = BTreeMap::new();
        let mut skipped = BTreeSet::new();
        for (segment_id, publication) in &catalog.segments {
            scope.check("cloud WAL catalog segment")?;
            validate_publication_identity(*segment_id, publication, catalog.fencing_epoch)?;
            let result = remote_source(
                Arc::clone(local),
                Arc::clone(remote),
                publication,
                timeout,
                read_window,
                limits,
                scope,
            );
            let Some(source) =
                recover_or_salvage(result, policy, &mut plan.opened_in_salvage_mode)?
            else {
                skipped.insert(*segment_id);
                continue;
            };
            sources.insert(*segment_id, source);
            plan.remote_segments.insert(
                *segment_id,
                crate::runtime::RecoveredCloudWalSegment {
                    max_sequence: publication.max_sequence,
                    writer_epoch: publication.writer_epoch,
                },
            );
        }
        let context = WalPlanningContext {
            local,
            ledger,
            policy,
            read_window,
            limits,
            scope,
        };
        let LocalSources {
            active: mut active_source,
            skipped: skipped_local,
        } = merge_local_sources(&context, &mut plan, &mut sources)?;
        skipped.extend(skipped_local);
        enforce_epoch_order(
            &mut plan,
            &mut sources,
            &mut active_source,
            ledger.open_segment(),
            policy,
            &mut skipped,
            scope,
        )?;
        stop_at_first_hole(
            &context,
            catalog,
            &skipped,
            &mut plan,
            &mut sources,
            &mut active_source,
        )?;
        let segments = assemble_planned_sources(
            &mut replay_fs,
            sources,
            active_source.map(|source| (ledger.open_segment(), source)),
            scope,
        )?;
        scope.check("cloud WAL planning completion")?;
        Ok(Self {
            fs: crate::io::scope_fs(Arc::new(replay_fs), scope.clone()),
            segments,
            plan,
            next_remote_id,
        })
    }
}

/// Expose the planned sources as one WAL directory and return the order
/// replay reads them in.
fn assemble_planned_sources(
    replay_fs: &mut StreamingWalFs,
    sources: BTreeMap<u64, ReplaySource>,
    active_source: Option<(SegmentId, ReplaySource)>,
    scope: &DeadlineScope,
) -> MidgeResult<ReplaySegments> {
    let mut sealed = Vec::with_capacity(sources.len());
    for (segment_id, source) in sources {
        scope.check("cloud WAL source assembly")?;
        let name = crate::wal::segment_file_name(segment_id);
        sealed.push(FsPath::new(format!("{}/{name}", crate::wal::WAL_DIR)));
        replay_fs.insert(name, source.fs, source.path)?;
    }
    let active = match active_source {
        Some((segment_id, source)) => {
            let name = crate::wal::segment_file_name(segment_id);
            replay_fs.insert(name.clone(), source.fs, source.path)?;
            Some(FsPath::new(format!("{}/{name}", crate::wal::WAL_DIR)))
        }
        None => None,
    };
    Ok(ReplaySegments::new(sealed, active))
}

fn next_segment_id(highest: Option<u64>) -> MidgeResult<u64> {
    highest
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| MidgeError::ResourceLimit("WAL segment identity space exhausted".into()))
}

struct WalPlanningContext<'a> {
    local: &'a Arc<dyn Fs>,
    ledger: &'a WalLedger,
    policy: RecoveryPolicy,
    read_window: usize,
    limits: StreamingReplayLimits,
    scope: &'a DeadlineScope,
}

struct LocalSources {
    active: Option<ReplaySource>,
    /// Local-only segments that failed validation and were not replayed.
    skipped: BTreeSet<u64>,
}

/// The ledger's members as replay sources: each sealed member unless the
/// catalog already supplies it (then the two must match), and the open one.
fn merge_local_sources(
    context: &WalPlanningContext<'_>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
) -> MidgeResult<LocalSources> {
    let WalPlanningContext {
        local,
        ledger,
        policy,
        read_window,
        limits,
        scope,
    } = *context;
    scope.check("local WAL source discovery")?;
    let mut skipped = BTreeSet::new();
    for segment_id in ledger.sealed() {
        scope.check("local WAL source selection")?;
        let selected = select_local_segment(
            local,
            segment_id,
            policy,
            limits,
            &mut plan.opened_in_salvage_mode,
        )?;
        let Some((path, segment)) = selected else {
            skipped.insert(segment_id);
            continue;
        };
        if let Some(remote) = sources.get(&segment_id) {
            if !wal_sources_equal(
                (remote.fs.as_ref(), &remote.path),
                (local.as_ref(), &path),
                read_window,
            )? {
                return Err(MidgeError::RecoveryFailed(format!(
                    "validated local and cloud WAL bytes diverge for '{}'; refusing ambiguous recovery",
                    crate::wal::segment_file_name(segment_id)
                )));
            }
        } else {
            sources.insert(
                segment_id,
                ReplaySource {
                    fs: Arc::clone(local),
                    path,
                },
            );
            plan.local_segments.insert(segment_id, segment);
        }
    }
    let open = ledger.open_segment();
    let active = if plan.set_aside_from.is_some_and(|from| from <= open) {
        None
    } else {
        active_local_source(local, open, policy, limits, plan)?
    };
    Ok(LocalSources { active, skipped })
}

/// A ledger whose open segment is below the catalog's next id belongs to a
/// cache another writer succeeded: this ledger records each seal before it
/// uploads, so only another writer publishes at or past its open segment.
/// The cache may rejoin only when it holds nothing the catalog lacks: its
/// open segment is empty, and every sealed member is cataloged or below the
/// catalog's oldest segment (retired, so covered by SSTs). Telling a stale
/// unpublished tail from the successor's needs ownership (#820), so any
/// other cache is refused rather than replayed.
fn ensure_nothing_unpublished(
    local: &dyn Fs,
    ledger: &WalLedger,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    next_remote_id: SegmentId,
) -> MidgeResult<()> {
    let open = ledger.open_segment();
    let open_bytes = match local.metadata(&crate::wal::segment_key(open)) {
        Ok(metadata) => Some(metadata.len),
        Err(FsError::NotFound(_)) => None,
        Err(error) => return Err(error.into_midge()),
    };
    let oldest = catalog
        .segments
        .keys()
        .next()
        .copied()
        .unwrap_or(next_remote_id);
    let unpublished: Vec<SegmentId> = ledger
        .sealed()
        .filter(|id| *id >= oldest && !catalog.segments.contains_key(id))
        .collect();
    if open_bytes == Some(0) && unpublished.is_empty() {
        return Ok(());
    }
    Err(MidgeError::RecoveryFailed(format!(
        "the local WAL ledger's open segment {open} is below the cloud catalog's next segment {next_remote_id}: another writer published past this cache, which holds WAL the catalog lacks (open segment bytes {open_bytes:?}, unpublished sealed segments {unpublished:?})"
    )))
}

fn validate_publication_identity(
    segment_id: u64,
    publication: &crate::wal::cloud_catalog::PublishedWalSegment,
    fencing_epoch: u64,
) -> MidgeResult<()> {
    if segment_id != publication.segment_id
        || publication.object_key
            != crate::wal::segment_object_key(segment_id, publication.writer_epoch)
        || publication.writer_epoch > fencing_epoch
        || publication.size_bytes == 0
    {
        return Err(MidgeError::RecoveryFailed(
            "invalid cloud WAL publication identity".into(),
        ));
    }
    Ok(())
}

fn recover_or_salvage<T>(
    result: MidgeResult<T>,
    policy: RecoveryPolicy,
    salvaged: &mut bool,
) -> MidgeResult<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(
            error
            @ (MidgeError::ResourceLimit(_) | MidgeError::NoSpace(_) | MidgeError::Timeout(_)),
        ) => Err(error),
        Err(error) if policy == RecoveryPolicy::Salvage && error.is_salvageable() => {
            *salvaged = true;
            tracing::warn!(%error, "skipping invalid WAL source during salvage recovery");
            Ok(None)
        }
        Err(error) => Err(MidgeError::RecoveryFailed(format!(
            "WAL source validation failed: {error}"
        ))),
    }
}

fn remote_source(
    local: Arc<dyn Fs>,
    remote: Arc<dyn StorageBackend>,
    publication: &crate::wal::cloud_catalog::PublishedWalSegment,
    timeout: Duration,
    read_window: usize,
    limits: StreamingReplayLimits,
    scope: &DeadlineScope,
) -> MidgeResult<ReplaySource> {
    scope.check("cloud WAL HEAD submission")?;
    let deadline = if scope.deadline().is_bounded() {
        scope.deadline()
    } else {
        OperationDeadline::from_budget(timeout)
    };
    let (tx, rx) = std::sync::mpsc::channel();
    remote.submit_range_head_request(
        crate::storage::StorageRequest::new(&publication.object_key, deadline, timeout),
        tx,
    );
    scope.check("cloud WAL HEAD wait")?;
    let metadata = match rx.recv_timeout(deadline.clamp(timeout)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(metadata),
            ..
        }) => metadata,
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Err(error),
            ..
        }) => {
            let message = format!("cloud WAL {} HEAD: {error}", publication.object_key);
            // A cataloged segment that no longer exists is lost data, not a
            // transient failure, so salvage may skip it.
            return Err(
                if error.kind() == crate::storage::StorageErrorKind::Timeout {
                    MidgeError::Timeout(error.message().to_string())
                } else if error.is_not_found() {
                    MidgeError::Corruption(message)
                } else {
                    MidgeError::RecoveryFailed(message)
                },
            );
        }
        Ok(other) => {
            return Err(MidgeError::RecoveryFailed(format!(
                "unexpected cloud WAL HEAD response: {other:?}"
            )))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            return Err(MidgeError::Timeout("cloud WAL HEAD timed out".into()))
        }
        Err(error) => {
            return Err(MidgeError::RecoveryFailed(format!(
                "cloud WAL HEAD failed: {error}"
            )))
        }
    };
    scope.check("cloud WAL HEAD completion")?;
    let fs: Arc<dyn Fs> = Arc::new(
        crate::storage::remote_sst::RemoteSstFs::for_object(
            local,
            remote,
            publication.object_key.clone(),
            metadata,
            timeout,
        )
        .with_deadline(scope.deadline()),
    );
    let fs =
        crate::telemetry::recovery_progress::observe_reads(crate::io::scope_fs(fs, scope.clone()));
    let path = FsPath::new(publication.object_key.clone());
    validate_wal_source(
        fs.as_ref(),
        &path,
        publication.size_bytes,
        publication.content_crc32c,
        read_window,
    )?;
    // Inspection also uses the bounded range window even for a large frame.
    let mut buffered = StreamingWalFs::new(read_window)?;
    let canonical = crate::wal::segment_file_name(publication.segment_id);
    buffered.insert(canonical.clone(), Arc::clone(&fs), path.clone())?;
    let file = buffered
        .open(&FsPath::new(canonical), READ_ONLY)
        .map_err(FsError::into_midge)?;
    let prefix = inspect_sealed_wal_file(file.as_ref(), &path, limits)?;
    if prefix.max_sequence != publication.max_sequence
        || prefix.writer_epoch != publication.writer_epoch
    {
        return Err(MidgeError::Corruption(format!(
            "cloud WAL {} sequence or epoch differs from its catalog proof",
            publication.object_key
        )));
    }
    Ok(ReplaySource { fs, path })
}

fn select_local_segment(
    fs: &Arc<dyn Fs>,
    segment_id: SegmentId,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
    salvaged: &mut bool,
) -> MidgeResult<Option<(FsPath, crate::runtime::RecoveredCloudWalSegment)>> {
    let path = crate::wal::segment_key(segment_id);
    let result = (|| {
        let file = fs.open(&path, READ_ONLY).map_err(FsError::into_midge)?;
        inspect_local_sealed_wal_file(file.as_ref(), &path, limits)
    })();
    Ok(recover_or_salvage(result, policy, salvaged)?.map(|prefix| {
        (
            path,
            crate::runtime::RecoveredCloudWalSegment {
                max_sequence: prefix.max_sequence,
                writer_epoch: prefix.writer_epoch,
            },
        )
    }))
}

/// The open member as a replay source, cut back to its verified prefix.
/// Bytes past that prefix that salvage drops, or that hold no complete
/// record, are kept in a retained copy first. A member salvage cannot read
/// is set aside whole.
fn active_local_source(
    fs: &Arc<dyn Fs>,
    open: SegmentId,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
    plan: &mut CloudWalRecoveryPlan,
) -> MidgeResult<Option<ReplaySource>> {
    let path = crate::wal::segment_key(open);
    let Some(file) = recover_or_salvage(
        fs.open(&path, READ_ONLY).map_err(FsError::into_midge),
        policy,
        &mut plan.opened_in_salvage_mode,
    )?
    else {
        plan.set_aside(open);
        return Ok(None);
    };
    let length = file.len().map_err(FsError::into_midge)?;
    let mut salvaged = false;
    let prefix = match inspect_wal_file(file.as_ref(), &path, limits) {
        Ok(prefix) => prefix,
        Err(failure)
            if matches!(
                failure.error(),
                MidgeError::ResourceLimit(_) | MidgeError::NoSpace(_)
            ) =>
        {
            return Err(failure.error().replay())
        }
        Err(failure) if failure.is_incomplete_tail() => failure.verified_prefix(),
        Err(failure) if policy == RecoveryPolicy::Salvage && failure.error().is_salvageable() => {
            plan.opened_in_salvage_mode = true;
            salvaged = true;
            tracing::warn!(error = %failure.error(), "salvaging verified active WAL prefix");
            failure.verified_prefix()
        }
        Err(failure) => {
            return Err(MidgeError::RecoveryFailed(format!(
                "active WAL failed validation: {}",
                failure.error()
            )))
        }
    };
    if salvaged {
        let suffix_max =
            max_verified_suffix_sequence(file.as_ref(), &path, prefix.valid_bytes as u64, limits)?;
        plan.max_unreplayed_sequence = plan.max_unreplayed_sequence.max(suffix_max.unwrap_or(0));
    }
    drop(file);
    if prefix.valid_bytes as u64 != length {
        if salvaged || prefix.record_count == 0 {
            // Salvage drops acknowledged records past the corruption, and
            // bytes without a complete record are unexplained; keep the
            // original before cutting the only copy.
            CloudStartupRecovery::retain_local_wal_copy(fs.as_ref(), &path)?;
            fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)
                .map_err(FsError::into_midge)?;
        }
        crate::io::truncate_durably(fs.as_ref(), &path, prefix.valid_bytes as u64)
            .map_err(FsError::into_midge)?;
    }
    if prefix.record_count == 0 {
        return Ok(None);
    }
    plan.active_wal = Some(crate::runtime::RecoveredCloudActiveWal {
        max_sequence: prefix.max_sequence,
        writer_epoch: prefix.writer_epoch,
        record_count: prefix.record_count,
        valid_bytes: prefix.valid_bytes,
    });
    Ok(Some(ReplaySource {
        fs: Arc::clone(fs),
        path,
    }))
}

fn enforce_epoch_order(
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
    open: SegmentId,
    policy: RecoveryPolicy,
    skipped: &mut BTreeSet<u64>,
    scope: &DeadlineScope,
) -> MidgeResult<()> {
    let mut highest_epoch = 0;
    let mut stale = Vec::new();
    for segment_id in sources.keys() {
        scope.check("WAL epoch order")?;
        let segment = plan
            .remote_segments
            .get(segment_id)
            .or_else(|| plan.local_segments.get(segment_id))
            .expect("source has recovered metadata");
        if segment.writer_epoch < highest_epoch {
            if policy == RecoveryPolicy::Strict {
                return Err(MidgeError::RecoveryFailed(format!(
                    "recovered WAL writer epoch regression at segment {segment_id}"
                )));
            }
            plan.opened_in_salvage_mode = true;
            tracing::warn!(
                segment_id,
                "skipping stale-epoch WAL segment during salvage recovery"
            );
            stale.push(*segment_id);
        } else {
            highest_epoch = segment.writer_epoch;
        }
    }
    for segment_id in stale {
        skipped.insert(segment_id);
        sources.remove(&segment_id);
        plan.remote_segments.remove(&segment_id);
        plan.local_segments.remove(&segment_id);
    }
    if plan
        .active_wal
        .is_some_and(|wal| wal.writer_epoch < highest_epoch)
    {
        if policy == RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(
                "recovered WAL writer epoch regression at active WAL".into(),
            ));
        }
        plan.opened_in_salvage_mode = true;
        tracing::warn!("skipping stale-epoch active WAL during salvage recovery");
        if let Some(wal) = plan.active_wal.take() {
            plan.max_unreplayed_sequence = plan.max_unreplayed_sequence.max(wal.max_sequence);
        }
        if active.take().is_some() {
            plan.set_aside(open);
        }
    }
    Ok(())
}

/// Highest verified sequence anywhere in a WAL file being set aside. If the
/// file cannot be scanned, salvage must not reuse an unknown sequence range.
fn verified_max_sequence(
    local: &dyn Fs,
    segment_id: SegmentId,
    limits: StreamingReplayLimits,
) -> MidgeResult<u64> {
    let wal_path = crate::wal::segment_key(segment_id);
    let file = match local.open(&wal_path, READ_ONLY) {
        Ok(file) => file,
        // A member salvage already found missing holds no sequence.
        Err(FsError::NotFound(_)) => return Ok(0),
        Err(error) => return Err(error.into_midge()),
    };
    match inspect_wal_file(file.as_ref(), &wal_path, limits) {
        Ok(prefix) => Ok(prefix.max_sequence),
        Err(failure) => {
            if matches!(failure.error(), MidgeError::Timeout(_)) {
                return Err(failure.error().replay());
            }
            let prefix = failure.verified_prefix();
            let suffix = max_verified_suffix_sequence(
                file.as_ref(),
                &wal_path,
                prefix.valid_bytes as u64,
                limits,
            )?;
            Ok(prefix.max_sequence.max(suffix.unwrap_or(0)))
        }
    }
}

/// Salvage keeps a consistent prefix of history: once a segment is lost,
/// nothing after it may replay, or a delete in the lost segment could be
/// undone while newer writes stay visible. Later sources are set aside
/// (local members dropped from the ledger, cloud objects kept) and the
/// sequence floor is lifted above them.
fn stop_at_first_hole(
    context: &WalPlanningContext<'_>,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    skipped: &BTreeSet<u64>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
) -> MidgeResult<()> {
    let WalPlanningContext {
        local,
        ledger,
        limits,
        scope,
        ..
    } = *context;
    // Segments upload and retire in id order, so a local file below the
    // oldest cataloged segment was already retired, meaning SSTs cover it.
    // A leaked, corrupt copy of it is not a hole.
    let oldest_needed = catalog.segments.keys().next().copied().unwrap_or(0);
    let Some(&hole) = skipped
        .range(oldest_needed..)
        .find(|segment_id| !sources.contains_key(segment_id))
    else {
        return Ok(());
    };
    tracing::warn!(
        segment_id = hole,
        "stopping salvage replay at lost WAL segment; setting later WAL aside"
    );
    let mut max_sequence = 0;
    // Every member at or past the hole goes, including failed copies, or the
    // next open would find the same hole and drop newer writes. The open
    // member is always among them: every skipped id is below it.
    for segment_id in ledger.members().filter(|segment_id| *segment_id >= hole) {
        scope.check("WAL salvage frontier")?;
        // A corrupt local-only file is in neither the catalog nor the plan;
        // its verified prefix is the best record of what it held.
        max_sequence = max_sequence.max(verified_max_sequence(local.as_ref(), segment_id, limits)?);
    }
    plan.set_aside(hole);
    let dropped: Vec<u64> = sources.range(hole..).map(|(id, _)| *id).collect();
    for segment_id in dropped {
        sources.remove(&segment_id);
        for segment in [
            plan.remote_segments.remove(&segment_id),
            plan.local_segments.remove(&segment_id),
        ]
        .into_iter()
        .flatten()
        {
            max_sequence = max_sequence.max(segment.max_sequence);
        }
    }
    plan.unreplayed_segments = catalog
        .segments
        .range(hole..)
        .map(|(_, publication)| publication.clone())
        .collect();
    for publication in &plan.unreplayed_segments {
        max_sequence = max_sequence.max(publication.max_sequence);
    }
    if let Some(wal) = plan.active_wal.take() {
        max_sequence = max_sequence.max(wal.max_sequence);
    }
    // Dropped from the ledger only after startup persists the floor that
    // covers them; see `CloudWalRecoveryPlan::commit_set_aside_within`.
    active.take();
    plan.max_unreplayed_sequence = plan.max_unreplayed_sequence.max(max_sequence);
    Ok(())
}
