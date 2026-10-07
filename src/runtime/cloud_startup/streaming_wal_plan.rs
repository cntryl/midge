//! Catalog-authorized cloud WAL replay with no local segment staging.

use super::streaming_wal_fs::{validate_wal_source, wal_sources_equal, StreamingWalFs};
use super::{CloudStartupRecovery, CloudWalRecoveryPlan};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::config::RecoveryPolicy;
use crate::io::FsError;
use crate::io::{Fs, FsPath, OpenMode, OpenOptions};
use crate::storage::{StorageBackend, StorageEvent, StorageOutcome};
use crate::wal::recovery::streaming::{
    inspect_local_sealed_wal_file, inspect_sealed_wal_file, inspect_wal_file,
    max_verified_suffix_sequence, StreamingReplayLimits,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
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
    pub(crate) plan: CloudWalRecoveryPlan,
    pub(crate) next_segment_id: u64,
}

impl StreamingCloudWalRecovery {
    pub(crate) fn build(
        db_path: &Path,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<Self> {
        Self::build_within(
            db_path,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
            &DeadlineScope::new(OperationDeadline::unbounded()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_within(
        db_path: &Path,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("cloud WAL planning")?;
        let local: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(db_path).map_err(FsError::into_midge)?);
        Self::build_with_local_fs_within(
            db_path,
            &local,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
            scope,
        )
    }

    /// `build` over an explicit filesystem rooted at `db_path`. Every local
    /// WAL mutation (rename, removal, retained copy, truncation) goes through
    /// `local`, so fault injection reaches the destructive salvage steps.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(crate) fn build_with_local_fs(
        db_path: &Path,
        local: &Arc<dyn Fs>,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<Self> {
        Self::build_with_local_fs_within(
            db_path,
            local,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
            &DeadlineScope::new(OperationDeadline::unbounded()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_with_local_fs_within(
        db_path: &Path,
        local: &Arc<dyn Fs>,
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
        let mut replay_fs = StreamingWalFs::new(read_window)?;
        let mut plan = CloudWalRecoveryPlan {
            remote_segments: BTreeMap::new(),
            local_segments: BTreeMap::new(),
            active_wal: None,
            opened_in_salvage_mode: false,
            unreplayed_segments: Vec::new(),
            max_unreplayed_sequence: catalog.sequence_floor,
            set_aside_local_paths: Vec::new(),
        };
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
            db_path,
            local,
            policy,
            read_window,
            limits,
            scope,
        };
        let LocalSources {
            active: mut active_source,
            next_segment_id: next_local_id,
            skipped: skipped_local,
        } = merge_local_sources(&context, &mut plan, &mut sources)?;
        skipped.extend(skipped_local);
        enforce_epoch_order(
            db_path,
            &mut plan,
            &mut sources,
            &mut active_source,
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
        assemble_planned_sources(&mut replay_fs, sources, active_source, scope)?;
        scope.check("cloud WAL planning completion")?;
        Ok(Self {
            fs: crate::io::scope_fs(Arc::new(replay_fs), scope.clone()),
            plan,
            next_segment_id: next_remote_id.max(next_local_id),
        })
    }
}

fn assemble_planned_sources(
    replay_fs: &mut StreamingWalFs,
    sources: BTreeMap<u64, ReplaySource>,
    active_source: Option<ReplaySource>,
    scope: &DeadlineScope,
) -> MidgeResult<()> {
    for (segment_id, source) in sources {
        scope.check("cloud WAL source assembly")?;
        replay_fs.insert(
            crate::wal::segment_file_name(segment_id),
            source.fs,
            source.path,
        )?;
    }
    if let Some(source) = active_source {
        replay_fs.insert(crate::wal::ACTIVE_FILE_NAME.into(), source.fs, source.path)?;
    }
    Ok(())
}

fn next_segment_id(highest: Option<u64>) -> MidgeResult<u64> {
    highest
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| MidgeError::ResourceLimit("WAL segment identity space exhausted".into()))
}

struct WalPlanningContext<'a> {
    db_path: &'a Path,
    local: &'a Arc<dyn Fs>,
    policy: RecoveryPolicy,
    read_window: usize,
    limits: StreamingReplayLimits,
    scope: &'a DeadlineScope,
}

struct LocalSources {
    active: Option<ReplaySource>,
    next_segment_id: u64,
    /// Local-only segments that failed validation and were not replayed.
    skipped: BTreeSet<u64>,
}

fn merge_local_sources(
    context: &WalPlanningContext<'_>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
) -> MidgeResult<LocalSources> {
    let WalPlanningContext {
        db_path,
        local,
        policy,
        read_window,
        limits,
        scope,
    } = *context;
    scope.check("local WAL source discovery")?;
    let paths = CloudStartupRecovery::collect_local_wal_paths(
        &db_path.join("wal"),
        policy,
        &mut plan.opened_in_salvage_mode,
    )?;
    let Some((segments, active)) = paths else {
        return Ok(LocalSources {
            active: None,
            next_segment_id: 1,
            skipped: BTreeSet::new(),
        });
    };
    let mut skipped = BTreeSet::new();
    // Even skipped or quarantined local identities may not be reused. Check
    // exhaustion before normalization can rename any source files.
    let next_local_id = next_segment_id(segments.keys().copied().max())?;
    for (segment_id, paths) in segments {
        scope.check("local WAL source selection")?;
        let selected = select_local_segment(
            local,
            segment_id,
            paths,
            policy,
            limits,
            read_window,
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
    let active = active
        .map(|path| active_local_source(local, &path, policy, limits, plan))
        .transpose()?
        .flatten();
    Ok(LocalSources {
        active,
        next_segment_id: next_local_id,
        skipped,
    })
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

pub(crate) fn local_path(path: &Path) -> MidgeResult<FsPath> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| MidgeError::RecoveryFailed("local WAL filename is not UTF-8".into()))?;
    Ok(FsPath::new(format!("wal/{name}")))
}

fn select_local_segment(
    fs: &Arc<dyn Fs>,
    segment_id: u64,
    mut paths: Vec<PathBuf>,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
    read_window: usize,
    salvaged: &mut bool,
) -> MidgeResult<Option<(FsPath, crate::runtime::RecoveredCloudWalSegment)>> {
    let canonical_name = crate::wal::segment_file_name(segment_id);
    paths.sort_by_key(|path| {
        (
            path.file_name()
                .is_none_or(|name| name != canonical_name.as_str()),
            path.clone(),
        )
    });
    let mut selected: Option<(PathBuf, crate::runtime::RecoveredCloudWalSegment)> = None;
    let mut aliases = Vec::new();
    for path in paths {
        let source_path = local_path(&path)?;
        let result = (|| {
            let file = fs
                .open(&source_path, READ_ONLY)
                .map_err(FsError::into_midge)?;
            inspect_local_sealed_wal_file(file.as_ref(), &source_path, limits)
        })();
        let Some(prefix) = recover_or_salvage(result, policy, salvaged)? else {
            aliases.push(path);
            continue;
        };
        if let Some((selected_path, _)) = &selected {
            if !wal_sources_equal(
                (fs.as_ref(), &local_path(selected_path)?),
                (fs.as_ref(), &source_path),
                read_window,
            )? {
                if policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "conflicting duplicate local WAL files for segment {segment_id}"
                    )));
                }
                *salvaged = true;
                tracing::warn!(
                    segment_id,
                    "retaining canonical local WAL alias during salvage recovery"
                );
            }
            aliases.push(path);
        } else {
            selected = Some((
                path,
                crate::runtime::RecoveredCloudWalSegment {
                    max_sequence: prefix.max_sequence,
                    writer_epoch: prefix.writer_epoch,
                },
            ));
        }
    }
    let Some((selected_path, segment)) = selected else {
        return Ok(None);
    };
    let canonical_path = selected_path.with_file_name(canonical_name);
    canonicalize_aliases(
        fs.as_ref(),
        &selected_path,
        &canonical_path,
        &aliases,
        read_window,
    )?;
    Ok(Some((local_path(&canonical_path)?, segment)))
}

fn canonicalize_aliases(
    fs: &dyn Fs,
    selected: &Path,
    canonical: &Path,
    aliases: &[PathBuf],
    read_window: usize,
) -> MidgeResult<()> {
    let mut changed = false;
    let canonical_path = local_path(canonical)?;
    if selected != canonical {
        if fs.exists(&canonical_path).map_err(FsError::into_midge)? {
            CloudStartupRecovery::quarantine_local_wal_alias(fs, &canonical_path)?;
        }
        // Rename within one WAL directory preserves the verified bytes without
        // requiring a second complete local copy or changing the inode.
        fs.rename_atomic(&local_path(selected)?, &canonical_path)
            .map_err(FsError::into_midge)?;
        changed = true;
    }
    for alias in aliases {
        let alias_path = local_path(alias)?;
        if alias == canonical || !fs.exists(&alias_path).map_err(FsError::into_midge)? {
            continue;
        }
        let equal = match wal_sources_equal((fs, &canonical_path), (fs, &alias_path), read_window) {
            Err(error @ MidgeError::Timeout(_)) => return Err(error),
            result => result.unwrap_or(false),
        };
        if equal {
            fs.remove_file(&alias_path).map_err(FsError::into_midge)?;
        } else {
            CloudStartupRecovery::quarantine_local_wal_alias(fs, &alias_path)?;
        }
        changed = true;
    }
    if changed {
        fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    Ok(())
}

fn active_local_source(
    fs: &Arc<dyn Fs>,
    active: &Path,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
    plan: &mut CloudWalRecoveryPlan,
) -> MidgeResult<Option<ReplaySource>> {
    let path = local_path(active)?;
    let Some(file) = recover_or_salvage(
        fs.open(&path, READ_ONLY).map_err(FsError::into_midge),
        policy,
        &mut plan.opened_in_salvage_mode,
    )?
    else {
        quarantine_active(fs.as_ref(), active)?;
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
    if prefix.record_count == 0 {
        if length > 0 {
            quarantine_active(fs.as_ref(), active)?;
        }
        return Ok(None);
    }
    if prefix.valid_bytes as u64 != length {
        if salvaged {
            // Salvage drops acknowledged records past the corruption; keep the
            // original bytes before cutting the only copy.
            CloudStartupRecovery::retain_local_wal_copy(fs.as_ref(), &path)?;
            fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)
                .map_err(FsError::into_midge)?;
        }
        let mut file = fs
            .open(
                &path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        file.truncate(prefix.valid_bytes as u64)
            .map_err(FsError::into_midge)?;
        file.sync(crate::io::Durability::Durable)
            .map_err(FsError::into_midge)?;
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

fn quarantine_active(fs: &dyn Fs, active: &Path) -> MidgeResult<()> {
    let path = local_path(active)?;
    if fs.exists(&path).map_err(FsError::into_midge)? {
        CloudStartupRecovery::quarantine_local_wal_alias(fs, &path)?;
        fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    Ok(())
}

fn enforce_epoch_order(
    db_path: &Path,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
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
            plan.set_aside_local_paths
                .push(db_path.join("wal").join(crate::wal::ACTIVE_FILE_NAME));
        }
    }
    Ok(())
}

/// Highest verified sequence anywhere in a WAL file being set aside. If the
/// file cannot be scanned, salvage must not reuse an unknown sequence range.
fn verified_max_sequence(
    local: &dyn Fs,
    path: &Path,
    limits: StreamingReplayLimits,
) -> MidgeResult<u64> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(MidgeError::RecoveryFailed(
            "local WAL filename is not UTF-8".into(),
        ));
    };
    let wal_path = FsPath::new(format!("wal/{name}"));
    let file = local
        .open(&wal_path, READ_ONLY)
        .map_err(FsError::into_midge)?;
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
/// (local files renamed, cloud objects kept) and the sequence floor is
/// lifted above them.
fn stop_at_first_hole(
    context: &WalPlanningContext<'_>,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    skipped: &BTreeSet<u64>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
) -> MidgeResult<()> {
    let WalPlanningContext {
        db_path,
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
    let wal_dir = db_path.join("wal");
    // Every local file at or past the hole goes, including failed copies,
    // or the next open would find the same hole and drop newer writes.
    let mut local_paths = Vec::new();
    let entries = match std::fs::read_dir(&wal_dir) {
        Ok(entries) => entries.collect::<Result<Vec<_>, _>>()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        scope.check("WAL salvage discovery")?;
        if entry
            .file_name()
            .to_str()
            .and_then(crate::wal::parse_segment_id)
            .is_some_and(|segment_id| segment_id >= hole)
        {
            local_paths.push(entry.path());
        }
    }
    let local = crate::io::scope_fs(
        Arc::new(crate::io::RealFs::new(db_path).map_err(FsError::into_midge)?),
        scope.clone(),
    );
    for path in &local_paths {
        scope.check("WAL salvage frontier")?;
        // A corrupt local-only file is in neither the catalog nor the plan;
        // its verified prefix is the best record of what it held.
        max_sequence = max_sequence.max(verified_max_sequence(local.as_ref(), path, limits)?);
    }
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
    if active.take().is_some() {
        local_paths.push(wal_dir.join(crate::wal::ACTIVE_FILE_NAME));
    }
    // Renamed only after startup persists the floor that covers them; see
    // `CloudWalRecoveryPlan::set_aside_local_wal`.
    plan.set_aside_local_paths.extend(local_paths);
    plan.set_aside_local_paths.sort();
    plan.set_aside_local_paths.dedup();
    plan.max_unreplayed_sequence = plan.max_unreplayed_sequence.max(max_sequence);
    Ok(())
}
