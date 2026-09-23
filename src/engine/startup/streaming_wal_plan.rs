//! Catalog-authorized cloud WAL replay with no local segment staging.

use super::streaming_wal_fs::{validate_wal_source, wal_sources_equal, StreamingWalFs};
use super::{CloudStartupRecovery, CloudWalRecoveryPlan};
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsPath, OpenMode, OpenOptions};
use crate::storage::{StorageBackend, StorageEvent, StorageOutcome};
use crate::wal::recovery::streaming::{
    inspect_sealed_wal_file, inspect_wal_file, StreamingReplayLimits,
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

pub(super) struct StreamingCloudWalRecovery {
    pub(super) fs: Arc<dyn Fs>,
    pub(super) plan: CloudWalRecoveryPlan,
    pub(super) next_segment_id: u64,
}

impl StreamingCloudWalRecovery {
    pub(super) fn build(
        db_path: &Path,
        remote: &Arc<dyn StorageBackend>,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
    ) -> MidgeResult<Self> {
        let next_remote_id = next_segment_id(catalog.segments.keys().copied().max())?;
        let mut replay_fs = StreamingWalFs::new(read_window)?;
        let local: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(db_path)?);
        let mut plan = CloudWalRecoveryPlan {
            remote_segments: BTreeMap::new(),
            local_segments: BTreeMap::new(),
            active_wal: None,
            opened_in_salvage_mode: false,
            unreplayed_segments: Vec::new(),
            max_unreplayed_sequence: catalog.sequence_floor,
        };
        let mut sources = BTreeMap::new();
        let mut skipped = BTreeSet::new();
        for (segment_id, publication) in &catalog.segments {
            validate_publication_identity(*segment_id, publication, catalog.fencing_epoch)?;
            let result = remote_source(
                Arc::clone(&local),
                Arc::clone(remote),
                publication,
                timeout,
                read_window,
                limits,
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
        let LocalSources {
            active: mut active_source,
            next_segment_id: next_local_id,
            skipped: skipped_local,
        } = merge_local_sources(
            db_path,
            &local,
            &mut plan,
            &mut sources,
            policy,
            read_window,
            limits,
        )?;
        skipped.extend(skipped_local);
        enforce_epoch_order(db_path, &mut plan, &mut sources, &mut active_source, policy)?;
        stop_at_first_hole(
            db_path,
            catalog,
            &skipped,
            &mut plan,
            &mut sources,
            &mut active_source,
            limits,
        )?;
        for (segment_id, source) in sources {
            replay_fs.insert(
                crate::wal::segment_file_name(segment_id),
                source.fs,
                source.path,
            )?;
        }
        if let Some(source) = active_source {
            replay_fs.insert(crate::wal::ACTIVE_FILE_NAME.into(), source.fs, source.path)?;
        }
        Ok(Self {
            fs: Arc::new(replay_fs),
            plan,
            next_segment_id: next_remote_id.max(next_local_id),
        })
    }
}

fn next_segment_id(highest: Option<u64>) -> MidgeResult<u64> {
    highest
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| MidgeError::ResourceLimit("WAL segment identity space exhausted".into()))
}

struct LocalSources {
    active: Option<ReplaySource>,
    next_segment_id: u64,
    /// Local-only segments that failed validation and were not replayed.
    skipped: BTreeSet<u64>,
}

fn merge_local_sources(
    db_path: &Path,
    local: &Arc<dyn Fs>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    policy: RecoveryPolicy,
    read_window: usize,
    limits: StreamingReplayLimits,
) -> MidgeResult<LocalSources> {
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
        Err(error @ (MidgeError::ResourceLimit(_) | MidgeError::NoSpace(_))) => Err(error),
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
) -> MidgeResult<ReplaySource> {
    let (tx, rx) = std::sync::mpsc::channel();
    remote.submit_range_head(&publication.object_key, timeout, tx);
    let metadata = match rx.recv_timeout(timeout) {
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
            return Err(if error.is_not_found() {
                MidgeError::Corruption(message)
            } else {
                MidgeError::RecoveryFailed(message)
            });
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
    let fs: Arc<dyn Fs> = Arc::new(crate::storage::remote_sst::RemoteSstFs::for_object(
        local,
        remote,
        publication.object_key.clone(),
        metadata,
        timeout,
    ));
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
    let file = buffered.open(&FsPath::new(canonical), READ_ONLY)?;
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

fn local_path(path: &Path) -> MidgeResult<FsPath> {
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
            let file = fs.open(&source_path, READ_ONLY)?;
            inspect_sealed_wal_file(file.as_ref(), &source_path, limits)
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
    if selected != canonical {
        if canonical.try_exists()? {
            CloudStartupRecovery::quarantine_local_wal_alias(canonical)?;
        }
        // Rename within one WAL directory preserves the verified bytes without
        // requiring a second complete local copy or changing the inode.
        std::fs::rename(selected, canonical)?;
        changed = true;
    }
    for alias in aliases {
        if alias == canonical || !alias.try_exists()? {
            continue;
        }
        let equal = wal_sources_equal(
            (fs, &local_path(canonical)?),
            (fs, &local_path(alias)?),
            read_window,
        )
        .unwrap_or(false);
        if equal {
            std::fs::remove_file(alias)?;
        } else {
            CloudStartupRecovery::quarantine_local_wal_alias(alias)?;
        }
        changed = true;
    }
    if changed {
        fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)?;
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
        fs.open(&path, READ_ONLY).map_err(MidgeError::from),
        policy,
        &mut plan.opened_in_salvage_mode,
    )?
    else {
        quarantine_active(fs.as_ref(), active)?;
        return Ok(None);
    };
    let length = file.len()?;
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
            CloudStartupRecovery::retain_local_wal_copy(active)?;
            fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)?;
        }
        let file = std::fs::OpenOptions::new().write(true).open(active)?;
        file.set_len(prefix.valid_bytes as u64)?;
        file.sync_all()?;
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
    if active.try_exists()? {
        CloudStartupRecovery::quarantine_local_wal_alias(active)?;
        fs.sync_dir(&FsPath::new("wal"), crate::io::Durability::Durable)?;
    }
    Ok(())
}

fn enforce_epoch_order(
    db_path: &Path,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
    policy: RecoveryPolicy,
) -> MidgeResult<()> {
    let mut highest_epoch = 0;
    let mut stale = Vec::new();
    for segment_id in sources.keys() {
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
        if let Some(source) = active.as_ref() {
            quarantine_active(
                source.fs.as_ref(),
                &db_path.join("wal").join(crate::wal::ACTIVE_FILE_NAME),
            )?;
        }
        plan.active_wal = None;
        *active = None;
    }
    Ok(())
}

/// Highest sequence in the verified prefix of a WAL file being set aside, or
/// zero when it cannot be read. Best effort: it only lifts the sequence floor.
fn verified_max_sequence(local: &dyn Fs, path: &Path, limits: StreamingReplayLimits) -> u64 {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return 0;
    };
    let wal_path = FsPath::new(format!("wal/{name}"));
    let file = match local.open(&wal_path, READ_ONLY) {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(%error, path = %path.display(), "cannot read WAL set aside by salvage");
            return 0;
        }
    };
    match inspect_wal_file(file.as_ref(), &wal_path, limits) {
        Ok(prefix) => prefix.max_sequence,
        Err(failure) => failure.verified_prefix().max_sequence,
    }
}

/// Salvage keeps a consistent prefix of history: once a segment is lost,
/// nothing after it may replay, or a delete in the lost segment could be
/// undone while newer writes stay visible. Later sources are set aside
/// (local files renamed, cloud objects kept) and the sequence floor is
/// lifted above them.
fn stop_at_first_hole(
    db_path: &Path,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    skipped: &BTreeSet<u64>,
    plan: &mut CloudWalRecoveryPlan,
    sources: &mut BTreeMap<u64, ReplaySource>,
    active: &mut Option<ReplaySource>,
    limits: StreamingReplayLimits,
) -> MidgeResult<()> {
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
        if entry
            .file_name()
            .to_str()
            .and_then(crate::wal::parse_segment_id)
            .is_some_and(|segment_id| segment_id >= hole)
        {
            local_paths.push(entry.path());
        }
    }
    let local_paths_empty = local_paths.is_empty();
    let local = crate::io::RealFs::new(db_path)?;
    for path in local_paths {
        // A corrupt local-only file is in neither the catalog nor the plan;
        // its verified prefix is the best record of what it held.
        max_sequence = max_sequence.max(verified_max_sequence(&local, &path, limits));
        CloudStartupRecovery::quarantine_local_wal_alias(&path)?;
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
        quarantine_active(&local, &wal_dir.join(crate::wal::ACTIVE_FILE_NAME))?;
    } else if !local_paths_empty {
        std::fs::File::open(&wal_dir)?.sync_all()?;
    }
    plan.max_unreplayed_sequence = plan.max_unreplayed_sequence.max(max_sequence);
    Ok(())
}
