//! Cloud WAL recovery planning I/O (#844): read what the pure planner
//! (`wal::planner`) decides over, then expose the planned sources as one
//! read-only replay view, with no local segment staging.
//!
//! The cataloged objects are probed (HEAD, checksum, inspection), then the
//! cache's ledger members the lineage needs: its tail, and under salvage the
//! local copy of a shared member whose object is unusable. The planner
//! decides; this module exposes the verified open prefix without changing
//! its backing file. Startup commits the sequence floor before cutting it.

use super::streaming_wal_fs::{validate_wal_source, StreamingWalFs};
use super::CloudStartupRecovery;
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsError, FsPath, ReadFs};
use crate::storage::{StorageBackend, StorageEvent, StorageOutcome};
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::ledger::{SegmentId, WalLedger};
use crate::wal::planner::{
    Lineage, OpenCut, OpenEnd, OpenProbe, Probe, Probes, RecoveryPlan, Source,
};
use crate::wal::recovery::streaming::{
    inspect_sealed_wal_file, inspect_wal_file, max_verified_suffix_sequence, StreamingReplayLimits,
};
use crate::wal::recovery::ReplaySegments;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests;

pub(crate) struct StreamingCloudWalRecovery {
    pub(crate) fs: Arc<dyn ReadFs>,
    /// The planned WAL files in `fs`, in replay order.
    pub(crate) segments: ReplaySegments,
    pub(crate) plan: RecoveryPlan,
}

impl StreamingCloudWalRecovery {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        local: &Arc<dyn Fs>,
        ledger: Option<&WalLedger>,
        remote: &Arc<dyn StorageBackend>,
        catalog: &WalPublicationCatalog,
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
    /// `ledger` names; a cache without a ledger replays only the catalog.
    /// The open member's cut goes through `local`, so fault injection
    /// reaches it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_within(
        local: &Arc<dyn Fs>,
        ledger: Option<&WalLedger>,
        remote: &Arc<dyn StorageBackend>,
        catalog: &WalPublicationCatalog,
        policy: RecoveryPolicy,
        timeout: Duration,
        read_window: usize,
        limits: StreamingReplayLimits,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("cloud WAL planning")?;
        let local = &crate::io::scope_fs(Arc::clone(local), scope.clone());
        let lineage = crate::wal::planner::lineage(catalog, ledger);
        if let (Lineage::Diverged, Some(ledger)) = (lineage, ledger) {
            tracing::warn!(
                open_segment = ledger.open_segment(),
                catalog_head = catalog.head().id,
                "another writer published past this cache; setting its local WAL aside"
            );
        }
        let mut probes = Probes::default();
        let mut remote_sources = BTreeMap::new();
        for entry in catalog.entries() {
            scope.check("cloud WAL catalog segment")?;
            let source = PublishedObject {
                local: Arc::clone(local),
                remote: Arc::clone(remote),
                timeout,
                read_window,
                limits,
                scope,
            };
            match source.probe(entry) {
                Ok((replay_source, probe)) => {
                    remote_sources.insert(entry.id(), replay_source);
                    probes.remote.insert(entry.id(), probe);
                }
                Err(error) => {
                    probes.remote.insert(entry.id(), classify(error)?);
                }
            }
        }
        if let Some(ledger) = ledger {
            for member in ledger.member_facts() {
                scope.check("local WAL member probe")?;
                let id = member.id();
                if !needs_local_probe(lineage, policy, catalog, &probes, id) {
                    continue;
                }
                let probe = if member.sealed.is_some() {
                    probe_sealed_member(local.as_ref(), id, policy, limits)?
                } else {
                    probe_open_member(local.as_ref(), id, member.durable_bytes, policy, limits)?
                };
                probes.local.insert(id, probe);
            }
        }
        let plan = crate::wal::planner::plan(policy, catalog, ledger, &probes)?;
        let (fs, segments) =
            assemble_planned_sources(&plan, remote_sources, local, read_window, scope)?;
        scope.check("cloud WAL planning completion")?;
        Ok(Self { fs, segments, plan })
    }
}

/// Whether planning needs the cache's copy of member `id`: the tail of a
/// joined ledger, and under salvage a shared member whose object is
/// unusable, or the open member of a ledger the catalog lost history of.
fn needs_local_probe(
    lineage: Lineage,
    policy: RecoveryPolicy,
    catalog: &WalPublicationCatalog,
    probes: &Probes,
    id: SegmentId,
) -> bool {
    let salvage = policy == RecoveryPolicy::Salvage;
    match lineage {
        Lineage::Joined => {
            id > catalog.head().id
                || (salvage
                    && catalog.entry(id).is_some()
                    && !matches!(probes.remote.get(&id), Some(Probe::Sealed { .. })))
        }
        Lineage::Behind { covered: false } => salvage,
        Lineage::Fresh | Lineage::Behind { covered: true } | Lineage::Diverged => false,
    }
}

/// A failed probe as the planner reads it: damage salvage may skip, or the
/// error recovery stops on.
fn classify(error: MidgeError) -> MidgeResult<Probe> {
    match error {
        MidgeError::Corruption(reason) => Ok(Probe::Damaged {
            reason,
            max_verified: 0,
        }),
        error
        @ (MidgeError::ResourceLimit(_) | MidgeError::NoSpace(_) | MidgeError::Timeout(_)) => {
            Err(error)
        }
        error => Err(MidgeError::RecoveryFailed(format!(
            "WAL source validation failed: {error}"
        ))),
    }
}

/// A cataloged object as a replay source: where it reads, and its key.
type RemoteSource = (Arc<dyn ReadFs>, FsPath);

/// How a cataloged object is read: through the cache's root for range
/// buffering, from `remote`, bounded by `timeout` and `scope`.
struct PublishedObject<'a> {
    local: Arc<dyn Fs>,
    remote: Arc<dyn StorageBackend>,
    timeout: Duration,
    read_window: usize,
    limits: StreamingReplayLimits,
    scope: &'a DeadlineScope,
}

impl PublishedObject<'_> {
    /// Read `entry`'s object: HEAD, then its size and checksum, then every
    /// frame through its footer, which must seal the entry's link. Returns
    /// the object as a replay source, with what it holds.
    ///
    /// # Errors
    ///
    /// `Corruption` for an object that is gone or is not the entry's
    /// segment; the read error otherwise.
    fn probe(&self, entry: &CatalogEntry) -> MidgeResult<(RemoteSource, Probe)> {
        let key = entry.object_key();
        let scope = self.scope;
        scope.check("cloud WAL HEAD submission")?;
        let deadline = if scope.deadline().is_bounded() {
            scope.deadline()
        } else {
            OperationDeadline::from_budget(self.timeout)
        };
        let (tx, rx) = std::sync::mpsc::channel();
        self.remote.submit_range_head_request(
            crate::storage::StorageRequest::new(&key, deadline, self.timeout),
            tx,
        );
        scope.check("cloud WAL HEAD wait")?;
        let metadata = match rx.recv_timeout(deadline.clamp(self.timeout)) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(metadata),
                ..
            }) => metadata,
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => {
                let message = format!("cloud WAL {key} HEAD: {error}");
                // A cataloged segment that no longer exists is lost data,
                // not a transient failure, so salvage may skip it.
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
                Arc::clone(&self.local),
                Arc::clone(&self.remote),
                key.clone(),
                metadata,
                self.timeout,
            )
            .with_deadline(scope.deadline()),
        );
        let fs = crate::telemetry::recovery_progress::observe_reads(crate::io::scope_fs(
            fs,
            scope.clone(),
        ));
        self.inspect(entry, &key, fs)
    }

    fn inspect(
        &self,
        entry: &CatalogEntry,
        key: &str,
        fs: Arc<dyn Fs>,
    ) -> MidgeResult<(RemoteSource, Probe)> {
        let path = FsPath::new(key);
        validate_wal_source(
            fs.as_ref(),
            &path,
            entry.size_bytes(),
            entry.crc32c(),
            self.read_window,
        )?;
        // Inspection also uses the bounded range window even for a large
        // frame.
        let mut buffered = StreamingWalFs::new(self.read_window)?;
        let canonical = crate::wal::segment_file_name(entry.id());
        buffered.insert(
            canonical.clone(),
            Arc::clone(&fs) as Arc<dyn ReadFs>,
            path.clone(),
        )?;
        let file = buffered
            .open_read(&FsPath::new(canonical))
            .map_err(FsError::into_midge)?;
        let prefix = inspect_sealed_wal_file(file.as_ref(), &path, self.limits)?;
        let (Some(scan), Some(identity)) = (prefix.scan.as_ref(), prefix.identity()) else {
            return Err(MidgeError::Corruption(format!(
                "cloud WAL {key} is not a sealed segment"
            )));
        };
        entry.check(&identity).map_err(MidgeError::Corruption)?;
        let footer = scan.footer().ok_or_else(|| {
            MidgeError::Corruption(format!("cloud WAL {key} has no sealed footer"))
        })?;
        entry
            .check_facts(&footer.sealed_facts())
            .map_err(MidgeError::Corruption)?;
        Ok((
            (fs as Arc<dyn ReadFs>, path),
            Probe::Sealed {
                header: *scan.header(),
                link: identity.link,
            },
        ))
    }
}

/// Read sealed ledger member `id` whole. A member whose file is gone or
/// damaged lost its records: salvage may skip it, strict recovery may not;
/// under salvage, what its verified frames hold still counts toward the
/// sequence floor.
///
/// # Errors
///
/// The read error when it is not damage.
fn probe_sealed_member(
    local: &dyn Fs,
    id: SegmentId,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
) -> MidgeResult<Probe> {
    let path = crate::wal::segment_key(id);
    let file = match open_member(local, &path) {
        Ok(file) => file,
        Err(error) => return classify(error),
    };
    let probe = match inspect_sealed_wal_file(file.as_ref(), &path, limits) {
        Ok(prefix) => match (prefix.scan.as_ref(), prefix.identity()) {
            (Some(scan), Some(identity)) => {
                return Ok(Probe::Sealed {
                    header: *scan.header(),
                    link: identity.link,
                })
            }
            _ => Probe::Damaged {
                reason: format!("sealed WAL segment {path} ends without its footer"),
                max_verified: 0,
            },
        },
        Err(error) => classify(error)?,
    };
    match probe {
        Probe::Damaged { reason, .. } if policy == RecoveryPolicy::Salvage => Ok(Probe::Damaged {
            reason,
            max_verified: verified_maximum(file.as_ref(), &path, limits)?,
        }),
        probe => Ok(probe),
    }
}

/// The highest sequence any verified frame of a damaged segment holds: its
/// verified prefix, then whatever verifies past the damage.
fn verified_maximum(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> MidgeResult<u64> {
    let prefix = match inspect_wal_file(file, path, limits) {
        Ok(prefix) => return Ok(prefix.max_sequence()),
        Err(failure) if matches!(failure.error(), MidgeError::Timeout(_)) => {
            return Err(failure.error().replay())
        }
        Err(failure) => failure.verified_prefix(),
    };
    let suffix = max_verified_suffix_sequence(file, path, prefix.valid_bytes as u64, limits)?;
    Ok(prefix.max_sequence().max(suffix.unwrap_or(0)))
}

/// Read the open ledger member `id` as far as it verifies, and under
/// salvage what verified frames past damage hold.
///
/// # Errors
///
/// A resource limit, or a read error that is not damage.
fn probe_open_member(
    local: &dyn Fs,
    id: SegmentId,
    durable_bytes: Option<u64>,
    policy: RecoveryPolicy,
    limits: StreamingReplayLimits,
) -> MidgeResult<Probe> {
    let path = crate::wal::segment_key(id);
    let file = match open_member(local, &path) {
        Ok(file) => file,
        Err(error) => return classify(error),
    };
    let len = file.len().map_err(FsError::into_midge)?;
    let (prefix, end) = match inspect_wal_file(file.as_ref(), &path, limits) {
        Ok(prefix) => (prefix, OpenEnd::Whole),
        Err(failure)
            if matches!(
                failure.error(),
                MidgeError::ResourceLimit(_) | MidgeError::NoSpace(_)
            ) =>
        {
            return Err(failure.error().replay())
        }
        Err(failure)
            if failure.is_incomplete_tail()
                && durable_bytes
                    .is_none_or(|bytes| failure.verified_prefix().valid_bytes as u64 >= bytes) =>
        {
            (failure.verified_prefix(), OpenEnd::Torn)
        }
        Err(failure)
            if failure.error().is_salvageable()
                && durable_bytes
                    .is_some_and(|bytes| failure.verified_prefix().valid_bytes as u64 >= bytes) =>
        {
            (failure.verified_prefix(), OpenEnd::Torn)
        }
        Err(failure) if failure.error().is_salvageable() => {
            let prefix = failure.verified_prefix();
            let suffix_max = if policy == RecoveryPolicy::Salvage {
                max_verified_suffix_sequence(
                    file.as_ref(),
                    &path,
                    prefix.valid_bytes as u64,
                    limits,
                )?
                .unwrap_or(0)
            } else {
                0
            };
            (
                prefix,
                OpenEnd::Damaged {
                    reason: failure.error().to_string(),
                    suffix_max,
                },
            )
        }
        Err(failure) => {
            return Err(MidgeError::RecoveryFailed(format!(
                "active WAL failed validation: {}",
                failure.error()
            )))
        }
    };
    let Some(scan) = prefix.scan.as_ref() else {
        let suffix_max = match &end {
            OpenEnd::Damaged { suffix_max, .. } => *suffix_max,
            OpenEnd::Whole | OpenEnd::Torn => 0,
        };
        return Ok(Probe::Damaged {
            reason: format!("open WAL segment {path} has no readable header"),
            max_verified: suffix_max,
        });
    };
    Ok(Probe::Open(OpenProbe {
        header: *scan.header(),
        len,
        valid_bytes: prefix.valid_bytes as u64,
        record_count: prefix.record_count(),
        max_sequence: prefix.max_sequence(),
        end,
    }))
}

/// A ledger member's file, which must exist: one that is gone lost its
/// records.
fn open_member<'a>(
    fs: &'a dyn Fs,
    path: &FsPath,
) -> MidgeResult<Box<dyn crate::io::ReadFile + 'a>> {
    fs.open_read(path).map_err(|error| match error {
        FsError::NotFound(_) => MidgeError::Corruption(format!(
            "WAL segment {path} is history but its file is missing"
        )),
        error => error.into_midge(),
    })
}

/// Cut the open member back to the prefix replay applies, keeping a durable
/// copy of the original first when the cut drops what salvage or a crash
/// cannot explain.
pub(super) fn apply_open_cut(wal: &dyn Fs, cut: OpenCut) -> MidgeResult<()> {
    let path = FsPath::new(crate::wal::segment_file_name(cut.id));
    if cut.retain {
        CloudStartupRecovery::retain_local_wal_copy(wal, &path)?;
        wal.sync_dir(&FsPath::new(""), crate::io::Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    crate::io::truncate_durably(wal, &path, cut.valid_bytes)
        .map(drop)
        .map_err(FsError::into_midge)
}

/// Expose the planned sources as one WAL directory, and return it with the
/// order replay reads them in.
fn assemble_planned_sources(
    plan: &RecoveryPlan,
    mut remote: BTreeMap<SegmentId, RemoteSource>,
    local: &Arc<dyn Fs>,
    read_window: usize,
    scope: &DeadlineScope,
) -> MidgeResult<(Arc<dyn ReadFs>, ReplaySegments)> {
    let mut replay_fs = StreamingWalFs::new(read_window)?;
    for planned in &plan.replay {
        scope.check("cloud WAL source assembly")?;
        let (fs, path) = match planned.source {
            Source::Remote => remote.remove(&planned.id).ok_or_else(|| {
                MidgeError::Internal(format!(
                    "cloud WAL segment {} was planned without its object",
                    planned.id
                ))
            })?,
            Source::Local => (
                Arc::clone(local) as Arc<dyn ReadFs>,
                crate::wal::segment_key(planned.id),
            ),
        };
        replay_fs.insert(crate::wal::segment_file_name(planned.id), fs, path)?;
        if let Some(cut) = plan.open_cut.filter(|cut| cut.id == planned.id) {
            replay_fs.limit_prefix(&crate::wal::segment_file_name(planned.id), cut.valid_bytes)?;
        }
    }
    let segments = ReplaySegments::from_plan(plan.anchor, &plan.replay, |id| {
        FsPath::new(format!(
            "{}/{}",
            crate::wal::WAL_DIR,
            crate::wal::segment_file_name(id)
        ))
    });
    Ok((
        crate::io::scope_read_fs(Arc::new(replay_fs), scope.clone()),
        segments,
    ))
}
