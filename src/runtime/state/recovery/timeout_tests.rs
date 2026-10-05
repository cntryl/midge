//! Actual local metadata reads must not select salvage/default state on timeout.

use super::*;
use crate::config::RecoveryPolicy;
use crate::io::traits::{DirEntry, HostAddressing, Metadata};
use crate::io::{Durability, File, FsPath, FsResult, OpenOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const READ_BUDGET: Duration = Duration::from_millis(1);
const READ_DELAY: Duration = Duration::from_millis(10);

#[derive(Clone, Default)]
struct ReadEvidence {
    completed_reads: Arc<AtomicUsize>,
    mutations: Arc<AtomicUsize>,
    completed_lists: Arc<AtomicUsize>,
    read_failed: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone, Copy)]
enum ReadDelivery {
    Timeout,
    SuccessfulUntilExpiry(crate::common::OperationDeadline),
    IoBeforeQuarantine,
}

struct TimedReadFs {
    inner: crate::io::RealFs,
    target: &'static str,
    evidence: ReadEvidence,
    delay: Duration,
    delivery: ReadDelivery,
}

struct TimedReadFile<'a> {
    inner: Box<dyn File + 'a>,
    evidence: ReadEvidence,
    delay: Duration,
    delivery: ReadDelivery,
}

impl File for TimedReadFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
        let started = Instant::now();
        let bytes = self.inner.read_at(offset, len)?;
        let completed = self.evidence.completed_reads.fetch_add(1, Ordering::AcqRel) + 1;
        if let ReadDelivery::SuccessfulUntilExpiry(deadline) = self.delivery {
            if completed == 2 {
                // The parser consumed the first genuine read to request this
                // next one. Successful bytes spend the original shared budget;
                // the scoped production file decides whether to accept them.
                while !deadline.is_expired() {
                    std::thread::sleep(deadline.remaining());
                }
            }
            return Ok(bytes);
        }
        // This is an actual RealFs read whose controlled local caller budget
        // expires before its result is delivered. It is not provider evidence.
        std::thread::sleep(self.delay);
        if matches!(self.delivery, ReadDelivery::IoBeforeQuarantine) {
            self.evidence.read_failed.store(true, Ordering::Release);
            return Err(FsError::Io(
                "controlled failure after genuine WAL read".into(),
            ));
        }
        if matches!(self.delivery, ReadDelivery::Timeout) && started.elapsed() >= READ_BUDGET {
            return Err(FsError::Timeout("timed local metadata read".into()));
        }
        Ok(bytes)
    }

    fn write_at(&mut self, offset: u64, bytes: bytes::Bytes) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.write_at(offset, bytes)
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.truncate(len)
    }

    fn append(&mut self, bytes: bytes::Bytes) -> FsResult<u64> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.append(bytes)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.sync(durability)
    }
}

impl Fs for TimedReadFs {
    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let inner = self.inner.open(path, options)?;
        if path.0 != self.target {
            return Ok(inner);
        }
        Ok(Box::new(TimedReadFile {
            inner,
            evidence: self.evidence.clone(),
            delay: self.delay,
            delivery: self.delivery,
        }))
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let entries = self.inner.list_dir(path)?;
        self.evidence.completed_lists.fetch_add(1, Ordering::AcqRel);
        if self.evidence.read_failed.load(Ordering::Acquire) {
            return Err(FsError::Timeout("timed local quarantine inventory".into()));
        }
        Ok(entries)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.remove_file(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.evidence.mutations.fetch_add(1, Ordering::AcqRel);
        self.inner.rename_atomic(from, to)
    }
}

fn timed_fs(root: &std::path::Path, target: &'static str) -> (Arc<dyn Fs>, ReadEvidence) {
    read_fs(root, target, READ_DELAY, ReadDelivery::Timeout)
}

fn read_fs(
    root: &std::path::Path,
    target: &'static str,
    delay: Duration,
    delivery: ReadDelivery,
) -> (Arc<dyn Fs>, ReadEvidence) {
    let evidence = ReadEvidence::default();
    let fs = TimedReadFs {
        inner: crate::io::RealFs::new(root).expect("open real metadata filesystem"),
        target,
        evidence: evidence.clone(),
        delay,
        delivery,
    };
    (Arc::new(fs), evidence)
}

fn assert_preserved_read(
    root: &std::path::Path,
    target: &str,
    bytes: &[u8],
    evidence: &ReadEvidence,
) {
    assert!(evidence.completed_reads.load(Ordering::Acquire) > 0);
    assert_eq!(evidence.mutations.load(Ordering::Acquire), 0);
    assert_eq!(std::fs::read(root.join(target)).unwrap(), bytes);
}

fn assert_manifest_timeout(policy: RecoveryPolicy) {
    // Arrange: persist a genuine nonempty manifest before the timed read view.
    let directory = tempfile::tempdir().expect("create actual manifest directory");
    let manifest = Manifest {
        last_persisted_sequence: 7,
        ..Manifest::default()
    };
    crate::metadata::ManifestPersistence::save(directory.path(), &manifest)
        .expect("persist actual manifest");
    let target = crate::metadata::files::MANIFEST_SNAPSHOT;
    let before = std::fs::read(directory.path().join(target)).unwrap();
    let (fs, evidence) = timed_fs(directory.path(), target);
    let mut opened_in_salvage_mode = false;

    // Act: the real file returns a typed local timeout before parser admission.
    let result = RuntimeState::load_manifest(
        directory.path(),
        false,
        policy,
        &fs,
        &mut opened_in_salvage_mode,
    );

    // Assert: timeout is not corruption or a reason to use an empty manifest.
    assert_preserved_read(directory.path(), target, &before, &evidence);
    assert!(
        matches!(&result, Err(MidgeError::Timeout(message)) if message.contains("timed local metadata read")),
        "manifest timeout must stay typed under {policy:?}, result={result:?}"
    );
    assert!(!opened_in_salvage_mode);
    assert_eq!(evidence.completed_reads.load(Ordering::Acquire), 1);
}

fn assert_intent_timeout(policy: RecoveryPolicy) {
    // Arrange: a genuine persisted intent must not become an empty log.
    let directory = tempfile::tempdir().expect("create actual intent directory");
    let intents = [IntentLogEntry::WalSynced {
        segment_id: 3,
        seqno: 7,
    }];
    crate::runtime::IntentPersistence::save(directory.path(), &intents)
        .expect("persist actual intent log");
    let target = crate::metadata::files::INTENT_LOG;
    let before = std::fs::read(directory.path().join(target)).unwrap();
    let (fs, evidence) = timed_fs(directory.path(), target);
    let mut opened_in_salvage_mode = false;

    // Act: run the existing real recovery loader through the timed file view.
    let result = RuntimeState::load_intent_log(false, policy, &fs, &mut opened_in_salvage_mode);

    // Assert: the original log survives and the typed timeout escapes fallback.
    assert_preserved_read(directory.path(), target, &before, &evidence);
    assert!(
        matches!(&result, Err(MidgeError::Timeout(message)) if message.contains("timed local metadata read")),
        "intent timeout must stay typed under {policy:?}, result={result:?}"
    );
    assert!(!opened_in_salvage_mode);
    assert_eq!(evidence.completed_reads.load(Ordering::Acquire), 1);
}

#[test]
fn should_preserve_typed_manifest_timeout_when_strict_recovery_read_exceeds_budget() {
    assert_manifest_timeout(RecoveryPolicy::Strict);
}

#[test]
fn should_preserve_manifest_when_salvage_recovery_read_exceeds_budget() {
    assert_manifest_timeout(RecoveryPolicy::Salvage);
}

#[test]
fn should_preserve_typed_intent_timeout_when_strict_recovery_read_exceeds_budget() {
    assert_intent_timeout(RecoveryPolicy::Strict);
}

#[test]
fn should_preserve_intent_log_when_salvage_recovery_read_exceeds_budget() {
    assert_intent_timeout(RecoveryPolicy::Salvage);
}

fn seed_journal(root: &std::path::Path) -> Vec<u8> {
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(root).unwrap());
    let store = crate::metadata::store::ManifestStore::new(fs);
    for id in 1..=16 {
        assert_eq!(
            store
                .append(
                    &crate::metadata::journal::ManifestEdit::CreateColumnFamily {
                        id,
                        name: format!("deadline-cf-{id}"),
                        created_at: u64::from(id),
                    }
                )
                .unwrap(),
            u64::from(id)
        );
    }
    std::fs::read(root.join(crate::metadata::files::JOURNAL)).unwrap()
}

fn assert_journal_timeout(policy: RecoveryPolicy, aggregate: bool) {
    // Arrange: seed sixteen genuine durable edits and their fsync markers.
    let directory = tempfile::tempdir().unwrap();
    let target = crate::metadata::files::JOURNAL;
    let before = seed_journal(directory.path());
    // Act: successful individual reads still consume one aggregate budget.
    let (result, evidence, salvaged, scope) =
        run_journal_with_controlled_read_expiry(directory.path(), policy, aggregate);

    // Assert: neither policy may checkpoint or truncate a timed-out journal.
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert!(!salvaged);
    assert_preserved_read(directory.path(), target, &before, &evidence);
    let reads = evidence.completed_reads.load(Ordering::Acquire);
    if aggregate {
        assert_eq!(reads, 2);
        assert!(scope.deadline().is_expired());
    } else {
        assert_eq!(reads, 1);
    }
    assert!(!directory
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .exists());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    let healthy = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(healthy.edit_checkpoint_id, 16);
    assert_eq!(healthy.column_families.len(), 16);
    for id in 1..=16 {
        assert!(healthy
            .column_families
            .iter()
            .any(|cf| cf.id == id && cf.name == format!("deadline-cf-{id}")));
    }
}

fn run_journal_with_controlled_read_expiry(
    root: &std::path::Path,
    policy: RecoveryPolicy,
    aggregate: bool,
) -> (MidgeResult<Manifest>, ReadEvidence, bool, DeadlineScope) {
    crate::failpoints::with_read_gate(|| {
        let inner = crate::io::RealFs::new(root).unwrap();
        let evidence = ReadEvidence::default();
        // Admit setup before capturing the real budget; a successful second
        // read forces its expiry without depending on per-read sleep timing.
        let scope = DeadlineScope::new(crate::common::OperationDeadline::from_budget(
            Duration::from_secs(5),
        ));
        let (delivery, delay) = if aggregate {
            (
                ReadDelivery::SuccessfulUntilExpiry(scope.deadline()),
                Duration::ZERO,
            )
        } else {
            (ReadDelivery::Timeout, READ_DELAY)
        };
        let fs: Arc<dyn Fs> = Arc::new(TimedReadFs {
            inner,
            target: crate::metadata::files::JOURNAL,
            evidence: evidence.clone(),
            delay,
            delivery,
        });
        let fs = crate::io::scope_fs(fs, scope.clone());
        let mut salvaged = false;
        let result = RuntimeState::load_manifest_within(
            root,
            false,
            policy,
            &fs,
            &mut salvaged,
            Some(&scope),
        );
        (result, evidence, salvaged, scope)
    })
}

#[test]
fn should_preserve_journal_when_strict_recovery_read_returns_timeout() {
    assert_journal_timeout(RecoveryPolicy::Strict, false);
}

#[test]
fn should_preserve_journal_when_salvage_recovery_read_returns_timeout() {
    assert_journal_timeout(RecoveryPolicy::Salvage, false);
}

#[test]
fn should_stop_strict_journal_replay_when_successful_reads_exhaust_shared_budget() {
    assert_journal_timeout(RecoveryPolicy::Strict, true);
}

#[test]
fn should_stop_salvage_journal_replay_when_successful_reads_exhaust_shared_budget() {
    assert_journal_timeout(RecoveryPolicy::Salvage, true);
}

#[test]
fn should_preserve_typed_quarantine_timeout_when_salvage_follows_local_wal_read_failure() {
    // Arrange: the wrapper forwards a genuine WAL read before an explicitly
    // injected local Io failure. Its next genuine directory listing times out.
    // This is recovery error-policy coverage, not a native provider failure.
    let directory = tempfile::tempdir().unwrap();
    let wal_dir = directory.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"acknowledged"),
        Some(bytes::Bytes::from_static(b"value")),
        7,
        1,
    );
    let payload = crate::wal::encoding::encode(&record).unwrap();
    let mut before = Vec::new();
    crate::wal::frame::append_frame(&mut before, &payload).unwrap();
    let target = crate::wal::ACTIVE_FILE_NAME;
    std::fs::write(wal_dir.join(target), &before).unwrap();
    let (fs, evidence) = read_fs(
        &wal_dir,
        target,
        Duration::ZERO,
        ReadDelivery::IoBeforeQuarantine,
    );

    // Act: exercise the actual replay and its production salvage boundary.
    let result = RuntimeState::replay_wal_storage_within(
        &wal_dir,
        &directory.path().join("sst"),
        RecoveryPolicy::Salvage,
        &Manifest::default(),
        HashMap::new(),
        None,
        fs.as_ref(),
    );

    // Assert: a secondary timeout retains its type and all source bytes.
    let Err(error) = result else {
        panic!("local read failure must stop recovery");
    };
    assert!(
        matches!(error, MidgeError::Timeout(message) if message == "timed local quarantine inventory")
    );
    assert!(evidence.read_failed.load(Ordering::Acquire));
    assert!(evidence.completed_lists.load(Ordering::Acquire) >= 2);
    assert_eq!(evidence.completed_reads.load(Ordering::Acquire), 1);
    assert_preserved_read(&wal_dir, target, &before, &evidence);
    assert_eq!(std::fs::read_dir(wal_dir).unwrap().count(), 1);
}
