use cntryl_midge::{
    CloudWritePolicy, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const BLOCKED_UPLOAD_FAILPOINT: &str = "midge::cloud::before_wal_upload";

struct UploadRelease {
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl UploadRelease {
    fn release(&self) {
        let (released, changed) = &*self.gate;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }
}

impl Drop for UploadRelease {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
fn should_release_primary_lease_given_shutdown_timeout_when_shutdown_completes() {
    // Arrange
    let scenario = fail::FailScenario::setup();
    let temp_dir = tempfile::TempDir::new().expect("create cloud shutdown directory");
    let db_path = temp_dir.path().join("db");
    let lease_loss_calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = Arc::clone(&release_gate);
    let callback_fired = Arc::new(AtomicBool::new(false));
    let fired_in_callback = Arc::clone(&callback_fired);
    fail::cfg_callback(BLOCKED_UPLOAD_FAILPOINT, move || {
        fired_in_callback.store(true, Ordering::SeqCst);
        let _ = entered_tx.try_send(());
        let (released, changed) = &*callback_gate;
        let mut released = released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*released {
            released = changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    })
    .expect("configure blocked WAL upload boundary");
    let mut engine = Engine::open(cloud_options(&db_path, Arc::clone(&lease_loss_calls)))
        .expect("open cloud engine");
    let release = UploadRelease { gate: release_gate };
    let default_cf = engine
        .get_column_family("default")
        .expect("default column family");
    let mut transaction = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin cloud transaction");
    transaction
        .put(b"shutdown-key".to_vec(), b"shutdown-value".to_vec(), None)
        .expect("stage cloud write");
    transaction
        .commit(WriteOptions::cloud_async())
        .expect("commit CloudAsync write");
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("upload worker must reach deterministic blocked boundary");
    let wal_before_shutdown = local_wal_files(&db_path);
    assert!(
        !wal_before_shutdown.is_empty(),
        "CloudAsync write must have a sealed local WAL before upload"
    );

    // Act: the caller's budget expires while the event loop keeps ownership of
    // the blocked worker and lease. No lease-loss notification is appropriate
    // for an orderly, caller-bounded shutdown.
    let shutdown_started = Instant::now();
    let first_shutdown = engine.shutdown(Duration::from_millis(50));
    let shutdown_elapsed = shutdown_started.elapsed();
    let wal_after_timeout = local_wal_files(&db_path);
    let competing = Engine::open(cloud_options(&db_path, Arc::clone(&lease_loss_calls)));

    // Assert
    assert!(matches!(first_shutdown, Err(MidgeError::Timeout(_))));
    assert!(
        shutdown_elapsed < Duration::from_millis(500),
        "shutdown exceeded its caller budget by too much: {shutdown_elapsed:?}"
    );
    assert!(callback_fired.load(Ordering::SeqCst));
    assert!(
        wal_before_shutdown.is_subset(&wal_after_timeout),
        "timed-out shutdown removed local WAL needed for recovery: before={wal_before_shutdown:?}, after={wal_after_timeout:?}"
    );
    assert!(matches!(competing, Err(MidgeError::LeaseHeld(_))));
    assert_eq!(lease_loss_calls.load(Ordering::SeqCst), 0);

    // Act: let the runtime's shorter injected cloud-drain deadline expire
    // before releasing the real upload worker. The retained cleanup reaper
    // must preserve that eventual durability result after the first Engine
    // caller has already timed out.
    std::thread::sleep(Duration::from_millis(150));
    release.release();
    let terminal_shutdown = engine.shutdown(Duration::from_secs(5));
    let replayed_shutdown = engine.shutdown(Duration::from_millis(50));

    // Assert: every later caller observes the runtime's terminal error rather
    // than a synthetic cleanup success.
    let terminal_message = match terminal_shutdown {
        Err(MidgeError::Internal(message)) => message,
        other => panic!("expected terminal cloud-drain error, got {other:?}"),
    };
    assert!(terminal_message.contains("cloud uploads"));
    assert!(matches!(
        replayed_shutdown,
        Err(MidgeError::Internal(message)) if message == terminal_message
    ));
    assert!(
        !db_path.join(".midge_leader.lock").exists(),
        "completed shutdown cleanup must remove the acquisition lock"
    );
    fail::remove(BLOCKED_UPLOAD_FAILPOINT);
    scenario.teardown();
    let mut reopened = Engine::open(reopen_options(&db_path, Arc::clone(&lease_loss_calls)))
        .expect("reopen only after blocked runtime has exited");
    let default_cf = reopened
        .get_column_family("default")
        .expect("reopened default column family");
    let read = reopened
        .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
        .expect("begin recovery read");

    // Assert
    assert_eq!(
        read.get(b"shutdown-key").expect("read recovered value"),
        Some(bytes::Bytes::from_static(b"shutdown-value"))
    );
    drop(read);
    assert_eq!(lease_loss_calls.load(Ordering::SeqCst), 0);
    reopened
        .shutdown(Duration::from_secs(5))
        .expect("shutdown reopened engine");
}

fn cloud_options(db_path: &Path, lease_loss_calls: Arc<AtomicUsize>) -> cntryl_midge::OpenOptions {
    cloud_options_with_drain_timeout(db_path, lease_loss_calls, Duration::from_millis(100))
}

fn reopen_options(db_path: &Path, lease_loss_calls: Arc<AtomicUsize>) -> cntryl_midge::OpenOptions {
    cloud_options_with_drain_timeout(db_path, lease_loss_calls, Duration::from_secs(5))
}

fn cloud_options_with_drain_timeout(
    db_path: &Path,
    lease_loss_calls: Arc<AtomicUsize>,
    shutdown_cloud_drain_timeout: Duration,
) -> cntryl_midge::OpenOptions {
    OpenOptions::cloud_simulated(db_path, "shutdown-bucket", "shutdown/")
        .background_compaction(false)
        .cloud_write_policy(CloudWritePolicy {
            eventual_flush_segment_gap: 128,
            wal_seal_min_segment_bytes: usize::MAX,
            wal_seal_max_flush_delay: Duration::from_mins(1),
            wal_seal_max_pending_writes: 1,
        })
        .shutdown_cloud_drain_timeout_for_testing(shutdown_cloud_drain_timeout)
        .on_lease_loss(move || {
            lease_loss_calls.fetch_add(1, Ordering::SeqCst);
        })
        .build()
        .expect("build cloud shutdown options")
}

fn local_wal_files(db_path: &Path) -> BTreeSet<String> {
    let wal_dir = db_path.join("wal");
    let Ok(entries) = std::fs::read_dir(&wal_dir) else {
        return BTreeSet::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let metadata = entry.metadata().ok()?;
            (metadata.is_file() && metadata.len() > 0)
                .then(|| entry.file_name().to_string_lossy().into_owned())
        })
        .collect()
}
