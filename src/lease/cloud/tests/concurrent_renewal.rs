//! Exercise simulated lease replacement while real filesystem readers are live.

use super::{test_config, CloudStorageLease};
use crate::io::{Fs, FsPath, OpenMode, OpenOptions, RealFs};
use crate::lease::traits::{LeaderStore, PrimaryLease};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

fn validate_during_renewal(
    fs: &RealFs,
    store: &dyn LeaderStore,
    holder: &str,
    epoch: u64,
    running: &AtomicBool,
    ready: &mpsc::Sender<()>,
) -> Result<usize, String> {
    // Keep the old handle open across every atomic leader replacement. A new
    // validator must see the current record while this handle keeps its bytes.
    let file = fs
        .open(
            &FsPath::new(".midge_leader"),
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(|error| format!("open pinned leader: {error}"))?;
    let len = file
        .len()
        .map_err(|error| format!("pinned leader length: {error}"))?;
    let before = file
        .read_at(0, len)
        .map_err(|error| format!("read pinned leader: {error}"))?;
    store
        .validate_epoch(holder, epoch)
        .map_err(|error| format!("initial epoch validation: {error}"))?;
    ready
        .send(())
        .map_err(|error| format!("announce ready validator: {error}"))?;

    let started = Instant::now();
    let mut validations = 0;
    while running.load(Ordering::Acquire) {
        if started.elapsed() >= Duration::from_secs(30) {
            return Err("renewals did not finish within the validator deadline".to_string());
        }
        store
            .validate_epoch(holder, epoch)
            .map_err(|error| format!("concurrent epoch validation {validations}: {error}"))?;
        validations += 1;
    }
    let after = file
        .read_at(0, len)
        .map_err(|error| format!("read pinned leader after renewal: {error}"))?;
    if after != before {
        return Err("atomic leader replacement changed the pinned reader's bytes".to_string());
    }
    Ok(validations)
}

#[test]
fn should_preserve_simulated_lease_when_renewing_with_concurrent_epoch_validation() {
    // Arrange
    let directory = tempfile::tempdir().expect("lease directory");
    let lease = Arc::new(CloudStorageLease::new(test_config(), directory.path()));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let holder = lease.holder_id();
    let store = lease.get_leader_store().expect("leader store");
    let before = store
        .read_current()
        .expect("read initial leader")
        .expect("initial leader exists");
    let fs = RealFs::open_existing(directory.path()).expect("reader filesystem");
    let running = AtomicBool::new(true);
    let (ready, received) = mpsc::channel();

    // Act
    let (renewals, validations) = std::thread::scope(|scope| {
        let reader = scope.spawn({
            let fs = &fs;
            let store = store.as_ref();
            let holder = &holder;
            let running = &running;
            move || validate_during_renewal(fs, store, holder, epoch, running, &ready)
        });
        let renewals = (|| {
            received
                .recv_timeout(Duration::from_secs(30))
                .map_err(|error| format!("wait for pinned validator: {error}"))?;
            for index in 0..32 {
                lease
                    .renew()
                    .map_err(|error| format!("simulated renewal {index}: {error}"))?;
            }
            Ok::<(), String>(())
        })();
        running.store(false, Ordering::Release);
        (renewals, reader.join().expect("validator thread panicked"))
    });

    // Assert
    assert!(
        renewals.is_ok() && validations.is_ok(),
        "renewal result: {renewals:?}; concurrent validation result: {validations:?}"
    );
    assert!(validations.expect("successful validations") > 0);
    assert_eq!(
        lease.epoch(),
        epoch,
        "renewal must preserve the writer epoch"
    );
    store
        .validate_epoch(&holder, epoch)
        .expect("current holder remains authoritative");
    let after = store
        .read_current()
        .expect("read renewed leader")
        .expect("renewed leader exists");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&after.acquired_at).expect("renewed timestamp")
            > chrono::DateTime::parse_from_rfc3339(&before.acquired_at).expect("initial timestamp"),
        "a newly opened leader must expose the completed timestamp refresh"
    );
    lease.release().expect("release lease");
}
