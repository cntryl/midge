//! #808: non-corruption replay errors must preserve acknowledged WAL data.
//! The engine runs on a crash device so a WAL read can fail on demand.

#![cfg(feature = "internal-testing")]

use cntryl_midge::__internal::io::Fs;
use cntryl_midge::__internal::simulation::{CrashDevice, Fault, FaultEffect, FaultError, FaultOp};
use cntryl_midge::{Engine, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const KEYS: usize = 12;

fn options(named: &Path, device: &CrashDevice) -> OpenOptions {
    let root: Arc<dyn Fs> = Arc::new(device.fs());
    OpenOptions::local(named)
        .background_compaction(false)
        .recovery_policy(RecoveryPolicy::Salvage)
        .filesystem_for_testing(root)
        .build()
        .expect("options")
}

/// Acknowledged keys a healthy open does not return.
fn missing_acknowledged_writes(engine: &Engine) -> Vec<usize> {
    let cf = engine.get_column_family("default").expect("default");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");
    (0..KEYS)
        .filter(|index| {
            tx.get(format!("acked-{index:03}").as_bytes())
                .expect("get")
                .as_deref()
                != Some(b"v".as_slice())
        })
        .collect()
}

/// Fail the `nth` WAL read of an open on a copy of `device`, then open the
/// copy again without faults. Returns the acknowledged keys the healthy
/// open lost, or `None` once the open makes fewer than `nth` WAL reads.
fn fail_wal_read(named: &Path, device: &CrashDevice, nth: u64) -> Option<Vec<usize>> {
    let trial = device.fork();
    let fault = trial.inject(Fault {
        op: FaultOp::Read,
        path: "wal".into(),
        nth,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let reopened = Engine::open(options(named, &trial));
    trial.disarm_faults();
    if !trial.fired(fault) {
        return None;
    }
    match reopened {
        Ok(mut engine) => engine
            .shutdown(Duration::from_secs(10))
            .expect("shutdown faulted open"),
        Err(error) => eprintln!("open with WAL read {nth} failing returned {error:?}"),
    }
    let mut healthy = Engine::open(options(named, &trial)).expect("healthy open after injected Io");
    let missing = missing_acknowledged_writes(&healthy);
    healthy
        .shutdown(Duration::from_secs(10))
        .expect("healthy shutdown");
    Some(missing)
}

#[test]
fn should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error() {
    // Arrange: sync-acknowledged writes live only in the WAL. The healthy
    // control proves recovery replays every one of them.
    let named = tempfile::tempdir().expect("named directory");
    let device = CrashDevice::new();
    let mut engine = Engine::open(options(named.path(), &device)).expect("open");
    let cf = engine.get_column_family("default").expect("default");
    for index in 0..KEYS {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(
            format!("acked-{index:03}").into_bytes(),
            b"v".to_vec(),
            None,
        )
        .expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
    }
    engine.shutdown(Duration::from_secs(10)).expect("shutdown");
    drop(engine);
    let mut control =
        Engine::open(options(named.path(), &device)).expect("healthy recovery control");
    let control_missing = missing_acknowledged_writes(&control);
    control
        .shutdown(Duration::from_secs(10))
        .expect("control shutdown");
    drop(control);

    // Act: each WAL read the open makes fails in turn, on its own copy of
    // the device, with a plain I/O error, which is not evidence of
    // corruption.
    let trials: Vec<(u64, Vec<usize>)> = (1..)
        .map_while(|nth| fail_wal_read(named.path(), &device, nth).map(|missing| (nth, missing)))
        .collect();

    // Assert: whether a faulted open failed or salvaged, the next healthy
    // open recovers every acknowledged value. Quarantining the WAL on an I/O
    // error loses them.
    assert!(
        control_missing.is_empty(),
        "control lost {control_missing:?}"
    );
    assert!(!trials.is_empty(), "no WAL read fault fired");
    let lost: Vec<_> = trials
        .iter()
        .filter(|(_, missing)| !missing.is_empty())
        .collect();
    assert!(
        lost.is_empty(),
        "acknowledged writes vanished after salvage open: (failed read, lost keys) {lost:?}"
    );
}
