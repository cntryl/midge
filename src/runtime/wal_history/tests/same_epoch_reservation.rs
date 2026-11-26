use super::super::{WalHistory, WalHistoryFence};
use cntryl_midge_core::common::MidgeResult;
use cntryl_midge_core::io::{Fs, FsError, RealFs};
use cntryl_midge_durability::lease::{fs_leader_store::FsLeaderStore, LeaderStore};
use cntryl_midge_durability::wal::fs::FsWalWriterIo;
use cntryl_midge_durability::wal::{WalOpKind, WalRecord, WalWriter, WAL_DIR};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[test]
fn should_complete_same_epoch_reservation_while_background_append_holds_native_fence(
) -> MidgeResult<()> {
    // Arrange: the engine's WAL writer and event loop share one filesystem
    // lease and can request effects concurrently in the same epoch.
    let directory = tempfile::tempdir().expect("temporary database root");
    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).map_err(FsError::into_midge)?);
    let store = Arc::new(FsLeaderStore::new(
        Arc::clone(&fs),
        cntryl_midge_durability::lease::LeaseClock::default(),
    ));
    let lease = store.acquire_leadership("holder").unwrap();
    let epoch = lease.epoch;
    let wal = cntryl_midge_core::io::subdirectory(Arc::clone(&fs), WAL_DIR);
    let fence = WalHistoryFence::new(
        Arc::clone(&store) as Arc<dyn LeaderStore>,
        "holder".into(),
        epoch,
        Duration::from_secs(2),
    );
    let mut history = WalHistory::open_with_fence(Arc::clone(&wal), 1, epoch, &fence)?;
    let writer = FsWalWriterIo::new_with_counters(
        &cntryl_midge_durability::wal::segment_file_name(1),
        Arc::clone(&wal),
        Duration::from_secs(2),
        cntryl_midge_core::telemetry::CounterSink::default(),
    )?;
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let first_effect = Arc::new(AtomicBool::new(true));
    let authority_store = Arc::clone(&store);
    let authority_entered = Arc::clone(&entered);
    let authority_resume = Arc::clone(&resume);
    let authority_first_effect = Arc::clone(&first_effect);
    writer.set_write_authority(Some(Arc::new(move |operation: &mut dyn FnMut()| {
        let mut run = || {
            if authority_first_effect.swap(false, Ordering::AcqRel) {
                authority_entered.wait();
                authority_resume.wait();
            }
            operation();
        };
        authority_store
            .with_writer_epoch("holder", epoch, Duration::from_secs(2), &mut run)
            .map_err(|error| error.into_validation_error("background WAL append"))
    })));
    let writer = Arc::new(writer);
    let append_writer = Arc::clone(&writer);
    let append = std::thread::spawn(move || {
        append_writer.append_record_accounted(&WalRecord::new(
            WalOpKind::Put,
            bytes::Bytes::from_static(b"concurrent-key"),
            Some(bytes::Bytes::from_static(b"value")),
            1,
        ))
    });
    entered.wait();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let reserve = std::thread::spawn(move || {
        started_tx.send(()).expect("signal reservation start");
        result_tx
            .send(history.reserve(10))
            .expect("send reservation result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("reservation thread started");

    // Act: same-epoch effects share the continuously held native grant.
    // Reservation finishes before the background append is resumed.
    let early_result = result_rx.recv_timeout(Duration::from_secs(1));
    resume.wait();
    append
        .join()
        .expect("background append thread completed")
        .map_err(|failure| failure.error)?;
    reserve.join().expect("reservation thread completed");

    // Assert
    early_result.expect("reservation completed while append was paused")?;
    drop(writer);
    Ok(())
}
