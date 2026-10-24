//! Public per-family flush ownership through real worker holds and restart.
use cntryl_midge::{Engine, MidgeResult, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

struct HeldBuilds {
    entered: crossbeam::channel::Receiver<std::thread::ThreadId>,
    release: crossbeam::channel::Sender<()>,
}

impl HeldBuilds {
    fn new() -> Self {
        let (entered_tx, entered) = crossbeam::channel::bounded(64);
        let (release, release_rx) = crossbeam::channel::bounded(64);
        fail::cfg_callback("midge::flush_worker::before_build", move || {
            entered_tx.send(std::thread::current().id()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        Self { entered, release }
    }

    fn two_workers(&self) {
        let first = self
            .entered
            .recv_timeout(Duration::from_secs(5))
            .expect("first CF build entered");
        let second = self
            .entered
            .recv_timeout(Duration::from_secs(5))
            .expect("second CF build entered before releasing first");
        assert_ne!(
            first, second,
            "two independently held builds need two compute workers"
        );
    }
}

impl Drop for HeldBuilds {
    fn drop(&mut self) {
        // Unwind releases every admitted callback before scoped callers join.
        for _ in 0..64 {
            let _ = self.release.try_send(());
        }
    }
}

fn assert_family_value(engine: &Engine, name: &str, value: &[u8]) -> MidgeResult<()> {
    let cf = engine
        .get_column_family(name)
        .expect("known fixture family");
    let transaction = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(transaction.get(b"key")?.as_deref(), Some(value));
    Ok(())
}

#[test]
fn should_flush_two_column_families_concurrently_and_recover_both_publications() -> MidgeResult<()>
{
    let scenario = fail::FailScenario::setup();
    // Arrange
    let directory = tempfile::tempdir()?;
    let options = || {
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .flush_concurrency(2)
            .compaction_concurrency(2)
            .build()
    };
    let mut engine = Engine::open(options()?)?;
    let first = engine.create_column_family("first")?;
    let second = engine.create_column_family("second")?;
    for (family, value) in [
        (&first, b"first-value".as_slice()),
        (&second, b"second-value".as_slice()),
    ] {
        let mut transaction = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        transaction.put(b"key".to_vec(), value.to_vec(), None)?;
        transaction.commit(WriteOptions::best_effort())?;
    }

    // Act: neither worker can finish before both production builds have entered.
    std::thread::scope(|scope| -> MidgeResult<()> {
        let held = HeldBuilds::new();
        let first_flush = scope.spawn(|| engine.flush_cf(&first));
        let second_flush = scope.spawn(|| engine.flush_cf(&second));
        held.two_workers();
        assert_family_value(&engine, "first", b"first-value")?;
        assert_family_value(&engine, "second", b"second-value")?;
        held.release.send(()).unwrap();
        held.release.send(()).unwrap();
        first_flush.join().expect("first flush caller")?;
        second_flush.join().expect("second flush caller")?;
        Ok(())
    })?;
    fail::remove("midge::flush_worker::before_build");
    engine.shutdown(Duration::from_secs(5))?;

    // Assert: both independently computed files passed serialized authority
    // publication, and a fresh reader recovers their actual durable contents.
    let mut reopened = Engine::open(options()?)?;
    assert_family_value(&reopened, "first", b"first-value")?;
    assert_family_value(&reopened, "second", b"second-value")?;
    reopened.shutdown(Duration::from_secs(5))?;
    scenario.teardown();
    Ok(())
}
