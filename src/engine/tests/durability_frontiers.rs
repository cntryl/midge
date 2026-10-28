use super::*;
use crossbeam::channel;

#[cfg(feature = "failpoints")]
#[test]
fn should_read_durability_frontiers_while_full_metrics_response_is_held() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let directory = tempfile::tempdir()?;
    let options = OpenOptions::cloud_simulated(directory.path(), "frontier-snapshot", "db")
        .background_compaction(false)
        .build()?;
    let mut engine = Engine::open(options)?;
    let column_family = engine.get_column_family("default").expect("default family");
    let mut transaction = engine.begin_tx(column_family.id(), TransactionMode::ReadWrite)?;
    transaction.put(b"key".to_vec(), b"value".to_vec(), None)?;
    let sequence = transaction.commit_with_sequence(WriteOptions::cloud_strict())?;

    let (entered_tx, entered_rx) = channel::bounded(1);
    let (release_tx, release_rx) = channel::bounded(1);
    fail::cfg_callback(
        "midge::runtime::before_get_runtime_metrics_response",
        move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        },
    )
    .expect("hold the full runtime metrics response");

    let metrics = engine.metrics();
    let metrics_thread = std::thread::spawn(move || metrics.get_runtime_metrics());
    let entered = entered_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let observed = engine.metrics().get_durability_frontiers();
    let observed_pending_uploads = engine.metrics().get_pending_cloud_uploads();
    let released = release_tx.send(()).is_ok();
    let runtime_metrics = metrics_thread.join().expect("metrics request thread");
    fail::remove("midge::runtime::before_get_runtime_metrics_response");
    scenario.teardown();

    engine.shutdown(Duration::from_secs(5))?;

    assert!(entered, "full metrics request did not reach its failpoint");
    assert!(released, "full metrics response gate was not released");
    let runtime_metrics = runtime_metrics?;
    assert_eq!(observed.current_sequence, sequence);
    assert_eq!(observed.wal_local_durable_seq, sequence);
    assert_eq!(observed.wal_cloud_durable_seq, sequence);
    assert_eq!(
        observed_pending_uploads,
        runtime_metrics.pending_cloud_uploads
    );
    Ok(())
}
