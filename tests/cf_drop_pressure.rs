use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn engine_with_memory_pressure() -> (Engine, u32, u32) {
    let engine = Engine::open(
        OpenOptions::in_memory()
            .with_memtable_size_limit(64 * 1024)
            .build()
            .expect("memory options"),
    )
    .expect("memory engine");
    let other = engine.create_column_family("other").expect("other family");
    let pressure = engine
        .create_column_family("pressure")
        .expect("pressure family");
    let mut transaction = engine
        .begin_tx(pressure.id(), TransactionMode::ReadWrite)
        .expect("pressure transaction");
    transaction
        .put(b"pressure".to_vec(), vec![7; 256 * 1024], None)
        .expect("stage pressure");
    transaction
        .commit(WriteOptions::buffered())
        .expect("commit pressure");
    assert!(
        !engine
            .wait_for_write_stall_clear(other.id(), Duration::from_millis(20))
            .expect("check initial pressure"),
        "the fixture must first create a global write stall"
    );
    (engine, other.id(), pressure.id())
}

#[test]
fn should_clear_other_family_stall_when_pressure_family_is_discarded() {
    // Arrange
    let (engine, other, pressure) = engine_with_memory_pressure();

    // Act
    engine
        .drop_column_family_discarding_unflushed(pressure)
        .expect("discard pressure family");
    let clear = engine
        .wait_for_write_stall_clear(other, Duration::from_secs(2))
        .expect("wait for pressure to clear");

    // Assert
    assert!(
        clear,
        "dropping the pressure family must release its memory charge"
    );
}

#[test]
fn should_wake_existing_waiter_when_pressure_family_is_discarded() {
    // Arrange
    let (engine, other, pressure) = engine_with_memory_pressure();
    let engine = Arc::new(engine);
    let waiter_engine = Arc::clone(&engine);
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        started_tx.send(()).expect("signal waiter start");
        result_tx
            .send(waiter_engine.wait_for_write_stall_clear(other, Duration::from_secs(3)))
            .expect("send wait result");
    });
    started_rx.recv().expect("waiter starts");
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_millis(200)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    // Act
    engine
        .drop_column_family_discarding_unflushed(pressure)
        .expect("discard pressure family");
    let clear = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("waiter wakes")
        .expect("wait succeeds");
    waiter.join().expect("join waiter");

    // Assert
    assert!(
        clear,
        "the previously parked waiter must observe released pressure"
    );
}

#[test]
fn should_keep_stall_when_an_unrelated_family_is_dropped() {
    // Arrange
    let (engine, other, pressure) = engine_with_memory_pressure();
    let empty = engine.create_column_family("empty").expect("empty family");

    // Act
    engine
        .drop_column_family(empty.id())
        .expect("drop empty family");
    let clear = engine
        .wait_for_write_stall_clear(other, Duration::from_millis(20))
        .expect("check remaining pressure");

    // Assert
    assert!(
        !clear,
        "another family's retained pressure must remain charged"
    );
    assert!(engine.get_column_family("pressure").is_some());
    assert_ne!(pressure, empty.id());
}
