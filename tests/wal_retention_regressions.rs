//! Public baseline reproducers for #808 cloud retention pins.
use cntryl_midge::__internal::wal::catalog::WalPublicationCatalog;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Pin {
    None,
    EmptyRange,
    DroppedFamily { discard: bool },
}

fn retained_segments(root: &std::path::Path) -> usize {
    let bytes = std::fs::read(root.join("cloud_store/wal/publication-catalog.v1.json"))
        .expect("published catalog");
    WalPublicationCatalog::decode(&bytes)
        .expect("catalog")
        .entries()
        .len()
}

fn retire_history(pin: Pin) -> usize {
    // Arrange: a special record precedes healthy flushed default-family commits.
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions::cloud_simulated(dir.path(), "audit", "retention")
        .background_compaction(false)
        .build()
        .unwrap();
    let mut engine = Engine::open(options).unwrap();
    let default = engine.get_column_family("default").unwrap();
    let mut blocker = engine
        .begin_tx(default.id(), TransactionMode::ReadWrite)
        .unwrap();
    blocker
        .put(b"blocker".to_vec(), b"value".to_vec(), None)
        .unwrap();
    blocker.commit(WriteOptions::cloud_strict()).unwrap();
    let family = match pin {
        Pin::DroppedFamily { .. } => engine.create_column_family("drop-me").unwrap(),
        Pin::None | Pin::EmptyRange => default.clone(),
    };
    let mut tx = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"seed".to_vec(), b"value".to_vec(), None).unwrap();
    if matches!(pin, Pin::EmptyRange) {
        tx.delete_range(b"r".to_vec(), b"r".to_vec()).unwrap();
    }
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    match pin {
        Pin::DroppedFamily { discard: true } => engine
            .drop_column_family_discarding_unflushed(family.id())
            .unwrap(),
        Pin::DroppedFamily { discard: false } => {
            engine.flush_cf(&family).unwrap();
            engine.drop_column_family(family.id()).unwrap();
        }
        Pin::None | Pin::EmptyRange => engine.flush_cf(&family).unwrap(),
    }

    // Act: keep the ordinary family advancing so the old range/family is the pin.
    for index in 0..8 {
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(
            format!("later-{index}").into_bytes(),
            b"value".to_vec(),
            None,
        )
        .unwrap();
        tx.commit(WriteOptions::cloud_strict()).unwrap();
        engine.flush_cf(&default).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while retained_segments(dir.path()) != 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let retained = retained_segments(dir.path());
    engine.shutdown(Duration::from_secs(10)).unwrap();
    retained
}

#[test]
fn should_retire_cloud_wal_when_every_ordinary_write_is_flushed() {
    // Arrange
    let pin = Pin::None;
    // Act
    let retained = retire_history(pin);
    // Assert
    assert_eq!(retained, 0);
}

#[test]
fn should_retire_cloud_wal_when_empty_range_has_no_replayable_effect() {
    // Arrange
    let pin = Pin::EmptyRange;
    // Act
    let retained = retire_history(pin);
    // Assert
    assert_eq!(
        retained, 0,
        "empty range pinned {retained} authoritative segments"
    );
}

#[test]
fn should_retire_cloud_wal_when_flushed_family_is_dropped() {
    // Arrange
    let pin = Pin::DroppedFamily { discard: false };
    // Act
    let retained = retire_history(pin);
    // Assert
    assert_eq!(
        retained, 0,
        "dropped family pinned {retained} authoritative segments"
    );
}

#[test]
fn should_retire_cloud_wal_when_unflushed_family_is_discarded() {
    // Arrange
    let pin = Pin::DroppedFamily { discard: true };
    // Act
    let retained = retire_history(pin);
    // Assert
    assert_eq!(
        retained, 0,
        "discarded family pinned {retained} authoritative segments"
    );
}
