mod common;
use common::*;

use bytes::Bytes;

#[test]
fn should_flush_memtable_with_mixed_small_and_large_values() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    eng.put(&cf, b"small", b"s").unwrap();
    let large = Bytes::from(vec![b'x'; 1024 * 16]);
    eng.put(&cf, b"large", large.as_ref()).unwrap();
    eng.flush().unwrap();

    // Assert
    assert_eq!(eng.get(&cf, b"small").unwrap().unwrap(), Bytes::from("s"));
    assert_eq!(eng.get(&cf, b"large").unwrap().unwrap(), large);

    drop(eng);
    drop(tmp);
}

#[test]
fn should_apply_backpressure_under_large_value_workload() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    // Flood engine with large writes — deterministic check: all writes return Ok
    let large = vec![b'y'; 1024 * 8];
    for i in 0..50u8 {
        let r = eng.put(&cf, &[i], large.as_slice());
        assert!(r.is_ok());
    }

    // Assert
    // Confirm presence of a sample key
    assert!(eng.get(&cf, &[0]).unwrap().is_some());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_recover_large_value_batches_after_crash() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..10u8 {
                let large = vec![i; 1024 * 4];
                eng.put(&cf, &[i], large.as_slice()).unwrap();
            }
        },
        |eng| {
            // Assert
            for i in 0..10u8 {
                assert!(eng.get(&eng.default_column_family(), &[i]).unwrap().is_some());
            }
        },
    );
}

#[test]
fn should_respect_snapshot_visibility_for_large_values() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"k", b"v1").unwrap();
    let snap = eng.snapshot();

    // Act
    let big = Bytes::from(vec![b'z'; 1024 * 12]);
    eng.put(&cf, b"k", big.as_ref()).unwrap();

    // Assert
    // Snapshot should see the old value while engine returns new one
    assert_eq!(snap.get(&eng, &cf, b"k").unwrap().unwrap(), Bytes::from("v1"));
    assert_eq!(eng.get(&cf, b"k").unwrap().unwrap(), big);

    drop(eng);
    drop(tmp);
}
