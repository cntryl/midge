//! Real inventory controls use the helper shared by the D/E benchmark adapters.
#![cfg(feature = "internal-testing")]

#[path = "../benches/bench_support/config.rs"]
mod config;
#[path = "../benches/bench_support/ycsb.rs"]
mod ycsb;

use cntryl_midge::{Engine, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_join_duration_clients_when_read_latest_inventory_spans_both_phases() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let opts = cntryl_midge::OpenOptions::local(dir.path())
        .build()
        .unwrap();
    let engine = Arc::new(Engine::open(opts.clone()).unwrap());
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(&engine, &cf, 128);
    let inventories: Vec<_> = (0..64)
        .map(|client| ycsb::inventory::InsertInventory::new(128, client))
        .collect();
    let mut completions = 0;

    // Act: use actual joined workers and the exact D operation adapter for both phases.
    for _ in 0..2 {
        let stats = ycsb::run_multi_client_for_duration_observed_with_stats(
            &engine,
            64,
            Duration::from_millis(100),
            |client, stop| {
                let inventory = inventories[client].clone();
                move |engine, cf, op_index| {
                    inventory
                        .read_latest_step(
                            engine,
                            cf.id(),
                            stop.as_ref(),
                            0xD0D0_EA5E_5678_9ABC,
                            op_index,
                            WriteOptions::sync(),
                        )
                        .unwrap()
                }
            },
        );
        completions += stats.operations;
    }
    let rows = ycsb::inventory::verify_inventory(&engine, cf.id(), 128, &inventories).unwrap();
    let inserts: u64 = inventories
        .iter()
        .map(ycsb::inventory::InsertInventory::committed)
        .sum();
    let hits: u64 = inventories
        .iter()
        .map(ycsb::inventory::InsertInventory::read_hits)
        .sum();
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("worker leaked engine"));
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(opts).unwrap();
    let cf = reopened.get_column_family("cf1").unwrap();
    let recovered =
        ycsb::inventory::verify_inventory(&reopened, cf.id(), 128, &inventories).unwrap();
    reopened.shutdown(Duration::from_secs(30)).unwrap();

    // Assert
    assert!(hits > 0);
    assert!(inserts > 0);
    assert_eq!(completions, hits + inserts);
    assert_eq!(rows, recovered);
}

#[test]
fn should_select_existing_latest_keys_when_clients_have_sparse_insert_namespaces() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let opts = cntryl_midge::OpenOptions::local(dir.path())
        .build()
        .unwrap();
    let mut engine = Engine::open(opts.clone()).unwrap();
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(&engine, &cf, 128);
    let clients: Vec<_> = [0, 1, 63]
        .into_iter()
        .map(|client| ycsb::inventory::InsertInventory::new(128, client))
        .collect();
    let stop = AtomicBool::new(false);
    let mut misses = Vec::new();

    // Act: both selectors cover every logical position at zero, one and several inserts.
    for (inventory, client_id) in clients.iter().zip([0_u64, 1, 63]) {
        for inserted in 0..=4 {
            if inserted > 0 {
                assert!(inventory
                    .insert(&engine, cf.id(), &stop, inserted, WriteOptions::sync())
                    .unwrap());
            }
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let newest = if inserted == 0 {
                127
            } else {
                128 + (client_id << 32) + inserted
            };
            assert_eq!(inventory.read_key(true, 0), ycsb::make_key(newest));
            assert_eq!(
                inventory.read_key(false, 127 + inserted),
                ycsb::make_key(newest)
            );
            for hot in [false, true] {
                for entropy in 0..132 {
                    let key = inventory.read_key(hot, entropy);
                    if tx.get(&key).unwrap().is_none() {
                        misses.push((hot, inserted, entropy));
                    }
                }
            }
        }
    }
    let rows = ycsb::inventory::verify_inventory(&engine, cf.id(), 128, &clients).unwrap();
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(opts).unwrap();
    let recovered_cf = reopened.get_column_family("cf1").unwrap();
    let recovered_rows =
        ycsb::inventory::verify_inventory(&reopened, recovered_cf.id(), 128, &clients).unwrap();
    reopened.shutdown(Duration::from_secs(30)).unwrap();

    // Assert
    assert!(misses.is_empty(), "absent inventory selections: {misses:?}");
    assert_eq!(rows, 140);
    assert_eq!(recovered_rows, rows);
}

#[test]
fn should_preserve_fresh_insert_inventory_when_measurement_follows_warmup() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let opts = cntryl_midge::OpenOptions::local(dir.path())
        .build()
        .unwrap();
    let mut engine = Engine::open(opts.clone()).unwrap();
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(&engine, &cf, 16);
    let clients: Vec<_> = [0, 1, 63]
        .into_iter()
        .map(|client| ycsb::inventory::InsertInventory::new(16, client))
        .collect();
    let stop = AtomicBool::new(false);

    // Act: measurement repeats deterministic operation indices on the same owner.
    for phase in 0..2 {
        for inventory in &clients {
            let phase_client = inventory.clone();
            for op_index in 0..4 {
                let before =
                    ycsb::inventory::verify_inventory(&engine, cf.id(), 16, &clients).unwrap();
                assert!(phase_client
                    .insert(&engine, cf.id(), &stop, op_index, WriteOptions::sync())
                    .unwrap());
                let after =
                    ycsb::inventory::verify_inventory(&engine, cf.id(), 16, &clients).unwrap();
                assert_eq!(after, before + 1, "phase {phase} insert overwrote a row");
            }
        }
    }
    stop.store(true, Ordering::Release);
    let stopped = clients[0]
        .insert(&engine, cf.id(), &stop, 0, WriteOptions::sync())
        .unwrap();
    let committed = clients[0].committed();
    stop.store(false, Ordering::Release);
    engine.flush_cf(&cf).unwrap();
    engine.drop_column_family(cf.id()).unwrap();
    let rejected = clients[0].insert(&engine, cf.id(), &stop, 0, WriteOptions::sync());
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(opts).unwrap();
    let missing_cf = reopened.get_column_family("cf1").is_none();
    reopened.shutdown(Duration::from_secs(30)).unwrap();

    // Assert
    assert!(!stopped);
    assert!(rejected.is_err(), "terminal errors must propagate");
    assert_eq!(clients[0].committed(), committed);
    assert_eq!(committed, 8);
    assert!(missing_cf);
}
