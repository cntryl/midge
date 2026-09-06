//! Overlapping generations across families, including destructive mutations.

use super::{fixture::Campaign, workload};
use cntryl_midge::{Engine, Query, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const FAMILIES: u8 = 3;
const ROUNDS: u8 = 8;
const KEYS: u8 = 32;

fn name(family: u8) -> String {
    format!("resource-family-{family}")
}

fn value(family: u8, round: u8, key: u8, size: usize) -> Vec<u8> {
    let mut bytes = vec![family.wrapping_mul(13) ^ round.wrapping_mul(7) ^ key; size.max(3)];
    bytes[..3].copy_from_slice(&[family, round, key]);
    bytes
}

struct Stop(Arc<AtomicBool>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn exercise(
    engine: &Engine,
    campaign: &Campaign,
    progress: &mut workload::WorkloadProgress,
) {
    let families: Vec<_> = (0..FAMILIES)
        .map(|family| {
            engine
                .create_column_family(&name(family))
                .expect("resource column family")
        })
        .collect();
    let stopped = Arc::new(AtomicBool::new(false));
    let scans = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let _stop = Stop(Arc::clone(&stopped));
        scope.spawn(|| {
            while !stopped.load(Ordering::Acquire) {
                for (family, cf) in families.iter().enumerate() {
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("maintenance snapshot");
                    let mut generation = None;
                    for entry in tx.scan(&Query::new()).expect("maintenance scan") {
                        let (key, actual) = entry.expect("maintenance entry");
                        let round = actual[1];
                        assert_eq!(
                            actual.as_ref(),
                            value(
                                u8::try_from(family).unwrap(),
                                round,
                                key[0],
                                campaign.profile.value_bytes
                            )
                        );
                        assert!(
                            generation.is_none_or(|previous| previous == round),
                            "torn generation within a snapshot"
                        );
                        generation = Some(round);
                    }
                    scans.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
        for round in 0..ROUNDS {
            for (family, cf) in families.iter().enumerate() {
                let family = u8::try_from(family).unwrap();
                loop {
                    progress.require_time("difficult write admission");
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .expect("overwrite transaction");
                    for key in 0..KEYS {
                        tx.put(
                            vec![key],
                            value(family, round, key, campaign.profile.value_bytes),
                            (key == 31).then_some(0),
                        )
                        .expect("overwrite value or expiring value");
                    }
                    tx.delete(vec![round % 2]).expect("point tombstone");
                    let start = if round % 2 == 0 { 8 } else { 16 };
                    tx.delete_range(vec![start], vec![start + 8])
                        .expect("range tombstone");
                    match tx.commit(WriteOptions::cloud_strict()) {
                        Ok(()) => break,
                        Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                            progress.report_wait(engine, "difficult write admission");
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        Err(error) => panic!("difficult cloud acknowledgment failed: {error}"),
                    }
                }
                engine.flush_cf(cf).expect("overlapping SST generation");
            }
            if round % 4 == 3 {
                workload::compact_all(engine, progress);
            }
        }
    });
    assert!(scans.load(Ordering::Relaxed) > 0);
    verify(engine, campaign);
}

pub(super) fn verify(engine: &Engine, campaign: &Campaign) {
    for family in 0..FAMILIES {
        let cf = engine
            .get_column_family(&name(family))
            .expect("persisted resource family");
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("verify resource snapshot");
        let live = |key: u8| key != 1 && !(16..24).contains(&key) && key != 31;
        for key in 0..KEYS {
            let expected =
                live(key).then(|| value(family, ROUNDS - 1, key, campaign.profile.value_bytes));
            assert_eq!(
                tx.get(&[key]).expect("resource point read").as_deref(),
                expected.as_deref()
            );
        }
        let actual: Vec<_> = tx
            .scan(&Query::new())
            .expect("resource complete scan")
            .map(|entry| entry.expect("resource scan entry").0.to_vec())
            .collect();
        let expected: Vec<_> = (0..KEYS)
            .filter(|key| live(*key))
            .map(|key| vec![key])
            .collect();
        assert_eq!(
            actual, expected,
            "complete keyset after overwrites, tombstones and TTLs"
        );
    }
}
