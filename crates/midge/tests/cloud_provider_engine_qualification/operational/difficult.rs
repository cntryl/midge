//! Overlapping generations across families, including destructive mutations.

use super::{fixture::Campaign, workload};
use cntryl_midge::{Bytes, ColumnFamilyHandle, Engine, Query, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const FAMILIES: u8 = 3;
const ROUNDS: u8 = 8;
const KEYS: u8 = 32;
const GENERATION_KEY: &[u8] = &[u8::MAX];

fn name(family: u8) -> String {
    format!("resource-family-{family}")
}

fn value_size(campaign: &Campaign) -> usize {
    campaign
        .profile
        .value_bytes
        .min(usize::try_from(campaign.profile.local_bytes / 1024).unwrap_or(usize::MAX))
        .max(3)
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
    for (family, cf) in families.iter().enumerate() {
        write_generation(
            engine,
            cf,
            u8::try_from(family).unwrap(),
            0,
            campaign,
            progress,
        );
    }
    let stopped = Arc::new(AtomicBool::new(false));
    let scans = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let _stop = Stop(Arc::clone(&stopped));
        scope.spawn(|| {
            while !stopped.load(Ordering::Acquire) {
                for (family, cf) in families.iter().enumerate() {
                    assert_snapshot(engine, cf, u8::try_from(family).unwrap(), campaign);
                    scans.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
        for round in 1..ROUNDS {
            for (family, cf) in families.iter().enumerate() {
                let family = u8::try_from(family).unwrap();
                write_generation(engine, cf, family, round, campaign, progress);
            }
            if round % 4 == 3 {
                workload::compact_all(engine, progress);
            }
        }
    });
    assert!(scans.load(Ordering::Relaxed) > 0);
    super::clock::expire_values();
    verify(engine, campaign);
}

fn write_generation(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    family: u8,
    round: u8,
    campaign: &Campaign,
    progress: &mut workload::WorkloadProgress,
) {
    loop {
        progress.require_time("difficult write admission");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("overwrite transaction");
        for key in 0..KEYS {
            tx.put(
                vec![key],
                value(family, round, key, value_size(campaign)),
                (key == 31).then_some(1),
            )
            .expect("overwrite value or expiring value");
        }
        tx.put(GENERATION_KEY.to_vec(), vec![round], None)
            .expect("atomic generation marker");
        tx.delete(vec![round % 2]).expect("point tombstone");
        let start = if round.is_multiple_of(2) { 8 } else { 16 };
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

fn assert_snapshot(engine: &Engine, cf: &ColumnFamilyHandle, family: u8, campaign: &Campaign) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("maintenance snapshot");
    let marker = tx
        .get(GENERATION_KEY)
        .expect("generation read")
        .expect("seeded generation");
    assert_eq!(marker.len(), 1);
    let round = marker[0];
    assert!(round < ROUNDS);
    let start = if round.is_multiple_of(2) { 8 } else { 16 };
    let mut expected = Vec::new();
    for key in 0..KEYS {
        let value = (key != round % 2 && !(start..start + 8).contains(&key))
            .then(|| Bytes::from(value(family, round, key, value_size(campaign))));
        assert_eq!(tx.get(&[key]).expect("snapshot point read"), value);
        if let Some(value) = value {
            expected.push((Bytes::from(vec![key]), value));
        }
    }
    expected.push((Bytes::copy_from_slice(GENERATION_KEY), marker));
    let actual = tx
        .scan(&Query::new())
        .expect("maintenance scan")
        .try_collect()
        .expect("complete maintenance snapshot");
    assert_eq!(
        actual, expected,
        "every row must belong to the marker's complete generation"
    );
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
            let expected = live(key).then(|| value(family, ROUNDS - 1, key, value_size(campaign)));
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
            .chain(std::iter::once(GENERATION_KEY.to_vec()))
            .collect();
        assert_eq!(
            tx.get(GENERATION_KEY).expect("final generation"),
            Some(Bytes::from(vec![ROUNDS - 1]))
        );
        assert_eq!(
            actual, expected,
            "complete keyset after overwrites, tombstones and TTLs"
        );
    }
}
