//! Concurrent large-value flushes, overlapping generations and delayed uploads.

use super::{fixture::Campaign, workload};
use cntryl_midge::{ColumnFamilyHandle, Engine, Query, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const FAMILIES: u8 = 4;
const ROUNDS: u8 = 8;

fn name(family: u8) -> String {
    format!("flush-stress-{family}")
}

fn value_size(campaign: &Campaign) -> usize {
    let target = campaign.profile.memtable_bytes.unwrap_or_else(|| {
        usize::try_from(campaign.profile.local_bytes / 8)
            .unwrap_or(usize::MAX)
            .min(campaign.profile.memory_bytes / 16)
    });
    (target / 8)
        .min(campaign.profile.memory_bytes / 128)
        .max(16)
}

fn value(family: u8, round: u8, key: u8, size: usize) -> Vec<u8> {
    let mut state = 0x243f_6a88_85a3_08d3_u64
        ^ u64::from(family) << 16
        ^ u64::from(round) << 8
        ^ u64::from(key);
    let mut bytes = vec![0; size];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    bytes[..3].copy_from_slice(&[family, round, key]);
    bytes
}

struct Stop(Arc<AtomicBool>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct UploadDelay;
impl Drop for UploadDelay {
    fn drop(&mut self) {
        fail::remove("midge::cloud::inject_fail_sst_upload");
    }
}

fn seed(engine: &Engine) -> Vec<ColumnFamilyHandle> {
    let families: Vec<_> = (0..FAMILIES)
        .map(|family| {
            engine
                .create_column_family(&name(family))
                .expect("stress family")
        })
        .collect();
    for cf in &families {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for key in 0..64_u8 {
            tx.put(vec![b'd', key], b"deleted by later ranges".to_vec(), None)
                .unwrap();
        }
        tx.commit(WriteOptions::cloud_strict())
            .expect("seed tombstone targets");
        engine.flush_cf(cf).expect("seed older SST");
    }
    families
}

pub(super) fn exercise(
    engine: &Engine,
    campaign: &Campaign,
    progress: &mut workload::WorkloadProgress,
) {
    let families = seed(engine);
    let delayed = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&delayed);
    fail::cfg_callback("midge::cloud::inject_fail_sst_upload", move || {
        if counter.fetch_add(1, Ordering::Relaxed) < 32 {
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .expect("delay SST uploads");
    let _delay = UploadDelay;
    let stopped = Arc::new(AtomicBool::new(false));
    let scans = AtomicU64::new(0);
    let overlap = AtomicU64::new(0);
    let acknowledged = AtomicU64::new(0);
    let deadline = Instant::now() + Duration::from_secs(campaign.profile.timeout_seconds);
    std::thread::scope(|scope| {
        let _stop = Stop(Arc::clone(&stopped));
        scope.spawn(|| {
            while !stopped.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "stress scan deadline");
                let metrics = engine.get_runtime_metrics().expect("stress metrics");
                if metrics.active_compactions > 0 && metrics.flush_inflight > 0 {
                    overlap.fetch_add(1, Ordering::Relaxed);
                }
                for (family, cf) in families.iter().enumerate() {
                    scan(
                        engine,
                        cf,
                        u8::try_from(family).unwrap(),
                        value_size(campaign),
                    );
                    scans.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let maintenance = scope.spawn(|| {
            while acknowledged.load(Ordering::Acquire) < u64::from(FAMILIES) * 2 {
                assert!(Instant::now() < deadline, "stress maintenance deadline");
                std::thread::sleep(Duration::from_millis(5));
            }
            engine
                .compact_all()
                .expect("compaction while writers continue");
        });
        let writers: Vec<_> = families
            .iter()
            .enumerate()
            .map(|(family, cf)| {
                let acknowledged = &acknowledged;
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        write(
                            engine,
                            cf,
                            u8::try_from(family).unwrap(),
                            round,
                            value_size(campaign),
                            deadline,
                        );
                        acknowledged.fetch_add(1, Ordering::Relaxed);
                        engine.flush_cf(cf).expect("concurrent stress flush");
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().expect("stress writer");
        }
        maintenance.join().expect("concurrent stress compaction");
        workload::compact_all(engine, progress);
    });
    let evidence = serde_json::json!({
        "families": FAMILIES,
        "rounds": ROUNDS,
        "value_bytes": value_size(campaign),
        "acknowledged_transactions": acknowledged.load(Ordering::Relaxed),
        "delayed_uploads": delayed.load(Ordering::Relaxed).min(32),
        "verified_maintenance_scans": scans.load(Ordering::Relaxed),
        "flush_compaction_overlap_samples": overlap.load(Ordering::Relaxed),
    });
    std::fs::write(
        campaign.artifacts.join("flush-stress.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    eprintln!("MIDGE_FLUSH_STRESS {evidence}");
    assert_eq!(
        acknowledged.load(Ordering::Relaxed),
        u64::from(FAMILIES) * u64::from(ROUNDS)
    );
    assert!(scans.load(Ordering::Relaxed) > 0);
    assert!(delayed.load(Ordering::Relaxed) > 0);
    if std::env::var_os("MIDGE_QUALIFICATION_CHILD_RUNNER").is_some() {
        assert!(
            overlap.load(Ordering::Relaxed) > 0,
            "enforced profile must exercise simultaneous flush and compaction"
        );
    }
    verify(engine, campaign);
}

fn write(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    family: u8,
    round: u8,
    size: usize,
    deadline: Instant,
) {
    loop {
        assert!(
            Instant::now() < deadline,
            "concurrent stress admission deadline"
        );
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("stress write");
        let key = round % 4;
        tx.put(vec![b'l', key], value(family, round, key, size), None)
            .expect("large value");
        tx.delete(vec![b'l', (round + 1) % 4])
            .expect("point tombstone");
        for part in 0..8_u8 {
            tx.delete_range(vec![b'd', part * 8], vec![b'd', (part + 1) * 8])
                .expect("dense range tombstones");
        }
        match tx.commit(WriteOptions::cloud_strict()) {
            Ok(()) => return,
            Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("stress acknowledgment failed: {error}"),
        }
    }
}

fn scan(engine: &Engine, cf: &ColumnFamilyHandle, family: u8, size: usize) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("stress snapshot");
    for entry in tx.scan(&Query::new()).expect("stress scan") {
        let (key, actual) = entry.expect("stress scan item");
        if key[0] == b'd' {
            assert_eq!(actual.as_ref(), b"deleted by later ranges");
        } else {
            assert_eq!(actual.as_ref(), value(family, actual[1], key[1], size));
        }
    }
}

pub(super) fn verify(engine: &Engine, campaign: &Campaign) {
    for family in 0..FAMILIES {
        let cf = engine
            .get_column_family(&name(family))
            .expect("recovered stress family");
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("stress verify");
        for key in 0..64_u8 {
            assert!(tx.get(&[b'd', key]).expect("range-deleted key").is_none());
        }
        assert!(tx.get(&[b'l', 0]).expect("point-deleted key").is_none());
        for key in 1..4_u8 {
            assert_eq!(
                tx.get(&[b'l', key]).expect("stress live value").as_deref(),
                Some(value(family, key + 4, key, value_size(campaign)).as_slice())
            );
        }
        let keys: Vec<_> = tx
            .scan(&Query::new())
            .expect("stress complete keyset")
            .map(|entry| entry.unwrap().0.to_vec())
            .collect();
        assert_eq!(
            keys,
            (1..4_u8).map(|key| vec![b'l', key]).collect::<Vec<_>>()
        );
    }
}
