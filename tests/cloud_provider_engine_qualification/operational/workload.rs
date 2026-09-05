//! A repeatable outage and mixed-workload campaign against the public API.

use super::fixture::{self, Campaign};
use cntryl_midge::{ColumnFamilyHandle, Engine, Query, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROUNDS: u64 = 12;
const KEYS_PER_ROUND: u64 = 8;

pub(super) fn expected_keys() -> impl Iterator<Item = Vec<u8>> {
    (0..ROUNDS).flat_map(|round| {
        (0..KEYS_PER_ROUND).map(move |index| format!("pressure-{round:04}-{index:04}").into_bytes())
    })
}

struct StopReader(Arc<AtomicBool>);
impl Drop for StopReader {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn exercise(engine: &Engine, campaign: &Campaign) {
    let cf = super::super::default_cf(engine);
    let reads = AtomicU64::new(0);
    let stopped = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let _stop = StopReader(Arc::clone(&stopped));
        scope.spawn(|| read_during_maintenance(engine, campaign, &stopped, &reads));
        for round in 0..ROUNDS {
            write_round(engine, &cf, round);
            if round == 0 {
                fail_uploads_then_resume(engine, &cf, campaign);
            } else {
                engine
                    .flush_cf(&cf)
                    .expect("flush mixed-workload generation");
            }
            if round % 4 == 3 {
                engine.compact_all().expect("mixed-workload compaction");
            }
        }
        verify(engine);
    });
    assert!(
        reads.load(Ordering::Relaxed) > 0,
        "foreground reads must progress during maintenance"
    );
}

fn write_round(engine: &Engine, cf: &ColumnFamilyHandle, round: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match try_write_round(engine, cf, round) {
            Ok(()) => return,
            Err(cntryl_midge::MidgeError::WriteStall(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("mixed cloud-strict acknowledgment failed: {error}"),
        }
    }
}

fn try_write_round(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    round: u64,
) -> cntryl_midge::MidgeResult<()> {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("mixed write transaction");
    for index in 0..KEYS_PER_ROUND {
        let key = format!("pressure-{round:04}-{index:04}").into_bytes();
        tx.put(
            key,
            fixture::value(round * KEYS_PER_ROUND + index, 2048),
            None,
        )
        .expect("mixed write");
    }
    tx.commit(WriteOptions::cloud_strict())
}

fn fail_uploads_then_resume(engine: &Engine, cf: &ColumnFamilyHandle, campaign: &Campaign) {
    let before = engine.get_runtime_metrics().expect("pre-outage metrics");
    fail::cfg("midge::cloud::inject_fail_sst_upload", "return").expect("SST upload outage");
    let initial_error = engine
        .flush_cf(cf)
        .expect_err("outage must reject SST publication");
    assert!(
        initial_error
            .to_string()
            .contains("cloud SST upload failed"),
        "unexpected outage: {initial_error}"
    );
    let duration = std::env::var("MIDGE_QUALIFICATION_OUTAGE_SECONDS")
        .map_or(2, |value| value.parse::<u64>().expect("outage duration"));
    let until = Instant::now() + Duration::from_secs(duration);
    while Instant::now() < until {
        let metrics = engine.get_runtime_metrics().expect("outage metrics");
        assert!(metrics.hybrid_total_committed_bytes <= campaign.profile.local_bytes);
        std::thread::sleep(Duration::from_millis(25));
    }
    let failed = engine
        .get_runtime_metrics()
        .expect("retained outage metrics");
    assert!(failed.flush_failures_total > before.flush_failures_total);
    assert!(
        failed
            .local_storage
            .as_ref()
            .expect("hybrid pressure snapshot")
            .usage
            .flush_staging_reserved_bytes
            > 0,
        "failed staging remains charged"
    );
    fail::remove("midge::cloud::inject_fail_sst_upload");
    let deadline = Instant::now() + Duration::from_secs(60);
    let settled = loop {
        let metrics = engine
            .get_runtime_metrics()
            .expect("retry progress metrics");
        if metrics.flush_publish_count > before.flush_publish_count
            && metrics.flush_inflight == 0
            && metrics.flush_queue_depth == 0
        {
            break metrics;
        }
        assert!(
            Instant::now() < deadline,
            "flush did not recover after outage: {metrics:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    std::fs::write(
        campaign.artifacts.join("pressure.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "outage_seconds": duration, "before": before, "failed": failed, "settled": settled,
        }))
        .expect("pressure JSON"),
    )
    .expect("pressure evidence");
}

fn read_during_maintenance(
    engine: &Engine,
    campaign: &Campaign,
    stopped: &AtomicBool,
    reads: &AtomicU64,
) {
    let cf = super::super::default_cf(engine);
    let query = Query::new().prefix(b"data-".as_slice().into()).limit(4);
    while !stopped.load(Ordering::Acquire) {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("concurrent read transaction");
        assert_eq!(
            tx.get(&fixture::key(0))
                .expect("concurrent point read")
                .as_deref(),
            Some(fixture::value(0, campaign.profile.value_bytes).as_slice())
        );
        let mut count = 0;
        for (index, entry) in tx.scan(&query).expect("concurrent scan").enumerate() {
            let (key, value) = entry.expect("scan item");
            assert_eq!(key.as_ref(), fixture::key(index as u64));
            assert_eq!(
                value.as_ref(),
                fixture::value(index as u64, campaign.profile.value_bytes)
            );
            count += 1;
        }
        assert_eq!(count, campaign.records.min(4));
        reads.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn verify(engine: &Engine) {
    let cf = super::super::default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("mixed workload verification");
    for round in 0..ROUNDS {
        for index in 0..KEYS_PER_ROUND {
            let key = format!("pressure-{round:04}-{index:04}");
            assert_eq!(
                tx.get(key.as_bytes())
                    .expect("acknowledged mixed value")
                    .as_deref(),
                Some(fixture::value(round * KEYS_PER_ROUND + index, 2048).as_slice())
            );
        }
    }
}
