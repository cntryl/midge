//! Shared real-Engine fixture for read-amplification benchmarks and tests.

use cntryl_midge::{
    Bytes, ColumnFamilyId, Engine, OpenOptions, Query, ReadAmpMetricsSnapshot,
    RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
};
use tempfile::TempDir;

pub const KEYS_PER_SST: usize = 256;
const SST_GENERATIONS: usize = 3;
const VALUE_SIZE: usize = 96;
pub const SCAN_WIDTH: usize = 8;

pub struct ReadAmpFixture {
    pub engine: Engine,
    pub cf_id: ColumnFamilyId,
    _directory: TempDir,
}

impl ReadAmpFixture {
    #[must_use]
    pub fn new() -> Self {
        let directory = TempDir::new().expect("create read amplification database directory");
        let options = OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()
            .expect("build read amplification options");
        let engine = Engine::open(options).expect("open read amplification database");
        let cf = engine
            .create_column_family("read-amplification")
            .expect("create read amplification column family");

        for generation in 0..SST_GENERATIONS {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin read amplification fixture write");
            for index in 0..KEYS_PER_SST {
                tx.put(
                    key(index * 2),
                    vec![
                        u8::try_from(generation).expect("fixture generation fits in u8");
                        VALUE_SIZE
                    ],
                    None,
                )
                .expect("put fixture value");
            }
            tx.commit(WriteOptions::buffered())
                .expect("commit read amplification fixture write");
            engine.flush_cf(&cf).expect("flush fixture generation");
        }

        Self {
            engine,
            cf_id: cf.id(),
            _directory: directory,
        }
    }
}

impl Default for ReadAmpFixture {
    fn default() -> Self {
        Self::new()
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:010}").into_bytes()
}

pub struct ReadWorkload {
    point_keys: Vec<Vec<u8>>,
    scan_queries: Vec<Query>,
    rows_per_scan: usize,
    expected_point_hits: u64,
    expected_point_misses: u64,
}

impl ReadWorkload {
    #[must_use]
    pub fn new(point_reads: usize, scans: usize) -> Self {
        let point_keys = (0..point_reads)
            .map(|index| {
                let selected = (index * 13) % KEYS_PER_SST;
                key(selected * 2 + usize::from(index % 4 == 3))
            })
            .collect();
        let scan_queries = (0..scans)
            .map(|index| {
                let selected = (index * 7) % (KEYS_PER_SST - SCAN_WIDTH);
                Query::new()
                    .start_key(Bytes::from(key(selected * 2)))
                    .limit(SCAN_WIDTH)
            })
            .collect();
        Self {
            point_keys,
            scan_queries,
            rows_per_scan: SCAN_WIDTH,
            expected_point_hits: u64::try_from(point_reads * 3 / 4)
                .expect("point-hit count fits in u64"),
            expected_point_misses: u64::try_from(point_reads / 4)
                .expect("point-miss count fits in u64"),
        }
    }

    /// Point misses only, for measuring per-miss SST filter work (#922).
    #[must_use]
    pub fn misses_only(point_misses: usize) -> Self {
        Self {
            point_keys: (0..point_misses)
                .map(|index| key((index % KEYS_PER_SST) * 2 + 1))
                .collect(),
            scan_queries: Vec::new(),
            rows_per_scan: SCAN_WIDTH,
            expected_point_hits: 0,
            expected_point_misses: u64::try_from(point_misses)
                .expect("point-miss count fits in u64"),
        }
    }

    /// Full scans over the fixture: every key once, merged across the three
    /// overlapping SSTs (#945). The observability test shares this module and
    /// does not run long scans.
    #[allow(dead_code)]
    #[must_use]
    pub fn long_scans(scans: usize) -> Self {
        Self {
            point_keys: Vec::new(),
            scan_queries: (0..scans).map(|_| Query::new()).collect(),
            rows_per_scan: KEYS_PER_SST,
            expected_point_hits: 0,
            expected_point_misses: 0,
        }
    }

    #[must_use]
    pub fn expected_point_reads(&self) -> u64 {
        u64::try_from(self.point_keys.len()).expect("point-read count fits in u64")
    }

    #[must_use]
    pub fn expected_scans(&self) -> u64 {
        u64::try_from(self.scan_queries.len()).expect("scan count fits in u64")
    }

    #[must_use]
    pub fn expected_point_hits(&self) -> u64 {
        self.expected_point_hits
    }

    #[must_use]
    pub fn expected_point_misses(&self) -> u64 {
        self.expected_point_misses
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ReadWorkloadResult {
    pub point_reads: u64,
    pub point_hits: u64,
    pub point_misses: u64,
    pub scans: u64,
    pub scan_rows: u64,
}

#[must_use]
pub fn run_workload(fixture: &ReadAmpFixture, workload: &ReadWorkload) -> ReadWorkloadResult {
    let tx = fixture
        .engine
        .begin_tx(fixture.cf_id, TransactionMode::ReadOnly)
        .expect("begin read amplification read transaction");
    let mut result = ReadWorkloadResult {
        point_reads: 0,
        point_hits: 0,
        point_misses: 0,
        scans: 0,
        scan_rows: 0,
    };
    for key in &workload.point_keys {
        let value = tx.get(key).expect("get fixture key");
        result.point_reads += 1;
        if let Some(value) = value {
            assert_eq!(value.as_ref(), &[2; VALUE_SIZE]);
            result.point_hits += 1;
        } else {
            result.point_misses += 1;
        }
    }
    for query in &workload.scan_queries {
        let mut rows = 0;
        for row in tx.scan(query).expect("scan fixture") {
            let (_, value) = row.expect("read fixture scan row");
            assert_eq!(value.as_ref(), &[2; VALUE_SIZE]);
            rows += 1;
        }
        assert_eq!(
            rows, workload.rows_per_scan,
            "each scan must return the workload's row count"
        );
        result.scans += 1;
        result.scan_rows += u64::try_from(rows).expect("scan row count fits in u64");
    }
    result
}

#[derive(Debug)]
pub struct ReadMetricsDelta {
    pub reads: u64,
    pub ssts_touched: u64,
    pub l0_ssts_touched: u64,
    pub blocks_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[must_use]
pub fn metrics_delta(
    read_before: &ReadAmpMetricsSnapshot,
    read_after: &ReadAmpMetricsSnapshot,
    runtime_before: &RuntimeMetricsSnapshot,
    runtime_after: &RuntimeMetricsSnapshot,
) -> ReadMetricsDelta {
    let subtract = |after: u64, before: u64| after.checked_sub(before).expect("metric decreased");
    ReadMetricsDelta {
        reads: subtract(read_after.reads_total, read_before.reads_total),
        ssts_touched: subtract(
            read_after.ssts_touched_total,
            read_before.ssts_touched_total,
        ),
        l0_ssts_touched: subtract(
            read_after.l0_ssts_touched_total,
            read_before.l0_ssts_touched_total,
        ),
        blocks_read: subtract(read_after.blocks_read_total, read_before.blocks_read_total),
        cache_hits: subtract(runtime_after.cache_hits, runtime_before.cache_hits),
        cache_misses: subtract(runtime_after.cache_misses, runtime_before.cache_misses),
    }
}
