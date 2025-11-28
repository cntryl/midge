//! Tier 3 — System LSM Benchmarks
//!
//! Target Runtime: 10–30 seconds
//! Frequency: Nightly / on-demand
//!
//! Covers:
//! - WAL append
//! - memtable insert
//! - flush to SST
//! - reopen from disk
//! - L0 → L1 compaction
//! - mixed read/write workloads

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::criterion_config;
use rand::Rng;
use std::hint::black_box;
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Wire real Midge engine
// ---------------------------------------------------------------------------

use cntryl_midge::api::column_family::ColumnFamilyHandle;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

type Db = MidgeEngine;

fn open_db(path: &Path) -> Db {
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.to_path_buf(),
        },
        enable_compaction: true,
        wal_buffer_size: 1024 * 1024,
        memtable_size: 4 * 1024 * 1024,
        ..Default::default()
    };
    MidgeEngine::open(opts).expect("Failed to open database")
}

fn force_flush(db: &Db) {
    db.flush().expect("flush failed");
}

fn force_compact_l0(db: &Db) {
    let cf = db.default_column_family();
    db.compact_level(&cf, 0).expect("compact L0 failed");
}

fn db_put(db: &Db, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) {
    db.put(cf, key, value).expect("put failed");
}

fn db_get(db: &Db, cf: &ColumnFamilyHandle, key: &[u8]) {
    let _ = db.get(cf, key).expect("get failed");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_kv_pairs(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // key: user:{BE_u64}:profile
        let mut key = Vec::with_capacity(5 + 8 + 8);
        key.extend_from_slice(b"user:");
        key.extend_from_slice(&(i as u64).to_be_bytes());
        key.extend_from_slice(b":profile");

        // value: {"id":123,"name":"User123"}
        let value = format!("{{\"id\":{},\"name\":\"User{}\"}}", i, i).into_bytes();

        out.push((key, value));
    }
    out
}

// ---------------------------------------------------------------------------
// 1. WAL + Memtable Writes
// ---------------------------------------------------------------------------

fn bench_system_wal_write(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_wal_write");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[1_000usize, 10_000, 100_000] {
        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let path = tmp.path().to_path_buf();
                    let db = open_db(&path);
                    let cf = db.default_column_family();
                    let kvs = make_kv_pairs(n);
                    (tmp, db, cf, kvs)
                },
                |(_tmp, db, cf, kvs)| {
                    for (k, v) in kvs {
                        db_put(&db, &cf, &k, &v);
                    }
                    black_box(db);
                },
                BatchSize::SmallInput,
            );
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 2. Flush + Reopen + Point Reads
// ---------------------------------------------------------------------------

fn bench_system_flush_reopen_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_flush_reopen_read");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[10_000usize, 50_000] {
        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let path = tmp.path().to_path_buf();
                    let db = open_db(&path);
                    let cf = db.default_column_family();

                    let kvs = make_kv_pairs(n);
                    for (k, v) in &kvs {
                        db_put(&db, &cf, k, v);
                    }

                    force_flush(&db);

                    (tmp, path, kvs)
                },
                |(_tmp, path, kvs)| {
                    let db = open_db(&path);
                    let cf = db.default_column_family();

                    let mut rng = rand::thread_rng();
                    for _ in 0..1_000 {
                        let idx = rng.gen_range(0..kvs.len());
                        db_get(&db, &cf, &kvs[idx].0);
                    }

                    black_box(db);
                },
                BatchSize::SmallInput,
            );
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 3. L0 → L1 Compaction
// ---------------------------------------------------------------------------

fn bench_system_l0_compaction(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_l0_compaction");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[50_000usize, 100_000] {
        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let path = tmp.path().to_path_buf();
                    let db = open_db(&path);
                    let cf = db.default_column_family();

                    let kvs = make_kv_pairs(n);
                    for (k, v) in &kvs {
                        db_put(&db, &cf, k, v);
                    }
                    force_flush(&db);

                    (tmp, db)
                },
                |(_tmp, db)| {
                    force_compact_l0(&db);
                    black_box(db);
                },
                BatchSize::SmallInput,
            );
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 4. Mixed Read/Write Hotspot Workload
// ---------------------------------------------------------------------------

fn bench_system_mixed_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_mixed_workload");
    g.sampling_mode(SamplingMode::Flat);

    let total_ops = 50_000usize;
    g.throughput(Throughput::Elements(total_ops as u64));

    g.bench_function("mixed_80r_20w_hotset", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let path = tmp.path().to_path_buf();
                let db = open_db(&path);
                let cf = db.default_column_family();

                let hot_kvs = make_kv_pairs(10_000);
                for (k, v) in &hot_kvs {
                    db_put(&db, &cf, k, v);
                }
                force_flush(&db);

                (tmp, db, cf, hot_kvs)
            },
            |(_tmp, db, cf, hot_kvs)| {
                let mut rng = rand::thread_rng();

                for _ in 0..total_ops {
                    let idx = rng.gen_range(0..hot_kvs.len());
                    let (ref k, ref v) = hot_kvs[idx];

                    if rng.gen_bool(0.8) {
                        db_get(&db, &cf, k);
                    } else {
                        db_put(&db, &cf, k, v);
                    }
                }

                black_box(db);
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

criterion_group! {
    name = tier3_system_lsm;
    config = criterion_config();
    targets =
        bench_system_wal_write,
        bench_system_flush_reopen_read,
        bench_system_l0_compaction,
        bench_system_mixed_workload
}

criterion_main!(tier3_system_lsm);
