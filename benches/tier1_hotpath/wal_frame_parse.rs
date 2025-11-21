//! Tier 1 — WAL frame parsing hot path (stub)
//!
//! Placeholder bench for WAL frame parsing; add real logic when available.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

// Mock WAL frame for hot path testing
#[derive(Debug)]
struct MockWalFrame {
    header: u32,
    key: Bytes,
    value: Bytes,
}

impl MockWalFrame {
    fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let header = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let key_len = u32::from_le_bytes(data[4..8].try_into().ok()?);

        if data.len() < 8 + key_len as usize + 4 {
            return None;
        }

        let key_start = 8;
        let key_end = key_start + key_len as usize;
        let key = Bytes::copy_from_slice(&data[key_start..key_end]);

        let value_len_start = key_end;
        let value_len = u32::from_le_bytes(data[value_len_start..value_len_start + 4].try_into().ok()?);

        if data.len() < value_len_start + 4 + value_len as usize {
            return None;
        }

        let value_start = value_len_start + 4;
        let value_end = value_start + value_len as usize;
        let value = Bytes::copy_from_slice(&data[value_start..value_end]);

        Some(Self { header, key, value })
    }

    fn scan_header_only(data: &[u8]) -> Option<u32> {
        if data.len() < 4 {
            None
        } else {
            Some(u32::from_le_bytes(data[0..4].try_into().unwrap()))
        }
    }
}

fn make_frame_data(key_size: usize, value_size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Header (4 bytes)
    data.extend_from_slice(&42u32.to_le_bytes());

    // Key length (4 bytes)
    data.extend_from_slice(&(key_size as u32).to_le_bytes());

    // Key data
    data.extend_from_slice(&vec![b'k'; key_size]);

    // Value length (4 bytes)
    data.extend_from_slice(&(value_size as u32).to_le_bytes());

    // Value data
    data.extend_from_slice(&vec![b'v'; value_size]);

    data
}

/// Benchmark small frame parsing
fn bench_wal_frame_parse_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_small");
    group.measurement_time(std::time::Duration::from_millis(200));

    let frame_data = make_frame_data(16, 64);

    group.bench_function("parse_small_frame", |b| {
        b.iter(|| {
            let frame = MockWalFrame::parse(&frame_data);
            black_box(frame);
        })
    });

    group.finish();
}

/// Benchmark medium frame parsing
fn bench_wal_frame_parse_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_medium");
    group.measurement_time(std::time::Duration::from_millis(200));

    let frame_data = make_frame_data(64, 1024);

    group.bench_function("parse_medium_frame", |b| {
        b.iter(|| {
            let frame = MockWalFrame::parse(&frame_data);
            black_box(frame);
        })
    });

    group.finish();
}

/// Benchmark large frame parsing
fn bench_wal_frame_parse_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse_large");
    group.measurement_time(std::time::Duration::from_millis(200));

    let frame_data = make_frame_data(256, 4096);

    group.bench_function("parse_large_frame", |b| {
        b.iter(|| {
            let frame = MockWalFrame::parse(&frame_data);
            black_box(frame);
        })
    });

    group.finish();
}

/// Benchmark header-only scanning
fn bench_wal_header_scan_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_header_scan_only");
    group.measurement_time(std::time::Duration::from_millis(200));

    let frame_data = make_frame_data(64, 1024);

    group.bench_function("header_scan_only", |b| {
        b.iter(|| {
            let header = MockWalFrame::scan_header_only(&frame_data);
            black_box(header);
        })
    });

    group.finish();
}

criterion_group! {
    name = wal_frame_parse_group;
    config = criterion_config();
    targets = bench_wal_frame_parse_small, bench_wal_frame_parse_medium, bench_wal_frame_parse_large, bench_wal_header_scan_only
}
criterion_main!(wal_frame_parse_group);
