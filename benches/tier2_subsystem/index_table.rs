use bytes::Bytes;
use cntryl_midge::sst::format::BlockHandle;
use cntryl_midge::sst::{BlockMeta, IndexTable};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn build_index_table(num_blocks: usize) -> IndexTable {
    let mut metas = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        let min_key = format!("key_{:010}", i * 1000);
        let max_key = format!("key_{:010}", i * 1000 + 999);
        let meta = BlockMeta::new(
            Bytes::from(min_key),
            Bytes::from(max_key),
            BlockHandle::new((i as u64) * 4096, 4096),
        );
        metas.push(meta);
    }
    IndexTable::new(metas)
}

fn index_table_find_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_table_find_block");
    group.throughput(Throughput::Elements(1000));

    for num_blocks in [10, 100, 1000].iter() {
        let table = black_box(build_index_table(*num_blocks));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| {
                b.iter(|| {
                    let mut hits = 0;
                    for i in 0..1000 {
                        let search_key = format!("key_{:010}", i * 100);
                        if table.find_block(search_key.as_bytes()).is_some() {
                            hits += 1;
                        }
                    }
                    black_box(hits)
                })
            },
        );
    }
    group.finish();
}

fn index_table_find_blocks_in_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_table_find_blocks_in_range");
    group.throughput(Throughput::Elements(100));

    for num_blocks in [10, 100, 1000].iter() {
        let table = black_box(build_index_table(*num_blocks));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| {
                b.iter(|| {
                    let mut total_blocks = 0;
                    for i in 0..100 {
                        let start = format!("key_{:010}", i * 10000);
                        let end = format!("key_{:010}", (i + 1) * 10000);
                        let blocks = table.find_blocks_in_range(start.as_bytes(), end.as_bytes());
                        total_blocks += blocks.len();
                    }
                    black_box(total_blocks)
                })
            },
        );
    }
    group.finish();
}

fn index_table_memory_footprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_table_memory_footprint");

    for num_blocks in [100, 1000, 10000].iter() {
        let table = build_index_table(*num_blocks);

        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| b.iter(|| black_box(table.memory_usage())),
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    index_table_find_block,
    index_table_find_blocks_in_range,
    index_table_memory_footprint
);
criterion_main!(benches);
