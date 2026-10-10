//! Test references copied from the pre-optimization formulas at 921c478b.
use super::*;
use crate::common::resource_budget::ResourceBudget;
use std::cell::Cell;

thread_local! {
    static QUERY_WORK: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(super) fn record_work(units: usize) {
    QUERY_WORK.with(|work| {
        if let Some(count) = work.get() {
            work.set(Some(count.saturating_add(units)));
        }
    });
}

fn query_work(writer: &FsSstWriter) -> usize {
    QUERY_WORK.with(|work| work.set(Some(0)));
    std::hint::black_box(writer.estimated_size_bytes());
    std::hint::black_box(writer.encoded_size_upper_bound());
    QUERY_WORK.with(|work| work.replace(None).unwrap())
}

fn streaming_writer(
    budget: &ResourceBudget,
    policy: CompressionPolicy,
) -> MidgeResult<FsSstWriter> {
    let mut writer = FsSstWriter::new_with_budget(
        Arc::new(crate::io::MockFs::new()),
        policy,
        4096,
        Some(budget.clone()),
    );
    writer.streaming = Some(StreamingState::new(Some(budget.clone()))?);
    Ok(writer)
}

fn append_inventory(writer: &mut FsSstWriter, count: u32) -> MidgeResult<()> {
    for index in 0..count {
        let key = format!("key-{index:08}");
        writer.add_range_tombstone(key.as_bytes(), b"zzzzzzzzzzzz", u64::from(index))?;
        writer.add_sorted_with_meta(
            key.as_bytes(),
            Some(&[b'v'; 2048]),
            u64::from(index),
            EntryType::Put,
            None,
        )?;
    }
    Ok(())
}

fn compression_policies() -> [CompressionPolicy; 6] {
    use crate::codec::CompressionAlgo;
    [
        CompressionPolicy::None,
        CompressionPolicy::Fixed(CompressionAlgo::None),
        CompressionPolicy::Fixed(CompressionAlgo::Lz4),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        CompressionPolicy::default(),
    ]
}

fn assert_estimates_match(writer: &FsSstWriter) {
    assert_eq!(
        writer.estimated_size_bytes(),
        writer.legacy_estimated_size_bytes()
    );
    assert_eq!(
        writer.encoded_size_upper_bound(),
        writer.legacy_encoded_size_upper_bound()
    );
}

#[test]
fn should_preserve_each_size_estimate_when_versions_ttls_deletes_and_ranges_stream(
) -> MidgeResult<()> {
    for policy in compression_policies() {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024 * 1024);
        let mut writer = streaming_writer(&budget, policy)?;
        for index in 0..64_u64 {
            let start = format!("range-{index:08}");
            assert_estimates_match(&writer);
            writer.add_range_tombstone(start.as_bytes(), b"zzzzzzzzzzzzzzzzzzzz", index)?;
        }

        // Act: append through the real sorted writer.
        // Assert: compare every admission estimate, including empty and partial blocks.
        for index in 0..256_u64 {
            let key = format!("structured-key-{index:08}");
            for sequence in [index * 2 + 1, index * 2] {
                let deleted = sequence.is_multiple_of(11);
                let value = (!deleted).then_some([b'v'; 1024].as_slice());
                assert_estimates_match(&writer);
                let expected_next = writer.legacy_encoded_size_upper_bound().map(|bound| {
                    bound
                        .saturating_add(512)
                        .saturating_add(key.len().saturating_mul(12))
                        .saturating_add(value.map_or(0, <[u8]>::len).saturating_mul(2))
                });
                let reserved = budget.used();
                assert_eq!(
                    writer.encoded_size_upper_bound_after_sorted_entry(key.as_bytes(), value),
                    expected_next
                );
                assert_eq!(
                    budget.used(),
                    reserved,
                    "queries must not reserve retained memory"
                );
                writer.add_sorted_with_meta(
                    key.as_bytes(),
                    value,
                    sequence,
                    if deleted {
                        EntryType::Delete
                    } else {
                        EntryType::Put
                    },
                    Some(u64::MAX),
                )?;
                assert_estimates_match(&writer);
                assert!(writer.encoded_size_upper_bound().unwrap() <= expected_next.unwrap());
            }
        }
        let bound = writer.encoded_size_upper_bound().unwrap();
        let bytes = Box::new(writer).finish_bytes()?;
        assert!(bytes.len() <= bound);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak() <= budget.limit());
    }
    Ok(())
}

#[test]
fn should_preserve_size_totals_when_sorted_append_or_reservation_fails() -> MidgeResult<()> {
    // Arrange
    let budget = ResourceBudget::new(128 * 1024);
    let mut writer = streaming_writer(&budget, CompressionPolicy::None)?;
    append_inventory(&mut writer, 8)?;
    let expected = (
        writer.estimated_size_bytes(),
        writer.encoded_size_upper_bound(),
        budget.used(),
    );

    // Act
    let order_error = writer.add_sorted_with_meta(b"earlier", Some(b"v"), 1, EntryType::Put, None);
    let budget_error = writer.add_sorted_with_meta(
        b"zzz",
        Some(&vec![b'v'; 256 * 1024]),
        1,
        EntryType::Put,
        None,
    );

    // Assert
    assert!(matches!(
        order_error,
        Err(crate::MidgeError::InvalidArgument(_))
    ));
    assert!(matches!(
        budget_error,
        Err(crate::MidgeError::ResourceLimit(_))
    ));
    assert_eq!(
        (
            writer.estimated_size_bytes(),
            writer.encoded_size_upper_bound(),
            budget.used()
        ),
        expected
    );
    assert_estimates_match(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_charge_size_totals_when_index_and_range_metadata_are_retained() -> MidgeResult<()> {
    // Arrange
    let budget = ResourceBudget::new(128 * 1024);
    let mut writer = streaming_writer(&budget, CompressionPolicy::None)?;
    assert_eq!(budget.used(), 0);

    // Act: a second point flushes the first block; one range is retained separately.
    writer.add_range_tombstone(b"a", b"z", 1)?;
    writer.add_sorted_with_meta(b"a", Some(&[b'v'; 3000]), 1, EntryType::Put, None)?;
    writer.add_sorted_with_meta(b"b", Some(&[b'v'; 3000]), 2, EntryType::Put, None)?;

    // Assert: charge both cached totals alongside the retained metadata.
    let range_bytes = std::mem::size_of::<RangeTombstone>() + 2;
    let current_entry_bytes = std::mem::size_of::<PendingEntry>() + 4 + 3000 + 64;
    let index_and_bloom_bytes =
        std::mem::size_of::<(Vec<u8>, crate::sst::types::BlockHandle)>() + 1 + 16;
    assert_eq!(
        budget.used(),
        range_bytes
            + current_entry_bytes
            + index_and_bloom_bytes
            + 2 * std::mem::size_of::<usize>()
    );
    assert_estimates_match(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_saturate_retained_size_totals_when_an_append_exceeds_usize() -> MidgeResult<()> {
    // Arrange: exercise overflow without allocating an unrepresentable inventory.
    let budget = ResourceBudget::new(1024 * 1024);
    let mut writer = streaming_writer(&budget, CompressionPolicy::None)?;
    writer.range_tombstones.size_upper_bound = usize::MAX - 1;

    // Act
    append_inventory(&mut writer, 4)?;

    // Assert
    assert_eq!(writer.range_tombstones.size_upper_bound, usize::MAX);
    assert_eq!(writer.encoded_size_upper_bound(), Some(usize::MAX));
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_match_default_bloom_storage_when_only_its_size_is_requested() {
    // Arrange
    let counts = (0..1024).chain([4096, 65536]);
    // Act
    // Assert
    for count in counts {
        assert_eq!(
            crate::sst::bloom::BloomWriter::default_size_bytes(count),
            crate::sst::bloom::BloomWriter::with_defaults(count).size_bytes()
        );
    }
    assert!(crate::sst::bloom::BloomWriter::default_size_bytes(usize::MAX) >= 8);
}

#[cfg(unix)]
fn thread_cpu_time() -> std::time::Duration {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: the writable timespec is valid for the duration of this call.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &raw mut time) };
    assert_eq!(
        result,
        0,
        "thread CPU clock: {}",
        std::io::Error::last_os_error()
    );
    std::time::Duration::new(
        u64::try_from(time.tv_sec).unwrap(),
        u32::try_from(time.tv_nsec).unwrap(),
    )
}

#[cfg(unix)]
fn size_preflight_cpu(writer: &FsSstWriter, legacy: bool) -> std::time::Duration {
    let started = thread_cpu_time();
    for _ in 0..4000 {
        let writer = std::hint::black_box(writer);
        if legacy {
            std::hint::black_box(writer.legacy_estimated_size_bytes());
            std::hint::black_box(writer.legacy_encoded_size_upper_bound());
        } else {
            std::hint::black_box(writer.estimated_size_bytes());
            std::hint::black_box(writer.encoded_size_upper_bound());
        }
    }
    thread_cpu_time().saturating_sub(started)
}

#[cfg(unix)]
#[test]
#[ignore = "manual isolated CPU comparison; not an engine throughput qualification"]
fn should_halve_size_preflight_cpu_when_streaming_inventory_is_large() -> MidgeResult<()> {
    // Arrange: seeding, block encoding, compression, and finalization are outside the CPU window.
    let budget = ResourceBudget::new(32 * 1024 * 1024);
    let mut writer = streaming_writer(&budget, CompressionPolicy::None)?;
    append_inventory(&mut writer, 1024)?;
    assert_estimates_match(&writer);
    let mut baseline = std::time::Duration::ZERO;
    let mut candidate = std::time::Duration::ZERO;

    // Act: retain all five matched pairs and alternate their order.
    for trial in 0..5 {
        let (old, new) = if trial % 2 == 0 {
            (
                size_preflight_cpu(&writer, true),
                size_preflight_cpu(&writer, false),
            )
        } else {
            let new = size_preflight_cpu(&writer, false);
            (size_preflight_cpu(&writer, true), new)
        };
        eprintln!(
            "size-preflight CPU trial={trial} queries=8000 baseline_ns={} candidate_ns={}",
            old.as_nanos(),
            new.as_nanos()
        );
        baseline += old;
        candidate += new;
    }

    // Assert: this finite gate applies only to isolated queries at this inventory.
    eprintln!(
        "size-preflight CPU total baseline_ns={} candidate_ns={}",
        baseline.as_nanos(),
        candidate.as_nanos()
    );
    assert!(
        candidate * 2 <= baseline,
        "isolated preflight CPU gate missed"
    );
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_keep_size_preflight_work_constant_when_streaming_partitions_accumulate_blocks(
) -> MidgeResult<()> {
    // Arrange: real scratch-backed writers, each retaining both blocks and ranges.
    let small_budget = ResourceBudget::new(16 * 1024 * 1024);
    let large_budget = ResourceBudget::new(16 * 1024 * 1024);
    let mut small = streaming_writer(&small_budget, CompressionPolicy::None)?;
    let mut large = streaming_writer(&large_budget, CompressionPolicy::None)?;
    append_inventory(&mut small, 8)?;
    append_inventory(&mut large, 1024)?;

    // Act: count actual retained entries visited, not fixed query call sites.
    let small_work = query_work(&small);
    let large_work = query_work(&large);
    // Positive control: the same collection iterators count each real visit.
    let walked = |writer: &FsSstWriter| {
        QUERY_WORK.with(|work| work.set(Some(0)));
        writer
            .streaming
            .as_ref()
            .unwrap()
            .pipeline
            .block_index_entries
            .iter()
            .for_each(|entry| {
                std::hint::black_box(entry);
            });
        writer.range_tombstones.iter().for_each(|entry| {
            std::hint::black_box(entry);
        });
        QUERY_WORK.with(|work| work.replace(None).unwrap())
    };
    let small_walk = walked(&small);
    let large_walk = walked(&large);
    eprintln!("size-preflight actual visits: small={small_work}, large={large_work}; walk controls={small_walk}/{large_walk}");
    assert!(small_walk > 0 && large_walk > small_walk && large_walk >= 1500);

    // Assert
    assert!(
        large
            .streaming
            .as_ref()
            .unwrap()
            .pipeline
            .block_index_entries
            .len()
            > 500
    );
    assert_eq!(
        large.estimated_size_bytes(),
        large.legacy_estimated_size_bytes()
    );
    assert_eq!(
        large.encoded_size_upper_bound(),
        large.legacy_encoded_size_upper_bound()
    );
    assert!(
        small_work == 0 && large_work == 0,
        "size query work must stay constant: small={small_work}, large={large_work}"
    );
    drop(small);
    drop(large);
    assert_eq!(small_budget.used(), 0);
    assert_eq!(large_budget.used(), 0);
    Ok(())
}

impl FsSstWriter {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Keep the original DynSstWriter formula and return contract for differential tests"
    )]
    pub(super) fn legacy_encoded_size_upper_bound(&self) -> Option<usize> {
        // A trie has at most two nodes per boundary key; each node and edge
        // uses fewer than 64 bytes of integer framing. Key payloads also
        // appear in the block index and the two metadata bounds. The factors
        // below allow their copies plus worst-case fixed-compressor growth.
        let ranges = self
            .range_tombstones
            .as_slice()
            .iter()
            .fold(0usize, |total, range| {
                total.saturating_add(
                    self.additional_range_tombstone_size_upper_bound(&range.start, &range.end)
                        .unwrap_or(usize::MAX),
                )
            });
        let fixed = ranges.saturating_add(crate::memtable::size_bound::FIXED_SST_BYTES);
        let Some(streaming) = &self.streaming else {
            return Some(self.entries.iter().fold(fixed, |total, entry| {
                total.saturating_add(crate::memtable::size_bound::point_bytes(
                    entry.key.len(),
                    entry.value.as_ref().map_or(0, Vec::len),
                ))
            }));
        };
        let pipeline = &streaming.pipeline;
        let index =
            pipeline
                .block_index_entries
                .as_slice()
                .iter()
                .fold(0usize, |total, (key, _)| {
                    total
                        .saturating_add(256)
                        .saturating_add(key.len().saturating_mul(8))
                });
        let current_index = pipeline.current_first_key.as_ref().map_or(0, |key| {
            256usize.saturating_add(key.len().saturating_mul(8))
        });
        let current_bloom =
            crate::sst::bloom::BloomWriter::with_defaults(pipeline.current_block_keys.len())
                .size_bytes();
        let key_bounds = pipeline
            .smallest_key
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(pipeline.largest_key.as_ref().map_or(0, Vec::len))
            .saturating_mul(2);
        Some(
            fixed
                .saturating_add(
                    usize::try_from(streaming.sink.offset().unwrap_or(u64::MAX))
                        .unwrap_or(usize::MAX),
                )
                .saturating_add(pipeline.current_block.len().saturating_mul(2))
                .saturating_add(index)
                .saturating_add(current_index)
                .saturating_add(key_bounds)
                .saturating_add(
                    pipeline
                        .block_bloom
                        .size_bytes()
                        .saturating_add(current_bloom)
                        .saturating_mul(2),
                ),
        )
    }

    pub(super) fn legacy_estimated_size_bytes(&self) -> usize {
        if let Some(streaming) = &self.streaming {
            let pipeline = &streaming.pipeline;
            let persisted =
                usize::try_from(streaming.sink.offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
            let index = pipeline.block_index_entries.as_slice().iter().fold(
                pipeline
                    .block_index_entries
                    .len()
                    .saturating_mul(
                        std::mem::size_of::<(Vec<u8>, crate::sst::types::BlockHandle)>(),
                    ),
                |total, (key, _)| total.saturating_add(key.len()),
            );
            let current_bloom = if pipeline.current_block_keys.is_empty() {
                0
            } else {
                crate::sst::bloom::BloomWriter::with_defaults(pipeline.current_block_keys.len())
                    .size_bytes()
                    .saturating_add(13)
            };
            let bloom = pipeline
                .block_bloom
                .size_bytes()
                .saturating_add(current_bloom);
            return persisted
                .saturating_add(pipeline.current_block.len())
                .saturating_add(index.saturating_mul(2))
                .saturating_add(bloom)
                .saturating_add(16 * 1024);
        }
        self.entries.iter().fold(0usize, |total, entry| {
            total
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.as_ref().map_or(0, Vec::len))
                .saturating_add(32)
        })
    }
}
