use super::*;
use crate::common::resource_budget::ResourceBudget;
use std::cell::Cell;

thread_local! {
    static VISITS: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(super) fn record_visit() {
    VISITS.with(|visits| {
        if let Some(count) = visits.get() {
            visits.set(Some(count + 1));
        }
    });
}

fn measured_visits(query: impl FnOnce()) -> usize {
    VISITS.with(|visits| visits.set(Some(0)));
    query();
    VISITS.with(|visits| visits.replace(None).unwrap())
}

fn streaming_writer(budget: &ResourceBudget) -> MidgeResult<FsSstWriter> {
    let mut writer = FsSstWriter::new_with_budget(
        Arc::new(crate::io::MockFs::new()),
        CompressionPolicy::None,
        4096,
        Some(budget.clone()),
    );
    writer.streaming = Some(StreamingState::new(Some(budget.clone()), &writer.fs)?);
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

#[test]
fn should_keep_size_query_work_constant_when_streaming_partitions_accumulate_blocks(
) -> MidgeResult<()> {
    // Arrange: append real entries through the scratch-backed streaming writer.
    let budgets = [
        ResourceBudget::new(16 * 1024 * 1024),
        ResourceBudget::new(16 * 1024 * 1024),
    ];
    let mut writers = [
        streaming_writer(&budgets[0])?,
        streaming_writer(&budgets[1])?,
    ];
    append_inventory(&mut writers[0], 8)?;
    append_inventory(&mut writers[1], 1024)?;

    // Act: measure actual visits to retained metadata during both production queries.
    let queries = writers.each_ref().map(|writer| {
        measured_visits(|| {
            std::hint::black_box(writer.estimated_size_bytes());
            std::hint::black_box(writer.encoded_size_upper_bound());
        })
    });
    let walks = writers.each_ref().map(|writer| {
        measured_visits(|| {
            let pipeline = &writer.streaming.as_ref().unwrap().pipeline;
            pipeline.block_index_entries.iter().for_each(|entry| {
                std::hint::black_box(entry);
            });
            writer.range_tombstones.iter().for_each(|range| {
                std::hint::black_box(range);
            });
        })
    });

    // Assert: the same iterators count real walks, but sizing does not scale with them.
    assert!(walks[0] > 0 && walks[1] >= 1500 && walks[1] > walks[0]);
    assert_eq!(queries, [0, 0], "size queries traversed retained metadata");
    drop(writers);
    assert!(budgets.iter().all(|budget| budget.used() == 0));
    Ok(())
}

fn reference_estimate(writer: &FsSstWriter) -> usize {
    let streaming = writer.streaming.as_ref().unwrap();
    let pipeline = &streaming.pipeline;
    let index = pipeline.block_index_entries.iter().fold(
        pipeline
            .block_index_entries
            .len()
            .saturating_mul(std::mem::size_of::<(Vec<u8>, crate::sst::types::BlockHandle)>()),
        |total, (key, _)| total.saturating_add(key.len()),
    );
    let current_bloom = if pipeline.current_block_keys.is_empty() {
        0
    } else {
        crate::sst::bloom::BloomWriter::with_defaults(pipeline.current_block_keys.len())
            .size_bytes()
            .saturating_add(13)
    };
    usize::try_from(streaming.sink.offset().unwrap())
        .unwrap()
        .saturating_add(pipeline.current_block.len())
        .saturating_add(index.saturating_mul(2))
        .saturating_add(
            pipeline
                .block_bloom
                .size_bytes()
                .saturating_add(current_bloom),
        )
        .saturating_add(16 * 1024)
}

fn reference_bound(writer: &FsSstWriter) -> usize {
    let streaming = writer.streaming.as_ref().unwrap();
    let pipeline = &streaming.pipeline;
    let ranges = writer.range_tombstones.iter().fold(0usize, |total, range| {
        total.saturating_add(crate::memtable::size_bound::range_bytes(
            range.start.len(),
            range.end.len(),
        ))
    });
    let index = pipeline
        .block_index_entries
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
    ranges
        .saturating_add(crate::memtable::size_bound::FIXED_SST_BYTES)
        .saturating_add(usize::try_from(streaming.sink.offset().unwrap()).unwrap())
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
        )
}

fn assert_sizing_matches(writer: &FsSstWriter) {
    assert_eq!(writer.estimated_size_bytes(), reference_estimate(writer));
    assert_eq!(
        writer.encoded_size_upper_bound(),
        Some(reference_bound(writer))
    );
}

fn append_versions(
    streaming: &mut FsSstWriter,
    buffered: &mut FsSstWriter,
) -> MidgeResult<Vec<crate::sst::traits::RawSstVersion>> {
    let mut expected = Vec::new();
    for index in 0..64_u64 {
        let key = format!("structured-key-{index:08}");
        for seq in [index * 2 + 1, index * 2] {
            let deleted = seq.is_multiple_of(11);
            let value = (!deleted).then_some([b'v'; 1024].as_slice());
            let kind = if deleted {
                EntryType::Delete
            } else {
                EntryType::Put
            };
            assert_sizing_matches(streaming);
            let next_bound = reference_bound(streaming)
                .saturating_add(512)
                .saturating_add(key.len().saturating_mul(12))
                .saturating_add(value.map_or(0, <[u8]>::len).saturating_mul(2));
            assert_eq!(
                streaming.encoded_size_upper_bound_after_sorted_entry(key.as_bytes(), value),
                Some(next_bound)
            );
            streaming.add_sorted_with_meta(key.as_bytes(), value, seq, kind, Some(u64::MAX))?;
            buffered.add_with_meta(key.as_bytes(), value, seq, kind, Some(u64::MAX))?;
            assert_sizing_matches(streaming);
            assert!(streaming.encoded_size_upper_bound().unwrap() <= next_bound);
            expected.push(crate::sst::traits::RawSstVersion {
                key: key.as_bytes().to_vec(),
                seq,
                is_tombstone: deleted,
                value: value.map(<[u8]>::to_vec),
                expiration: Some(u64::MAX),
            });
        }
    }
    Ok(expected)
}

#[test]
fn should_preserve_streaming_output_contract_when_metadata_grows() -> MidgeResult<()> {
    use crate::codec::CompressionAlgo;
    for policy in [
        CompressionPolicy::None,
        CompressionPolicy::Fixed(CompressionAlgo::None),
        CompressionPolicy::Fixed(CompressionAlgo::Lz4),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        CompressionPolicy::default(),
    ] {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
        let budget = ResourceBudget::new(16 * 1024 * 1024);
        let mut streaming = FsSstWriter::new_with_budget(
            Arc::clone(&fs),
            policy.clone(),
            4096,
            Some(budget.clone()),
        );
        streaming.streaming = Some(StreamingState::new(Some(budget.clone()), &fs)?);
        let mut buffered = FsSstWriter::new(Arc::clone(&fs), policy, 4096);
        assert_sizing_matches(&streaming);
        for index in 0..4_u64 {
            let start = format!("range-{index:08}");
            streaming.add_range_tombstone(start.as_bytes(), b"zzzzzzzzzzzz", index)?;
            buffered.add_range_tombstone(start.as_bytes(), b"zzzzzzzzzzzz", index)?;
        }

        // Act: preserve every version and exact bytes produced by the shared pipeline.
        let expected = append_versions(&mut streaming, &mut buffered)?;
        let bound = streaming.encoded_size_upper_bound().unwrap();
        let bytes = Box::new(streaming).finish_bytes()?;
        let reference_bytes = Box::new(buffered).finish_bytes()?;
        crate::sst::fs::persist_sst_bytes_to_path(&fs, &bytes, Path::new("sizing.sst"))?;
        let reader = FsSstFactoryIo::new(fs, 4096).open(Path::new("sizing.sst"))?;
        let ranges = reader.range_tombstones();
        let actual = reader
            .raw_version_cursor(None, None)?
            .collect::<MidgeResult<Vec<_>>>()?;

        // Assert
        assert_eq!(bytes, reference_bytes);
        assert!(bytes.len() <= bound);
        assert_eq!(actual, expected);
        assert_eq!(ranges.len(), 4);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak() <= budget.limit());
    }
    Ok(())
}

#[test]
fn should_preserve_streaming_state_when_sorted_append_is_rejected() -> MidgeResult<()> {
    // Arrange
    let budget = ResourceBudget::new(128 * 1024);
    let mut writer = streaming_writer(&budget)?;
    append_inventory(&mut writer, 8)?;
    let before = (
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
        before,
    );
    assert_sizing_matches(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_charge_size_totals_only_when_index_and_range_metadata_are_retained() -> MidgeResult<()> {
    // Arrange
    let budget = ResourceBudget::new(128 * 1024);
    let mut writer = streaming_writer(&budget)?;
    assert_eq!(budget.used(), 0);
    assert_sizing_matches(&writer);

    // Act: the second point flushes the first block and retains its index.
    writer.add_range_tombstone(b"a", b"z", 1)?;
    writer.add_sorted_with_meta(b"a", Some(&[b'v'; 3000]), 1, EntryType::Put, None)?;
    writer.add_sorted_with_meta(b"b", Some(&[b'v'; 3000]), 2, EntryType::Put, None)?;

    // Assert: each scalar is charged once alongside the metadata it summarizes.
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
    assert_sizing_matches(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_preserve_empty_size_totals_when_first_range_reservation_is_rejected() -> MidgeResult<()> {
    // Arrange: the range fits without its scalar; ownership must admit both together.
    let range_bytes = std::mem::size_of::<RangeTombstone>() + 2;
    let budget = ResourceBudget::new(range_bytes + std::mem::size_of::<usize>() - 1);
    let mut writer = streaming_writer(&budget)?;
    let before = (
        writer.estimated_size_bytes(),
        writer.encoded_size_upper_bound(),
    );

    // Act
    let error = writer.add_range_tombstone(b"a", b"z", 1);

    // Assert
    assert!(matches!(error, Err(crate::MidgeError::ResourceLimit(_))));
    assert_eq!(budget.used(), 0);
    assert_eq!(
        (
            writer.estimated_size_bytes(),
            writer.encoded_size_upper_bound()
        ),
        before,
    );
    assert_sizing_matches(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_preserve_size_totals_when_first_block_reservation_is_rejected() -> MidgeResult<()> {
    // Arrange: the next point fits, but its block cannot reserve compression workspace.
    let budget = ResourceBudget::new(8 * 1024);
    let mut writer = streaming_writer(&budget)?;
    writer.add_sorted_with_meta(b"a", Some(&[b'v'; 3000]), 1, EntryType::Put, None)?;
    let used = budget.used();

    // Act
    let error = writer.add_sorted_with_meta(b"b", Some(&[b'v'; 3000]), 2, EntryType::Put, None);

    // Assert
    assert!(matches!(error, Err(crate::MidgeError::ResourceLimit(_))));
    assert_eq!(budget.used(), used);
    assert!(writer
        .streaming
        .as_ref()
        .unwrap()
        .pipeline
        .block_index_entries
        .is_empty());
    assert_sizing_matches(&writer);
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_saturate_range_size_bound_when_retained_total_exceeds_usize() -> MidgeResult<()> {
    // Arrange: exercise the arithmetic boundary without an impossible allocation.
    let budget = ResourceBudget::new(128 * 1024);
    let mut writer = streaming_writer(&budget)?;
    writer.range_tombstones.encoded_bound = usize::MAX - 1;

    // Act
    writer.add_range_tombstone(b"a", b"z", 1)?;

    // Assert
    assert_eq!(writer.encoded_size_upper_bound(), Some(usize::MAX));
    drop(writer);
    assert_eq!(budget.used(), 0);
    Ok(())
}
