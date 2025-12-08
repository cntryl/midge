/// Phase 2 TDD Tests: Fence Pointers & Tombstone Awareness
///
/// Tests that BlockMeta fence pointers and tombstone awareness are properly
/// used throughout the read path and compaction logic.
use bytes::Bytes;
use cntryl_midge::sst::block_meta::BlockMeta;
use cntryl_midge::sst::format::BlockHandle;

/// Test: BlockMeta tracks fence pointers (min/max keys)
#[test]
fn should_track_fence_pointers_in_block_meta() {
    // Arrange
    let meta = BlockMeta::new(
        Bytes::from("apple"),
        Bytes::from("apricot"),
        BlockHandle::new(100, 1024),
    );

    // Act
    // (assertions check metadata)

    // Assert
    assert_eq!(meta.min_key, Bytes::from("apple"));
    assert_eq!(meta.max_key, Bytes::from("apricot"));
    assert!(!meta.has_tombstones);
    assert!(meta.tombstone_min.is_none());
    assert!(meta.tombstone_max.is_none());
}

/// Test: BlockMeta can track tombstone ranges
#[test]
fn should_track_tombstone_ranges_in_block_meta() {
    // Arrange
    let meta = BlockMeta::new(
        Bytes::from("apple"),
        Bytes::from("cherry"),
        BlockHandle::new(100, 1024),
    );

    // Act: Add tombstone info
    let meta = meta.with_tombstones(
        true,
        Some(Bytes::from("banana")),
        Some(Bytes::from("blueberry")),
    );

    // Assert
    assert!(meta.has_tombstones);
    assert_eq!(meta.tombstone_min, Some(Bytes::from("banana")));
    assert_eq!(meta.tombstone_max, Some(Bytes::from("blueberry")));
}

/// Test: BlockMeta can determine if fully covered by tombstones
#[test]
fn should_detect_block_fully_covered_by_tombstones() {
    // Arrange: Block [b, d] fully covered by tombstones [a, e]
    let meta = BlockMeta::new(
        Bytes::from("b"),
        Bytes::from("d"),
        BlockHandle::new(100, 1024),
    )
    .with_tombstones(true, Some(Bytes::from("a")), Some(Bytes::from("e")));

    // Act
    let is_covered = meta.might_be_fully_covered();

    // Assert
    assert!(is_covered);
}

/// Test: BlockMeta can detect partial tombstone coverage
#[test]
fn should_detect_partial_tombstone_coverage() {
    // Arrange: Block [b, d] partially covered by tombstones [c, e]
    let meta = BlockMeta::new(
        Bytes::from("b"),
        Bytes::from("d"),
        BlockHandle::new(100, 1024),
    )
    .with_tombstones(true, Some(Bytes::from("c")), Some(Bytes::from("e")));

    // Act
    let is_covered = meta.might_be_fully_covered();

    // Assert: Not fully covered (starts before tombstone)
    assert!(!is_covered);
}

/// Test: BlockMeta can determine if block is in range
#[test]
fn should_check_key_containment_in_block() {
    // Arrange
    let meta = BlockMeta::new(
        Bytes::from("apple"),
        Bytes::from("banana"),
        BlockHandle::new(100, 1024),
    );

    // Act
    let contains_apple = meta.contains_key(b"apple");
    let contains_apricot = meta.contains_key(b"apricot");
    let contains_banana = meta.contains_key(b"banana");
    let contains_aardvark = meta.contains_key(b"aardvark");
    let contains_cherry = meta.contains_key(b"cherry");

    // Assert
    // Keys within range
    assert!(contains_apple);
    assert!(contains_apricot);
    assert!(contains_banana);

    // Keys outside range
    assert!(!contains_aardvark);
    assert!(!contains_cherry);
}

/// Test: BlockMeta can determine range intersection
#[test]
fn should_check_range_intersection() {
    // Arrange
    let meta = BlockMeta::new(
        Bytes::from("b"),
        Bytes::from("d"),
        BlockHandle::new(100, 1024),
    );

    // Act: Check various range intersections
    let intersects_ac = meta.range_intersects(b"a", b"c");
    let intersects_ce = meta.range_intersects(b"c", b"e");
    let intersects_bd = meta.range_intersects(b"b", b"d");
    let intersects_ab = meta.range_intersects(b"a", b"b");
    let intersects_ef = meta.range_intersects(b"e", b"f");

    // Assert
    // Intersecting ranges
    assert!(intersects_ac); // [a, c) intersects [b, d]
    assert!(intersects_ce); // [c, e) intersects [b, d]
    assert!(intersects_bd); // [b, d) intersects [b, d]

    // Non-intersecting ranges
    assert!(!intersects_ab); // [a, b) doesn't intersect [b, d]
    assert!(!intersects_ef); // [e, f) doesn't intersect [b, d]
}

/// Test: Range scan should skip blocks outside the scan range using fence pointers
#[test]
fn should_use_fence_pointers_to_skip_blocks_in_range_scan() {
    // Arrange: Three blocks
    let blocks = vec![
        BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
        BlockMeta::new(
            Bytes::from("d"),
            Bytes::from("f"),
            BlockHandle::new(100, 100),
        ),
        BlockMeta::new(
            Bytes::from("g"),
            Bytes::from("z"),
            BlockHandle::new(200, 100),
        ),
    ];

    // Act: Find blocks for range [e, h)
    let mut blocks_in_range = Vec::new();
    for block in &blocks {
        if block.range_intersects(b"e", b"h") {
            blocks_in_range.push(block);
        }
    }

    // Assert: Should include blocks 2 and 3, skip block 1
    assert_eq!(blocks_in_range.len(), 2);
    assert_eq!(blocks_in_range[0].min_key, Bytes::from("d"));
    assert_eq!(blocks_in_range[1].min_key, Bytes::from("g"));
}

/// Test: Compaction should skip blocks fully covered by tombstones
#[test]
fn should_skip_blocks_fully_covered_by_tombstones_in_compaction() {
    // Arrange: Three blocks, middle one fully covered by tombstones
    let mut blocks_to_compact = vec![
        BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
        BlockMeta::new(
            Bytes::from("d"),
            Bytes::from("f"),
            BlockHandle::new(100, 100),
        )
        .with_tombstones(true, Some(Bytes::from("c")), Some(Bytes::from("g"))),
        BlockMeta::new(
            Bytes::from("h"),
            Bytes::from("z"),
            BlockHandle::new(200, 100),
        ),
    ];

    // Act: Filter out blocks that don't need reading (fully covered by tombstones)
    blocks_to_compact.retain(|block| !block.might_be_fully_covered());

    // Assert: Only blocks 1 and 3 need to be read
    assert_eq!(blocks_to_compact.len(), 2);
    assert_eq!(blocks_to_compact[0].min_key, Bytes::from("a"));
    assert_eq!(blocks_to_compact[1].min_key, Bytes::from("h"));
}

/// Test: Iterator should use fence pointers to efficiently determine next valid block
#[test]
fn should_use_fence_pointers_in_iterator_next_block() {
    // Arrange: Simulate iterator state with blocks
    let blocks = vec![
        BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100)),
        BlockMeta::new(
            Bytes::from("d"),
            Bytes::from("f"),
            BlockHandle::new(100, 100),
        ),
        BlockMeta::new(
            Bytes::from("g"),
            Bytes::from("z"),
            BlockHandle::new(200, 100),
        ),
    ];

    // Act: Find next block given current key "e"
    let current_key = b"e";
    let mut next_block = None;
    for block in &blocks {
        if block.contains_key(current_key) {
            next_block = Some(block);
            break;
        }
    }

    // Assert: Should find block 2 (d-f range contains "e")
    assert!(next_block.is_some());
    assert_eq!(next_block.unwrap().min_key, Bytes::from("d"));
}

/// Test: Multiple tombstone ranges in different blocks
#[test]
fn should_track_multiple_tombstone_ranges() {
    // Arrange
    // Block1: [a, c] with tombstone [a, d) - fully covered (d > c)
    let block1 = BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100))
        .with_tombstones(true, Some(Bytes::from("a")), Some(Bytes::from("d")));

    // Block2: [d, f] with tombstone [e, g) - NOT fully covered (e > d, so min not covered)
    let block2 = BlockMeta::new(
        Bytes::from("d"),
        Bytes::from("f"),
        BlockHandle::new(100, 100),
    )
    .with_tombstones(true, Some(Bytes::from("e")), Some(Bytes::from("g")));

    // Act
    let block1_covered = block1.might_be_fully_covered();
    let block2_covered = block2.might_be_fully_covered();

    // Assert
    assert!(block1_covered);
    assert!(!block2_covered); // Not fully covered (starts before tombstone)
}

/// Test: BlockMeta without tombstones
#[test]
fn should_handle_block_meta_without_tombstones() {
    // Arrange
    let meta = BlockMeta::new(
        Bytes::from("a"),
        Bytes::from("z"),
        BlockHandle::new(100, 1024),
    );

    // Act
    let has_tombstones = meta.has_tombstones;
    let min_tombstone = meta.tombstone_min.is_none();
    let max_tombstone = meta.tombstone_max.is_none();
    let fully_covered = meta.might_be_fully_covered();

    // Assert: Block not marked as having tombstones
    assert!(!has_tombstones);
    assert!(min_tombstone);
    assert!(max_tombstone);
    assert!(!fully_covered);
}

/// Test: Fence pointer ordering consistency
#[test]
fn should_maintain_fence_pointer_ordering() {
    // Arrange: Three blocks in order
    let block1 = BlockMeta::new(Bytes::from("a"), Bytes::from("c"), BlockHandle::new(0, 100));
    let block2 = BlockMeta::new(
        Bytes::from("d"),
        Bytes::from("f"),
        BlockHandle::new(100, 100),
    );
    let block3 = BlockMeta::new(
        Bytes::from("g"),
        Bytes::from("z"),
        BlockHandle::new(200, 100),
    );

    // Act: Check ordering relationships
    let order_12 = block1.max_key <= block2.min_key;
    let order_23 = block2.max_key <= block3.min_key;

    // Assert: Each block's max_key <= next block's min_key
    assert!(order_12);
    assert!(order_23);
}
