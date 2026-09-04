use crate::sst::compression::{CompressionAlgo, CompressionPolicy};
use crate::sst::fs::FsSstFactoryIo;
use crate::sst::traits::SstFactory;
use std::{path::Path, sync::Arc};

#[test]
fn should_read_default_sequence_after_writer_add() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(dir.path()).unwrap()), 4096);
    let mut writer = factory.create().unwrap();
    writer.add(b"key", b"value").unwrap();
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("probe.sst")).unwrap();
    // Act
    let reader = factory.open(Path::new("probe.sst")).unwrap();
    let result = reader.get(b"key").unwrap();
    // Assert
    assert_eq!(result.as_deref(), Some(b"value".as_slice()));
}

#[test]
fn should_read_empty_key_when_trie_has_deep_leftmost_branch() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(dir.path()).unwrap()), 4096);
    let mut writer = factory.create().unwrap();
    let value = vec![b'v'; 4096];
    writer.add_with_meta(b"", Some(&value), 1, 0, None).unwrap();
    for n in (1..=300).rev() {
        let mut key = vec![b'a'; n];
        key.push(b'b');
        writer
            .add_with_meta(&key, Some(&value), 1, 0, None)
            .unwrap();
    }
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("probe.sst")).unwrap();
    let reader = factory.open(Path::new("probe.sst")).unwrap();
    assert_eq!(reader.scan_range(None, None).unwrap().len(), 301);
    // Act
    let result = reader.get(b"").unwrap();
    // Assert
    assert_eq!(result.as_deref().map(<[u8]>::len), Some(value.len()));
}

#[test]
fn should_reject_oversized_entry_before_sst_writer_accepts_it() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(dir.path()).unwrap()), 4096);
    let value = vec![b'v'; 64 * 1024 * 1024];
    // Act
    for sorted in [false, true] {
        let mut writer = factory.create().unwrap();
        let result = if sorted {
            writer.add_sorted_with_meta(b"key", Some(&value), 1, 0, None)
        } else {
            writer.add_with_meta(b"key", Some(&value), 1, 0, None)
        };
        // Assert
        assert!(matches!(result, Err(crate::MidgeError::ResourceLimit(_))));
    }
}

#[test]
fn should_bound_zstd_allocation_by_reserved_decoded_size() {
    // Arrange
    use crate::sst::compression::{
        compress_block_with_trailer, decompress_block_with_trailer, decompressed_size_with_trailer,
    };
    let encoded = compress_block_with_trailer(
        &[b'v'; 512],
        &CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
    )
    .unwrap();
    let reserved = decompressed_size_with_trailer(&encoded).unwrap();
    // Act
    let decoded = decompress_block_with_trailer(&encoded)
        .unwrap()
        .try_into_mut()
        .unwrap();
    // Assert
    assert!(
        decoded.capacity() <= reserved,
        "reserved {reserved}, actual retained allocation {}",
        decoded.capacity()
    );
}

#[test]
fn should_preserve_zero_sequence_states_in_both_scan_directions() {
    use crate::sst::{types::KeyState, SstStateReader};
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let fs = Arc::new(crate::io::RealFs::new(dir.path()).unwrap());
    let factory = FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create().unwrap();
    writer.add(b"a", b"value").unwrap();
    writer.add(b"b", b"").unwrap();
    writer
        .add_with_meta(b"c", Some(b"expired"), 0, 0, Some(1))
        .unwrap();
    writer
        .add_with_meta(b"d", Some(b"masked"), 0, 0, None)
        .unwrap();
    writer.add_with_meta(b"d", None, 0, 2, None).unwrap();
    crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("zero.sst")).unwrap();
    let reader = Arc::new(
        crate::sst::fs::SstFileIo::open_with_real_fs(&dir.path().join("zero.sst")).unwrap(),
    );
    // Act
    let forward = reader
        .clone()
        .state_scan(None, None, false, 0, 2)
        .collect::<crate::MidgeResult<Vec<_>>>()
        .unwrap();
    let mut reverse = reader
        .clone()
        .state_scan(None, None, true, 0, 2)
        .collect::<crate::MidgeResult<Vec<_>>>()
        .unwrap();
    reverse.reverse();
    // Assert
    assert_eq!(forward.len(), 4);
    assert_eq!(forward, reverse);
    for (key, state) in &forward {
        assert_eq!(&reader.get_state_at_with_time(key, 0, 2).unwrap(), state);
    }
    assert!(matches!(&forward[0].1,KeyState::Value(value,0,None,0) if value.as_ref()==b"value"));
    assert!(matches!(&forward[1].1,KeyState::Value(value,0,None,0) if value.is_empty()));
    assert!(matches!(forward[2].1, KeyState::Tombstone(0)));
    assert!(matches!(forward[3].1, KeyState::Tombstone(0)));
}

#[test]
fn should_return_all_prefix_blocks_when_trie_exceeds_256_levels() {
    use crate::sst::trie::{builder::TrieBuilder, reader::TrieReader};
    // Arrange
    let mut builder = TrieBuilder::new();
    for (id, n) in (1..=300).rev().enumerate() {
        let mut key = vec![b'a'; n];
        key.push(b'b');
        builder.add_key(&key, u32::try_from(id).unwrap()).unwrap();
    }
    let reader = TrieReader::new(&builder.finish()).unwrap();
    // Act
    let blocks = reader.find_prefix_range(b"a");
    // Assert
    assert_eq!(blocks, (0..300).collect::<Vec<_>>());
    assert_eq!(reader.seek_next(b""), Some(0));
}

#[test]
fn should_roundtrip_maximum_decoded_entry_when_writing_sorted_or_unsorted() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let factory = FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(dir.path()).unwrap()), 4096)
        .with_compression_policy(CompressionPolicy::Fixed(CompressionAlgo::Lz4));
    let value = vec![b'v'; crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE - 29];
    for sorted in [false, true] {
        let mut writer = factory.create().unwrap();
        // Act
        if sorted {
            writer
                .add_sorted_with_meta(b"key", Some(&value), 1, 0, Some(u64::MAX))
                .unwrap();
        } else {
            writer
                .add_with_meta(b"key", Some(&value), 1, 0, Some(u64::MAX))
                .unwrap();
        }
        crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("max.sst")).unwrap();
        let reader = factory.open(Path::new("max.sst")).unwrap();
        // Assert
        assert_eq!(
            reader.get(b"key").unwrap().as_deref(),
            Some(value.as_slice())
        );
    }
}
