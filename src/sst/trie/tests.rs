//! Integration tests for trie module

#[cfg(test)]
mod integration_tests {
    use crate::sst::trie::{TrieBuilder, TrieReader};

    #[test]
    fn should_roundtrip_simple_trie() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"banana", 1).unwrap();
        builder.add_key(b"cherry", 2).unwrap();

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert
        assert_eq!(reader.find_block(b"apple"), Some(0));
        assert_eq!(reader.find_block(b"banana"), Some(1));
        assert_eq!(reader.find_block(b"cherry"), Some(2));
    }

    #[test]
    fn should_handle_hierarchical_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"user/alice/profile", 0).unwrap();
        builder.add_key(b"user/alice/settings", 1).unwrap();
        builder.add_key(b"user/bob/profile", 2).unwrap();
        builder.add_key(b"user/bob/settings", 3).unwrap();

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert - exact lookups
        assert_eq!(reader.find_block(b"user/alice/profile"), Some(0));
        assert_eq!(reader.find_block(b"user/bob/settings"), Some(3));

        // Assert - prefix ranges
        let alice_blocks = reader.find_prefix_range(b"user/alice");
        assert!(alice_blocks.contains(&0));
        assert!(alice_blocks.contains(&1));
        assert_eq!(alice_blocks.len(), 2);

        let all_user_blocks = reader.find_prefix_range(b"user/");
        assert_eq!(all_user_blocks.len(), 4);
    }

    #[test]
    fn should_handle_shared_prefixes() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"test", 0).unwrap();
        builder.add_key(b"testing", 1).unwrap();
        builder.add_key(b"tester", 2).unwrap();
        builder.add_key(b"testament", 3).unwrap();

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert
        assert_eq!(reader.find_block(b"test"), Some(0));
        assert_eq!(reader.find_block(b"testing"), Some(1));
        assert_eq!(reader.find_block(b"tester"), Some(2));
        assert_eq!(reader.find_block(b"testament"), Some(3));

        // Prefix search
        let test_blocks = reader.find_prefix_range(b"test");
        assert_eq!(test_blocks.len(), 4);
    }

    #[test]
    fn should_handle_large_trie() {
        // Arrange
        let mut builder = TrieBuilder::new();
        for i in 0..1000 {
            let key = format!("key_{:08}", i);
            builder.add_key(key.as_bytes(), i).unwrap();
        }

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert - spot checks
        assert_eq!(reader.find_block(b"key_00000000"), Some(0));
        assert_eq!(reader.find_block(b"key_00000500"), Some(500));
        assert_eq!(reader.find_block(b"key_00000999"), Some(999));

        // Prefix search
        let key_00_blocks = reader.find_prefix_range(b"key_0000");
        assert!(key_00_blocks.len() >= 100); // key_00000000 through key_00000099
    }

    #[test]
    fn should_handle_empty_trie() {
        // Arrange
        let builder = TrieBuilder::new();

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert
        assert_eq!(reader.find_block(b"anything"), None);
        assert_eq!(reader.find_prefix_range(b"any").len(), 0);
    }

    #[test]
    fn should_handle_single_char_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"a", 0).unwrap();
        builder.add_key(b"b", 1).unwrap();
        builder.add_key(b"c", 2).unwrap();

        // Act
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Assert
        assert_eq!(reader.find_block(b"a"), Some(0));
        assert_eq!(reader.find_block(b"b"), Some(1));
        assert_eq!(reader.find_block(b"c"), Some(2));
        assert_eq!(reader.find_block(b"d"), None);
    }
}
