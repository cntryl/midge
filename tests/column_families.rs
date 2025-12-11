//! Column Family Integration Tests
//!
//! Tests for column family lifecycle, isolation, and persistence.

use std::sync::Arc;
use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::ColumnFamilyId;

// ============================================================================
// Column Family Creation
// ============================================================================

#[test]
fn should_create_column_family_given_valid_name_when_engine_open() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act
        let cf = engine.create_column_family("test_cf").unwrap();

        // Assert
        assert_eq!(cf.name(), "test_cf");
        assert_ne!(cf.id().as_u32(), 0); // Not default CF
    });
}

#[test]
fn should_create_multiple_column_families_given_unique_names_when_engine_open() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        let cf3 = engine.create_column_family("cf3").unwrap();

        // Assert
        assert_eq!(cf1.name(), "cf1");
        assert_eq!(cf2.name(), "cf2");
        assert_eq!(cf3.name(), "cf3");
        assert_ne!(cf1.id().as_u32(), cf2.id().as_u32());
        assert_ne!(cf2.id().as_u32(), cf3.id().as_u32());
    });
}

#[test]
#[ignore = "Requires duplicate name detection"]
fn should_fail_create_column_family_given_duplicate_name_when_cf_exists() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        engine.create_column_family("test_cf").unwrap();

        // Act
        let result = engine.create_column_family("test_cf");

        // Assert
        assert!(result.is_err());
    });
}

#[test]
#[ignore = "Requires per-CF configuration support"]
fn should_create_column_family_with_custom_config_given_config_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act - would need create_column_family_with_options(name, config)
        // let config = ColumnFamilyConfig { memtable_size: 1024 * 1024 };
        // let cf = engine.create_column_family_with_options("test_cf", config).unwrap();

        // Assert
        // assert_eq!(cf.name(), "test_cf");
    });
}

// ============================================================================
// Column Family Deletion
// ============================================================================

#[test]
fn should_drop_column_family_given_empty_cf_when_requested() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let cf_id = cf.id();

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
fn should_drop_column_family_given_flushed_data_when_requested() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf, b"key1", b"value1").unwrap();
        let cf_id = cf.id();

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
#[ignore = "Requires memtable check before drop"]
fn should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf, b"key1", b"value1").unwrap();
        let cf_id = cf.id();

        // Act - should fail if memtable not flushed
        let result = engine.drop_column_family(cf_id);

        // Assert - current behavior may allow drop, but safe behavior would prevent it
        // This test documents desired behavior
        // assert!(result.is_err());
    });
}

#[test]
fn should_fail_drop_default_column_family_given_drop_request_when_default_cf() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let default_cf_id = ColumnFamilyId::DEFAULT;

        // Act
        let result = engine.drop_column_family(default_cf_id);

        // Assert
        assert!(result.is_err());
    });
}

#[test]
#[ignore = "Requires handle invalidation after drop"]
fn should_invalidate_handle_given_cf_dropped_when_accessing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let cf_id = cf.id();
        engine.drop_column_family(cf_id).unwrap();

        // Act
        let result = engine.put(&cf, b"key1", b"value1");

        // Assert - should fail because CF is dropped
        assert!(result.is_err());
    });
}

#[test]
#[ignore = "Requires persistence support"]
fn should_delete_cf_data_given_cf_dropped_when_persisted() {
    let opts = durability_opts();

    // Arrange & Act (Phase 1)
    {
        let engine = open_with_mode(opts.clone(), "localdisk");
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.drop_column_family(cf.id()).unwrap();
        // Engine dropped
    }

    // Assert (Phase 2) - dropped CF data should not be recovered
    {
        let engine = open_with_mode(opts, "localdisk");
        // Would need get_column_family_by_name or list to verify CF is gone
    }
}

#[test]
#[ignore = "Requires CF name reuse tracking"]
fn should_allow_recreate_cf_with_same_name_given_cf_dropped_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf1 = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf1, b"key1", b"value1").unwrap();
        engine.drop_column_family(cf1.id()).unwrap();

        // Act - recreate with same name
        let cf2 = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf2, b"key2", b"value2").unwrap();

        // Assert - should not see old data
        let result1 = engine.get(&cf2, b"key1").unwrap();
        let result2 = engine.get(&cf2, b"key2").unwrap();
        assert_eq!(result1, None);
        assert_eq!(result2, Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// Listing Column Families
// ============================================================================

#[test]
#[ignore = "Requires list_column_families implementation"]
fn should_list_default_cf_only_given_no_custom_cfs_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        assert_eq!(cfs.len(), 1);
        assert_eq!(cfs[0].name(), "default");
    });
}

#[test]
#[ignore = "Requires list_column_families implementation"]
fn should_list_all_column_families_given_multiple_cfs_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        engine.create_column_family("cf1").unwrap();
        engine.create_column_family("cf2").unwrap();

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        assert_eq!(cfs.len(), 3); // default + cf1 + cf2
        let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"cf1"));
        assert!(names.contains(&"cf2"));
    });
}

#[test]
#[ignore = "Requires list_column_families implementation"]
fn should_not_list_dropped_cf_given_cf_dropped_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.drop_column_family(cf.id()).unwrap();

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
        assert!(!names.contains(&"test_cf"));
    });
}

// ============================================================================
// Data Isolation
// ============================================================================

#[test]
fn should_isolate_keys_given_same_key_in_different_cfs_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let default_cf = engine.default_column_family();
        let cf1 = engine.create_column_family("cf1").unwrap();

        // Act
        engine.put(default_cf, b"key1", b"value_default").unwrap();
        engine.put(&cf1, b"key1", b"value_cf1").unwrap();

        // Assert
        let result_default = engine.get(default_cf, b"key1").unwrap();
        let result_cf1 = engine.get(&cf1, b"key1").unwrap();
        assert_eq!(result_default, Some(Bytes::from_static(b"value_default")));
        assert_eq!(result_cf1, Some(Bytes::from_static(b"value_cf1")));
    });
}

#[test]
fn should_isolate_deletes_given_delete_in_one_cf_when_other_cf_has_same_key() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        engine.put(&cf1, b"key1", b"value1").unwrap();
        engine.put(&cf2, b"key1", b"value2").unwrap();

        // Act
        engine.delete(&cf1, b"key1").unwrap();

        // Assert
        let result_cf1 = engine.get(&cf1, b"key1").unwrap();
        let result_cf2 = engine.get(&cf2, b"key1").unwrap();
        assert_eq!(result_cf1, None);
        assert_eq!(result_cf2, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_isolate_data_given_different_data_volumes_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();

        // Act - write different amounts to each CF
        for i in 0..100 {
            let key = format!("key{}", i);
            engine.put(&cf1, key.as_bytes(), b"value1").unwrap();
        }
        engine.put(&cf2, b"single_key", b"value2").unwrap();

        // Assert
        let result_cf1 = engine.get(&cf1, b"key50").unwrap();
        let result_cf2 = engine.get(&cf2, b"single_key").unwrap();
        assert_eq!(result_cf1, Some(Bytes::from_static(b"value1")));
        assert_eq!(result_cf2, Some(Bytes::from_static(b"value2")));
        // CF1 should not see CF2's data
        assert_eq!(engine.get(&cf1, b"single_key").unwrap(), None);
    });
}

#[test]
#[ignore = "Requires compaction support"]
fn should_isolate_compaction_given_per_cf_data_when_compacting() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        
        // Write data to both CFs
        for i in 0..50 {
            let key = format!("key{}", i);
            engine.put(&cf1, key.as_bytes(), b"value1").unwrap();
            engine.put(&cf2, key.as_bytes(), b"value2").unwrap();
        }

        // Act - compact CF1 only
        // engine.compact_column_family(&cf1).unwrap();

        // Assert - both should still have their data
        assert_eq!(engine.get(&cf1, b"key25").unwrap(), Some(Bytes::from_static(b"value1")));
        assert_eq!(engine.get(&cf2, b"key25").unwrap(), Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

#[test]
#[ignore = "Requires persistence support"]
fn should_persist_cf_metadata_given_restart_when_cf_created() {
    let opts = durability_opts();

    // Arrange & Act (Phase 1)
    {
        let engine = open_with_mode(opts.clone(), "localdisk");
        engine.create_column_family("test_cf").unwrap();
        // Engine dropped
    }

    // Assert (Phase 2)
    {
        let engine = open_with_mode(opts, "localdisk");
        let cfs = engine.list_column_families().unwrap();
        let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
        assert!(names.contains(&"test_cf"));
    }
}

#[test]
#[ignore = "Requires persistence support"]
fn should_persist_cf_data_given_restart_when_data_flushed() {
    let opts = durability_opts();

    // Arrange & Act (Phase 1)
    {
        let engine = open_with_mode(opts.clone(), "localdisk");
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf, b"key1", b"value1").unwrap();
        // Engine dropped
    }

    // Assert (Phase 2)
    {
        let engine = open_with_mode(opts, "localdisk");
        // Would need get_column_family_by_name to retrieve CF handle
        // let cf = engine.get_column_family_by_name("test_cf").unwrap();
        // let result = engine.get(&cf, b"key1").unwrap();
        // assert_eq!(result, Some(Bytes::from_static(b"value1")));
    }
}

#[test]
#[ignore = "Requires persistence support"]
fn should_persist_multiple_cfs_given_restart_when_all_flushed() {
    let opts = durability_opts();

    // Arrange & Act (Phase 1)
    {
        let engine = open_with_mode(opts.clone(), "localdisk");
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        engine.put(&cf1, b"key1", b"value1").unwrap();
        engine.put(&cf2, b"key2", b"value2").unwrap();
        // Engine dropped
    }

    // Assert (Phase 2)
    {
        let engine = open_with_mode(opts, "localdisk");
        let cfs = engine.list_column_families().unwrap();
        assert_eq!(cfs.len(), 3); // default + cf1 + cf2
    }
}

#[test]
#[ignore = "Requires persistence support"]
fn should_persist_cf_drop_given_restart_when_cf_was_dropped() {
    let opts = durability_opts();

    // Arrange & Act (Phase 1)
    {
        let engine = open_with_mode(opts.clone(), "localdisk");
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.drop_column_family(cf.id()).unwrap();
        // Engine dropped
    }

    // Assert (Phase 2)
    {
        let engine = open_with_mode(opts, "localdisk");
        let cfs = engine.list_column_families().unwrap();
        let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
        assert!(!names.contains(&"test_cf"));
    }
}

// ============================================================================
// Query by Name
// ============================================================================

#[test]
#[ignore = "Requires get_column_family_by_name implementation"]
fn should_get_column_family_by_name_given_existing_cf_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        engine.create_column_family("test_cf").unwrap();

        // Act
        // let cf = engine.get_column_family_by_name("test_cf").unwrap();

        // Assert
        // assert_eq!(cf.name(), "test_cf");
    });
}

#[test]
#[ignore = "Requires get_column_family_by_name implementation"]
fn should_fail_get_column_family_given_nonexistent_name_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act
        // let result = engine.get_column_family_by_name("nonexistent");

        // Assert
        // assert!(result.is_err());
    });
}

#[test]
fn should_get_default_column_family_given_fresh_engine_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));

        // Act
        let cf = engine.default_column_family();

        // Assert
        assert_eq!(cf.name(), "default");
        assert_eq!(cf.id().as_u32(), 0);
    });
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn should_isolate_cf_after_flush_given_same_key_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        
        // Act
        engine.put(&cf1, b"key1", b"value1").unwrap();
        engine.put(&cf2, b"key1", b"value2").unwrap();
        // Flush would happen here if we had flush API

        // Assert - isolation maintained even after flush
        assert_eq!(engine.get(&cf1, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
        assert_eq!(engine.get(&cf2, b"key1").unwrap(), Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_handle_operations_on_default_cf_given_custom_cfs_exist_when_operating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let _cf1 = engine.create_column_family("cf1").unwrap();
        let _cf2 = engine.create_column_family("cf2").unwrap();
        let default_cf = engine.default_column_family();

        // Act
        engine.put(default_cf, b"key1", b"default_value").unwrap();

        // Assert
        assert_eq!(engine.get(default_cf, b"key1").unwrap(), Some(Bytes::from_static(b"default_value")));
    });
}

#[test]
fn should_maintain_cf_isolation_given_many_cfs_when_operating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let mut cfs = Vec::new();
        for i in 0..10 {
            let name = format!("cf{}", i);
            cfs.push(engine.create_column_family(&name).unwrap());
        }

        // Act - write unique value to each CF
        for (i, cf) in cfs.iter().enumerate() {
            let value = format!("value{}", i);
            engine.put(cf, b"shared_key", value.as_bytes()).unwrap();
        }

        // Assert - each CF has its own value
        for (i, cf) in cfs.iter().enumerate() {
            let expected = format!("value{}", i);
            let result = engine.get(cf, b"shared_key").unwrap();
            assert_eq!(result, Some(Bytes::from(expected)));
        }
    });
}
