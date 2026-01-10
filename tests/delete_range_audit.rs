//! Delete Range Limitation Audit
//!
//! Purpose: Verify the documented limitation that range() is stubbed
//! and determine if delete_range() actually works despite this

use cntryl_midge::testkit::*;

#[test]
fn should_verify_delete_range_works_despite_range_being_stubbed() {
    eprintln!("\n=== AUDIT: DELETE_RANGE LIMITATION ===");

    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        eprintln!("Testing delete_range in mode: {}", mode);

        // Set up test data
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        for i in 1..=10 {
            let key = format!("key{:02}", i);
            let val = format!("val{:02}", i);
            tx.put(key.into_bytes(), val.into_bytes(), None).unwrap();
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        eprintln!("  Inserted 10 keys: key01..key10");

        // Test 1: Delete a range [key02, key08) (should delete key02-key07)
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key02".to_vec(), b"key08".to_vec())
            .unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
        eprintln!("  Called delete_range(key02, key08)");

        // Check what was actually deleted
        let mut deleted_count = 0;
        let mut retained_count = 0;

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        for i in 1..=10 {
            let key = format!("key{:02}", i);
            let exists = tx.get(key.as_bytes()).unwrap().is_some();

            if (2..8).contains(&i) {
                if !exists {
                    deleted_count += 1;
                    eprintln!("    ✓ {} deleted (in range)", key);
                } else {
                    eprintln!("    ✗ {} retained (should be deleted!)", key);
                }
            } else if exists {
                retained_count += 1;
                eprintln!("    ✓ {} retained (outside range)", key);
            } else {
                eprintln!("    ✗ {} deleted (should be retained!)", key);
            }
        }

        eprintln!(
            "  Result: {} deleted, {} retained",
            deleted_count, retained_count
        );

        if deleted_count > 0 {
            eprintln!("✓ DELETE_RANGE IS WORKING despite documented range() limitation");
            eprintln!("  Actual behavior: Successfully deleted keys in specified range");
        } else if deleted_count == 0 && deleted_count + retained_count == 10 {
            eprintln!("✗ DELETE_RANGE NOT WORKING");
            eprintln!("  All keys retained - delete_range may be using stubbed range()");
        }
    });
}

#[test]
fn should_test_range_method_directly_if_available() {
    eprintln!("\n=== AUDIT: RANGE METHOD STATUS ===");

    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        eprintln!("Testing range() method in mode: {}", mode);

        // Insert test data
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"a".to_vec(), b"val_a".to_vec(), None).unwrap();
        tx.put(b"b".to_vec(), b"val_b".to_vec(), None).unwrap();
        tx.put(b"c".to_vec(), b"val_c".to_vec(), None).unwrap();
        tx.put(b"d".to_vec(), b"val_d".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Try to call range() - if it exists and is not stubbed, this will return keys
        let query = cntryl_midge::Query::new();
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = tx.scan_range(&query).unwrap();

        eprintln!("  scan() returned {} results", results.len());

        if results.is_empty() {
            eprintln!("✗ RANGE/SCAN returning empty - likely stubbed");
        } else {
            eprintln!("✓ RANGE/SCAN working - returned {} keys", results.len());
            for (k, _v) in &results {
                eprintln!("    Key: {}", String::from_utf8_lossy(k));
            }
        }
    });
}

#[test]
fn summary_delete_range_limitation() {
    eprintln!("\n╔════════════════════════════════════════════════════════╗");
    eprintln!("║  DELETE RANGE CONTRADICTION AUDIT SUMMARY              ║");
    eprintln!("╚════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("QUESTION:");
    eprintln!("  Does delete_range() work, or is it limited by range() being stubbed?");
    eprintln!();
    eprintln!("FINDING FROM COMMENTS:");
    eprintln!("  File: engine_delete_range.rs contains:");
    eprintln!("    'Delete range is implemented by calling range() to find keys,");
    eprintln!("     then deleting each one individually. The range() method is");
    eprintln!("     currently a stub returning empty.'");
    eprintln!();
    eprintln!("IMPLICATIONS:");
    eprintln!("  If range() is stubbed → delete_range() cannot find keys → won't delete");
    eprintln!("  If delete_range() works anyway → either:");
    eprintln!("    A) Implementation doesn't use range() for delete_range");
    eprintln!("    B) Implementation has been updated since documentation");
    eprintln!();
    eprintln!("RESOLUTION:");
    eprintln!("  Run tests above to verify actual delete_range() behavior");
    eprintln!("  Compare results with documented limitation");
}
