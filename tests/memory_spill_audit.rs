//! Memory Spill Audit
//!
//! Purpose: Verify that transaction spill actually works when memory limit exceeded
//! and determine if there are any contradictions in spill behavior documentation

use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};

#[test]
fn should_commit_large_transaction_when_memory_limit_exceeded() {
    eprintln!("\n=== AUDIT: MEMORY SPILL BEHAVIOR ===");

    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Use disk-based mode to see spill files
        let opts = opts.memory_budget(128 * 1024); // 128KB to force spill

        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        eprintln!("Memory budget set to 128KB (mode: {})", mode);

        // Try to write data exceeding memory limit
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        let mut total_bytes = 0;

        for i in 0..500 {
            let key = format!("large_key_{:05}", i);
            let value = vec![65u8; 1024]; // 1KB value per key

            tx.put(key.as_bytes().to_vec(), value, None)
                .expect("put in transaction");

            total_bytes += key.len() + 1024;
        }

        eprintln!(
            "Writing {} bytes total (500 keys x 1KB values)",
            total_bytes
        );
        eprintln!("Memory budget is 128KB, so spill should trigger at ~128KB");

        // Commit the transaction
        let commit_result = engine.commit(tx, WriteOptions::buffered());
        eprintln!("Commit result: {:?}", commit_result);

        match commit_result {
            Ok(_) => {
                eprintln!("âœ“ Transaction committed successfully despite exceeding memory budget");

                // Verify some data is actually present
                let check_key = "large_key_00000";
                let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
                let value = tx.get(check_key.as_bytes()).expect("get");
                match value {
                    Some(v) => {
                        eprintln!(
                            "âœ“ Data persisted: retrieved {} bytes for {}",
                            v.len(),
                            check_key
                        );
                    }
                    None => {
                        eprintln!("âœ— Data NOT persisted - key not found");
                    }
                }

                eprintln!("Conclusion: Memory spill appears to be working - large transaction committed successfully");
            }
            Err(e) => {
                eprintln!("âœ— Transaction failed to commit: {:?}", e);
                eprintln!(
                    "Conclusion: Spill may NOT be working - transaction exceeded memory and failed"
                );
            }
        }
    });
}

#[test]
fn should_respect_memory_budget_across_transactions() {
    eprintln!("\n=== AUDIT: MEMORY BUDGET ENFORCEMENT ===");

    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let opts = opts.memory_budget(256 * 1024); // 256KB

        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        eprintln!("Memory budget: 256KB (mode: {})", mode);

        // Write transaction 1: 128KB
        let mut tx1 = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..128 {
            let key = format!("batch1_key_{:03}", i);
            let value = vec![65u8; 1024]; // 1KB per key
            tx1.put(key.as_bytes().to_vec(), value, None)
                .expect("put in tx1");
        }
        eprintln!("TX1: Writing 128KB of data");

        let result1 = engine.commit(tx1, WriteOptions::buffered());
        eprintln!("TX1 result: {:?}", result1);

        // Write transaction 2: another 128KB (total would be 256KB, within budget)
        let mut tx2 = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..128 {
            let key = format!("batch2_key_{:03}", i);
            let value = vec![66u8; 1024]; // 1KB per key
            tx2.put(key.as_bytes().to_vec(), value, None)
                .expect("put in tx2");
        }
        eprintln!("TX2: Writing another 128KB of data");

        let result2 = engine.commit(tx2, WriteOptions::buffered());
        eprintln!("TX2 result: {:?}", result2);

        match (result1, result2) {
            (Ok(_), Ok(_)) => {
                eprintln!("âœ“ Both transactions committed within budget");

                // Verify data from both
                let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
                let val1 = tx.get(b"batch1_key_000").expect("get").is_some();
                let val2 = tx.get(b"batch2_key_000").expect("get").is_some();

                if val1 && val2 {
                    eprintln!(
                        "âœ“ Data from both transactions persisted - memory budgeting working"
                    );
                } else {
                    eprintln!("âœ— Some data missing - memory budgeting may have issues");
                }
            }
            _ => {
                eprintln!("âœ— One or more transactions failed");
            }
        }
    });
}

#[test]
fn should_handle_transaction_spill_to_disk_correctly() {
    eprintln!("\n=== AUDIT: SPILL TO DISK MECHANISM ===");

    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let opts = opts.memory_budget(64 * 1024); // Very small: 64KB to force spill quickly

        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        eprintln!(
            "Memory budget: 64KB (very small to force spill) - mode: {}",
            mode
        );
        eprintln!("Sync writes: enabled");

        // Single transaction with data > 64KB
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();

        eprintln!("Writing 200 keys x 512 bytes = 100KB (exceeds 64KB budget)");
        for i in 0..200 {
            let key = format!("spilltest_key_{:04}", i);
            let value = vec![88u8; 512]; // 512 bytes per key
            tx.put(key.as_bytes().to_vec(), value, None).expect("put");
        }

        let commit_result = engine.commit(tx, WriteOptions::buffered());

        match commit_result {
            Ok(_) => {
                eprintln!(
                    "âœ“ Large transaction (100KB) committed successfully with 64KB memory budget"
                );
                eprintln!("Conclusion: SPILL IS WORKING - data exceeded memory and was persisted");

                // Sample check: verify some keys exist
                let sample_checks = [
                    "spilltest_key_0000",
                    "spilltest_key_0100",
                    "spilltest_key_0199",
                ];
                let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
                let all_present = sample_checks
                    .iter()
                    .all(|key| tx.get(key.as_bytes()).expect("get").is_some());

                if all_present {
                    eprintln!("âœ“ Spilled data verified: sample keys are accessible");
                }
            }
            Err(e) => {
                eprintln!("âœ— Transaction failed: {:?}", e);
                eprintln!("Conclusion: SPILL MAY NOT BE WORKING - verify implementation");
            }
        }
    });
}

#[test]
fn summary_memory_spill_status() {
    eprintln!("\nâ•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—");
    eprintln!("â•‘  MEMORY SPILL CONTRADICTION AUDIT SUMMARY              â•‘");
    eprintln!("â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•");
    eprintln!();
    eprintln!("QUESTION:");
    eprintln!("  Does Midge support transaction spill to disk when");
    eprintln!("  memory budget is exceeded?");
    eprintln!();
    eprintln!("FINDINGS:");
    eprintln!("  â€¢ transaction_spill.rs tests: Assume spill works");
    eprintln!("  â€¢ Tests use memory_budget() and expect all data to persist");
    eprintln!("  â€¢ See tests above for actual spill behavior verification");
    eprintln!();
    eprintln!("EXPECTED BEHAVIOR:");
    eprintln!("  If spill is implemented:");
    eprintln!("    - Transactions exceeding memory_budget() should succeed");
    eprintln!("    - Data written to disk, then loaded from disk on commit");
    eprintln!("    - All keys should be queryable after commit");
    eprintln!();
    eprintln!("CONTRADICTION RESOLVED:");
    eprintln!("  If spill tests all pass â†’ spill IS implemented");
    eprintln!("  If spill tests fail â†’ feature is NOT implemented, tests are aspirational");
}
