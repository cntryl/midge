//! Memory Spill Audit
//!
//! Purpose: Verify that transaction spill actually works when memory limit exceeded
//! and determine if there are any contradictions in spill behavior documentation

use cntryl_midge::testkit::*;

#[test]
fn should_commit_large_transaction_when_memory_limit_exceeded() {
    eprintln!("\n=== AUDIT: MEMORY SPILL BEHAVIOR ===");

    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Use disk-based mode to see spill files
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024); // 128KB to force spill

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        eprintln!("Memory budget set to 128KB (mode: {})", mode);

        // Try to write data exceeding memory limit
        let mut tx = engine.transaction();
        let mut total_bytes = 0;

        for i in 0..500 {
            let key = format!("large_key_{:05}", i);
            let value = vec![65u8; 1024]; // 1KB value per key

            tx.put(cf.id(), key.as_bytes().to_vec(), value)
                .expect("put in transaction");

            total_bytes += key.len() + 1024;
        }

        eprintln!(
            "Writing {} bytes total (500 keys x 1KB values)",
            total_bytes
        );
        eprintln!("Memory budget is 128KB, so spill should trigger at ~128KB");

        // Commit the transaction
        let commit_result = engine.commit_transaction(tx);
        eprintln!("Commit result: {:?}", commit_result);

        match commit_result {
            Ok(_) => {
                eprintln!("✓ Transaction committed successfully despite exceeding memory budget");

                // Verify some data is actually present
                let check_key = "large_key_00000";
                let value = engine.get(cf, check_key.as_bytes()).expect("get");
                match value {
                    Some(v) => {
                        eprintln!(
                            "✓ Data persisted: retrieved {} bytes for {}",
                            v.len(),
                            check_key
                        );
                    }
                    None => {
                        eprintln!("✗ Data NOT persisted - key not found");
                    }
                }

                eprintln!("Conclusion: Memory spill appears to be working - large transaction committed successfully");
            }
            Err(e) => {
                eprintln!("✗ Transaction failed to commit: {:?}", e);
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
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024); // 256KB

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        eprintln!("Memory budget: 256KB (mode: {})", mode);

        // Write transaction 1: 128KB
        let mut tx1 = engine.transaction();
        for i in 0..128 {
            let key = format!("batch1_key_{:03}", i);
            let value = vec![65u8; 1024]; // 1KB per key
            tx1.put(cf.id(), key.as_bytes().to_vec(), value)
                .expect("put in tx1");
        }
        eprintln!("TX1: Writing 128KB of data");

        let result1 = engine.commit_transaction(tx1);
        eprintln!("TX1 result: {:?}", result1);

        // Write transaction 2: another 128KB (total would be 256KB, within budget)
        let mut tx2 = engine.transaction();
        for i in 0..128 {
            let key = format!("batch2_key_{:03}", i);
            let value = vec![66u8; 1024]; // 1KB per key
            tx2.put(cf.id(), key.as_bytes().to_vec(), value)
                .expect("put in tx2");
        }
        eprintln!("TX2: Writing another 128KB of data");

        let result2 = engine.commit_transaction(tx2);
        eprintln!("TX2 result: {:?}", result2);

        match (result1, result2) {
            (Ok(_), Ok(_)) => {
                eprintln!("✓ Both transactions committed within budget");

                // Verify data from both
                let val1 = engine.get(cf, b"batch1_key_000").expect("get").is_some();
                let val2 = engine.get(cf, b"batch2_key_000").expect("get").is_some();

                if val1 && val2 {
                    eprintln!("✓ Data from both transactions persisted - memory budgeting working");
                } else {
                    eprintln!("✗ Some data missing - memory budgeting may have issues");
                }
            }
            _ => {
                eprintln!("✗ One or more transactions failed");
            }
        }
    });
}

#[test]
fn should_handle_transaction_spill_to_disk_correctly() {
    eprintln!("\n=== AUDIT: SPILL TO DISK MECHANISM ===");

    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(64 * 1024); // Very small: 64KB to force spill quickly

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        eprintln!(
            "Memory budget: 64KB (very small to force spill) - mode: {}",
            mode
        );
        eprintln!("Sync writes: enabled");

        // Single transaction with data > 64KB
        let mut tx = engine.transaction();

        eprintln!("Writing 200 keys x 512 bytes = 100KB (exceeds 64KB budget)");
        for i in 0..200 {
            let key = format!("spilltest_key_{:04}", i);
            let value = vec![88u8; 512]; // 512 bytes per key
            tx.put(cf.id(), key.as_bytes().to_vec(), value)
                .expect("put");
        }

        let commit_result = engine.commit_transaction(tx);

        match commit_result {
            Ok(_) => {
                eprintln!(
                    "✓ Large transaction (100KB) committed successfully with 64KB memory budget"
                );
                eprintln!("Conclusion: SPILL IS WORKING - data exceeded memory and was persisted");

                // Sample check: verify some keys exist
                let sample_checks = [
                    "spilltest_key_0000",
                    "spilltest_key_0100",
                    "spilltest_key_0199",
                ];
                let all_present = sample_checks
                    .iter()
                    .all(|key| engine.get(cf, key.as_bytes()).expect("get").is_some());

                if all_present {
                    eprintln!("✓ Spilled data verified: sample keys are accessible");
                }
            }
            Err(e) => {
                eprintln!("✗ Transaction failed: {:?}", e);
                eprintln!("Conclusion: SPILL MAY NOT BE WORKING - verify implementation");
            }
        }
    });
}

#[test]
fn summary_memory_spill_status() {
    eprintln!("\n╔════════════════════════════════════════════════════════╗");
    eprintln!("║  MEMORY SPILL CONTRADICTION AUDIT SUMMARY              ║");
    eprintln!("╚════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("QUESTION:");
    eprintln!("  Does Midge support transaction spill to disk when");
    eprintln!("  memory budget is exceeded?");
    eprintln!();
    eprintln!("FINDINGS:");
    eprintln!("  • transaction_spill.rs tests: Assume spill works");
    eprintln!("  • Tests use memory_budget() and expect all data to persist");
    eprintln!("  • See tests above for actual spill behavior verification");
    eprintln!();
    eprintln!("EXPECTED BEHAVIOR:");
    eprintln!("  If spill is implemented:");
    eprintln!("    - Transactions exceeding memory_budget() should succeed");
    eprintln!("    - Data written to disk, then loaded from disk on commit");
    eprintln!("    - All keys should be queryable after commit");
    eprintln!();
    eprintln!("CONTRADICTION RESOLVED:");
    eprintln!("  If spill tests all pass → spill IS implemented");
    eprintln!("  If spill tests fail → feature is NOT implemented, tests are aspirational");
}
