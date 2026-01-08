#!/usr/bin/env python3
"""Fix all test files to use canonical transaction API"""

import re
import sys
from pathlib import Path

def fix_test_file(filepath):
    """Apply all necessary fixes to a test file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Add WriteOptions import if not present
    if 'use cntryl_midge::WriteOptions' not in content and 'WriteOptions::' in content:
        content = re.sub(
            r'(use cntryl_midge::testkit::\*;)',
            r'\1\nuse cntryl_midge::WriteOptions;',
            content
        )
    
    # Fix engine.transaction() -> engine.begin_tx(cf.id(), TransactionMode::ReadWrite)
    content = re.sub(
        r'engine\.transaction\(\)',
        r'engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap()',
        content
    )
    
    # Fix engine.commit_transaction(txn) -> engine.commit(txn, WriteOptions::default())
    content = re.sub(
        r'engine\.commit_transaction\(([^)]+)\)',
        r'engine.commit(\1, WriteOptions::default())',
        content
    )
    
    # Fix txn.put(cf.id(), key, value) -> txn.put(key, value, None)
    # Handle multiline cases
    content = re.sub(
        r'(txn\d?)\.put\(cf\.id\(\),\s*([^,]+),\s*([^)]+)\)',
        r'\1.put(\2, \3, None)',
        content
    )
    
    # Fix txn.delete(cf.id(), key) -> txn.delete(key)
    content = re.sub(
        r'(txn\d?)\.delete\(cf\.id\(\),\s*([^)]+)\)',
        r'\1.delete(\2)',
        content
    )
    
    # Fix txn.delete_range(cf.id(), start, end) -> txn.delete_range(start, end)
    content = re.sub(
        r'(txn\d?)\.delete_range\(cf\.id\(\),\s*([^,]+),\s*([^)]+)\)',
        r'\1.delete_range(\2, \3)',
        content
    )
    
    # Fix engine.begin_transaction(&cf) -> engine.begin_tx(cf.id(), TransactionMode::ReadWrite)
    content = re.sub(
        r'engine\.begin_transaction\(&cf\)',
        r'engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)',
        content
    )
    
    # Fix engine.commit_transaction_boxed(txn, opts) -> engine.commit(txn, opts)
    content = re.sub(
        r'engine\.commit_transaction_boxed\(([^,]+),\s*([^)]+)\)',
        r'engine.commit(\1, \2)',
        content
    )
    
    # Fix engine.get_transactional(cf, key, &txn) -> txn.get(key)
    content = re.sub(
        r'engine\.get_transactional\(cf,\s*([^,]+),\s*&(txn\d?)\)',
        r'\2.get(\1)',
        content
    )
    
    # Fix engine.rollback_transaction(txn) -> engine.rollback_transaction(txn) (keep as is, it exists)
    
    # Fix WriteOptions::new() patterns
    content = re.sub(
        r'WriteOptions::new\(\)\.disable_wal\(\)',
        r'WriteOptions::no_wal()',
        content
    )
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    test_dir = Path('tests')
    files_to_fix = [
        'transaction_conflicts.rs',
        'transaction_isolation.rs',
        'engine_snapshots.rs',
        'engine_write_batch.rs',
        'engine_ttl.rs',
        'engine_merge.rs',
        'merge_advanced.rs',
        'engine_wal.rs',
        'ingest_invariants.rs',
        'memory_spill_audit.rs',
        'hot_sst_tracking.rs',
        'read_amp_api.rs',
        'sst_reads_integration.rs',
    ]
    
    fixed_count = 0
    for filename in files_to_fix:
        filepath = test_dir / filename
        if filepath.exists():
            if fix_test_file(filepath):
                print(f"✓ Fixed {filename}")
                fixed_count += 1
            else:
                print(f"○ No changes needed for {filename}")
        else:
            print(f"✗ File not found: {filename}")
    
    print(f"\nFixed {fixed_count} files")

if __name__ == '__main__':
    main()
