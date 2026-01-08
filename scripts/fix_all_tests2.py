#!/usr/bin/env python3
"""Fix all test files to use canonical transaction API - comprehensive version"""

import re
import sys
from pathlib import Path

def fix_test_file(filepath):
    """Apply all necessary fixes to a test file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Add WriteOptions import if not present
    if 'use cntryl_midge::WriteOptions' not in content and ('WriteOptions::' in content or 'engine.commit(' in content):
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
    # Handle various patterns including multiline
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
    
    # Fix WriteOptions::new() patterns
    content = re.sub(
        r'WriteOptions::new\(\)\.disable_wal\(\)',
        r'WriteOptions::no_wal()',
        content
    )
    
    # Fix engine.put(cf, key, value).ok() patterns - convert to transaction
    content = re.sub(
        r'engine\.put\(cf,\s*([^,]+),\s*([^)]+)\)\.ok\(\)',
        r'{let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap(); tx.put(\1, \2, None).unwrap(); engine.commit(tx, WriteOptions::default()).ok()}',
        content
    )
    
    # Fix engine.put(cf, key, value).unwrap() patterns
    content = re.sub(
        r'engine\.put\(cf,\s*([^,]+),\s*([^)]+)\)\.unwrap\(\)',
        r'{let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap(); tx.put(\1, \2, None).unwrap(); engine.commit(tx, WriteOptions::default()).unwrap()}',
        content
    )
    
    # Fix engine.scan(cf, query) patterns - convert to transaction-based scan
    content = re.sub(
        r'engine\.scan\(cf,\s*([^)]+)\)',
        r'{let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap(); tx.scan_range(\1).unwrap()}',
        content
    )
    
    # Fix engine.range(cf, start, end) patterns
    content = re.sub(
        r'engine\.range\(cf,\s*([^,]+),\s*([^)]+)\)',
        r'{let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap(); tx.scan(\1, \2).unwrap()}',
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
        'engine_wal.rs',
        'engine_ttl.rs',
        'engine_merge.rs',
        'merge_advanced.rs',
        'ingest_invariants.rs',
        'hot_sst_tracking.rs',
        'read_amp_api.rs',
        'sst_reads_integration.rs',
        'engine_snapshots.rs',
        'engine_write_batch.rs',
        'transaction_conflicts.rs',
        'transaction_isolation.rs',
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
