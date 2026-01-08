#!/usr/bin/env python3
"""
Migrate test files to use the new transaction-scoped API.

Patterns to fix:
1. engine.transaction() -> engine.begin_tx(cf.id(), TransactionMode::ReadWrite)
2. engine.put(cf, key, val) -> tx.put(key, val, None); engine.commit(tx, WriteOptions::default())
3. engine.get(cf, key) -> tx.get(key) with ReadOnly transaction
4. engine.delete(cf, key) -> tx.delete(key); engine.commit(tx, WriteOptions::default())
5. engine.delete_range(cf, start, end) -> tx.delete_range(start, end); engine.commit(tx, WriteOptions::default())
6. engine.commit_transaction(tx) -> engine.commit(tx, WriteOptions::default())
7. engine.tx_get(tx, key) -> tx.get(key)
8. engine.snapshot() -> engine.begin_tx(cf.id(), TransactionMode::ReadOnly)
"""

import re
import sys
from pathlib import Path

def migrate_file(filepath):
    """Migrate a single test file to use transaction API."""
    content = filepath.read_text(encoding='utf-8')
    original = content
    
    # Pattern 1: engine.transaction() -> engine.begin_tx(cf.id(), TransactionMode::ReadWrite)
    content = re.sub(
        r'(\w+)\.transaction\(\)',
        r'\1.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap()',
        content
    )
    
    # Pattern 2: engine.commit_transaction(tx) -> engine.commit(tx, WriteOptions::default())
    content = re.sub(
        r'(\w+)\.commit_transaction\((\w+)\)',
        r'\1.commit(\2, cntryl_midge::WriteOptions::default())',
        content
    )
    
    # Pattern 3: engine.tx_get(tx, key) -> tx.get(key)
    content = re.sub(
        r'engine\.tx_get\((\w+),\s*([^)]+)\)',
        r'\1.get(\2)',
        content
    )
    
    # Check if file was modified
    if content != original:
        filepath.write_text(content, encoding='utf-8')
        return True
    return False

def main():
    repo_root = Path(__file__).parent.parent
    tests_dir = repo_root / "tests"
    examples_dir = repo_root / "examples"
    
    modified = []
    
    # Migrate test files
    for test_file in tests_dir.glob("*.rs"):
        if migrate_file(test_file):
            modified.append(test_file)
            print(f"✓ Migrated {test_file.name}")
    
    # Migrate example files  
    for example_file in examples_dir.glob("*.rs"):
        if migrate_file(example_file):
            modified.append(example_file)
            print(f"✓ Migrated {example_file.name}")
    
    print(f"\nMigrated {len(modified)} files")
    
    if modified:
        print("\nNote: This script handles common patterns.")
        print("Some manual fixes may still be needed for:")
        print("  - engine.put/get/delete calls (need to be wrapped in transactions)")
        print("  - engine.snapshot() calls (use begin_tx with ReadOnly)")
        print("  - Complex transaction workflows")

if __name__ == "__main__":
    main()
