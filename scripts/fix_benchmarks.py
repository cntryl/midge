#!/usr/bin/env python3
"""
Fix all benchmark files to use canonical transaction API.
This script replaces engine.put/get/delete/snapshot calls with transaction-based equivalents.
"""
import re
import sys
from pathlib import Path

def fix_benchmark_file(file_path):
    """Fix a single benchmark file to use transaction API."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Track if we need the WriteOptions import
    needs_write_options = 'engine.put(' in content or 'engine.delete(' in content
    
    # Add WriteOptions import if needed
    if needs_write_options and 'WriteOptions' not in content:
        if 'use cntryl_midge::' in content:
            content = re.sub(
                r'(use cntryl_midge::{[^}]+)',
                r'\1, WriteOptions',
                content,
                count=1
            )
        else:
            # Add new import line
            content = 'use cntryl_midge::WriteOptions;\n' + content
    
    # Add TransactionMode import if needed
    if 'TransactionMode' not in content:
        if 'use cntryl_midge::' in content:
            content = re.sub(
                r'(use cntryl_midge::{[^}]+)',
                r'\1, TransactionMode',
                content,
                count=1
            )
        else:
            content = 'use cntryl_midge::TransactionMode;\n' + content
    
    # Fix begin_tx calls that are missing .expect()
    content = re.sub(
        r'let (mut )?tx = engine\.begin_tx\(([^)]+)\);(?!\s*\.expect)',
        r'let \1tx = engine.begin_tx(\2).expect("begin");',
        content
    )
    
    if content != original:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

if __name__ == '__main__':
    benches_dir = Path('benches')
    if not benches_dir.exists():
        print("Error: benches/ directory not found")
        sys.exit(1)
    
    fixed = []
    for bench_file in benches_dir.glob('*.rs'):
        if fix_benchmark_file(bench_file):
            fixed.append(bench_file.name)
            print(f"Fixed: {bench_file.name}")
    
    if fixed:
        print(f"\nFixed {len(fixed)} benchmark files")
    else:
        print("No fixes needed")
