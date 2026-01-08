#!/usr/bin/env python3
"""
Comprehensive benchmark fixer - replaces all engine direct calls with transactions.
"""
import re
from pathlib import Path

PATTERNS = [
    # Pattern 1: e.put(cf, key, value)
    (
        r'(\s+)(\w+)\.put\(cf,\s*([^,]+),\s*([^)]+)\)',
        r'\1let mut tx = \2.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");\n\1tx.put(\3.to_vec(), \4.to_vec(), None).unwrap();\n\1\2.commit(tx, cntryl_midge::WriteOptions::default()).unwrap()'
    ),
    # Pattern 2: e.get(cf, key)
    (
        r'(\w+)\.get\(cf,\s*([^)]+)\)',
        r'{{ let tx = \1.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin"); tx.get(\2) }}'
    ),
    # Pattern 3: e.range(cf, start, end)
    (
        r'(\w+)\.range\(cf,\s*([^,]+),\s*([^)]+)\)',
        r'{{ let tx = \1.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin"); tx.scan(Some(\2), Some(\3)) }}'
    ),
]

def add_cf_id_declaration(content, function_name):
    """Add cf_id declaration after cf is defined."""
    pattern = rf'(fn {function_name}[^{{]*\{{[^}}]*let cf = [^;]+;)'
    match = re.search(pattern, content, re.DOTALL)
    if match and 'let cf_id = cf.id();' not in content[match.start():match.end()+200]:
        insert_pos = match.end()
        content = content[:insert_pos] + '\n    let cf_id = cf.id();' + content[insert_pos:]
    return content

def fix_file(filepath):
    """Fix a single benchmark file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Apply patterns
    for pattern, replacement in PATTERNS:
        content = re.sub(pattern, replacement, content)
    
    # Add cf_id declarations where needed
    if 'cf_id' in content and 'let cf_id = cf.id()' not in content:
        # Find all function definitions
        functions = re.findall(r'fn (\w+)', content)
        for func in functions:
            content = add_cf_id_declaration(content, func)
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

benches_dir = Path('benches')
for bench_file in sorted(benches_dir.glob('tier*.rs')):
    if fix_file(bench_file):
        print(f"Fixed: {bench_file.name}")
