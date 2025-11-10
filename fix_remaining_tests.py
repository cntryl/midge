import re
from pathlib import Path

def fix_test_file(filepath):
    """Fix a single test file"""
    path = Path(filepath)
    content = path.read_text(encoding='utf-8')
    original = content
    
    # 1. Add KvTransaction import if needed
    if 'use cntryl_midge::KvStore' in content and 'KvTransaction' not in content:
        content = content.replace(
            'use cntryl_midge::KvStore;',
            'use cntryl_midge::{KvStore, KvTransaction};'
        )
    
    # 2. Remove Box::new() from commit_transaction calls
    content = re.sub(
        r'\.commit_transaction\(Box::new\(([^)]+)\)',
        r'.commit_transaction(\1',
        content
    )
    
    # 3. Fix 3-arg put() calls to 2-arg
    # Pattern: .put(Bytes::from_static(b"key"), Bytes::from_static(b"val"), None)
    content = re.sub(
        r'\.put\(\s*Bytes::from_static\((b"[^"]+"))\s*,\s*Bytes::from_static\((b"[^"]+"))\s*,\s*None\s*\)',
        r'.put(\1, \2)',
        content,
        flags=re.MULTILINE
    )
    
    # For multi-line put calls
    content = re.sub(
        r'\.put\(\s*Bytes::from_static\((b"[^"]+"))\s*,\s*Bytes::from_static\((b"[^"]+"))\s*,\s*None\s*,\s*\)',
        r'.put(\1, \2)',
        content,
        flags=re.MULTILINE | re.DOTALL
    )
    
    # Save if changed
    if content != original:
        path.write_text(content, encoding='utf-8')
        return True
    return False

# Fix all test files
test_files = [
    'tests/txn_write_write_conflicts.rs',
    'tests/txn_transaction_spill_to_disk.rs',
    'tests/txn_transaction_lifecycle.rs',
    'tests/txn_optimistic_locking.rs',
    'tests/txn_durability.rs',
    'tests/engine_transactions.rs',
]

for f in test_files:
    if fix_test_file(f):
        print(f"Fixed {f}")
    else:
        print(f"No changes for {f}")
