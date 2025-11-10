#!/usr/bin/env python3
"""
Fix test files to work with EngineTransaction API.
1. Add KvTransaction import where needed
2. Remove Box::new() wrapping from commit_transaction calls
3. Update put() calls from 3-arg to 2-arg signature
"""

import re
import glob
from pathlib import Path

def fix_imports(content):
    """Add KvTransaction to imports if KvStore is imported but KvTransaction isn't"""
    # If has KvStore but not KvTransaction
    if 'use cntryl_midge::KvStore' in content and 'KvTransaction' not in content:
        content = content.replace(
            'use cntryl_midge::KvStore;',
            'use cntryl_midge::{KvStore, KvTransaction};'
        )
    return content

def fix_commit_calls(content):
    """Remove Box::new() from commit_transaction calls"""
    # Pattern: engine.commit_transaction(Box::new(txn), ...)
    content = re.sub(
        r'\.commit_transaction\(Box::new\(([^)]+)\)',
        r'.commit_transaction(\1',
        content
    )
    return content

def fix_put_calls(content):
    """Fix put() calls from 3-arg to 2-arg"""
    # Pattern: txn.put(Bytes::from_static(b"key"), Bytes::from_static(b"val"), None)
    # Replace with: txn.put(b"key", b"val")
    content = re.sub(
        r'\.put\(Bytes::from_static\((b"[^"]+"))\s*,\s*Bytes::from_static\((b"[^"]+"))\s*,\s*None\)',
        r'.put(\1, \2)',
        content
    )
    # Also handle non-None TTL cases - just remove the None parameter
    content = re.sub(
        r'\.put\((b"[^"]+"),\s*(b"[^"]+"),\s*None\)',
        r'.put(\1, \2)',
        content
    )
    return content

def main():
    test_files = glob.glob('tests/*.rs')
    
    for filepath in test_files:
        print(f"Processing {filepath}...")
        path = Path(filepath)
        content = path.read_text(encoding='utf-8')
        
        original = content
        content = fix_imports(content)
        content = fix_commit_calls(content)
        content = fix_put_calls(content)
        
        if content != original:
            path.write_text(content, encoding='utf-8')
            print(f"  ✓ Updated {filepath}")
        else:
            print(f"  - No changes needed for {filepath}")

if __name__ == '__main__':
    main()
