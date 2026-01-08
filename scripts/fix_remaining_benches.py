#!/usr/bin/env python3
"""
Systematically fix all remaining benchmark files.
Replace engine.put/get/delete with transaction equivalents.
"""
import re
from pathlib import Path

def fix_simple_benchmark(file_path):
    """Fix benchmark files with simple engine.put/get patterns."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    output = []
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Pattern: engine.put(cf, key, value)
        if 'engine.put(cf, ' in line and 'let mut tx = ' not in line:
            # Extract indentation
            indent = len(line) - len(line.lstrip())
            spaces = ' ' * indent
            
            # Parse the put call
            match = re.search(r'engine\.put\(cf,\s*([^,]+),\s*([^)]+)\)', line)
            if match:
                key_expr = match.group(1).strip()
                val_expr = match.group(2).strip()
                
                # Generate transaction version
                output.append(f'{spaces}let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");\n')
                output.append(f'{spaces}tx.put({key_expr}.to_vec(), {val_expr}.to_vec(), None).unwrap();\n')
                output.append(f'{spaces}engine.commit(tx, cntryl_midge::WriteOptions::default()).unwrap();\n')
                i += 1
                continue
        
        # Pattern: let _ = engine.get(cf, key)
        if 'engine.get(cf, ' in line and 'let tx = ' not in line:
            indent = len(line) - len(line.lstrip())
            spaces = ' ' * indent
            
            match = re.search(r'let\s+(\w+|\(_\))\s*=\s*engine\.get\(cf,\s*([^)]+)\)', line)
            if match:
                var_name = match.group(1).strip()
                key_expr = match.group(2).strip()
                
                output.append(f'{spaces}let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");\n')
                output.append(f'{spaces}let {var_name} = tx.get({key_expr})' + line[line.rindex(')'))+1:])
                i += 1
                continue
        
        # Pattern: assert!(engine.get(...))
        if 'assert!(engine.get(cf, ' in line:
            indent = len(line) - len(line.lstrip())
            spaces = ' ' * indent
            
            match = re.search(r'engine\.get\(cf,\s*([^)]+)\)', line)
            if match:
                key_expr = match.group(1).strip()
                rest_of_assert = line[line.index('engine.get')+ len('engine.get(cf, ' + key_expr + ')'):]
                
                output.append(f'{spaces}let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");\n')
                output.append(f'{spaces}assert!(tx.get({key_expr}){rest_of_assert}')
                i += 1
                continue
        
        output.append(line)
        i += 1
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(output)

def main():
    benches_to_fix = [
        'tier3_system_compaction.rs',
        'tier3_system_durability.rs',
        'tier3_system_engine.rs',
        'tier3_system_recovery.rs',
        'tier3_system_scan.rs',
        'tier3_system_sst.rs',
        'tier4_streaming_workload.rs',
        'tier4_system_durability_cloud.rs',
        'tier4_ycsb_workload_b.rs',
        'tier4_ycsb_workload_f.rs',
    ]
    
    benches_dir = Path('benches')
    for bench_name in benches_to_fix:
        bench_path = benches_dir / bench_name
        if bench_path.exists():
            print(f"Fixing {bench_name}...")
            fix_simple_benchmark(bench_path)
        else:
            print(f"Skipping {bench_name} (not found)")

if __name__ == '__main__':
    main()
