#!/usr/bin/env python3
import re
import sys

file_path = sys.argv[1]

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix begin_tx calls to add .expect("begin")  
# Pattern: let [mut] tx = engine.begin_tx(...);
# Replace with: let [mut] tx = engine.begin_tx(...).expect("begin");
content = re.sub(
    r'let (mut )?tx = engine\.begin_tx\(([^)]+)\);',
    r'let \1tx = engine.begin_tx(\2).expect("begin");',
    content
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Fixed {file_path}")
