#!/usr/bin/env python3
import sys

file_path = sys.argv[1]

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

output_lines = []
for line in lines:
    if 'engine.begin_tx(' in line and '.expect(' not in line and line.strip().endswith(');'):
        # Add .expect("begin") before the ;
        line = line.replace(');', ').expect("begin");')
    output_lines.append(line)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(output_lines)

print(f"Fixed {file_path}")
