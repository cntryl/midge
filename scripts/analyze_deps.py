#!/usr/bin/env python3
"""
Internal Dependency Analyzer for Midge

Analyzes crate dependencies between top-level modules.
"""

import re
import os
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict, List, Tuple

# Top-level modules in Midge
TOP_MODULES = {
    'api', 'cloud', 'common', 'config', 'core', 'fs', 
    'health', 'metrics', 'sst', 'wal'
}

# Foundation modules (no dependencies on other internal modules)
FOUNDATION_MODULES = {'common', 'metrics', 'error'}

# Layer groupings
LAYERS = {
    'Layer 0 (Foundation)': {'common', 'metrics'},
    'Layer 1 (Config & Cloud)': {'config', 'cloud'},
    'Layer 2 (Storage)': {'wal', 'sst', 'health'},
    'Layer 3 (Core Engine)': {'core'},
    'Layer 4 (Public API)': {'api'},
}

def extract_use_statements(file_path: str) -> Set[str]:
    """Extract all 'use crate::' statements from a Rust file."""
    imports = set()
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Match: use crate::MODULE or use crate::MODULE::submodule
            matches = re.findall(r'use\s+crate::([a-z_]+)', content)
            imports.update(matches)
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
    
    return imports

def analyze_module(module_name: str, repo_root: str) -> Dict[str, Set[str]]:
    """Analyze all dependencies within a module."""
    module_path = os.path.join(repo_root, 'src', module_name)
    internal_deps = defaultdict(set)
    
    if not os.path.isdir(module_path):
        return internal_deps
    
    # Walk through all .rs files in the module
    for root, dirs, files in os.walk(module_path):
        for file in files:
            if file.endswith('.rs'):
                file_path = os.path.join(root, file)
                imports = extract_use_statements(file_path)
                
                # Keep only top-level module dependencies
                top_deps = {imp for imp in imports if imp in TOP_MODULES and imp != module_name}
                if top_deps:
                    rel_path = os.path.relpath(file_path, module_path)
                    internal_deps[rel_path] = top_deps
    
    return internal_deps

def get_module_dependencies(module_name: str, repo_root: str) -> Set[str]:
    """Get all top-level module dependencies for a module (union across all files)."""
    internal_deps = analyze_module(module_name, repo_root)
    
    # Flatten to get unique dependencies
    all_deps = set()
    for deps in internal_deps.values():
        all_deps.update(deps)
    
    return all_deps

def check_layer_violations(repo_root: str) -> List[Tuple[str, str, str]]:
    """Check for violations of layering rules."""
    violations = []
    
    # Define allowed dependencies per layer
    allowed = {
        'common': set(),
        'metrics': set(),
        'config': {'common', 'metrics'},
        'cloud': {'common', 'metrics'},
        'wal': {'common', 'metrics', 'config'},
        'sst': {'common', 'metrics', 'config'},
        'health': {'common', 'metrics', 'config'},
        'core': {'common', 'metrics', 'config', 'cloud', 'wal', 'sst', 'health'},
        'api': {'common', 'metrics'},
        'fs': {'common', 'metrics'},
    }
    
    for module in TOP_MODULES:
        deps = get_module_dependencies(module, repo_root)
        allowed_deps = allowed.get(module, set())
        
        for dep in deps:
            if dep not in allowed_deps:
                violations.append((module, dep, f"Not in allowed list: {allowed_deps}"))
    
    return violations

def print_dependency_matrix(repo_root: str):
    """Print a dependency matrix."""
    modules = sorted(TOP_MODULES)
    deps_map = {}
    
    for module in modules:
        deps_map[module] = get_module_dependencies(module, repo_root)
    
    # Header
    print("\n" + "="*80)
    print("DEPENDENCY MATRIX")
    print("="*80)
    print("\n[Module] depends on →\n")
    
    # Create matrix
    header = "        | " + " | ".join(f"{m:8}" for m in modules)
    print(header)
    print("-" * len(header))
    
    for module in modules:
        row = f"{module:7} | "
        for dep_module in modules:
            if module == dep_module:
                marker = "  -   "
            elif dep_module in deps_map[module]:
                marker = "  ✓   "
            else:
                marker = "      "
            row += marker + "| "
        print(row)

def print_layer_analysis(repo_root: str):
    """Print analysis by architectural layers."""
    print("\n" + "="*80)
    print("LAYERED ARCHITECTURE ANALYSIS")
    print("="*80)
    
    deps_map = {}
    for module in TOP_MODULES:
        deps_map[module] = get_module_dependencies(module, repo_root)
    
    for layer_name in [
        'Layer 0 (Foundation)',
        'Layer 1 (Config & Cloud)',
        'Layer 2 (Storage)',
        'Layer 3 (Core Engine)',
        'Layer 4 (Public API)',
    ]:
        modules = LAYERS.get(layer_name, set())
        if not modules:
            continue
            
        print(f"\n{layer_name}:")
        print("-" * 40)
        
        for module in sorted(modules):
            deps = deps_map.get(module, set())
            if deps:
                print(f"  {module}: → {', '.join(sorted(deps))}")
            else:
                print(f"  {module}: (no dependencies)")

def print_violations(repo_root: str):
    """Print layering violations."""
    violations = check_layer_violations(repo_root)
    
    print("\n" + "="*80)
    print("LAYERING VIOLATIONS")
    print("="*80)
    
    if not violations:
        print("\n✓ No architectural violations detected!")
    else:
        print(f"\n✗ Found {len(violations)} violation(s):\n")
        for violator, target, reason in sorted(violations):
            print(f"  {violator:10} → {target:10}  ({reason})")

def main():
    repo_root = Path(__file__).parent.parent.as_posix()
    if not os.path.isdir(os.path.join(repo_root, 'src')):
        print(f"Error: Could not find src directory at {repo_root}")
        return
    
    print(f"\nAnalyzing Midge internal dependencies...")
    print(f"Repository root: {repo_root}\n")
    
    # Print analyses
    print_dependency_matrix(repo_root)
    print_layer_analysis(repo_root)
    print_violations(repo_root)
    
    # Print summary stats
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    all_edge_count = sum(len(deps) for deps in [
        get_module_dependencies(m, repo_root) for m in TOP_MODULES
    ])
    
    print(f"\nTotal modules: {len(TOP_MODULES)}")
    print(f"Total dependency edges: {all_edge_count}")

if __name__ == '__main__':
    main()
