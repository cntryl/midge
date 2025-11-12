#!/usr/bin/env python3
"""
Midge Dependency Validator

Validates that the architecture adheres to the layering model.
Can be run during CI to catch dependency violations early.

Usage:
  python scripts/validate_deps.py [--verbose] [--fix-report]
"""

import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict, List, Tuple

# Top-level modules
MODULES = {'api', 'cloud', 'common', 'config', 'core', 'fs', 'health', 'metrics', 'sst', 'wal'}

# Allowed dependencies per module (layers)
ALLOWED_DEPS = {
    'api': set(),
    'common': set(),
    'metrics': set(),
    'fs': {'common', 'metrics'},
    'config': {'common', 'metrics', 'cloud'},
    'cloud': {'common', 'metrics'},
    'wal': {'api', 'common', 'metrics', 'config'},
    'sst': {'api', 'common', 'metrics', 'config', 'cloud'},
    'health': {'api', 'common', 'metrics', 'config'},
    'core': {'api', 'common', 'metrics', 'config', 'cloud', 'wal', 'sst', 'health'},
}

class DepValidator:
    def __init__(self, repo_root: str, verbose: bool = False):
        self.repo_root = repo_root
        self.verbose = verbose
        self.violations: List[Tuple[str, str, str, int]] = []
        self.file_deps: Dict[str, Set[str]] = {}
    
    def extract_module_deps(self, file_path: str) -> Set[str]:
        """Extract top-level module imports from a Rust file."""
        deps = set()
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # Match: use crate::MODULE or use crate::MODULE::*
                    matches = re.findall(r'use\s+crate::([a-z_]+)', line)
                    for match in matches:
                        if match in MODULES:
                            deps.add(match)
        except Exception as e:
            if self.verbose:
                print(f"  Warning: Could not read {file_path}: {e}")
        
        return deps
    
    def validate_module(self, module: str) -> bool:
        """Validate all dependencies in a module."""
        module_path = os.path.join(self.repo_root, 'src', module)
        is_clean = True
        
        if not os.path.isdir(module_path):
            return True
        
        allowed = ALLOWED_DEPS.get(module, set())
        
        for root, dirs, files in os.walk(module_path):
            for file in files:
                if not file.endswith('.rs'):
                    continue
                
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.repo_root)
                deps = self.extract_module_deps(file_path)
                
                # Remove self-references
                deps.discard(module)
                
                # Check for violations
                for dep in deps:
                    if dep not in allowed:
                        is_clean = False
                        line_no = self._find_dep_line(file_path, dep)
                        self.violations.append((module, dep, rel_path, line_no))
                        
                        if self.verbose:
                            print(f"  VIOLATION: {rel_path}:{line_no}")
                            print(f"    {module} → {dep} (not in {allowed})")
        
        return is_clean
    
    def _find_dep_line(self, file_path: str, dep: str) -> int:
        """Find the line number where a dependency is imported."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f, 1):
                    if f'use crate::{dep}' in line:
                        return i
        except:
            pass
        return 0
    
    def validate_all(self) -> bool:
        """Validate all modules."""
        print("Validating Midge architecture...\n")
        all_clean = True
        
        for module in sorted(MODULES):
            if self.verbose:
                print(f"Checking {module}...")
            
            if not self.validate_module(module):
                all_clean = False
        
        return all_clean
    
    def print_results(self):
        """Print validation results."""
        if not self.violations:
            print("✓ Architecture validation: PASSED")
            print(f"  All {len(MODULES)} modules respect layer boundaries\n")
            return True
        
        print(f"✗ Architecture validation: FAILED")
        print(f"  Found {len(self.violations)} violation(s):\n")
        
        for source, target, file_path, line_no in sorted(self.violations):
            print(f"  {file_path}:{line_no}")
            print(f"    Module '{source}' depends on '{target}'")
            allowed = ALLOWED_DEPS.get(source, set())
            print(f"    Allowed dependencies: {', '.join(sorted(allowed)) or 'none'}")
            print()
        
        return False
    
    def print_layer_info(self):
        """Print layer information."""
        print("\nAllowed dependencies per layer:")
        print("-" * 50)
        
        for module in sorted(MODULES):
            deps = ALLOWED_DEPS.get(module, set())
            if deps:
                print(f"  {module:10} → {', '.join(sorted(deps))}")
            else:
                print(f"  {module:10} → (no dependencies)")
        print()

def main():
    repo_root = Path(__file__).parent.parent.as_posix()
    verbose = '--verbose' in sys.argv
    
    validator = DepValidator(repo_root, verbose)
    
    if not validator.validate_all():
        validator.print_layer_info()
        validator.print_results()
        sys.exit(1)
    
    validator.print_results()
    sys.exit(0)

if __name__ == '__main__':
    main()
