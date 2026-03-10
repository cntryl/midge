#!/usr/bin/env python3
"""Generate a Test & Bench Inventory for the repository.

Usage:
  python scripts/generate_inventory.py [--root PATH] [--output inventory.md] [--replace]

By default writes `inventory.md` in the repo root.
"""

from pathlib import Path
import re
import argparse
import sys
from datetime import datetime

FN_SHOULD = re.compile(r"^\s*fn\s+(should_[A-Za-z0-9_]+)", re.MULTILINE)
FN_BENCH = re.compile(r"^\s*fn\s+(bench_[A-Za-z0-9_]+)", re.MULTILINE)
# Match a stress test attribute followed by the function declaration (e.g. "#[stress_test]\nfn name(...)")
FN_STRESS = re.compile(r"^\s*#\s*\[\s*stress_test.*\]\s*\n\s*fn\s+([A-Za-z0-9_]+)", re.MULTILINE)
TEST_ATTR = re.compile(r"^\s*#\s*\[.*test.*\]")


def gather_tests(files, root):
    mapping = {}
    for p in sorted(files):
        names = []
        text = p.read_text(encoding="utf8")
        for m in FN_SHOULD.finditer(text):
            # determine if the match is commented-out by inspecting the text between
            # the start of the line and the match start
            line_start = text.rfind('\n', 0, m.start()) + 1
            prefix = text[line_start:m.start()].strip()
            if prefix.startswith('//') or prefix.startswith('///') or prefix.startswith('/*'):
                continue
            names.append(m.group(1))
        if names:
            # dedupe and sort
            uniq = sorted(set(names))
            mapping[rel(p, root).replace('\\\\', '/')]=uniq
    return mapping


def gather_benches(files, root):
    mapping = {}
    for p in sorted(files):
        names = []
        text = p.read_text(encoding="utf8")
        for m in FN_BENCH.finditer(text):
            line_start = text.rfind('\n', 0, m.start()) + 1
            prefix = text[line_start:m.start()].strip()
            if prefix.startswith('//') or prefix.startswith('///') or prefix.startswith('/*'):
                continue
            names.append(m.group(1))
        # Also discover stress tests annotated with #[stress_test]
        for m in FN_STRESS.finditer(text):
            # ensure attribute isn't commented out (handled by regex anchoring, but keep parity)
            line_start = text.rfind('\n', 0, m.start()) + 1
            prefix = text[line_start:m.start()].strip()
            if prefix.startswith('//') or prefix.startswith('///') or prefix.startswith('/*'):
                continue
            names.append(m.group(1))
        if names:
            # dedupe and sort
            mapping[rel(p, root).replace('\\\\', '/')]=sorted(set(names))
    return mapping


def rel(p, root):
    # use POSIX-style path (forward slashes) even on Windows
    return p.relative_to(root).as_posix()


def gather_docs(root):
    """Gather all documentation files in docs/ directory."""
    docs_dir = root / "docs"
    doc_files = []
    if docs_dir.exists():
        for doc_file in sorted(docs_dir.glob("**/*.md")):
            doc_files.append(rel(doc_file, root).replace('\\', '/'))
    return doc_files


def render_md(src_tests, integration_tests, benches, docs, root):
    lines = []
    generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    lines.append("# Test & Bench Inventory\n")
    lines.append(f"_Generated {generated_at} by `scripts/generate_inventory.py`._\n")
    lines.append("Complete inventory of all test and benchmark functions, documentation, and benchmarks across midge.\n")
    
    lines.append("**Documentation (docs/)**\n")
    if docs:
        for doc_file in docs:
            lines.append(f"- `{doc_file}`")
        lines.append("")
    else:
        lines.append("(none)\n")
    
    lines.append("**Src Tests**\n")

    if src_tests:
        for f, names in sorted(src_tests.items()):
            lines.append(f"- `{f}`")
            lines.append("  - tests:")
            for n in names:
                lines.append(f"    - `{n}`")
            lines.append("")
    else:
        lines.append("(none)\n")

    lines.append("**Integration Tests (tests/)**\n")
    if integration_tests:
        for f, names in sorted(integration_tests.items()):
            lines.append(f"- `{f}`")
            lines.append("  - tests:")
            for n in names:
                lines.append(f"    - `{n}`")
            lines.append("")
    else:
        lines.append("(none)\n")

    lines.append("**Benches & Stress (benches/)**\n")
    if benches:
        for f, names in sorted(benches.items()):
            lines.append(f"- `{f}`")
            lines.append("  - benches & stress:")
            for n in names:
                lines.append(f"    - `{n}`")
            lines.append("")
    else:
        lines.append("(none)\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--output", default="inventory.md")
    parser.add_argument("--replace", action="store_true", help="Replace existing inventory.md")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    src_files = list(root.glob("src/**/*.rs"))
    test_files = list(root.glob("tests/**/*.rs"))
    bench_files = list(root.glob("benches/**/*.rs"))

    src_tests = gather_tests(src_files, root)
    integration_tests = gather_tests(test_files, root)
    benches = gather_benches(bench_files, root)
    docs = gather_docs(root)

    md = render_md(src_tests, integration_tests, benches, docs, root)

    out_path = root / args.output
    out_path.write_text(md, encoding="utf8")

    print(f"Wrote inventory to {out_path}")
    print(f"Found {sum(len(v) for v in src_tests.values())} src tests in {len(src_tests)} files")
    print(f"Found {sum(len(v) for v in integration_tests.values())} integration tests in {len(integration_tests)} files")
    print(f"Found {sum(len(v) for v in benches.values())} benches/stress in {len(benches)} files")
    print(f"Found {len(docs)} documentation files")

    if args.replace:
        target = root / "inventory.md"
        target.write_text(md, encoding="utf8")
        print(f"Replaced {target} with generated inventory")


if __name__ == "__main__":
    main()
