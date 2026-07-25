#!/usr/bin/env python3
"""Validate the checked-in documentation inventory and local references."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "README.md", "CHANGELOG.md", "docs/README.md",
    "docs/user-guides/overview.md", "docs/user-guides/quick-start.md",
    "docs/user-guides/api-guide.md", "docs/user-guides/durability.md",
    "docs/user-guides/transaction-durability-contract.md",
    "docs/operations/operator-runbook.md", "docs/operations/cloud-setup.md",
}
FORBIDDEN_FILES = {
    "docs/development/one-dot-zero-contract.md",
    "docs/development/one-dot-zero-readiness-scorecard.md",
    "docs/operations/production-runbook.md",
    "docs/operations/resource-limits.md",
}
FORBIDDEN_TEXT = (
    "delete_range(&", "cache_stats", "MidgeError::IoError", "env_logger",
    "one-dot-zero", "production-runbook.md", "ACID", "Snappy",
    "rm -rf", "rm -r", "delete the database directory", "delete the lock file",
)

def main() -> int:
    errors: list[str] = []
    markdown = sorted(path for path in ROOT.rglob("*.md") if "target" not in path.parts)
    names = {path.relative_to(ROOT).as_posix() for path in markdown}
    errors.extend(f"missing required document: {path}" for path in sorted(REQUIRED - names))
    errors.extend(f"deleted document still present: {path}" for path in sorted(FORBIDDEN_FILES & names))

    heading_ids: dict[str, set[str]] = {}
    for path in markdown:
        rel = path.relative_to(ROOT).as_posix()
        ids: set[str] = set()
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
            if match:
                slug = re.sub(r"[^a-z0-9 -]", "", match.group(1).lower())
                slug = re.sub(r"\s+", "-", slug).strip("-")
                ids.add(slug)
        heading_ids[rel] = ids
        text = path.read_text(encoding="utf-8")
        for marker in FORBIDDEN_TEXT:
            if marker in text:
                errors.append(f"{rel}: forbidden documentation text {marker!r}")

        for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", text):
            parts = target.split("#", 1)[0].split()
            if not parts:
                continue
            target = parts[0]
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.is_file() and not (resolved.is_dir() and (resolved / "README.md").is_file()):
                errors.append(f"{rel}: broken local link {target}")

    for path in markdown:
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for target in re.findall(r"\[[^\]]*\]\(([^)]+)#([^)]+)\)", text):
            target_path, anchor = target
            if "://" in target_path:
                continue
            resolved = (path.parent / target_path).resolve()
            target_rel = resolved.relative_to(ROOT).as_posix()
            slug = re.sub(r"[^a-z0-9 -]", "", anchor.lower())
            slug = re.sub(r"\s+", "-", slug).strip("-")
            if slug not in heading_ids.get(target_rel, set()):
                errors.append(f"{rel}: broken anchor {target_path}#{anchor}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(markdown)} Markdown documents")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
