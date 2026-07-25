# Contributing to Midge

Thank you for your interest in contributing to Midge! This document provides guidelines and workflows for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing Requirements](#testing-requirements)
- [Submitting Changes](#submitting-changes)
- [Code Review Process](#code-review-process)
- [Style Guide](#style-guide)

## Code of Conduct

Be respectful, constructive, and professional. We're here to build reliable infrastructure software together.

## Getting Started

### Prerequisites

- Rust 1.70+ (latest stable recommended)
- cntryl-tools (install with `cargo install --git https://github.com/cntryl/tools --locked`)
- Python 3.8+ (only for `scripts/test_watchdog.py`)
- Git

### Fork and Clone

```bash
# Fork the repo on GitHub, then:
git clone https://github.com/YOUR_USERNAME/midge.git
cd midge
```

### Build and Test

```bash
# Build the project
cargo build --workspace

# Run all tests
cargo test

# Validate test structure (recommended)
cntryl-tools validate-tests

# Note: the current codebase contains some legacy violations; when changing or
# adding tests, prefer making them compliant and avoid increasing violations.

# Run benchmarks
cargo bench
```

### Explore the Codebase

Start with these docs:

- [architecture.md](docs/development/architecture.md) - Technical implementation guide
- [documentation hub](docs/README.md) - Current documentation inventory
- [testing.md](docs/development/testing.md) - Testing conventions and workflows
- [benchmarks.md](docs/development/benchmarks.md) - Benchmarking workflows and rules
- `.github/copilot-instructions.md` - Development conventions

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/my-feature
# or
git checkout -b fix/issue-123
```

**Branch naming:**

- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation changes
- `test/` - Test improvements

### 2. Make Changes

Follow [Code Standards](#code-standards) and [Testing Requirements](#testing-requirements).

### 3. Validate Before Committing

**Preflight checks (CI will run the Rust checks on PRs):**

```bash
# 1. All tests pass
cargo test

# 2. No clippy warnings (CI uses `-- -D warnings`)
cargo clippy --all-targets -- -D warnings

# 3. Code formatting
cargo fmt --check

# 4. Optional: test naming/AAA validator (not CI-gated yet)
cntryl-tools validate-tests
```

**Fix issues:**

```bash
# Auto-fix clippy warnings
cargo clippy --all-targets --fix

# Auto-format code
cargo fmt
```

### 4. Commit

**Commit message format:**

```
<type>: <short summary>

<optional detailed description>

<optional footer with issue references>
```

**Types:**

- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `docs:` - Documentation
- `perf:` - Performance improvement
- `chore:` - Build/tooling changes

**Examples:**

```
feat: add prefix trie support for scan acceleration

Implements a prefix trie index for SST blocks to accelerate
prefix-based range scans. Reduces block reads by ~40% for
prefix queries.

Closes #123
```

```
fix: prevent WAL corruption on partial write

Handle EINTR during WAL append to avoid partial record writes.
Added regression test for interrupted system calls.

Fixes #456
```

### 5. Push and Open PR

```bash
git push origin feature/my-feature
```

Open a Pull Request on GitHub with:

- Clear title describing the change
- Description of what and why (not just how)
- Link to related issues
- Any breaking changes noted

## Code Standards

### Layer Dependency Rules (Critical)

**Lower layers MUST NOT depend on higher layers.**

```
common/ → io/ → storage/ → wal/, sst/ → metadata/, iterators/
  → compaction/ → runtime/ → engine/
```

**How we enforce this:**

- Code review + keeping module boundaries clean
- CI runs `cargo clippy --all-targets -- -D warnings`, `cargo build`, and `cargo test`

See [architecture.md](docs/development/architecture.md) for details.

### Test Naming Convention (Required)

**All tests MUST follow this pattern:**

```rust
#[test]
fn should_{action}_when_{context}() {
    // Test implementation
}
```

**Examples:**

✅ Good:

```rust
#[test]
fn should_return_value_when_key_exists() { }

#[test]
fn should_return_none_when_key_not_found() { }

#[test]
fn should_flush_memtable_when_size_exceeds_threshold() { }
```

❌ Bad:

```rust
#[test]
fn test_get() { }  // Too vague

#[test]
fn key_exists() { }  // Missing 'should'

#[test]
fn test_should_get_value() { }  // 'test_' prefix unnecessary
```

**Rationale:**

- Enforces descriptive test names
- Acts as documentation
- Makes test failures self-explanatory
- cntryl-tools validate-tests can check this convention

### AAA Pattern (Arrange-Act-Assert)

For non-trivial tests (>5 lines), use explicit AAA comments:

```rust
#[test]
fn should_recover_writes_when_reopened_after_crash() {
    // Arrange
    let engine = Engine::open(test_opts())?;
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;

    // Act
    drop(engine);  // Simulate crash
    let engine = Engine::open(test_opts())?;
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let result = tx.get(b"key")?;

    // Assert
    assert_eq!(result, Some(b"value".as_ref().into()));
}
```

**Rules:**

- Exactly one `// Act` section per test
- Small tests (<5 lines) may omit AAA comments

### Clippy (Required)

**Zero clippy warnings allowed in CI.**

```bash
# Check locally
cargo clippy --all-targets

# Auto-fix where possible
cargo clippy --all-targets --fix
```

**Common issues:**

- Unused variables: Prefix with `_` or remove
- Unnecessary clones: Use references
- Complex expressions: Extract to named variables
- Missing trait bounds: Add where clauses

### Formatting

Use `rustfmt` with default settings:

```bash
cargo fmt
```

No custom style configurations. We use Rust community defaults.

## Testing Requirements

### Test Coverage

**All new code MUST have tests:**

- Public API: Integration tests in `tests/`
- Internal logic: Unit tests (inline or `mod tests`)
- Edge cases: Explicit tests for boundary conditions
- Error paths: Test error handling

### Test Organization

**Integration tests** (`tests/`):

- Test public API (Engine, Transaction, OpenOptions)
- End-to-end workflows
- Cross-module interactions

**Unit tests** (`src/*/mod.rs` or separate `tests.rs`):

- Internal module logic
- Helper functions
- Isolated components

### Benchmark Tests

**Hot path benchmarks** (`benches/tier1_*`):

- Must precompute all data outside `b.iter()`
- No allocations inside hot loop
- Use deterministic seeds (no RNG in hot loop)
- Use `black_box` on inputs/outputs
- Set `SamplingMode::Flat` for consistent results

**Example:**

```rust
// ✅ Good: Precomputed data
let keys: Vec<Vec<u8>> = (0..1000)
    .map(|i| format!("key:{}", i).into_bytes())
    .collect();

group.bench_function("get", |b| {
    b.iter(|| {
        for key in &keys {
            black_box(tx.get(black_box(key)));
        }
    });
});

// ❌ Bad: Allocation in hot loop
group.bench_function("get", |b| {
    b.iter(|| {
        for i in 0..1000 {
            let key = format!("key:{}", i).into_bytes();  // ❌ Allocates per iteration
            tx.get(&key);
        }
    });
});
```

### Test Validation

Recommended before submitting PR:

```bash
cntryl-tools validate-tests
```

This checks:

- Test naming convention (`should_{action}_when_{context}`)
- AAA marker presence for non-trivial tests (`// Arrange`, `// Act`, `// Assert`)
- Multiple `// Act` sections (multi-behavior heuristic)

Note: existing violations are present today; prefer not to increase them.

## Submitting Changes

### Pull Request Checklist

Before opening a PR, ensure:

- [ ] All tests pass: `cargo test`
- [ ] No clippy warnings: `cargo clippy --all-targets -- -D warnings`
- [ ] Code formatted: `cargo fmt`
- [ ] Test validator reviewed: `cntryl-tools validate-tests` (aim for compliance on new/changed tests)
- [ ] New tests added for new functionality
- [ ] Documentation updated (if API changed)
- [ ] Commit messages are clear and descriptive
- [ ] PR description explains what and why

### PR Description Template

```markdown
## Summary

Brief description of the change.

## Motivation

Why is this change needed? What problem does it solve?

## Changes

- Bullet list of specific changes
- Include any breaking changes
- Note any performance implications

## Testing

How was this tested? New tests added?

## Related Issues

Closes #123
Fixes #456
```

### CI Requirements

All PRs must pass CI checks:

- ✅ `cargo test` (all tests pass)
- ✅ `cargo clippy` (zero warnings)
- ✅ `cargo fmt --check` (code formatted)
- ✅ `cntryl-tools validate-tests` (test structure validation)
- ✅ No layer dependency violations

### Draft PRs

Use draft PRs for work-in-progress:

- Get early feedback
- Discuss approach before full implementation
- Mark as "Ready for review" when complete

## Code Review Process

### Review Timeline

- Initial response: Within 2-3 days
- Full review: Within 1 week
- Large PRs may take longer (consider splitting)

### What Reviewers Look For

1. **Correctness:** Does it work as intended?
2. **Tests:** Are edge cases covered?
3. **Design:** Does it fit the architecture?
4. **Clarity:** Is the code readable?
5. **Performance:** Any unnecessary overhead?
6. **Breaking changes:** Are they necessary and documented?

### Addressing Feedback

- Respond to all comments (even if just "Done")
- Push additional commits (don't force-push during review)
- Mark conversations as resolved when addressed
- Ask questions if feedback is unclear

### Approval and Merge

- At least one maintainer approval required
- All CI checks must pass
- Maintainers will merge (not contributors)
- Squash merge preferred for clean history

## Style Guide

### Code Organization

**Module structure:**

```rust
// Public API at top
pub struct MyType { }
pub fn public_function() { }

// Internal implementation
fn internal_helper() { }

// Tests at bottom
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_work_when_valid_input() { }
}
```

### Naming Conventions

**Types:** `PascalCase`

```rust
pub struct ColumnFamily { }
pub enum WriteOptions { }
```

**Functions/methods:** `snake_case`

```rust
pub fn begin_tx() { }
fn allocate_sequences() { }
```

**Constants:** `SCREAMING_SNAKE_CASE`

```rust
const MAX_MEMTABLE_SIZE: usize = 64 * MB;
const DEFAULT_BLOCK_SIZE: usize = 4096;
```

**Type parameters:** Single uppercase letter or `PascalCase`

```rust
fn process<T>(item: T) { }
fn store<Key, Value>(k: Key, v: Value) { }
```

### Error Handling

**Use `?` operator:**

```rust
// ✅ Good
pub fn open(opts: OpenOptions) -> MidgeResult<Engine> {
    let path = validate_path(&opts.path)?;
    let lease = acquire_lease(&path)?;
    Ok(Engine { lease })
}

// ❌ Bad
pub fn open(opts: OpenOptions) -> MidgeResult<Engine> {
    match validate_path(&opts.path) {
        Ok(path) => match acquire_lease(&path) {
            Ok(lease) => Ok(Engine { lease }),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}
```

**Provide context in errors:**

```rust
// ✅ Good
Err(MidgeError::Internal(format!(
    "failed to open WAL at {}: {}", path.display(), e
)))

// ❌ Bad
Err(MidgeError::Internal("WAL open failed".into()))
```

### Comments

**Document public API:**

```rust
/// Opens a database at the specified path.
///
/// # Errors
/// Returns `MidgeError::LockFailed` if another instance holds the lease.
/// Returns `MidgeError::Io` if the directory cannot be created.
pub fn open(opts: OpenOptions) -> MidgeResult<Engine> { }
```

**Explain non-obvious logic:**

```rust
// Allocate sequences idempotently to handle retries.
// Same request_id always gets same sequence number.
let seq = state.allocate_sequences_idempotent(request_id, count)?;
```

**Avoid obvious comments:**

```rust
// ❌ Bad
let x = 5;  // Set x to 5

// ✅ Good (no comment needed)
let x = 5;
```

### Imports

**Group imports:**

```rust
// Standard library
use std::collections::HashMap;
use std::path::PathBuf;

// External crates
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// Internal crates
use crate::common::{MidgeError, MidgeResult};
use crate::storage::Storage;
```

## Getting Help

- **Questions:** Open a GitHub Discussion
- **Bugs:** Open a GitHub Issue with reproduction steps
- **Security:** Email security concerns to maintainers (do not open public issue)

## Recognition

Contributors are recognized in:

- GitHub contributors list
- Release notes for significant contributions

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

---

**Thank you for contributing to Midge!** Your efforts help build reliable infrastructure for everyone.
