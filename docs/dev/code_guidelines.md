Excellent — this is already a world-class test guide.
Here’s the **matching “Idiomatic Code Guidelines”** document in the exact same structure, tone, and level of rigor:

---

# Idiomatic Code Guidelines

These guidelines define how we write **production Rust code** across the Midge repository to ensure clarity, safety, and performance. **All public modules follow these patterns** — this document reflects our actual validated practices in this repo: `tracing` for logs, `bytes::Bytes` for binary data, and `MidgeError`/`MidgeResult` for errors.

## Table of Contents

- [Code Organization Philosophy](#code-organization-philosophy)
- [Naming Convention](#naming-convention)
- [Error Handling](#error-handling)
- [Ownership and Borrowing](#ownership-and-borrowing)
- [Results, Options, and Panics](#results-options-and-panics)
- [Concurrency](#concurrency)
- [Performance and Memory](#performance-and-memory)
- [Imports and Visibility](#imports-and-visibility)
- [Logging and Instrumentation](#logging-and-instrumentation)
- [Documentation and Comments](#documentation-and-comments)
- [Formatting and Linting](#formatting-and-linting)
- [Unsafe Code Policy](#unsafe-code-policy)
- [Feature Flags & Public API](#feature-flags--public-api)
- [Code Reviews](#code-reviews)
- [Quick Reference Checklist](#quick-reference-checklist)
- [Appendix: Common Code Smells → Fixes](#appendix-common-code-smells--fixes)

---

## Code Organization Philosophy

**Principle:** Code is organized by **feature and behavior**, not by layer or type.

### Module Layout

Each file defines one major concept or feature:

```
src/
├── storage/
│   ├── memtable.rs
│   ├── fs.rs
│   └── file_manager.rs
├── wal/
│   ├── wal.rs
│   ├── wal_fs.rs
│   └── wal_mem.rs
├── index/
│   ├── bloom.rs
│   ├── range_tombstone.rs
│   └── merge_iterator.rs
└── utils/
    ├── codec.rs
    └── tlv.rs
```

### Principles

- **Feature-based organization** → Each file implements one logical concept.
- **Minimal public surface** → Export only what is required by other modules.
- **Separation of concerns** → Parsing, encoding, storage, and engine logic stay isolated.
- **Private-first** → All functions are private unless explicitly needed elsewhere.
- **Stable APIs** → Internal refactors must not break public API contracts.
- **Binary-first** → Keys/values are bytes (not strings). Respect internal key layout in storage paths.

---

## Naming Convention

### Function & Method Names

Use **clear verbs** describing intent and side-effects:

| Category    | Prefix                                | Example                                   |
| ----------- | ------------------------------------- | ----------------------------------------- |
| Constructor | `new`, `from_`, `with_`               | `new()`, `from_path()`, `with_capacity()` |
| Accessor    | `get`, `is`, `has`, `len`             | `get_entry()`, `is_empty()`               |
| Mutator     | `set`, `insert`, `update`, `remove`   | `insert_entry()`, `remove_key()`          |
| Behavior    | `apply`, `commit`, `flush`, `compact` | `apply_patch()`, `flush_memtable()`       |

### Structs, Enums, Traits

- **Structs**: `CamelCase` nouns → `FileManager`, `SstReader`, `BloomFilter`
- **Traits**: Describe capability → `Compressor`, `Serializer`, `Persistable`
- **Enums**: Singular noun → `WalOpKind`, `SstState`

### Constants & Statics

Uppercase with underscores:

```rust
pub const MAX_BLOCK_SIZE: usize = 4 * 1024;
```

### Module Names

Snake case, short and descriptive → `memtable`, `fs`, `range_tombstone`

---

## Error Handling

### Principle

Use `Result<T, E>` everywhere an operation can fail. Never panic in production code. In Midge, return `MidgeResult<T>` and map external errors into `MidgeError`.

### Pattern

```rust
fn load_manifest(path: &Path) -> MidgeResult<Manifest> {
    let bytes = std::fs::read(path).map_err(MidgeError::Io)?;
    Manifest::decode(&bytes)
}
```

### Guidelines

- Propagate errors with `?`, not `unwrap()`
- Convert external errors using `map_err()`
- Define errors in `crate::error` — all code returns `MidgeError`
- Prefer semantic errors (e.g., `MidgeError::Corruption`) over generic ones
- Include contextual info in errors (`file`, `seq`, etc.)

✅ **Good**

```rust
let file = File::open(path).map_err(|e| MidgeError::IoWithPath(path.into(), e))?;
```

❌ **Bad**

```rust
let file = File::open(path).unwrap(); // panics if missing
```

---

## Ownership and Borrowing

### Core Rules

1. **Prefer borrowed data** (`&[u8]`) over owned (`Vec<u8>`) unless you mutate or store it. Use `bytes::Bytes` at API boundaries for values.
2. **Clone intentionally** — if cloning, comment _why_:

   ```rust
   // Clone required because record is stored in multiple indexes
   let record = record.clone();
   ```

3. **Use `Arc` for shared ownership**. For mutable shared state, prefer minimizing lock scope and holding times.
4. **Return owned data from APIs** — consumers should decide borrowing.
5. **Binary data** — keys/values may be non-UTF8; avoid `String`/`&str` in storage/index paths. Internal keys encode `cf_id || user_key || inverted_seq || entry_type`.

---

## Results, Options, and Panics

### Rule

If a function can fail → `Result<T, E>`
If a value may be absent → `Option<T>`
Never use `unwrap()` or `expect()` outside tests.

### Patterns

✅ **Safe, idiomatic**

```rust
fn find(&self, key: &[u8]) -> Option<&Value> { ... }

fn try_load(&self, path: &Path) -> MidgeResult<Data> { ... }
```

❌ **Smell**

```rust
fs::read_to_string("config.toml").unwrap(); // Crashes if missing
```

### When Panic is Acceptable

- In **tests**
- In **clearly unrecoverable states** (e.g., logic invariant broken)
- When **asserting developer error**, not user input

---

## Concurrency

### Principles

Midge’s core engine is synchronous and uses background threads; async is reserved for network/cloud modules.

- Core: keep hot paths minimal; avoid unnecessary allocations and locking.
- Async modules: use Tokio; never block (`std::thread::sleep`, blocking fs) inside async.
- Use `tokio::time` and `tokio::fs` in async paths; keep lock hold times short in both worlds.

### Example (async only in cloud path)

```rust
use tokio::sync::Mutex;
use std::sync::Arc;

#[derive(Default)]
struct TokenCache { token: Option<String> }

async fn get_token(cache: Arc<Mutex<TokenCache>>) -> String {
  if let Some(t) = cache.lock().await.token.clone() {
    return t;
  }
  // fetch asynchronously from metadata service...
  let new = "bearer".to_string();
  cache.lock().await.token = Some(new.clone());
  new
}
```

---

## Performance and Memory

### Guidelines

- Use `Vec::with_capacity()` when size is predictable
- Use slices (`&[u8]`) over heap allocations
- Avoid small temporary allocations inside loops
- Use `Cow` for optional ownership
- Mark hot paths with `#[inline(always)]` only after profiling
- Use `cargo flamegraph` before and after optimizations

### Anti-Patterns

❌ Creating new `Vec<u8>` in tight loops
❌ Using `.clone()` when borrow suffices
❌ Using `String` for binary data

---

## Imports and Visibility

### Imports

- Group standard, external, and crate imports separately:

  ```rust
  use std::fs::File;
  use bytes::Bytes;
  use crate::error::MidgeResult;
  ```

### Visibility

- Keep everything **private by default**
- Only expose items required by external crates
- Re-export selectively from `lib.rs` for the public API

---

## Logging and Instrumentation

### Guidelines

- Use `tracing` with spans and structured fields
- Never log secrets or tokens; prefer short identifiers and sizes
- Choose log levels intentionally:

  | Level   | Purpose                             |
  | ------- | ----------------------------------- |
  | `debug` | Detailed internal behavior          |
  | `info`  | Normal lifecycle events             |
  | `warn`  | Recoverable anomalies               |
  | `error` | User-visible or data-loss scenarios |

### Example

```rust
use tracing::{info, warn, instrument};

#[instrument(skip(num_files))]
fn compaction_completed(level: u32, num_files: usize) {
  info!(level, files = num_files, "compaction completed");
}

fn high_memory(usage: usize) {
  warn!(bytes = usage, "high memory usage");
}
```

---

## Documentation and Comments

### Style

- Use Rustdoc (`///`) for all public functions, structs, and traits
- Write **purpose + constraints**, not just restated names

✅ **Good**

```rust
/// Appends a WAL record with optional TTL in milliseconds.
/// Returns the offset of the record within the active segment.
fn append_record(&mut self, record: &WalRecord) -> MidgeResult<WalPos> { ... }
```

❌ **Bad**

```rust
/// Adds record to WAL
fn append_record(...) { ... }
```

### Inline Comments

- Use `//` for reasoning, not narration
- Document _why_, not _what_:

  ```rust
  // Force WAL rotation after threshold to preserve durability guarantees
  ```

---

## Formatting and Linting

- Use `rustfmt` (edition 2024). Keep diffs minimal; avoid reformatting unrelated code.
 - Use `rustfmt` (edition 2021). Keep diffs minimal; avoid reformatting unrelated code.
- Fix clippy warnings in PRs; prefer denying `unwrap`/`expect` in production modules.
- Build + tests must pass locally and in CI.

---

## Unsafe Code Policy

- Unsafe is rare, localized, and documented with safety invariants.
- Add unit tests and, where applicable, fuzzing around unsafe boundaries (e.g., TLV parsing, codecs).

---

## Feature Flags & Public API

- Optional cloud providers are behind features: `cloud-aws`, `cloud-azure`, `cloud-gcp`, `cloud-all`.
- Keep `lib.rs` re-exports minimal and stable; public API changes require tests and docs.
- Do not leak internal key formats in public APIs; map to user-visible types.

---

## Code Reviews

### Reviewer Responsibilities

- Enforce safety and clarity before micro-optimization
- Flag all `.unwrap()` and `.expect()` in non-test code
- Verify error propagation correctness
- Ensure types and lifetimes are explicit and minimal
- Encourage smaller, focused functions
- Confirm structured logging for all I/O paths

---

## Quick Reference Checklist

### Style

- [ ] Feature-based module layout
- [ ] Minimal public API surface
- [ ] Clear naming and consistent verbs
- [ ] No unused imports or dead code

### Safety

- [ ] No `.unwrap()` or `.expect()` outside tests
- [ ] Error propagation via `?`
- [ ] All `Result` and `Option` handled explicitly
- [ ] No hidden panics in production paths

### Ownership

- [ ] Borrow when possible
- [ ] Clone only with justification
- [ ] Use `Arc` for shared immutability only

### Performance

- [ ] No unnecessary allocations in loops
- [ ] Pre-allocate known sizes
- [ ] Use profiling to justify optimizations

### Concurrency

- [ ] No blocking calls in async functions
- [ ] Use `tokio::fs`, `tokio::time`, `Mutex` for sync
- [ ] Avoid deadlocks and shared mutability

### Logging

- [ ] Use `tracing` with structured fields (no secrets)
- [ ] Correct log level for event severity

### Documentation

- [ ] Public items have meaningful Rustdoc
- [ ] Inline comments explain _why_, not _what_

---

## Document History

- **2025-10-20:** Updated to align with Midge codebase

  - Switched logging guidance to `tracing`; clarified sync core vs async cloud
  - Added formatting/linting, unsafe policy, feature flags & public API
  - Reinforced binary-first conventions (`Bytes`, `&[u8]`)

---

## Appendix: Common Code Smells → Fixes

| Smell                                 | Fix                                                  |
| ------------------------------------- | ---------------------------------------------------- |
| `unwrap()`/`expect()` in production   | Propagate with `?`; `map_err()` into `MidgeError`    |
| Using `String`/`&str` for binary data | Use `&[u8]`/`Bytes`                                  |
| Unnecessary `clone()`                 | Borrow or move; if clone needed, comment why         |
| Allocations in hot loops              | Reuse buffers; pre-allocate with `with_capacity()`   |
| Blocking in async                     | Use `tokio` equivalents (`tokio::fs`, `tokio::time`) |
| Long lock sections                    | Do heavy work outside critical sections              |
