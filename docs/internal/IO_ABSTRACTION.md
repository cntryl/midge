# I/O Abstraction Layer - Architecture Overview

## Purpose

The `src/io/` module provides a **domain-agnostic synchronous filesystem abstraction** that serves as the foundation for all filesystem interactions in Midge (SST, WAL, and other components).

## Key Principles

1. **Synchronous & Fast**: Blocking I/O required for random access patterns (SST seeks)
2. **Vectorized I/O First-Class**: `readv_at()`, `writev_at()`, `appendv()` for bulk efficiency
3. **Platform-Optimizable**: Implementations can use preadv/pwritev, direct I/O, etc.
4. **Completely Domain-Agnostic**: Zero knowledge of WAL, SST, storage concerns
5. **Fully Swappable**: RealFs, MockFs, ChaosFs for production, testing, chaos engineering

## Architecture Layers

```
┌─────────────────────────────────────────────────┐
│ Domain Layers: SST, WAL, Storage Orchestration │
│  (Use Arc<dyn Fs> trait objects)               │
└──────────────┬──────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────┐
│ src/io/ - Base Filesystem Abstraction           │
│                                                   │
│  ├─ Fs trait: filesystem operations             │
│  │  (open, remove_file, create_dir, etc.)      │
│  │                                              │
│  ├─ File trait: individual file I/O            │
│  │  └─ read_at(), write_at(), append()         │
│  │  └─ readv_at(), writev_at(), appendv()      │
│  │  └─ read_ranges() for multi-range reads     │
│  │                                              │
│  ├─ RealFs: Production impl via std::fs        │
│  ├─ MockFs: In-memory deterministic backend    │
│  └─ ChaosFs: Failure injection wrapper         │
└─────────────────────────────────────────────────┘
```

## Module Structure

### `src/io/traits.rs` (Core Abstractions - 320 lines)

Defines domain-agnostic contracts:

- **`FsError`**: 6 variants for error cases
  - `NotFound`, `AlreadyExists`, `Corruption`, `Io`, `Unavailable`, `Unsupported`
  
- **`Durability`**: Explicit sync control
  - `Unsafe`: No fsync (fast, may lose on crash)
  - `Durable`: fsync to device

- **`File` trait**: Individual file operations
  - **Core methods**: `read_at()`, `write_at()`, `append()`
  - **Vectorized methods**: `readv_at()`, `writev_at()`, `appendv()`, `read_ranges()`
  - **Control**: `sync()`, `close()`, `len()`, `caps()`
  - Default implementations for scalar operations fall back gracefully

- **`Fs` trait**: Filesystem operations
  - **File I/O**: `open()`, `remove_file()`, `exists()`, `metadata()`
  - **Directory I/O**: `create_dir_all()`, `list_dir()`, `remove_dir_all()`, `sync_dir()`
  - **Atomic ops**: `rename_atomic()`

- **`FileCaps`**: Capability flags for implementations
  - `READV_AT`, `WRITEV_AT`, `APPENDV`, `READ_RANGES`
  - Allows optimized implementations to advertise support

### `src/io/real.rs` (Production - 280 lines)

Real filesystem implementation via `std::fs`:

- **`RealFs`**: Wraps filesystem rooted at base_path
- Path sanitization: prevents `../` directory traversal
- Reads/writes delegate to `seek() + read_exact()/write_all()`
- Sync control: `Durability::Unsafe` (no-op), `Durability::Durable` (sync_all)

### `src/io/mock.rs` (Testing - 260 lines)

In-memory deterministic backend:

- **`MockFs`**: `Arc<Mutex<HashMap<String, Vec<u8>>>>`
- Zero actual I/O, fully deterministic
- Perfect for unit tests without filesystem dependencies
- Supports all operations: read, write, delete, list

### `src/io/chaos.rs` (Chaos Engineering - 230 lines)

Failure injection wrapper:

- **`ChaosFs`**: Wraps any `Arc<dyn Fs>`
- Per-operation counters: `open`, `read`, `write`, `delete`, `list`, `rename`
- Modulo-based injection: fail every Nth operation
- Passthrough when not failing, inject `FsError::Unavailable` when failing
- Enables **resilience testing** without code changes

## Integration: SST Layer

Created two new modules to gradually migrate SST to io::Fs:

### `src/sst/fs/reader_io.rs` (420 lines)

`SstFileIo` - io::Fs-backed SST reader:

- Identical functionality to `SstFile` but accepts `Arc<dyn Fs>`
- Allows using RealFs, MockFs, or ChaosFs
- Enables **transparent swapping** of filesystem backends
- Compatible with all SST features (bloom filters, caching, sparse indexing)

### `src/sst/fs/factory_io.rs` (50 lines)

`FsSstFactoryIo` - factory for io::Fs-backed readers:

- Creates SST readers with custom filesystem implementations
- Supports method chaining for ergonomic API
- Backward compatible: existing code continues using `FsSstFactory`

## Error Handling

Added `From<FsError> for MidgeError` conversion:

- Allows using `?` operator with FsResult in functions returning MidgeResult
- Maps FsError variants to appropriate MidgeError variants
- Transparent to callers

## Testing

- **13 io/ tests**: Path handling, capabilities, error cases
- **3 reader_io tests**: Type safety, method chaining
- **3 factory_io tests**: Factory creation, filesystem combinations
- **1578 total tests**: All existing + new io integration tests passing

## Usage Patterns

### Production (RealFs)

```rust
let fs = Arc::new(io::RealFs::new("/data")?);
let reader = sst::SstFileIo::open("data/table.sst", fs)?;
```

### Unit Testing (MockFs)

```rust
let fs = Arc::new(io::MockFs::new());
let reader = sst::SstFileIo::open("data/table.sst", fs)?;
// No actual I/O - fully in-memory and deterministic
```

### Chaos Testing (ChaosFs)

```rust
let base_fs = Arc::new(io::MockFs::new());
let chaos_fs = Arc::new(io::ChaosFs::new(base_fs, fail_every: 3));
let reader = sst::SstFileIo::open("data/table.sst", chaos_fs)?;
// Every 3rd operation will fail with FsError::Unavailable
```

## Backward Compatibility

- Original `SstFile` and `FsSstFactory` unchanged
- All existing code continues to work
- New `SstFileIo` and `FsSstFactoryIo` available for gradual migration
- No breaking changes to public APIs

## Next Steps

1. **Migrate SST completely**: Gradually move all call sites to `SstFileIo`
2. **Migrate WAL**: Create `reader_io` variant for WAL layer
3. **Clean up storage/**: Remove redundancy with io/ implementations
4. **Platform optimization**: Implement platform-specific preadv/pwritev in RealFs

## Benefits

- **Testability**: Swap filesystem implementation without code changes
- **Resilience**: Test failure scenarios with ChaosFs
- **Determinism**: MockFs enables repeatable test runs
- **Performance**: Vectorized I/O enables bulk optimizations
- **Clarity**: Single source of truth for filesystem contracts
- **Gradual migration**: Can update code incrementally without big-bang refactors
