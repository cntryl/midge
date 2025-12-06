# SST Index Format Specification

This document specifies the **current** SST index format and invariants in Midge. This is the locked baseline contract that all future enhancements must preserve.

## File Layout

An SST file has the following structure:

```
[Optional Header]
[Data Blocks...]
[Filter Block (Bloom)]
[Meta-Index Block]
[Index Block]
[Footer]
```

### Block Trailer

Each block ends with a trailer:
```
[compressed_body || restart_array || restart_count]
[compression_byte: 1 byte]
[crc32c_le: 4 bytes]
```

- `compression_byte`: 0 = none, 1 = snappy, etc.
- `crc32c_le`: 32-bit little-endian CRC checksum of compressed body + restart structure + compression byte

### Footer

Fixed 48-byte structure at end of file:

```
[metaindex_handle: varint64 offset + varint64 size]
[index_handle: varint64 offset + varint64 size]
[reserved: 40 bytes]
[magic_number: u64 = 0xdb4775248b80fb57 (RocksDB format magic)]
```

## Block Types

Blocks have logical types (not encoded in trailer, inferred from context):

| Type | Value | Purpose |
|------|-------|---------|
| Data | 0 | Key-value data block |
| Filter | 1 | Bloom filter block (per-SST) |
| Index | 2 | Sparse index block |
| MetaIndex | 3 | Meta-index block (points to bloom, index, etc.) |

## Index Format

### Sparse Index

The **sparse index** stores the **last key** of each data block, mapped to its `BlockHandle`.

**Entries:** For N data blocks, the index has N entries.

**Encoding:** TLV (Type-Length-Value) block format:
- Each entry: `[key_delta_varint][key_bytes][value_len_varint][value_bytes][restart_marker]`
- Value: `BlockHandle` encoded as `[offset_varint][size_varint]`

**Key Properties:**
1. **Sorted by key**: Index entries are ordered by their key (last key of each block)
2. **Last key per block**: `entries[i].key` = last key in `data_blocks[i]`
3. **Non-overlapping blocks**: If `entries[i].key < entries[i+1].key`, blocks are strictly non-overlapping

### Block Handle

`BlockHandle` encodes the physical location of a block:

```
[offset: varint64]  // Byte offset in SST file
[size: varint64]    // Byte size of block (including trailer)
```

## Bloom Filter Format

**Per-SST Bloom Filter** stored as a dedicated block:

- **Built from**: All keys in the SST (before any filtering or deletion)
- **Format**: Blocked bloom filter (optimized for cache locality)
- **Lookup**: Query before consulting index; if negative, skip entire SST
- **False positives**: Capped by bloom design; no false negatives

## Data Block Format

Each data block stores key-value pairs in **restart-based format** (similar to LevelDB):

```
[Entry 0: key_delta + value]
[Entry 1: key_delta + value]
...
[Restart Point 0: 4-byte offset to entry N]
[Restart Point 1: 4-byte offset to entry M]
...
[num_restarts: 4-byte little-endian count]
[compression_byte: 1 byte]
[crc32c: 4 bytes]
```

**Invariants:**
- Keys within a block are **strictly increasing** (lexicographic order)
- First key (entry 0) is **not delta-encoded** (full key)
- Subsequent keys are delta-encoded from the last restart point

## Invariants

### Format Invariants

1. **Magic Number**: Footer always ends with magic `0xdb4775248b80fb57`
2. **Footer Size**: Exactly 48 bytes, fixed location at EOF - 48
3. **Checksum**: Each block has valid CRC32C trailer
4. **Offset Ordering**: `metaindex_offset < index_offset < footer_offset`

### Index Invariants

1. **Sorted Index**: Sparse index entries sorted by key (last key of each block)
2. **Non-overlapping**: For all i < j: `index[i].key < index[j].key`
3. **Block Coverage**: Exactly one index entry per data block
4. **Handle Validity**: All `BlockHandle` offsets and sizes are within file bounds

### Data Block Invariants

1. **Key Ordering**: Keys within a block strictly increasing
2. **Fence Pointers**: Each block implicitly tracks:
   - `min_key` = first key in block
   - `max_key` = last key in block (= sparse index key for that block)
3. **No Overlaps**: For data blocks i and i+1: `block[i].max_key < block[i+1].min_key`

### Bloom Filter Invariants

1. **Complete Coverage**: Bloom filter includes all keys present in the SST
2. **No False Negatives**: If bloom returns "not present", key is definitely not in SST
3. **False Positive Rate**: Capped by bloom parameters; typically 1-2% for well-sized blooms

## Recovery & Consistency

### Normal Open

1. Seek to `file_size - 48` (footer location)
2. Read and parse footer
3. Validate magic number
4. Read meta-index block
5. Read index block and bloom block
6. Validate all checksums
7. Map index and bloom into memory

### Crash Safety

- **Atomic write**: Footer is written last; if file ends before footer, SST is incomplete
- **Partial blocks**: Truncated blocks detected by CRC checksum failure

## Future Extensions

These do **not** break the baseline format; they extend it orthogonally:

- **Per-Block Blooms**: Optional bloom offset per index entry (extended IndexEntry)
- **Tombstone Index**: Separate metadata block for range tombstones
- **Zone Maps**: Optional statistics block per data block
- **Format Version**: Bumped in footer if backward-incompatible changes needed

---

This spec is locked. All changes must preserve these invariants or update the format version in the footer.
