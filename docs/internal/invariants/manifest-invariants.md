# Manifest Invariants (Draft)

- Manifest edits are applied in sequence number order; readers must see a consistent view.
- Each edit is self-contained and atomic: add/remove file entries and version bumps happen together.
- The manifest file is append-only; compaction/rotation must preserve ordering of edits.
- A corrupted or missing trailing edit must not roll back previously durable edits.
- The current version pointer is advanced only after the edit is durable on disk.
