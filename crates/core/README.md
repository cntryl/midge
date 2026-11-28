# cntryl-midge-core

Core provides the primitives shared by Midge's engine and storage crates.

- `common` defines errors, clocks, deadlines, and resource accounting.
- `io` defines the filesystem interfaces and host-backed and simulated I/O.
- `codec`, `types`, and `cloud_layout` define shared compression, record types,
  and cloud object names.
- `telemetry` defines shared instrumentation. The optional `telemetry` feature
  enables tracing integration, and `telemetry-otlp` adds OTLP export.

Core has no default features. It provides building blocks for the other Midge
crates. Applications embedding the database use [cntryl-midge](../midge/README.md).

## Verify changes

Run from the repository root.

```sh
cargo test --locked -p cntryl-midge-core
cargo test --locked -p cntryl-midge-core --test compression_compatibility
```

See the [workspace architecture](../../docs/development/architecture.md) for the
write and recovery paths that use these primitives.
