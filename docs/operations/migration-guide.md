# Migration guide

Midge `0.1.0` is a 0.x crate. Treat upgrades as application migrations: read
the release notes, inspect [format compatibility](../development/format-compatibility.md),
run compatibility and recovery tests, and keep a verified application-level
backup before changing binaries.

1. Stop writers and complete `engine.shutdown(timeout)`.
2. Preserve the database directory and WAL as a recoverable copy.
3. Test the new binary against that copy with verification and compatibility
   checks.
4. Roll forward only after reads, writes, restart recovery, and required cloud
   qualification pass.

If the application intentionally abandons a database, recreate it from its
source of truth after preserving any required evidence. That is not a generic
repair step.
