# FAQ

## Is Midge production-ready?

Midge `0.1.0` is pre-1.0. The API, operational procedures, and cloud support
boundaries can change. Evaluate it with the repository's tests and your own
failure scenarios.

## Which storage mode should I use?

Use `in_memory` for ephemeral tests, `local` for local restart persistence,
`cloud_simulated` for deterministic cloud lifecycle tests, and `cloud` only
after qualifying the selected provider and feature set.

## What does commit mean?

Writes are buffered in a transaction until `commit(WriteOptions)`. The selected
write policy determines the acknowledgement boundary. Read the [transaction
durability contract](transaction-durability-contract.md) for the exact rules.

## Does Midge support range deletion?

Yes, through `Transaction::delete_range(start, end)` with a half-open range.
There is no standalone `Engine::delete_range` operation.

## How do I tune Midge?

Start with `Goal`, `MemoryBudget`, and `WorkloadProfile`. Add a documented
builder override only when measurements and tests justify it. Background
compaction can be enabled or disabled with `background_compaction`; disabling
it transfers responsibility for eventual flush/compaction progress to the
operator.

## How do I report a problem?

Include Midge version, Rust version, storage mode, feature flags, exact
`WriteOptions`, recovery policy, a minimal reproducer, and the relevant error or
verification output. Do not attach credentials or private database contents.
