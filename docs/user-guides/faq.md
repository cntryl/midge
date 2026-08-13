# FAQ

## Is Midge production-ready?

Midge `0.1.0` is pre-1.0. The API, operational procedures, and cloud support
boundaries can change. Cloud-backed storage is supported and continuously
qualified through Sqrzl; deployment-specific credentials, IAM, networking,
provider configuration, quotas, and capacity remain the adopter's responsibility.

## Which storage mode should I use?

Use `in_memory` for ephemeral tests, `local` for local restart persistence,
`cloud_simulated` for deterministic filesystem-backed cloud lifecycle tests,
and `cloud` for provider-backed storage after validating the deployment-specific
configuration and workload. Midge continuously qualifies its provider protocol
paths with Sqrzl.

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
