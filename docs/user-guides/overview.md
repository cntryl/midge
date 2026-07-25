# Overview

Midge is an embedded, single-process Rust LSM key-value engine. Version `0.1.0`
requires Rust `1.97` or newer. It is intended for evaluation and controlled
local deployments while the 0.x API and cloud operations continue to evolve.

## Storage modes

- `InMemory`: process-local state with no restart persistence.
- `Local`: WAL and SST files under the configured local path.
- `CloudSimulated`: filesystem-backed cloud behavior for deterministic tests.
- `Cloud`: an optional provider-backed local cache and remote store. Provider
  support is feature-gated and remains pre-1.0.

## Core model

Each transaction is bound to one column family. A read-only transaction reads a
snapshot. A read-write transaction buffers `put`, `delete`, and half-open
`delete_range` intents until `commit`. Commit validates the configured conflict
policy and publishes the transaction's writes together. Dropping an uncommitted
transaction abandons its buffered writes; orderly engine termination requires
`shutdown(timeout)`.

Durability is selected per commit: `sync`, `buffered`, `best_effort`,
`cloud_async`, or `cloud_strict`. The exact crash boundaries are defined in the
[transaction durability contract](transaction-durability-contract.md).

## Boundaries

Midge does not provide a server protocol, multi-process coordination, or a
stable 1.0 compatibility promise. Cloud setup, credentials, provider behavior,
and cache-loss recovery must be qualified with the relevant feature and test
environment before adoption.

Continue with the [quick start](quick-start.md), [API guide](api-guide.md), or
[documentation hub](../README.md).
