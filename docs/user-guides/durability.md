# Durability

The [transaction durability contract](transaction-durability-contract.md) is
the single external source of truth for commit acknowledgement, local WAL
behavior, cloud upload modes, flush, restart recovery, and shutdown.

This page is intentionally only a pointer. If another guide disagrees with the
contract, treat the contract, Rust API documentation, and recovery tests as the
authorities.

For the quick operational summary: `WriteOptions::cloud_async()` returns after
the local cloud-backed WAL barrier while seal and upload continue;
`WriteOptions::cloud_strict()` waits for seal and upload. These modes are
cloud-only, while `sync()` and `buffered()` are local-only. Non-cloud storage rejects
the cloud modes. Empty cloud-backed transactions still follow the
same policy validation and acknowledgement path; it does not invent durability.

Manifest publication requires one
required filesystem sync for its durable journal/checkpoint boundary. Midge
does not provide an escape hatch to skip that sync.
