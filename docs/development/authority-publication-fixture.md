# Composed authority and publication fixture

The ignored native regression in
[`authority_publication.rs`](../../tests/cloud_provider_engine_qualification/authority_publication.rs)
composes writer authority, immutable WAL upload, catalog publication, SST
replacement, remote reclamation and cache-loss recovery. It addresses #757.

Run the pinned local emulator and the fixture:

```sh
docker compose up -d sqrzl
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo test \
  --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints \
  authority_publication::should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup \
  -- --ignored --exact --nocapture
```

The existing cloud integration workflow includes it through the ignored provider
suite. `MIDGE_QUALIFICATION_ARTIFACT_DIR` overrides the default
`target/authority-publication` receipt directory. Each case retains sanitized
provider request observations, actual epochs/frontiers, the successor catalog,
the independent expected recovery map and two child logs/completion receipts.
Receipts include the Git base, fixture source digest and executable digest;
development runs may include uncommitted fixture changes. Authorization headers,
request payloads and provider credentials are excluded.

Each of three explicit schedules holds one real native WAL request before the
upstream PUT, after actual upstream success, or after success with the response
lost. A predecessor first receives a strict ACK for a durable prefix. The proxy
then disconnects its renewal writes, the real lease-loss hook fires, and a
separate-cache successor acquires a higher epoch after genuine persisted expiry.
The 18-second predecessor lease respects the production renewal write margin.
No fixture changes the lease record, fabricates an epoch or manufactures a
successful provider response.

The successor acknowledges overwrites, a deletion and atomic pairs, flushes
twice, compacts, and must observe successful remote WAL and obsolete SST
deletions. Its exact live scan/points are checked and it shuts down before the
held predecessor request is released. This ordering quiesces legitimate
successor control changes so the fixture can compare exact catalog bytes across
the stale completion. The held PUT must actually succeed and its object must
remain readable while excluded from recovery authority.

The predecessor strict waiter must return `Fenced`, its cloud durability frontier
must remain at the original prefix, and its subsequent request trace must contain
no forwarded control mutation or deletion. The retained unpublished WAL remains
runtime owned; shutdown must report that incomplete drain. Only the predecessor
shutdown drain budget is shortened to 100 ms through the existing testing option.

Two fresh child processes recover sequentially, each with an entirely absent
cache. The parent supplies an exact expected key/value inventory. Each child
checks a complete scan, every expected point, deleted and uncatalogued keys, and
successful shutdown, then writes a completion receipt. Parent process deadlines
kill and reap stalled children; zero selected child tests cannot pass.

Mutation validation advances the cloud frontier before authority validation in
the CloudAck handler. The fixture rejects that mutation with frontier 11 instead
of 7. The mutation is restored before green verification and is never committed.

These are three bounded S3 emulator schedules. They establish neither exhaustive
simulation nor live-provider qualification, and do not cover delayed metadata
CAS, stale compaction completion, held readers, TTL, random schedule reduction,
or sustained saturation. Those need separate composed schedules.
