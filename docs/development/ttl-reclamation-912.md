# TTL reclamation with a caller-supplied horizon (#912)

Status: accepted design (2026-11-18), implementation deferred. No reclamation code exists; #912 is closed
until a user needs TTL reclamation. This note answers the three
questions on [#912](https://github.com/cntryl/midge/issues/912) and sets the contract and slices.

## Problem

A value may carry an absolute wall-clock expiry. Reads hide it once the reader's clock time reaches the
expiry (`is_expired_at`). Compaction keeps the raw value and expiry forever
(`src/compaction/executor.rs`, `should_preserve_raw_expiration_when_compaction_precedes_clock_correction`),
so a workload that writes 1 GB a day with a 7-day TTL and rarely overwrites holds 30 GB after 30 days.

The property this protects, chosen on 2026-10-24: if the host wall clock jumps forward by mistake and is
later corrected, values that looked expired become readable again.

## Question 1: can reclamation keep that property?

No. Once compaction deletes a value's bytes, nothing can restore them. Every design that reclaims space
gives up recovery for the values it reclaims. A quarantine that keeps the bytes for a while only moves
the deletion later; it does not change who decides.

So the design question is not how to make deletion reversible. It is what authorizes the deletion.

## Question 2: what authorizes deletion?

Four contracts were weighed (Jev, 2026-11-18, as a lead):

| Contract | Deletion authority | Weight |
|---|---|---:|
| Caller-supplied horizon, unset by default | The application, explicitly and durably | 1.00 |
| Engine-clock grace period, opt-in | The host wall clock plus a margin | 0.00 |
| Wall clock cross-checked with monotonic time | The host clock, with forward jumps caught only while running | 0.00 |
| Never reclaim | Nobody | 0.00 |

The grace period and the monotonic check both leave the host clock as the authority. A clock that is
wrong by more than the grace, or wrong at process start, still deletes live data. That is the failure
the 2026-10-24 decision was made to avoid.

**Proposed contract.** The engine never deletes on its own clock.

- The application may record a *reclamation horizon* `H`, a wall-clock timestamp in milliseconds. There
  is one horizon per store, shared by every column family. It is unset by default.
- `H` is durable in the manifest and never moves backwards.
- Compaction may delete a value only if its expiry is at or before `H`.
- Reads treat any value whose expiry is at or before `H` as expired, whatever the clock says. This keeps
  reads and compaction in agreement after a restart with a corrected clock: a value is never visible on
  one read and reclaimed on the next.
- With `H` unset, nothing changes from today. A wrong host clock alone can never delete data.

Recording `H` is an explicit, irreversible statement by the caller: "everything that expired at or
before this time may be destroyed." An application that wants a grace period passes `now - grace`. A
built-in grace mode is left out of the first version.

A horizon later than the engine's observed clock is rejected with `InvalidArgument`. The caller can
authorize destroying values that have expired by both its own reckoning and the engine's, never values
the engine still considers live. If the host clock is behind, the call fails and data is retained.

## Question 3: mechanism

### Dropping without resurrecting an older version (R1, R2)

When compaction meets a value with `expiry <= H`:

- If the output is bottommost for the key's range, or every older version of the key is in this
  compaction, drop the value and the older versions.
- Otherwise replace the value with a point tombstone at the same sequence. The tombstone keeps masking
  older versions in deeper levels. The existing point-tombstone proof (`point_tombstone_gc_eligible`,
  snapshot horizon) removes it later. This reclaims the value bytes at once and reuses a proof the
  engine already trusts.

The horizon is captured once at plan time and stored in the plan, never re-read during execution.

### Readers that started before expiry (R3)

A reader that opened at time `T` with `T < expiry <= H` must keep seeing the value.

- Existing snapshots pin the SST files they opened. A compaction output that lacks the value does not
  affect a reader still holding the original file. **To verify in slice 1** with a test, since the pin
  registry tracks sequence and generation, not read time: hold a pre-expiry snapshot, advance `H`,
  compact, and read through the old snapshot.
- New readers take `max(clock, H)` as their effective read time for expiry, so they never expect a value
  that reclamation may already have removed.

### Finding cold expired data (R4)

SST metadata already carries an inert `earliest_expiration_millis`. A file whose earliest expiration is
at or before `H` becomes a candidate for a TTL rewrite even when its level is under target. Candidates
rank below overlap repair, critical L0 debt and ordinary level debt, and use the same job reservations
and budgets. Advancing `H` marks compaction due.

## Public surface

- `Engine::advance_ttl_reclamation_horizon(millis)`: durable before it returns; an older value than
  the stored one is a no-op; a value later than the engine's observed clock is `InvalidArgument`.
- `Engine::ttl_reclamation_horizon() -> Option<u64>`.
- Metrics: values reclaimed, bytes reclaimed, values converted to tombstones.

A new manifest field means a metadata format bump. Formats may break without migration.

## Failure analysis

| Scenario | Outcome |
|---|---|
| Host clock jumps forward, application does not advance `H` | Values look expired to reads; nothing is deleted; correcting the clock restores them. Same as today. |
| Application advances `H` from a wrong clock | Data up to `H` is lost. This is the caller's explicit decision; the API documents it as irreversible. |
| Crash after `H` is durable, before any compaction | Reads already treat `expiry <= H` as expired. Compaction reclaims later. |
| Crash during a reclaiming compaction | The existing intent and publication sequence applies; inputs stay authoritative until the manifest publishes. |
| Reader opened before expiry, `H` advanced afterwards | Keeps reading its pinned files (slice 1 test). |
| Cloud takeover by a new owner | `H` is in the manifest the new owner loads, so it reads and compacts with the same horizon. |

## Slices

1. **Horizon as a read-time fact.** Manifest field, the two API calls, reads use `max(clock, H)`.
   Tests: durable across restart, monotonic, a restart with an earlier clock still hides
   `expiry <= H`, the pre-expiry snapshot test above. No deletion yet.
2. **Reclaim in compaction.** Drop or convert to tombstone under the rule above, plan-time capture,
   metrics. Tests: #912 AC1 (old reader keeps its value, new reader sees expiry, bytes reclaimed after
   release, no older version resurrected), crash tests at the publication points.
3. **TTL-aware picking.** Candidates from `earliest_expiration_millis`; test that a cold file under
   target is rewritten after `H` advances.
4. **Measurement.** #912 AC2: a TTL workload row recording SST footprint before and after `H` advances.

Each slice is safe to ship alone. Slice 1 changes no on-disk data.

## Decisions

Weighed with Jev on 2026-11-18; strong leans adopted, weak ones scrapped in favour of the simpler form.

| Point | Weight | Decision |
|---|---:|---|
| Accept the caller-supplied horizon contract | 0.86 | Adopted |
| Reads honour `H` from slice 1 | 0.98 | Adopted: no release can delete what a read would still return |
| Reject a horizon later than the engine's observed clock | 0.97 | Adopted |
| Horizon per column family rather than per store | 0.61, weak | Scrapped: one horizon per store |

One horizon per store means an application cannot keep expired data in one family while reclaiming
another. That is a new issue if it is ever needed.

#958 (durable-read retention) was closed as not planned. If it returns, its retention rule must respect
`H`.
