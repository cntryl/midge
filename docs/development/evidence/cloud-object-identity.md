# Cloud object identity remediation evidence

This change addresses the first part of the six-part cloud storage remediation.
It does not implement publication workers, streaming transport, paged catalogs,
automatic migration, conditional SST range reads, complete local disk accounting,
or eager/lazy startup options. Those remain outstanding.

## Problem and contract

The cloud proof helper previously issued GET followed by HEAD. Different
same-length object versions could supply the bytes and identity, respectively.
The regression demonstrates an invalid primitive proof and an incorrectly
accepted conditional mutation. It does not demonstrate engine-level data loss.

Proof construction now consumes a metadata-bearing GET. The returned body,
length, and conditional identity describe the same response. Missing identity
and inconsistent length fail closed. Unsupported backends cannot fall back to
independent GET and HEAD calls. Hybrid proof construction uses this same contract
and retains its surrounding HEAD checks to reject observed changes or deletion.
Format-aware WAL/SST checksum validation remains with the existing consumers.

S3 (including OCI compatibility), Azure, and GCS GET metadata parsing checks any
supplied Content-Length against the body. Missing Content-Length is allowed for
chunked responses; it does not waive the identity requirement. This change does
not make transfers streaming or establish a bound on HTTP buffering.

The filesystem simulation derives its content identity from the bytes read
while holding its existing mutation locks. The mock cloud backend snapshots
bytes and generation under its mutation lock. Test wrappers forward the new
operation and preserve fault injection instead of relying on unsupported reads.

No persisted format or public OpenOptions behavior changes.

## Red results

On baseline `e8bcbad` with only the new regression tests added:

```text
cargo test --lib same_length_replacement -- --nocapture
running 2 tests
should_bind_proof_to_get_version_when_same_length_replacement_follows_get: FAILED
  left: "mock-gen-2", right: "mock-gen-1"
should_reject_stale_proof_mutations_when_same_length_replacement_follows_get: FAILED
  conditional write unexpectedly succeeded
0 passed; 2 failed
```

Before adding provider length validation:

```text
cargo test --lib should_reject_invalid_get_length -- --nocapture
running 3 tests
Azure, GCS, S3: FAILED
  accepted length 2 for a three-byte GET body
0 passed; 3 failed
```

Both focused runs passed after their corresponding fixes: 2/2 and 3/3.
Additional tests cover missing identity, mismatched lengths, and unsupported
metadata-bearing GET. Existing coverage includes disappearing objects, stale
conditional deletes, timeouts, deadline propagation, and recovery checksums.

## Qualification status

Results at initial review publication:

- `cargo fmt --check`: passed.
- `cargo build --workspace`: passed.
- `cargo test --workspace --all-features`: library portion passed (1,881 tests,
  four expected Sqrzl ignores); full workspace run remains in progress.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`:
  in progress. Fixed the initial documentation-spacing and helper-length findings.
- `cntryl-tools validate-tests`: passed, 2,648/2,648.
- `cargo test --lib --features sqrzl-tests storage::providers::qualification -- --ignored --test-threads=1`:
  passed, 4/4, zero skipped.
- `cargo test --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints -- --ignored --test-threads=1`:
  queued for rerun after restoring the local emulator's port mappings.
- `python3 scripts/validate_pr_acceptance.py /tmp/midge-cloud-identity-pr.md`:
  passed for the scoped identity-fix acceptance description.

Initial broad-test failures exposed wrappers missing the new read method; these
were corrected without removing the fault scenarios. Initial emulator attempts
failed closed on unavailable ports; the pinned container was recreated and all
four provider contracts subsequently passed. macOS loader stalls delayed the
remaining local checks; process samples showed `_dyld_start` and compiler-plugin
loading in `dyld`, rather than an engine stack. Pending checks are not green. The Sqrzl provider
contract now also consumes metadata-bearing proofs, performs a same-length
replacement, rejects stale conditional PUT and DELETE, checks that replacement
bytes survive, and distinguishes missing objects from transport failures.

The required Sqrzl image is:
`ghcr.io/sqrzl/sqrzl-emulator@sha256:876e017f850e53f3f4172cae459982cfe9435584fcb8e4640120b2e5fabfc624`.

## GCS qualification correction

The first hosted and local engine qualification runs both passed 6/7 and failed
GCS JSON recovery. HEAD returned the JSON metadata ETag while GET returned the
media ETag for the **same generation**. Comparing all metadata fields rejected a
valid object. This is distinct from the original mixed-version defect.

Identity selection now has one shared implementation for comparisons and
conditional headers: use generation when available, otherwise ETag. Comparisons
still require matching lengths and reject absent or mismatched identities.
Generation-aware comparison is used both during proof construction and during
guarded deletion revalidation. Added unit cases cover differing ETags at one
generation, changed or missing generations, changed length, changed ETag, and
missing identity. The full engine qualification must pass before this correction
is considered qualified.

## Performance and remaining acceptance

No before/after throughput, p50/p99, RSS, disk, or transfer-buffer benchmark has
been completed. The cloud proof path eliminates its separate HEAD call; this is
an implementation observation, not measured performance evidence. Hybrid proofs
retain both surrounding HEAD calls.

The six-part remediation is not complete. Its resource proofs, migration crash
matrix, lazy-read qualification, and benchmark matrix remain required. Passing
the identity regressions or existing suites does not establish those properties.
