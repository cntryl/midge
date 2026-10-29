# Conditional streaming file publication candidate (#915)

This is the implementation-first candidate on the local 0.5.0 branch. The
provider qualification and crash campaigns are still due.
The source is an owned file handle with the finalized length and CRC32C; the
uploader checks the exact stream against that identity before the request
that can commit it. Changing a completed source before upload must fail.

Files of at least 10 MiB use the streaming capability when the backend
advertises it. Parts are 5 MiB. Admission covers one source buffer plus the
provider's declared body copies and 256 KiB of fixed workspace; part receipt
metadata and completion lists are charged separately to the same budget.
Reservations follow outstanding provider bodies and blocking file reads,
including after a caller timeout. Unsupported backends retain the bounded
partition/buffered path. This is a byte-admission bound, not an RSS measurement.

| Provider | Data integrity and commitment |
| --- | --- |
| S3 | Initiate CRC32C/FULL_OBJECT, send and verify each part checksum, then commit the exact ordered part identities with the whole CRC32C and If-None-Match. The final stored identity and checksum must agree; a bare ETag never qualifies. |
| Azure | Validate Content-MD5 for each uniquely named block and for the ordered block-list body. Commit the list with If-None-Match. The composed proof binds the returned blob identity to those checked bytes; the unvalidated x-ms-blob-content-md5 property is not used as proof. |
| GCS JSON/XML | Initiate a resumable session with generation-match=0, then stream bounded Content-Range chunks and validate the final whole-object CRC32C plus generation identity. A session URI cannot change the authenticated origin. |

These differences follow the provider contracts:
[AWS checksum types](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html),
[S3 conditional completion](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html),
[Azure block-list integrity](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list),
[GCS resumable integrity](https://docs.cloud.google.com/storage/docs/resumable-uploads),
and [GCS generation preconditions](https://docs.cloud.google.com/storage/docs/xml-api/post-object-resumable).
GCS does not validate checksums on intermediate resumable chunks. Its XML
multipart API lacks preconditions, so that API is unsuitable for this
immutable-key contract; see
[GCS multipart limitations](https://docs.cloud.google.com/storage/docs/multipart-uploads).

Missing checksum receipts use identity-pinned bounded range readback against
the source file and its expected CRC32C. Wrong or contradictory receipts fail.
S3-compatible endpoints must opt into checksum receipts before advertising the
streaming protocol. That opt-in still needs provider qualification.

An isolated S3 part or Azure block can retry the same staging identity and
body. Catalog CAS, shared-key writes and final object commits do not receive
that replay permission. A failed whole-file attempt reconciles the original
immutable key before a later attempt. Unknown completion retains the same
source/key/checksum contract. Best-effort abort uses only the admitted session
and remaining deadline; Azure uncommitted blocks and other uncertain leftovers
remain non-authoritative residue for the separately proved #943 cleanup work.

Active native HTTP regressions cover 64 MiB streaming, exact source read
counts, maximum read size, budget admission, wire corruption and changing the
finalized source before commitment. They exercise the production provider
protocols. These regressions have been compiled, not run, during the coding
pass. They do not replace the public-engine publication/restart campaigns,
missing-receipt/unknown-result campaigns or Sqrzl qualification. No live cloud
provider result or performance improvement is claimed here.

## Sqrzl streamed receipt qualification (2026-10-28)

Sqrzl image
`sha256:da1a2a731fe6de71ce8241e3d9fda9ae48dec6015f8b531f4f3129e4ca9969a8`
passes the streamed receipt cases for S3-compatible checksum opt-in, Azure,
GCS XML and GCS JSON. Each case uploads 5 MiB plus one byte through the
production file-upload API, validates the receipt against the exact bytes and
length, and independently reads the completed object back. The S3-compatible
receipt-disabled pinned-readback case also passes.

With the Compose fixture credential supplied through
`SQRZL_SECRET_ACCESS_KEY`, run:

```text
cargo test --locked --lib --features sqrzl-tests storage::providers::qualification -- --test-threads=1 --nocapture
```

The suite reported 13 passed, zero failed and zero ignored. One case is local
Azure canonicalization; the optional real-S3 case returned without a configured
endpoint. The remaining eleven cases exercise the authenticated Sqrzl front
doors. The first invocation lacked the required fixture credential and failed
before provider requests; the configured rerun passed in 1.75 seconds.

This resolves the prior S3 checksum multipart and GCS resumable-session
capability gap reported in Sqrzl #79. It does not establish live-provider
qualification, fault-injected wrong/missing receipt behavior on this image,
engine crash/restart qualification, or the remaining performance evidence for
#915. Those scopes remain separate from this successful-upload protocol proof.

### Completion-response faults

The `storage::providers::qualification::receipt_faults` tests forward real
authenticated requests to that same Sqrzl image and change only successful
completion responses. All twelve schedules (four provider dialects, three
faults each) pass:

- Contradictory checksum receipts cannot produce a publication proof.
- Missing receipts either fail closed or require identity-pinned range
  verification; GCS JSON pins by generation query parameter.
- A lost successful completion reconciles the original immutable object by
  pinned range readback, with exactly one successful remote completion.

An independent direct read verifies every completed object against the source,
and publication releases its admitted memory. These tests exercise production
`HybridStorage` streaming publication over files larger than two 5 MiB parts.
The proxy preserves signed request headers and HEAD lengths, and redirects
resumable session locations back through its loopback endpoint. It does not
fabricate provider success. Run the qualification command above with the
`receipt_faults` filter to repeat these schedules.

### Multipart-sized compaction process-abort recovery

The provider-engine suite's `streamed_cloud_compaction_crash` regression passes
for S3 checksum opt-in, Azure, GCS XML and GCS JSON on the same Sqrzl image.
Each child writes two overlapping SST generations, then aborts at
`slice7::after_compaction_output_durable_before_manifest_publish`. The durable
compaction intent must describe a new output larger than 10 MiB at that
boundary, so metadata-only moves cannot satisfy the test. The existing crash
harness requires a synced exact-trigger sentinel and an actual process abort.

After the original writer lease expires, the parent deletes the complete local
cache, reopens through the real provider and verifies the committed input file
set and all 192 latest 64 KiB values. The unpublished streamed output does not
gain manifest authority. Both harness tests passed; the four recovery schedules
took 104.44 seconds in the local configured run.

```text
cargo test --locked --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints streamed_cloud_compaction_crash -- --test-threads=1 --nocapture
```

This adds the process-abort and cold-cache engine-manifest proof independently
of completion-response integrity and ambiguity. Performance evidence retains
its own gate.
