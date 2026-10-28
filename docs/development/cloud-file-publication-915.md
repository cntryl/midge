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
