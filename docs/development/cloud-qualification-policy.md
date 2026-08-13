# Cloud Qualification Policy

Midge cloud-backed storage is a supported pre-1.0 capability. Automated cloud
qualification is self-contained: the repository uses the Sqrzl multi-provider
emulator as its authoritative continuous qualification environment and does not
require contributor or CI access to live cloud accounts.

Pre-1.0 describes the stability boundary of the API, persisted formats, and
operational contract. It does not mean that the cloud implementation or its
automated qualification path is experimental.

## Qualification Model

Sqrzl exercises the provider wire contracts used by Midge for S3-compatible
storage, Azure Blob Storage, and Google Cloud Storage. The scheduled and release
workflows run both provider-level operations and engine-level cache-loss and
restart recovery through those provider front doors. An explicitly selected
qualification run fails if Sqrzl is unavailable; the tests never silently skip.

This makes Sqrzl qualification the reproducible evidence required by the Midge
repository. It is deterministic, credential-free, and available to every
contributor.

Manual real-cloud integration testing has a different purpose:

- validate that Sqrzl continues to match observed provider behavior
- validate deployment-specific credentials, IAM, endpoints, quotas, lifecycle
  rules, and network policy
- discover provider behavior that should become a permanent Sqrzl scenario and
  Midge regression test

Live-provider access is not a prerequisite for ordinary Midge CI and the
absence of live-provider credentials in CI is not a cloud-maturity defect.

## Evidence Loop

When manual provider testing finds a difference between Sqrzl and a cloud
service:

1. Record the provider request, response, and relevant failure semantics without
   retaining credentials or private data.
2. Reproduce the behavior in Sqrzl.
3. Add or strengthen the Midge provider or engine qualification test.
4. Pin the updated Sqrzl image by digest in `compose.yml`.
5. Run the scheduled/release cloud qualification gates.

The durable artifact is the deterministic emulator scenario and regression
test, not continued access to a particular cloud account.

## Claims and Boundaries

A passing Sqrzl qualification run supports claims about the provider protocol
paths and failure scenarios that the emulator models. It does not claim that a
particular deployment has correct IAM, sufficient quotas, acceptable latency,
provider availability, compliant lifecycle rules, or adequate capacity.

Provider-backed deployments must still qualify their own configuration and
workload. Those deployment responsibilities do not make the Midge cloud path
experimental; they are environmental conditions outside an embedded engine's
self-contained test boundary.

OCI uses the S3 Compatibility API rather than a native OCI client. Its narrower
support boundary is documented in the
[cloud setup guide](../operations/cloud-setup.md) and must not be generalized to
the native AWS, Azure, or GCP provider paths.

## Release Requirements

A release that includes cloud-backed storage must:

- use a digest-pinned Sqrzl image
- pass provider-level and engine-level Sqrzl qualification without skips
- pass cloud durability, lease, upload, cache-loss, and recovery suites
- document persisted-format and upgrade implications
- record any known mismatch discovered by manual real-cloud testing

Manual real-cloud results are valuable release evidence, but they supplement
rather than replace the self-contained qualification gates.
# Configuration validation and preflight

Provider qualification must exercise the public typed configuration,
side-effect-free validation, and read-only preflight surfaces for generic
S3-compatible and AWS protocol behavior, Azure Blob, GCS JSON and XML modes,
and OCI's derived S3-compatible mapping. A successful preflight is deployment
access evidence only: it does not replace Sqrzl mutation, conditional-write,
fencing, or deletion qualification.
