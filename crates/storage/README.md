# cntryl-midge-storage

Storage provides filesystem and cloud object I/O for Midge. It owns provider
configuration and signing, request dispatch and retries, conditional mutations,
multipart uploads, and range reads. `HybridStorage` combines local staging and
caching with remote object transport. The [engine](../midge/README.md) and
[Durability](../durability/README.md) use storage receipts and object identities
when publishing or reclaiming persisted state.

The default `cloud-all` feature enables AWS S3, Azure Blob Storage, Google
Cloud Storage, and OCI. Select providers with `cloud-aws`, `cloud-azure`,
`cloud-gcp`, and `cloud-oci`, or disable default features for a build without
native cloud providers.

## Verify changes

Run local tests from the repository root.

```sh
cargo test --locked -p cntryl-midge-storage
```

With the repository's Sqrzl service running, run the provider contract suite.

```sh
cargo test --locked -p cntryl-midge-storage --lib --features sqrzl-tests \
  providers::qualification -- --test-threads=1
```

See [cloud setup](../../docs/operations/cloud-setup.md) for provider configuration
and the [testing guide](../../docs/development/testing.md) for the Sqrzl setup.
