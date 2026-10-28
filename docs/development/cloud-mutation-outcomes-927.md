# Native conditional mutation qualification (#927)

The `storage::providers::qualification::mutation_faults` tests forward real
authenticated S3-compatible, Azure, GCS XML and GCS JSON requests to Sqrzl.
The local run used `latest` resolved to
`sha256:da1a2a731fe6de71ce8241e3d9fda9ae48dec6015f8b531f4f3129e4ca9969a8`.
Keep the tag on `latest` and record its resolved digest for subsequent runs.

All four tests passed, covering sixteen provider/schedule combinations:

- A successful conditional create whose response is dropped sends one request,
  reports a transport/timeout error, and leaves exactly one object with the
  expected bytes. It is not replayed into a false precondition failure.
- A successful conditional delete whose response is dropped sends one request
  and reports a transport/timeout error. A direct independent read proves the
  object is absent; the caller's error alone does not prove that outcome.
- A stale conditional delete sends one request, returns a precondition failure,
  and preserves the replacement bytes.
- A received conditional create is held until the original deadline produces
  its timeout callback. The object is independently absent at that point.
  Only then does the fixture forward the original request to Sqrzl; it succeeds
  and the exact object becomes readable. Timeout is not proof of no effect.

The lost-response cases configure three retries with zero delay, so observing
one request proves the mutation classification prevents blind replay. The
late-effect case uses the original 250 ms request budget and channel ordering,
not a sleep or a refreshed deadline. The proxy preserves signed headers and
does not fabricate provider success. It reuses the existing native HTTP fixture
and the narrowly scoped forwarding helper from receipt qualification.

With the Compose credential supplied through `SQRZL_SECRET_ACCESS_KEY`:

```text
cargo test --locked --lib --features sqrzl-tests mutation_faults -- --test-threads=1 --nocapture
```

The configured local run completed in 1.58 seconds with four passed, zero failed
and zero ignored tests. These are provider mutation/outcome proofs. The separate
startup inventory tests and benchmark establish concurrency, progress credit
and inventory timing; these cases do not claim startup or live-cloud latency.
