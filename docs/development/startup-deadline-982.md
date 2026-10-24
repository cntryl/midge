# Native startup deadline qualification

Run the existing native suite against the explicitly selected local Sqrzl:

```sh
cargo test --locked --features sqrzl-tests,failpoints \
  --test cloud_provider_engine_qualification startup_deadline:: -- --test-threads=1
```

The fixture requires the existing `MIDGE_SQRZL_ENDPOINT` and local emulator
credential configuration. It forwards signed S3 requests and real responses;
it does not qualify a live cloud account or other providers.

Two separate schedules prove different boundaries for #982:

| Schedule | Clock and control | Required outcome |
| --- | --- | --- |
| Native discovery and recovery timeouts | The original real 600 ms aggregate deadline starts before discovery. No manual expiry is installed. Native request caps remain 2 seconds. | Typed timeout, bounded elapsed time, no runtime admission, conditional cleanup and healthy same-path recovery. |
| Discovery exhausts the deadline before preparation | The existing earliest catalog HEAD pair takes 350 ms per request under the original real 600 ms envelope. The readiness hold is armed but never entered. | Exactly zero prepared/admitted/aborted runtime events, typed timeout, actual native cancellation, no lease mutation or acquired owner, and healthy same-path reopen. |
| Late prepared-runtime cancellation | The existing failpoint-only startup control waits for an actual `RuntimePrepared` event, then signals manual expiry of its one shared cancellation scope while the runtime remains held. | Exactly one prepared, zero admitted and one aborted event, typed timeout, normal owner exit, successful cleanup and unchanged acknowledged seed data. |

The controlled late-phase case does not prove a 600 ms elapsed-time bound.
Its existing test seam substitutes an unbounded wall-clock deadline until the
explicit expiry signal, so native discovery and recovery can reach the intended
phase independently of host scheduling. The unchanged real-time cases retain
the aggregate deadline proof. No per-phase deadline is restarted, and ordinary
opens without a scoped test installation keep their existing behavior.

The controlled caller uses the fixture's existing six-second receive and
cleanup bounds. Its RAII readiness hold releases before the caller joins even
when a broken baseline fails to return while held. An already delivered expiry
wins over later queued readiness, preventing delayed test dispatch from granting
runtime admission. The early case demonstrates why an armed hold boolean alone
cannot establish that preparation was reached. It has no acquired lease to
clean up; the exact acknowledged-seed recovery assertion belongs to the late
case, which establishes a real owner before its controlled cancellation.
