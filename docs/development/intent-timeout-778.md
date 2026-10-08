# Intent replay timeout attribution (#778)

The failed sanitized qualification run
[37686932618](https://github.com/cntryl/midge/actions/runs/37686932618)
at `eacf3d62128d3aabb5dae785904febe22a6c991b` timed out on the one-byte
input `C` after 326 prior mutations. The configured per-input timeout was
10 seconds. The existing 18-byte replay control passed first.

## Exact stack attribution

[Diagnostic run 37781816416](https://github.com/cntryl/midge/actions/runs/37781816416)
rebuilt the original commit with `nightly-2026-07-01` and `cargo-fuzz 0.13.2`.
Its ELF build ID was exactly `ae04f63d89eacbab25fbd11d07479699158cb40d`,
matching the failing binary. Symbol-table lookup and source-lined disassembly
resolve these original timeout frames:

| Binary offset | Symbol / source |
| --- | --- |
| `0x11374ad` | ASan `pthread_join` interceptor |
| `0x2b33990` | Rust Unix thread join |
| `0x1a98029` | Rust `JoinInner::join` |
| `0x17640dc` | `LeaseHeartbeat::stop`, watchdog join at `heartbeat.rs:341` |
| `0x1eb2907` | `StartupLease::drop` |
| `0x19fb6bc` | `EngineStartup::open_profiled` |
| `0x19aa917` | `Engine::open` |
| `0x1198f6a` | Strict open in `tests/support/fuzz_recovery.rs:25` |

Thus the observed blocked phase was startup-error cleanup joining the watchdog,
not intent parsing. The diagnostic passed the isolated input 128 times, 2,048
mutations from the original seed `1504688986`, and 2,048 runs from the preserved
failure corpus. These passes show intermittency; they do not dismiss the failure.

## Forced regression and fix

`LeaseValidity::wait_for_change` evaluates the external heartbeat `running`
atomic under the validity mutex before entering its condition-variable wait.
Shutdown previously set `running=false` and notified without that mutex. A
waiter could read `true`, receive the notification before registering, and then
sleep until its original lease deadline. Thread `unpark` does not wake this
condition variable; `stop` then waits in the watchdog join.

`should_wake_lease_waiter_when_stop_races_with_wait_registration` pauses after
the predicate reads `true`, stops and notifies concurrently, and resumes wait
registration. Before the fix it failed with a lost notification and exhausted
the two-second fixture wait. With notification serialized under the same mutex,
it passed in 0.11 seconds and retained the exact active lease validity.

The fix does not change lease authority, renewal, expiration, recovery policy,
or shutdown timeout. It synchronizes only the out-of-band stop notification.
The 64-iteration public regression preserves strict malformed-intent rejection,
exercises the existing salvage behavior, and verifies a fresh owner can open
and shut down after each fixture's malformed input is removed.

## Preserved qualification controls

`tests/fixtures/fuzz/intent_timeout_1.bin` preserves the exact failing input.
`intent_timeout_corpus.json` preserves all 60 corpus files from artifact
`11510999091`, including the original `seed0` name and each file's exact bytes.
The qualification workflow retains all four original fuzz targets and the
10-second per-input timeout. It additionally replays both timeout inputs, the
original mutation seed, and the full preserved corpus under pinned sanitizers.

Local focused validation passed the forced regression, all 142 lease unit
tests, all four fuzz-harness integration tests, formatting, test contracts,
and all-target/all-feature pedantic Clippy. Local ARM64 macOS sanitizer replay
passed 128 executions of the one-byte input. Hosted Linux qualification and
exact-head CI remain the delivery gates; a passing local replay alone is not
release qualification.
