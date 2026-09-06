# Cloud recovery cost baseline

The paired Sqrzl campaign compares instrumented baseline `8038ab8` with
bounded reader reuse at `8f3f395`. Each run creates the same deterministic
134,219,148-byte cloud WAL fixture containing 16,212 source records. The
configured local limit is 32 MiB, engine pools 64 MiB, and values 8 KiB.
Both campaigns pass interrupted recovery, complete local-directory loss,
the mixed workload and compaction phase, and final source/value verification.

| Observation | Baseline | Reader reuse |
| --- | ---: | ---: |
| Initial recovery including owner-expiry wait | 41,590 ms | 39,782 ms |
| Final cold reopen before query verification | 63,952 ms | 18,216 ms |
| Coverage time within final cold reopen | 61,882 ms | 16,107 ms |
| Coverage probes | 32,618 | 32,618 |
| Reader opens during coverage | 32,618 | 41 |
| Successfully verified SST bytes | 133,981,593 | 133,981,593 |
| HTTP range attempts during final cold reopen | 196,421 | 33,536 |
| Consumed range-response body bytes | 2,633,411,985 | 2,553,475,173 |
| Peak tracked local files across campaign | 6,995,187 bytes | 6,995,187 bytes |

This is one paired emulator observation, not a latency distribution or an AWS
recovery objective. Request count falls about 83%, while response body bytes
fall about 3%. Repeated data-block reads remain a substantial cost. The initial
recovery still includes lease-expiry waiting and durable checkpoint publication;
the strongest improvement is reopening SST-covered WAL. Object-store contents,
checksums, replay decisions, resource limits, and durability barriers are not
relaxed by the optimization.

Baseline artifact identifier: `a80bab90-45c5-4e5d-ab84-f1911b3c852b`.
Optimized artifact identifier: `f8227fcf-5d39-48e7-9198-7d263044d7a9`.
The baseline commit includes the deliberately failing repeated-probe regression:
100 probes submitted 500 ranges; the optimized regression permits at most 100.

Reproduce on each revision using the same environment and profile:

```sh
docker compose up -d sqrzl
MIDGE_QUALIFICATION_LOCAL_BYTES=33554432 \
MIDGE_QUALIFICATION_WAL_BYTES=134217728 \
MIDGE_QUALIFICATION_MEMORY_BYTES=67108864 \
MIDGE_QUALIFICATION_VALUE_BYTES=8192 \
MIDGE_QUALIFICATION_TIMEOUT_SECONDS=900 \
MIDGE_QUALIFICATION_ARTIFACT_DIR=/tmp/midge-recovery-cost \
  cargo test --release --test cloud_provider_engine_qualification \
  --features sqrzl-tests,failpoints \
  operational::should_recover_cloud_backlog_after_complete_local_disk_loss \
  -- --ignored --exact --nocapture
```

Set `MIDGE_QUALIFICATION_REVISION` to the tested revision. Compare
`verified-opened.json` to measure open before the verification workload, and
check `recovered.json` and `verified.json` for complete verification. The
[qualification contract](../cloud-operational-qualification.md) defines timing,
HTTP byte accounting, cancellation, and resource-observation boundaries.
