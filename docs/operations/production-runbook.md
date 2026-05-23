# Production Runbook

This runbook defines the minimum operator workflows Midge needs on the path to production stability.

It does not claim that every workflow is fully automated today. It defines the supported operational shape the project is converging toward.

## Supported Production Topology

Current production target:

- single-process embedded deployment
- local-disk storage mode
- strict recovery policy

## Sizing And Restart

- Size memory so normal traffic does not spend most of its time in `WriteStall`; `write_stalled` should be an exception state, not the baseline.
- Keep disk headroom for current SSTs, active WAL files, manifest state, and cleanup lag from compaction or failed publishes.
- After any unclean shutdown, run `midge verify --json <db-path>` before reopening the database to traffic.
- If `verify` reports degraded or salvage health, stop rollout and restore or repair before reopening.

## Pre-Deployment Checklist

- run `midge verify <db-path>` on the candidate data directory when applicable
- review the release changelog and migration guide
- confirm rollback posture for the target release
- confirm backups exist and have been recently tested
- confirm `WriteOptions` usage in the application matches data-criticality requirements

## Crash Recovery Investigation

1. capture application logs and `EngineHealth`
2. run `midge verify --json <db-path>`
3. inspect:
   - recovery metrics
   - manifest verification result
   - SST verification count
   - WAL replay counters
4. if health is degraded or salvage:
   - stop normal rollout
   - treat the node as requiring operator review

## Corruption Handling

- `CompatibilityError`: do not continue; resolve version/format mismatch
- `Corruption`: preserve files, capture verify output, restore or escalate
- `RecoveryFailed`: treat as hard-stop for strict recovery
- `NoSpace` and `WriteStall`: operator-actionable resource issue, not format corruption

## No-Space Recovery

1. stop or throttle writers
2. recover free space
3. rerun verification
4. reopen in strict mode
5. only use salvage workflows if explicitly documented for the environment

## Upgrade Workflow

1. backup
2. review migration note and rollback statement
3. run pre-upgrade `midge verify`
4. deploy candidate
5. run post-upgrade `midge verify`
6. validate recovery/runtime metrics and smoke workload

## Rollback Workflow

Use the release-specific rollback statement. If rollback is unsupported for a format step, restore from backup or use export/import only.

## Required Operator Signals

Before Midge is called production-stable, operators must be able to observe:

- health state
- recovery counters
- compaction failures
- write stalls
- obsolete-file backlog
- verification result
- restart/recovery time
