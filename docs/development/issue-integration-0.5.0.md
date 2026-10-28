# Midge 0.5.0 integration index

PR #992 is the draft integration PR for the 0.5.0 issue batch. The PR, issue
tracker, and workflow runs are the live status sources; this page is only a
navigation index.

- [Draft PR #992](https://github.com/cntryl/midge/pull/992)
- [Open 0.5.0 milestone issues](https://github.com/cntryl/midge/issues?q=is%3Aopen+milestone%3A0.5.0)
- [Benchmark workflow runs](https://github.com/cntryl/midge/actions/workflows/bench-tier2.yml)

Acceptance status and short benchmark summaries belong on each owning issue.
Raw benchmark reports and CI logs remain attached to GitHub Actions runs; they
are not checked into the repository. Keep an issue open until its listed
acceptance criteria are satisfied and the integration head is qualified.

## Current qualification focus

- [#946 detached conflict validation](https://github.com/cntryl/midge/issues/946):
  behavioral and crash evidence remain on the owning issue; absolute latency
  measurements are diagnostic.
- [#948 grandparent output cuts](https://github.com/cntryl/midge/issues/948):
  its source-pinned absolute workload measurements remain open; no comparison
  delta is required.
- [#943 durable delete intents and orphan sweeping](https://github.com/cntryl/midge/issues/943):
  the active-sweeper process-abort and per-turn request-budget regression passed
  on the current integration head; final PR qualification remains open.
- [#926 identity-preserving SST moves](https://github.com/cntryl/midge/issues/926):
  the ingest write-amplification readout is informational and is not a closure
  gate, per the accepted issue criteria.

Other issue-specific implementation, provider, recovery, fuzz, and performance
gates remain on their owning issues. The milestone issue list is authoritative
for the current count and status.
