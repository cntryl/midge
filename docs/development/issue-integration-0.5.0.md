# Local 0.5.0 issue integration

The user requested this workflow on 2026-10-24: one local branch,
`work/0.5.0-issue-integration`, with small commits for the issues and one PR
after the batch is complete. This supersedes the earlier per-issue PR train
for this batch. Keep implementation commits local; do not create individual
PRs or close issues as though local work had merged.

The branch starts at `b4d08ebe6a01d2a66eae1b4d4c155e081032f485`, including
the documentation changes already proposed by PR #980. That pre-existing PR
has a failed Core run at the base revision: the simulated cold-cache takeover
test returned `precondition failed: remote SST version changed` in
[run 40083997084](https://github.com/cntryl/midge/actions/runs/40083997084).
This is a baseline observation, not a reproduced or diagnosed integration
defect. Preserve it when interpreting broad validation.

## Working rules

- Work in dependency order, preserving a failing behavioral regression before
  each durability fix and recording the green commands with its commit.
- Prefer a self-contained passing commit for each issue or bounded slice.
  Keep red evidence in this log and commit messages; unresolved regressions
  remain active rather than being ignored or inverted.
- Keep all changes within issue scope. Use Cargo and existing cntryl-tools /
  cntryl-stress facilities; no new runners, scripts or workflow changes.
- Record performance targets and alternating A/B evidence before accepting
  optimizations. A missing measurement is not a passing result.
- The #904 detection-window decision still needs explicit implementation
  sign-off. Keep measurement/design gates on #922/#925/#928/#939/#958; the
  user's branch workflow does not settle their technical decisions.
- Local completion, merged delivery and release/provider qualification are
  separate states. The final PR must list remaining evidence and decisions.

## Scope snapshot

The 45 open issues at batch start are:

| Group | Issues |
| --- | --- |
| Correctness and assurance prerequisites | #893, #974, #975, #976, #977, #979 |
| Commit pipeline | #904, #946, #924, #978, #940, #941, #947, #905, #906 |
| Cloud pipeline and publication | #907, #916, #914, #915, #943, #913 |
| Background work | #909, #910, #908, #912, #942, #911, #948, #949 |
| Storage formats | #926, #918, #953, #952 |
| Other hardening | #923, #927, #933 |
| Measurement and tracking | #929, #932, #935, #937 |
| Gated evaluations/designs | #922, #925, #928, #939, #958 |

The authoritative requirements are the issue bodies and
[roadmap](roadmap-0.5.0.md), including the slice dependencies from
[the WAL design](wal-pipeline-873.md). A finding refuted by a production-path
regression is triaged explicitly rather than implemented to match its text.

## Progress

- Active: #974, local/cloud durable-floor parity. The new public salvage
  regression failed because the acknowledged first value was absent; the
  parity regression failed because cloud strict recovery accepted a missing
  acknowledged frame. Implementation and qualification are in progress.
- No issue is claimed delivered by this local branch yet.
