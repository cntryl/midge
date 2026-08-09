## Summary

## Linked issues

Closes #

## Acceptance audit

- [ ] Criterion: Copy one acceptance criterion from the linked issue.
  Evidence: Name the new or materially strengthened test and what exact result it asserts.
  Production entry point: Identify the shipping path exercised, or explain why this is a local invariant.
  Resolution: State whether this matches the requested approach; document any intentional alternative.

## Risk and compatibility

## Verification

## Review checklist

- [ ] Behavioral tests use the real production entry point, not only private helpers.
- [ ] New enum variants update `tests/coverage_manifests.rs`.
- [ ] Coexisting implementations have a differential test, or the unused variant is removed.
- [ ] Shared test infrastructure uses poison-tolerant locks and consistent failpoint gates.
- [ ] Failpoint governance relies on mechanical call-site discovery, not a hardcoded allowlist.
- [ ] Expensive coverage, mutation, real-provider, and Sqrzl checks remain scheduled/manual unless their CI cost is explicitly approved.
- [ ] Every linked issue criterion is represented above; pre-existing or renamed coverage is not claimed as new evidence.
