## Summary

## Risk and compatibility

## Verification

## Review checklist

- [ ] Behavioral tests use the real production entry point, not only private helpers.
- [ ] New enum variants update `tests/coverage_manifests.rs`.
- [ ] Coexisting implementations have a differential test, or the unused variant is removed.
- [ ] Shared test infrastructure uses poison-tolerant locks and consistent failpoint gates.
- [ ] Failpoint governance relies on mechanical call-site discovery, not a hardcoded allowlist.
- [ ] Expensive coverage, mutation, real-provider, and Sqrzl checks remain scheduled/manual unless their CI cost is explicitly approved.
