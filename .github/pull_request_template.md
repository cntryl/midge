## Summary

## Why are you making this contribution?

Explain how you encountered the problem or need, who or what it affects, and why
this repository and scope are appropriate.

## Linked issues

Closes #

## Acceptance audit

- [ ] Criterion: Copy one acceptance criterion from the linked issue.
  Evidence: Name the new or materially strengthened test and what exact result it asserts.
  Production entry point: Identify the shipping path exercised, or explain why this is a local invariant.
  Resolution: State whether this matches the requested approach; document any intentional alternative.

## Risk and compatibility

## Verification

List the exact tests, checks, or manual verification performed and their results.

## Tool assistance disclosure

Select exactly one:

- [ ] No AI or other generative tool materially assisted this contribution.
- [ ] AI or another generative tool materially assisted this contribution.

If assisted, identify the kind of tool used, what it assisted, and how you
reviewed and validated the resulting work. Do not include private prompts,
credentials, or confidential information.

## Contributor responsibility

- [ ] I understand the complete change and can explain or revise it.
- [ ] I reviewed the complete diff.
- [ ] I reported validation accurately and did not claim checks I did not run.
- [ ] I disclosed material generated assistance.
- [ ] I have the right to submit this work under the repository's license.

## Review checklist

- [ ] Behavioral tests use the real production entry point, not only private helpers.
- [ ] New enum variants update `tests/coverage_manifests.rs`.
- [ ] Coexisting implementations have a differential test, or the unused variant is removed.
- [ ] Shared test infrastructure uses poison-tolerant locks and consistent failpoint gates.
- [ ] Failpoint governance relies on mechanical call-site discovery, not a hardcoded allowlist.
- [ ] Expensive coverage and mutation checks remain scheduled/manual unless their CI cost is explicitly approved; Sqrzl remains the self-contained cloud qualification authority.
- [ ] A real-provider behavior difference is reproduced in Sqrzl and retained as a Midge regression test where applicable.
- [ ] Every linked issue criterion is represented above; pre-existing or renamed coverage is not claimed as new evidence.
