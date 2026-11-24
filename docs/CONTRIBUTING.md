# Contributing

- Every behaviour change needs tests (normal and failure paths). Every bug
  fix needs a regression test.
- Run `scripts/check.sh` before sending a change; CI runs the same commands.
- Persistent-format or durability-protocol changes require an RFC
  (`docs/rfcs/`) and an ADR update, version bump, and compatibility fixtures.
- New dependencies require an entry in `docs/project/DEPENDENCY_REVIEW.md` (license,
  maintenance, security history, why it is needed). Prefer none.
- `unsafe` is forbidden workspace-wide; lifting that requires an ADR with
  measured justification and documented invariants.
- Do not commit secrets, credentials, or real production data; benchmark
  fixtures must be synthetic.
- Performance claims require a benchmark report following
  `docs/benchmarks/methodology.md`.
- Sign-off/DCO policy: to be decided with the license (ADR-0016).
