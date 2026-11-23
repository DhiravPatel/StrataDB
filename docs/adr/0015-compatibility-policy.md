# ADR-0015: Compatibility and upgrade policy

- **Status:** Accepted
- **Spec references:** §30, COMPATIBILITY.md

## Decision
- Independently versioned formats: WAL v1, segment v1, event encoding v1,
  manifest v1 (and later index, backup, API).
- Readers reject unknown versions with `Unsupported` — never guess.
- Before 1.0, a format bump may drop old-version readers **only** with an
  offline migration tool and release-note warning. From 1.0, readers support
  the previous major format version.
- Format changes require an RFC and compatibility fixtures under
  `tests/fixtures/` (golden files written by the old version).
