# ADR-0016: Project license

- **Status:** Proposed — **requires project-owner sign-off** (legal decision)
- **Spec references:** §31.1

## Review
All current runtime dependencies are permissive: MIT, Apache-2.0, MIT OR
Apache-2.0, Unlicense OR MIT, and Unicode-3.0 (unicode-ident). See
DEPENDENCY_REVIEW.md.

## Recommendation
**Apache-2.0**: OSI-approved, explicit patent grant (spec asks for patent
review), compatible with every current dependency, and the Rust-ecosystem norm.
Dual MIT/Apache-2.0 is the alternative if maximum permissiveness is preferred.

## Status of the repository
No `LICENSE` file and no `license` field are present until the owner confirms;
`publish = false` prevents accidental publication.
