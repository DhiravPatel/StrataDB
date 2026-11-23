# ADR-0013: Authentication and authorization

- **Status:** Proposed
- **Spec references:** §18.2–§18.4

## Proposal
- API tokens: random 256-bit secrets shown once; server stores only a SHA-256
  verifier keyed by token ID; constant-time comparison (`subtle`); expiry and
  revocation in the catalog.
- No default credentials. First start creates an admin token only via an
  explicit `strata init` bootstrap step that prints it once.
- RBAC permissions exactly as spec §18.3. Checks live in service methods
  (storage `write_batch` already re-validates tenant ownership).
- Tenant is derived from the principal; queries receive a tenant-scoped
  `Snapshot`, so the executor cannot address another tenant's data.
