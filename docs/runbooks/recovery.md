# Runbook: startup recovery and corruption

Recovery runs automatically on every open (`strata status --data-dir D` shows the report).

**First step for any incident:** `strata doctor --data-dir D` — read-only (never runs
recovery, never modifies files, prints no secrets). It checks formats, permissions,
lock, disk reserve, manifest, every segment (full verify + Bloom decode), WAL
(torn tail vs mid-log corruption), unreferenced/quarantined files, clock skew,
token-store mode and the audit chain; non-zero exit if any check fails.

| Symptom (error) | Meaning | Action |
|-----------------|---------|--------|
| `unavailable: data directory … is in use` | another process holds `LOCK` | stop the other process; never delete `LOCK` while it runs |
| report `wal_truncated_bytes > 0` | torn WAL tail from a crash was discarded (unacknowledged data only) | none; investigate if frequent |
| `corruption: … followed by valid frames; refusing to truncate` | damage in the middle of the last WAL segment | stop; copy the data dir; inspect; restart with ForceTruncate only accepting the reported loss |
| `corruption: … in a non-final WAL segment` / `WAL gap` | WAL damaged or segments deleted | restore from backup |
| `corruption: MANIFEST …` | manifest damaged | restore from backup (do not hand-edit) |
| `MANIFEST is missing but WAL or segment files exist` | manifest deleted | restore from backup; StrataDB refuses to reinitialize over data |
| `segment referenced by the manifest is missing` / `does not match its manifest entry` | segment deleted/replaced | restore from backup |
| report `segments_quarantined > 0` | unreferenced segment moved to `quarantine/` | inspect; delete manually only if proven redundant |
| `engine is in failed state` | WAL/manifest I/O failed (e.g. disk full) | free space / fix disk, restart; recovery re-establishes the durable state |

Always copy the whole data directory before manual intervention.
