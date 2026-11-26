# Benchmark methodology

Every report must use the spec Appendix C template and include:
- exact hardware, OS and filesystem, plus the hardware profile label
  (spec VIII.27; laptops are **HW-REF-7**);
- StrataDB commit and build profile;
- competitor versions and configurations;
- dataset generator and seed;
- durability mode;
- cache state;
- repetitions (≥ 5) with p50/p95/p99 and variance;
- raw JSON results committed under `benchmarks/reports/`;
- exact reproduction commands.

Comparisons require equivalent durability, data, query results, cache state
and concurrency (spec §27.6). Developer-laptop numbers must be labelled as
such and never presented as production capacity.

New measurements use the scenario harness (docs/benchmarks/harness.md). It
writes the manifest, raw samples, confidence intervals and plan assertions
automatically. Before/after comparisons on shared machines should use CPU
time and interleaved runs, and must state the observed noise floor
(see `benchmarks/reports/2026-11-26-p0-measurement-system.md`).
