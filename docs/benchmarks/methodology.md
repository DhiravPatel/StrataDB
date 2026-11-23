# Benchmark methodology

**No benchmark results exist yet.** Every future report must use the spec
Appendix C template and include: exact hardware/OS/filesystem, StrataDB
commit and build profile, competitor versions and configs, dataset generator
+ seed, durability mode, cache state (cold = after `purge` / page-cache drop,
documented), repetitions (≥ 5) with p50/p95/p99 and variance, raw JSON
results committed under `benchmarks/reports/`, and exact reproduction
commands. Comparisons require equivalent durability, data, query results,
cache state and concurrency (spec §27.6). Developer-laptop numbers must be
labelled as such and never presented as production capacity.
