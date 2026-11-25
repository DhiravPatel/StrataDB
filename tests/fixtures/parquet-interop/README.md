# Parquet interop fixtures (ADR-0025)

Written by independent Parquet implementations — **not** by StrataDB — so the
CLI import is tested against real third-party files. Never regenerate them to
make a test pass.

| File | Writer | Codec | Rows |
|------|--------|-------|------|
| `pyarrow_snappy.parquet` | pyarrow 26.0.1 | SNAPPY (pyarrow default) | 3 |
| `pyarrow_gzip.parquet` | pyarrow 26.0.1 | GZIP | 3 |
| `pyarrow_lz4.parquet` | pyarrow 26.0.1 | LZ4_RAW | 3 |
| `pyarrow_zstd.parquet` | pyarrow 26.0.1 | ZSTD (unsupported → clear error) | 3 |
| `duckdb_snappy.parquet` | DuckDB 2.0.1 | SNAPPY (DuckDB default) | 2 |

pyarrow schema: `timestamp` timestamp[ns, UTC]; `message` string; `level`
string (nullable); `status` int32; `ratio` double; `ok` bool;
`ctx` struct<user string, tries int64>; `tags` list<string>;
`labels` map<string, string>. DuckDB: `event_time` TIMESTAMPTZ (µs), `body`,
`severity`, `n` BIGINT. Generation script: see the commit that added this
directory (pyarrow `pq.write_table(t, path, compression=c)`; DuckDB
`COPY (...) TO 'f' (FORMAT parquet)`).
