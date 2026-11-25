#!/usr/bin/env bash
# End-to-end Parquet interop check (ADR-0025): varied events -> strata ingest
# -> Parquet + NDJSON export -> independent read with pyarrow (and DuckDB).
# Usage: PYTHON=/path/to/python-with-pyarrow scripts/parquet_interop.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
"$PY" -c "import pyarrow" || { echo "pyarrow missing: pip install pyarrow duckdb" >&2; exit 2; }
cargo build -q -p stratadb-cli
S=./target/debug/strata
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
"$PY" - "$W/in.ndjson" <<'PYEOF'
import json, sys, time
now = time.time_ns()
with open(sys.argv[1], "w") as f:
    for i in range(500):
        o = {"event_time": now - (500 - i) * 1_000_003, "service": "svc%d" % (i % 3),
             "body": ["plain %d" % i, {"k": [1, 2.5, None, True], "i": i},
                      'quote " comma , and\nnewline', 18446744073709551000 - i][i % 4],
             "trace_id": "%032x" % i, "event_id": "e%d" % i,
             "attributes": {"n": i, "f": i + 0.5, "nested": {"a": [i]}},
             "resource": {"host": "h%d" % (i % 2)}}
        sev = ["ERROR", "warning", 17, "boom", None][i % 5]
        if sev is not None:
            o["severity"] = sev
        f.write(json.dumps(o) + "\n")
PYEOF
$S init --data-dir "$W/d" >/dev/null
$S ingest --data-dir "$W/d" --tenant t --file "$W/in.ndjson" >/dev/null
$S export --data-dir "$W/d" --tenant t --format parquet --output "$W/x.parquet" --page-size 128 >/dev/null
$S export --data-dir "$W/d" --tenant t --output "$W/x.ndjson" >/dev/null
"$PY" scripts/parquet_interop_check.py "$W/x.parquet" "$W/x.ndjson"
