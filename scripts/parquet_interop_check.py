#!/usr/bin/env python3
"""Independent check of a StrataDB Parquet export (ADR-0025).

Usage:
  strata export --data-dir D --tenant T --format parquet --output x.parquet
  strata export --data-dir D --tenant T --output x.ndjson
  python3 scripts/parquet_interop_check.py x.parquet x.ndjson

Reads the Parquet file with pyarrow (required) and DuckDB (if installed) and
compares every row with the NDJSON export of the same data. Exits non-zero on
any mismatch. Install with `pip install pyarrow duckdb`.
"""
import calendar
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq

EXPECTED = ["event_time", "service", "severity", "severity_text", "body", "body_json",
            "trace_id", "span_id", "event_id", "attributes", "resource_attributes"]
CANONICAL = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}


def rfc3339_ns(s):
    """'2026-11-20T10:30:00.123456789Z' -> integer nanoseconds (exact)."""
    date, frac = s[:-1].split(".")
    secs = calendar.timegm(tuple(int(x) for x in date.replace("T", "-").replace(":", "-").split("-")))
    return secs * 1_000_000_000 + int(frac.ljust(9, "0"))


def check_row(r, e, t_ns):
    assert t_ns == rfc3339_ns(e["event_time"]), (t_ns, e["event_time"])
    for k in ("service", "trace_id", "span_id", "event_id"):
        assert r[k] == e.get(k), (k, r[k], e.get(k))
    body = e.get("body")
    if isinstance(body, str):
        assert r["body"] == body and r["body_json"] is None, (r, e)
    elif body is None:
        assert r["body"] is None and r["body_json"] is None, (r, e)
    else:
        assert r["body"] is None and json.loads(r["body_json"]) == body, (r, e)
    for k in ("attributes", "resource_attributes"):
        got = json.loads(r[k]) if r[k] else {}
        assert got == e.get(k, {}), (k, got, e.get(k))
    sev = e.get("severity")
    # The NDJSON export writes the canonical name, the original text, or a number.
    if sev is None:
        assert r["severity"] is None and r["severity_text"] is None, (r, e)
    elif isinstance(sev, str) and sev in CANONICAL:
        assert r["severity"] == sev and r["severity_text"] is None, (r, e)
    else:
        assert r["severity_text"] == str(sev), (r, e)


def main(parquet_path, ndjson_path):
    t = pq.read_table(parquet_path)
    assert t.column_names == EXPECTED, t.column_names
    assert str(t.schema.field("event_time").type) == "timestamp[ns, tz=UTC]", t.schema
    # JSON columns: pyarrow >= 19 maps the Parquet JSON logical type to the
    # `arrow.json` extension type (older versions: plain string).
    for name in ("attributes", "resource_attributes", "body_json"):
        ty = t.schema.field(name).type
        assert ty == pa.string() or "json" in str(ty), (name, ty)
    times = t.column("event_time").cast(pa.int64()).to_pylist()
    # Timestamps are compared as exact int64 nanoseconds above; Python
    # datetimes cannot hold nanoseconds.
    rows = t.drop_columns(["event_time"]).to_pylist()
    events = [json.loads(line) for line in open(ndjson_path) if line.strip()]
    assert len(rows) == len(events), (len(rows), len(events))
    for r, e, ns in zip(rows, events, times):
        check_row(r, e, ns)
    print(f"pyarrow {pa.__version__}: {len(rows)} rows, schema and values match")
    try:
        import duckdb
    except ImportError:
        print("duckdb not installed: skipped")
        return
    con = duckdb.connect()
    n, ids = con.execute(
        "SELECT count(*), list(event_id ORDER BY event_time, event_id) FROM read_parquet(?)",
        [parquet_path]).fetchone()
    assert n == len(events), n
    want = sorted((rfc3339_ns(e["event_time"]), e.get("event_id")) for e in events)
    assert ids == [i for _, i in want], "DuckDB order/ids differ"
    print(f"duckdb {duckdb.__version__}: {n} rows readable, ids and order match")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(*sys.argv[1:3])
