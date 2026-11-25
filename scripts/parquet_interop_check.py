#!/usr/bin/env python3
"""Independent check of a StrataDB Parquet export with pyarrow (ADR-0025).

Usage: strata export --data-dir D --tenant T --format parquet --output x.parquet
       python3 scripts/parquet_interop_check.py x.parquet x.ndjson
where x.ndjson is an NDJSON export of the same data (strata export ... --output x.ndjson).
Exits non-zero on any mismatch. Requires `pip install pyarrow`.
"""
import json
import sys

import pyarrow.parquet as pq

EXPECTED = ["event_time", "service", "severity", "severity_text", "body", "body_json",
            "trace_id", "span_id", "event_id", "attributes", "resource_attributes"]


def main(parquet_path, ndjson_path):
    t = pq.read_table(parquet_path)
    assert t.column_names == EXPECTED, t.column_names
    assert str(t.schema.field("event_time").type) == "timestamp[ns, tz=UTC]", t.schema
    rows = t.to_pylist()
    events = [json.loads(l) for l in open(ndjson_path) if l.strip()]
    assert len(rows) == len(events), (len(rows), len(events))
    for r, e in zip(rows, events):
        assert r["event_id"] == e.get("event_id"), (r, e)
        body = e.get("body")
        if isinstance(body, str):
            assert r["body"] == body and r["body_json"] is None, (r, e)
        elif body is not None:
            assert json.loads(r["body_json"]) == body, (r, e)
        assert (json.loads(r["attributes"]) if r["attributes"] else {}) == e.get("attributes", {})
    print(f"ok: {len(rows)} rows, schema and values match")


if __name__ == "__main__":
    main(*sys.argv[1:3])
