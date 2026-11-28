//! Prometheus text format (0.0.4) parsing and the dashboard summary.
//!
//! [`parse`] turns `GET /api/v1/metrics` into families of samples (bounded
//! in lines and samples). [`summarize`] picks the series the dashboard shows
//! (docs/operations/metrics.md) and, given the previous scrape of the same
//! server, turns counters into per-second rates and histograms into
//! quantiles over the interval between the two scrapes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;
use serde_json::{json, Map, Value};

/// Most samples accepted from one scrape.
pub const MAX_SAMPLES: usize = 200_000;
/// Longest line accepted.
pub const MAX_LINE: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

impl Kind {
    fn parse(s: &str) -> Self {
        match s {
            "counter" => Kind::Counter,
            "gauge" => Kind::Gauge,
            "histogram" => Kind::Histogram,
            "summary" => Kind::Summary,
            _ => Kind::Untyped,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    /// Non-finite values (`NaN`, `±Inf`) serialize as JSON `null`.
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Family {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: Kind,
    pub help: String,
    pub samples: Vec<Sample>,
}

/// One parsed scrape.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Scrape {
    pub families: Vec<Family>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics line {}: {}", self.line, self.message)
    }
}

fn is_name_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c == b':'
}

fn is_name_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':'
}

fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse::<f64>().ok(),
    }
}

/// Parse one sample line: `name{l="v",...} value [timestamp]`.
fn parse_sample(line: &str) -> Result<Sample, String> {
    let b = line.as_bytes();
    if b.is_empty() || !is_name_start(b[0]) {
        return Err("expected a metric name".into());
    }
    let mut i = 1;
    while i < b.len() && is_name_char(b[i]) {
        i += 1;
    }
    let name = line[..i].to_owned();
    let mut labels = BTreeMap::new();
    if i < b.len() && b[i] == b'{' {
        i += 1;
        loop {
            while i < b.len() && (b[i] == b' ' || b[i] == b',') {
                i += 1;
            }
            if i >= b.len() {
                return Err("unterminated label set".into());
            }
            if b[i] == b'}' {
                i += 1;
                break;
            }
            let start = i;
            if !is_name_start(b[i]) {
                return Err("expected a label name".into());
            }
            while i < b.len() && is_name_char(b[i]) {
                i += 1;
            }
            let key = line[start..i].to_owned();
            if i + 1 >= b.len() || b[i] != b'=' || b[i + 1] != b'"' {
                return Err(format!("expected =\" after label {key}"));
            }
            i += 2;
            let mut val = String::new();
            let mut chars = line[i..].char_indices();
            let mut closed = None;
            while let Some((off, c)) = chars.next() {
                match c {
                    '"' => {
                        closed = Some(off);
                        break;
                    }
                    '\\' => match chars.next() {
                        Some((_, 'n')) => val.push('\n'),
                        Some((_, '\\')) => val.push('\\'),
                        Some((_, '"')) => val.push('"'),
                        Some((_, other)) => {
                            val.push('\\');
                            val.push(other);
                        }
                        None => return Err("unterminated label value".into()),
                    },
                    c => val.push(c),
                }
            }
            let Some(off) = closed else {
                return Err("unterminated label value".into());
            };
            i += off + 1;
            labels.insert(key, val);
        }
    }
    let rest = line[i..].trim();
    let mut parts = rest.split_ascii_whitespace();
    let Some(v) = parts.next() else {
        return Err("missing sample value".into());
    };
    let value = parse_value(v).ok_or_else(|| format!("invalid sample value {v:?}"))?;
    if let Some(ts) = parts.next() {
        if ts.parse::<i64>().is_err() {
            return Err(format!("invalid timestamp {ts:?}"));
        }
    }
    if parts.next().is_some() {
        return Err("trailing characters after the sample".into());
    }
    Ok(Sample {
        name,
        labels,
        value,
    })
}

/// Name of the family a sample belongs to, given the declared families.
fn family_of<'a>(sample: &'a str, types: &HashMap<String, Kind>) -> &'a str {
    for suffix in ["_bucket", "_sum", "_count", "_created"] {
        if let Some(base) = sample.strip_suffix(suffix) {
            if matches!(
                types.get(base),
                Some(Kind::Histogram | Kind::Summary | Kind::Counter)
            ) {
                return base;
            }
        }
    }
    sample
}

fn family_entry<'a>(
    fams: &'a mut HashMap<String, Family>,
    order: &mut Vec<String>,
    name: &str,
    kind: Kind,
) -> &'a mut Family {
    if !fams.contains_key(name) {
        order.push(name.to_owned());
    }
    fams.entry(name.to_owned()).or_insert_with(|| Family {
        name: name.to_owned(),
        kind,
        help: String::new(),
        samples: Vec::new(),
    })
}

/// Parse a Prometheus text exposition.
pub fn parse(text: &str) -> Result<Scrape, ParseError> {
    let mut order: Vec<String> = Vec::new();
    let mut fams: HashMap<String, Family> = HashMap::new();
    let mut types: HashMap<String, Kind> = HashMap::new();
    let mut n_samples = 0usize;
    for (idx, raw) in text.lines().enumerate() {
        let lineno = idx + 1;
        let err = |message: String| ParseError {
            line: lineno,
            message,
        };
        if raw.len() > MAX_LINE {
            return Err(err(format!("line longer than {MAX_LINE} bytes")));
        }
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(meta) = line.strip_prefix('#') {
            let meta = meta.trim_start();
            let mut it = meta.splitn(3, ' ');
            match (it.next(), it.next(), it.next()) {
                (Some("TYPE"), Some(name), Some(t)) => {
                    let k = Kind::parse(t.trim());
                    types.insert(name.to_owned(), k);
                    family_entry(&mut fams, &mut order, name, k).kind = k;
                }
                (Some("HELP"), Some(name), help) => {
                    family_entry(&mut fams, &mut order, name, Kind::Untyped).help = help
                        .unwrap_or("")
                        .replace("\\n", "\n")
                        .replace("\\\\", "\\");
                }
                _ => {} // other comments
            }
            continue;
        }
        let s = parse_sample(line).map_err(err)?;
        n_samples += 1;
        if n_samples > MAX_SAMPLES {
            return Err(err(format!("more than {MAX_SAMPLES} samples")));
        }
        // The TYPE line precedes its samples in a valid exposition.
        let fam = family_of(&s.name, &types).to_owned();
        let kind = types.get(&fam).copied().unwrap_or(Kind::Untyped);
        family_entry(&mut fams, &mut order, &fam, kind)
            .samples
            .push(s);
    }
    Ok(Scrape {
        families: order.into_iter().filter_map(|n| fams.remove(&n)).collect(),
    })
}

/// A cumulative histogram: `(upper bound, cumulative count)` in bound order.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    pub buckets: Vec<(f64, f64)>,
    pub sum: f64,
    pub count: f64,
}

impl Histogram {
    /// The `q` quantile, interpolated linearly within the bucket as
    /// Prometheus' `histogram_quantile` does. `None` without observations.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let total = self.buckets.last().map(|b| b.1)?;
        if total <= 0.0 || !(0.0..=1.0).contains(&q) {
            return None;
        }
        let rank = q * total;
        let mut prev_bound = 0.0;
        let mut prev_count = 0.0;
        for &(le, c) in &self.buckets {
            if c >= rank {
                if le.is_infinite() {
                    // Above the highest finite bound: report that bound.
                    return Some(prev_bound);
                }
                let in_bucket = c - prev_count;
                if in_bucket <= 0.0 {
                    return Some(le);
                }
                return Some(prev_bound + (le - prev_bound) * ((rank - prev_count) / in_bucket));
            }
            prev_bound = le;
            prev_count = c;
        }
        Some(prev_bound)
    }

    pub fn mean(&self) -> Option<f64> {
        (self.count > 0.0).then(|| self.sum / self.count)
    }

    /// Observations between `prev` and `self` (counter resets give `None`).
    pub fn since(&self, prev: &Histogram) -> Option<Histogram> {
        if prev.buckets.len() != self.buckets.len() || self.count < prev.count {
            return None;
        }
        let mut buckets = Vec::with_capacity(self.buckets.len());
        for (a, b) in self.buckets.iter().zip(&prev.buckets) {
            if a.0 != b.0 || a.1 < b.1 {
                return None;
            }
            buckets.push((a.0, a.1 - b.1));
        }
        Some(Histogram {
            buckets,
            sum: self.sum - prev.sum,
            count: self.count - prev.count,
        })
    }
}

impl Scrape {
    pub fn family(&self, name: &str) -> Option<&Family> {
        self.families.iter().find(|f| f.name == name)
    }

    /// Every sample with exactly this series name, whatever its family.
    fn series<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Sample> + 'a {
        self.families
            .iter()
            .flat_map(|f| f.samples.iter())
            .filter(move |s| s.name == name)
    }

    fn matches(s: &Sample, filter: &[(&str, &str)]) -> bool {
        filter
            .iter()
            .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
    }

    /// Sum of the samples named `name` (exact series name) matching `filter`.
    pub fn value(&self, name: &str, filter: &[(&str, &str)]) -> Option<f64> {
        let mut found = false;
        let mut total = 0.0;
        for s in self.series(name) {
            if Self::matches(s, filter) {
                found = true;
                total += s.value;
            }
        }
        found.then_some(total)
    }

    /// Values of `name` keyed by the label `by`.
    pub fn by_label(&self, name: &str, by: &str) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        for s in self.series(name) {
            if let Some(k) = s.labels.get(by) {
                *out.entry(k.clone()).or_insert(0.0) += s.value;
            }
        }
        out
    }

    /// The histogram family `name`, summed over every label except `le`
    /// among the samples matching `filter`.
    pub fn histogram(&self, name: &str, filter: &[(&str, &str)]) -> Option<Histogram> {
        let fam = self.family(name)?;
        let mut by_le: BTreeMap<OrdF64, f64> = BTreeMap::new();
        let (mut sum, mut count, mut any) = (0.0, 0.0, false);
        let bucket = format!("{name}_bucket");
        let sum_name = format!("{name}_sum");
        let count_name = format!("{name}_count");
        for s in fam.samples.iter().filter(|s| Self::matches(s, filter)) {
            if s.name == bucket {
                let Some(le) = s.labels.get("le").and_then(|v| parse_value(v)) else {
                    continue;
                };
                *by_le.entry(OrdF64(le)).or_insert(0.0) += s.value;
                any = true;
            } else if s.name == sum_name {
                sum += s.value;
            } else if s.name == count_name {
                count += s.value;
            }
        }
        any.then(|| Histogram {
            buckets: by_le.into_iter().map(|(k, v)| (k.0, v)).collect(),
            sum,
            count,
        })
    }
}

/// Total order for bucket bounds (no NaN reaches here: `le` NaN is skipped).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

fn num(v: Option<f64>) -> Value {
    match v {
        Some(x) if x.is_finite() => json!(x),
        _ => Value::Null,
    }
}

/// A label filter: every `(label, value)` must match.
type Filter = &'static [(&'static str, &'static str)];
/// (dashboard key, series or family name, label filter)
type Pick = (&'static str, &'static str, Filter);

/// Histograms shown on the dashboard: (key, family, label filter).
const LATENCIES: &[Pick] = &[
    ("query", "stratadb_query_seconds", &[]),
    (
        "ingest",
        "stratadb_ingest_ack_seconds",
        &[("endpoint", "ingest")],
    ),
    (
        "otlp_ingest",
        "stratadb_ingest_ack_seconds",
        &[("endpoint", "otlp")],
    ),
    ("wal_sync", "stratadb_wal_sync_seconds", &[]),
    ("flush", "stratadb_flush_seconds", &[]),
];

/// Counters turned into per-second rates: (key, series, filter).
const RATES: &[Pick] = &[
    ("ingest_events_per_s", "stratadb_events_written_total", &[]),
    (
        "ingest_batches_per_s",
        "stratadb_batches_written_total",
        &[],
    ),
    ("queries_per_s", "stratadb_queries_total", &[]),
    (
        "query_errors_per_s",
        "stratadb_queries_total",
        &[("outcome", "error")],
    ),
    (
        "query_rejections_per_s",
        "stratadb_queries_total",
        &[("outcome", "rejected")],
    ),
    (
        "rows_scanned_per_s",
        "stratadb_query_rows_scanned_total",
        &[],
    ),
    (
        "segment_bytes_read_per_s",
        "stratadb_query_segment_bytes_read_total",
        &[],
    ),
    ("wal_bytes_per_s", "strata_wal_bytes_total", &[]),
];

/// Point-in-time gauges and totals: (key, series).
const GAUGES: &[(&str, &str)] = &[
    ("events_written_total", "stratadb_events_written_total"),
    ("memtable_bytes", "stratadb_memtable_bytes"),
    ("memtable_rows", "stratadb_memtable_rows"),
    ("segment_bytes", "stratadb_segment_bytes"),
    ("segments", "stratadb_segments"),
    ("disk_free_bytes", "stratadb_disk_free_bytes"),
    ("engine_failed", "stratadb_engine_failed"),
    ("wal_durable_seq", "stratadb_wal_durable_seq"),
    ("write_stalls_total", "stratadb_write_stalls_total"),
    ("write_rejections_total", "stratadb_write_rejections_total"),
    ("quota_rejections_total", "stratadb_quota_rejections_total"),
    ("compactions_total", "stratadb_compactions_total"),
    ("flush_failures_total", "stratadb_flush_failures_total"),
    ("snapshots_open", "strata_snapshots_open"),
    (
        "snapshot_oldest_age_seconds",
        "strata_snapshot_oldest_age_seconds",
    ),
    ("obsolete_retained_bytes", "strata_obsolete_retained_bytes"),
    ("memory_budget_bytes", "strata_memory_budget_bytes"),
    ("memory_resident_bytes", "strata_memory_resident_bytes"),
    ("memory_untracked_bytes", "strata_memory_untracked_bytes"),
    ("thread_budget", "strata_thread_budget"),
    ("scan_pool_workers", "strata_scan_pool_workers"),
    ("blocking_pool_busy", "strata_blocking_pool_busy"),
    ("blocking_pool_queued", "strata_blocking_pool_queued"),
    ("query_log_dropped_total", "strata_query_log_dropped_total"),
];

fn latency_json(h: Option<&Histogram>) -> Value {
    let ms = |v: Option<f64>| num(v.map(|s| s * 1000.0));
    match h {
        None => Value::Null,
        Some(h) => json!({
            "count": h.count,
            "mean_ms": ms(h.mean()),
            "p50_ms": ms(h.quantile(0.50)),
            "p95_ms": ms(h.quantile(0.95)),
            "p99_ms": ms(h.quantile(0.99)),
        }),
    }
}

/// The dashboard summary of `cur`; with `prev` (and the seconds between the
/// two scrapes), rates and interval latencies.
pub fn summarize(cur: &Scrape, prev: Option<(&Scrape, f64)>) -> Value {
    let prev = prev.filter(|(_, dt)| *dt > 0.0);
    let mut gauges = Map::new();
    for (k, series) in GAUGES {
        gauges.insert((*k).to_owned(), num(cur.value(series, &[])));
    }
    let mut rates = Map::new();
    for (k, series, filter) in RATES {
        let r = prev.and_then(|(p, dt)| {
            let (a, b) = (cur.value(series, filter)?, p.value(series, filter)?);
            // A counter reset (server restart) makes the delta meaningless.
            (a >= b).then(|| (a - b) / dt)
        });
        rates.insert((*k).to_owned(), num(r));
    }
    let mut lifetime = Map::new();
    let mut interval = Map::new();
    for (k, fam, filter) in LATENCIES {
        let h = cur.histogram(fam, filter);
        lifetime.insert((*k).to_owned(), latency_json(h.as_ref()));
        let d = match (&h, prev) {
            (Some(h), Some((p, _))) => p.histogram(fam, filter).and_then(|ph| h.since(&ph)),
            _ => None,
        };
        interval.insert((*k).to_owned(), latency_json(d.as_ref()));
    }
    let tracked = cur.by_label("strata_memory_tracked_bytes", "region");
    let limits = cur.by_label("strata_memory_limit_bytes", "region");
    let peaks = cur.by_label("strata_memory_peak_bytes", "region");
    let regions: Vec<Value> = tracked
        .iter()
        .map(|(r, used)| {
            json!({
                "region": r,
                "tracked_bytes": used,
                "limit_bytes": num(limits.get(r).copied()),
                "peak_bytes": num(peaks.get(r).copied()),
            })
        })
        .collect();
    json!({
        "interval_s": num(prev.map(|p| p.1)),
        "gauges": gauges,
        "rates": rates,
        "queries_by_outcome": cur.by_label("stratadb_queries_total", "outcome"),
        "threads_by_role": cur.by_label("strata_threads", "role"),
        "workload": {
            "slots": cur.by_label("strata_workload_slots", "class"),
            "running": cur.by_label("strata_workload_running", "class"),
            "queued": cur.by_label("strata_workload_queued", "class"),
        },
        "latency": {"lifetime": lifetime, "interval": interval},
        "memory_regions": regions,
        "families": cur.families.len(),
    })
}

/// The previous scrape per connection, for rates. Bounded: one entry per
/// connection name, at most `cap` names (the oldest is dropped).
#[derive(Debug)]
pub struct Tracker {
    cap: usize,
    last: Mutex<HashMap<String, (Instant, Scrape)>>,
}

/// Scrapes closer together than this reuse the older baseline, so several
/// browser tabs polling at once do not shrink the interval to nothing.
const MIN_INTERVAL_S: f64 = 1.0;

impl Tracker {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Summarize `cur` against the previous scrape of `conn`, then remember it.
    pub fn observe(&self, conn: &str, cur: Scrape) -> Value {
        self.observe_at(conn, cur, Instant::now())
    }

    pub fn observe_at(&self, conn: &str, cur: Scrape, now: Instant) -> Value {
        let mut g = self.last.lock().unwrap_or_else(|e| e.into_inner());
        let prev = g
            .get(conn)
            .map(|(t, s)| (now.saturating_duration_since(*t).as_secs_f64(), s));
        let summary = match prev {
            Some((dt, p)) if dt > 0.0 => summarize(&cur, Some((p, dt))),
            _ => summarize(&cur, None),
        };
        if matches!(prev, Some((dt, _)) if dt < MIN_INTERVAL_S) {
            // Too soon after the baseline: keep it for the next poll.
            return summary;
        }
        if g.len() >= self.cap && !g.contains_key(conn) {
            if let Some(oldest) = g
                .iter()
                .min_by_key(|(_, (t, _))| *t)
                .map(|(k, _)| k.clone())
            {
                g.remove(&oldest);
            }
        }
        g.insert(conn.to_owned(), (now, cur));
        summary
    }

    pub fn forget(&self, conn: &str) {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(conn);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
    use super::*;
    use std::time::Duration;

    const TEXT: &str = r#"# HELP stratadb_queries_total Queries by outcome.
# TYPE stratadb_queries_total counter
stratadb_queries_total{outcome="ok"} 10
stratadb_queries_total{outcome="error"} 2
# HELP stratadb_query_seconds Query request latency (all outcomes).
# TYPE stratadb_query_seconds histogram
stratadb_query_seconds_bucket{le="0.001"} 2
stratadb_query_seconds_bucket{le="0.01"} 6
stratadb_query_seconds_bucket{le="0.1"} 10
stratadb_query_seconds_bucket{le="+Inf"} 12
stratadb_query_seconds_sum 0.5
stratadb_query_seconds_count 12
# TYPE stratadb_ingest_ack_seconds histogram
stratadb_ingest_ack_seconds_bucket{endpoint="ingest",le="0.01"} 4
stratadb_ingest_ack_seconds_bucket{endpoint="ingest",le="+Inf"} 4
stratadb_ingest_ack_seconds_sum{endpoint="ingest"} 0.02
stratadb_ingest_ack_seconds_count{endpoint="ingest"} 4
stratadb_ingest_ack_seconds_bucket{endpoint="otlp",le="0.01"} 0
stratadb_ingest_ack_seconds_bucket{endpoint="otlp",le="+Inf"} 1
stratadb_ingest_ack_seconds_sum{endpoint="otlp"} 1
stratadb_ingest_ack_seconds_count{endpoint="otlp"} 1
# TYPE strata_memory_tracked_bytes gauge
strata_memory_tracked_bytes{region="ingest"} 774
strata_memory_tracked_bytes{region="query"} 0
# TYPE strata_memory_limit_bytes gauge
strata_memory_limit_bytes{region="ingest"} 2000
stratadb_events_written_total 100
weird_label{path="a\"b\\c\nd",x="y"} NaN 1700000000000
"#;

    #[test]
    fn parses_families_labels_and_escapes() {
        let s = parse(TEXT).unwrap();
        let q = s.family("stratadb_queries_total").unwrap();
        assert_eq!(q.kind, Kind::Counter);
        assert_eq!(q.help, "Queries by outcome.");
        assert_eq!(q.samples.len(), 2);
        let h = s.family("stratadb_query_seconds").unwrap();
        assert_eq!(h.kind, Kind::Histogram);
        assert_eq!(h.samples.len(), 6);
        let w = s.family("weird_label").unwrap();
        assert_eq!(w.kind, Kind::Untyped);
        assert_eq!(w.samples[0].labels["path"], "a\"b\\c\nd");
        assert!(w.samples[0].value.is_nan());
        assert_eq!(s.value("stratadb_events_written_total", &[]), Some(100.0));
        assert_eq!(s.value("stratadb_queries_total", &[]), Some(12.0));
        assert_eq!(
            s.value("stratadb_queries_total", &[("outcome", "error")]),
            Some(2.0)
        );
        // NaN serializes as null, so the JSON is valid.
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"value\":null"), "{j}");
    }

    #[test]
    fn rejects_malformed_lines() {
        for bad in [
            "1abc 1",
            "x{a=\"1\" 2",
            "x{a=1} 2",
            "x",
            "x abc",
            "x 1 2 3",
            "x 1 notatime",
        ] {
            let e = parse(bad).unwrap_err();
            assert_eq!(e.line, 1, "{bad}: {e}");
        }
        let long = format!("x{{a=\"{}\"}} 1", "v".repeat(MAX_LINE));
        assert!(parse(&long).is_err());
    }

    #[test]
    fn histogram_quantiles_interpolate_like_prometheus() {
        let s = parse(TEXT).unwrap();
        let h = s.histogram("stratadb_query_seconds", &[]).unwrap();
        assert_eq!(h.count, 12.0);
        // rank 6 of 12 → exactly the 0.01 bound.
        assert!((h.quantile(0.5).unwrap() - 0.01).abs() < 1e-12);
        // rank 3 → in (0.001, 0.01]: 0.001 + 0.009 * (1/4).
        assert!((h.quantile(0.25).unwrap() - 0.00325).abs() < 1e-12);
        // rank 11.88 → in +Inf: highest finite bound.
        assert_eq!(h.quantile(0.99), Some(0.1));
        assert_eq!(h.mean(), Some(0.5 / 12.0));
        // Label filters select one endpoint.
        let i = s
            .histogram("stratadb_ingest_ack_seconds", &[("endpoint", "ingest")])
            .unwrap();
        assert_eq!((i.count, i.buckets.len()), (4.0, 2));
        let all = s.histogram("stratadb_ingest_ack_seconds", &[]).unwrap();
        assert_eq!(all.count, 5.0);
        let empty = Histogram {
            buckets: vec![(1.0, 0.0), (f64::INFINITY, 0.0)],
            sum: 0.0,
            count: 0.0,
        };
        assert_eq!(empty.quantile(0.5), None);
    }

    #[test]
    fn interval_histograms_and_resets() {
        let a = Histogram {
            buckets: vec![(0.1, 1.0), (1.0, 2.0), (f64::INFINITY, 2.0)],
            sum: 0.5,
            count: 2.0,
        };
        let b = Histogram {
            buckets: vec![(0.1, 1.0), (1.0, 5.0), (f64::INFINITY, 5.0)],
            sum: 2.0,
            count: 5.0,
        };
        let d = b.since(&a).unwrap();
        assert_eq!(d.count, 3.0);
        assert_eq!(d.buckets[0].1, 0.0);
        assert!(a.since(&b).is_none(), "counter reset");
    }

    #[test]
    fn summary_rates_and_tracker() {
        let s1 = parse(TEXT).unwrap();
        let text2 = TEXT
            .replace(
                "stratadb_events_written_total 100",
                "stratadb_events_written_total 300",
            )
            .replace("{outcome=\"ok\"} 10", "{outcome=\"ok\"} 30");
        let s2 = parse(&text2).unwrap();
        let first = summarize(&s1, None);
        assert_eq!(first["rates"]["ingest_events_per_s"], Value::Null);
        assert_eq!(first["gauges"]["events_written_total"], json!(100.0));
        assert_eq!(first["queries_by_outcome"]["error"], json!(2.0));
        assert_eq!(first["memory_regions"][0]["region"], "ingest");
        assert_eq!(first["memory_regions"][0]["limit_bytes"], json!(2000.0));
        assert_eq!(first["memory_regions"][1]["limit_bytes"], Value::Null);
        assert!(first["latency"]["lifetime"]["query"]["p50_ms"].is_number());

        let t = Tracker::new(2);
        let t0 = Instant::now();
        let a = t.observe_at("x", s1.clone(), t0);
        assert_eq!(a["interval_s"], Value::Null);
        let b = t.observe_at("x", s2, t0 + Duration::from_secs(2));
        assert_eq!(b["interval_s"], json!(2.0));
        assert_eq!(b["rates"]["ingest_events_per_s"], json!(100.0));
        assert_eq!(b["rates"]["queries_per_s"], json!(10.0));
        // No new queries in the interval: no interval latency observations.
        assert_eq!(b["latency"]["interval"]["query"]["count"], json!(0.0));
        assert_eq!(b["latency"]["interval"]["query"]["p50_ms"], Value::Null);
        // A restart (counters went down) gives no rate rather than a negative one.
        let c = t.observe_at("x", s1, t0 + Duration::from_secs(4));
        assert_eq!(c["rates"]["ingest_events_per_s"], Value::Null);
        // Bounded by connection count.
        t.observe_at("y", Scrape::default(), t0);
        t.observe_at("z", Scrape::default(), t0);
        assert!(t.last.lock().unwrap().len() <= 2);
    }
}
