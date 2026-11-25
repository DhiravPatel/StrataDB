//! Request-level metrics (spec §2.5, §3.2, §25): ingest acknowledgement and
//! query latency histograms, query outcomes and work counters.
//!
//! Label sets are fixed and small (endpoint, outcome); there is deliberately
//! no tenant label — tenant ids would make series cardinality unbounded and
//! would disclose tenant names to anyone holding `cluster.read`.

use std::sync::atomic::{AtomicU64, Ordering};

use stratadb_common::metrics::Histogram;
use stratadb_query::QueryStats;

/// Ingest endpoints with their own latency series.
#[derive(Debug, Clone, Copy)]
pub enum Endpoint {
    Ingest = 0,
    Otlp = 1,
}

const ENDPOINTS: [&str; 2] = ["ingest", "otlp"];

/// Query outcomes (label values of `stratadb_queries_total`).
pub const OUTCOMES: [&str; 6] = ["ok", "invalid", "timeout", "cancelled", "rejected", "error"];

#[derive(Debug)]
pub struct ServerMetrics {
    ingest_seconds: [Histogram; 2],
    query_seconds: Histogram,
    queries: [AtomicU64; 6],
    rows_scanned: AtomicU64,
    rows_matched: AtomicU64,
    bytes_read: AtomicU64,
    segments_scanned: AtomicU64,
    segments_pruned: AtomicU64,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            ingest_seconds: [Histogram::latency(), Histogram::latency()],
            query_seconds: Histogram::latency(),
            queries: Default::default(),
            rows_scanned: AtomicU64::new(0),
            rows_matched: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            segments_scanned: AtomicU64::new(0),
            segments_pruned: AtomicU64::new(0),
        }
    }
}

impl ServerMetrics {
    /// A committed (acknowledged) ingest request took `d` end to end.
    pub fn ingest_acked(&self, ep: Endpoint, d: std::time::Duration) {
        self.ingest_seconds[ep as usize].observe_duration(d);
    }

    /// A query finished with `outcome` (an [`OUTCOMES`] value) after `d`.
    pub fn query_done(&self, outcome: &str, d: std::time::Duration) {
        self.query_seconds.observe_duration(d);
        let i = OUTCOMES.iter().position(|o| *o == outcome).unwrap_or(5);
        self.queries[i].fetch_add(1, Ordering::Relaxed);
    }

    /// Work done by a successful query.
    pub fn query_work(&self, s: &QueryStats) {
        self.rows_scanned
            .fetch_add(s.rows_scanned, Ordering::Relaxed);
        self.rows_matched
            .fetch_add(s.rows_matched, Ordering::Relaxed);
        self.bytes_read.fetch_add(s.bytes_read, Ordering::Relaxed);
        self.segments_scanned
            .fetch_add(s.segments_scanned as u64, Ordering::Relaxed);
        let pruned = s.segments_pruned_time
            + s.segments_pruned_keys
            + s.segments_pruned_severity
            + s.segments_pruned_bloom
            + s.segments_pruned_ngram
            + s.segments_pruned_cursor;
        self.segments_pruned
            .fetch_add(pruned as u64, Ordering::Relaxed);
    }

    /// Map an error kind string (`Error::kind().as_str()`) to an outcome.
    pub fn outcome_of(kind: &str) -> &'static str {
        match kind {
            "invalid_input" | "unsupported" => "invalid",
            "timeout" => "timeout",
            "cancelled" => "cancelled",
            "resource_exhausted" => "rejected",
            _ => "error",
        }
    }

    /// Append this server's series in Prometheus text format.
    pub fn render(&self, out: &mut String) {
        for (i, ep) in ENDPOINTS.iter().enumerate() {
            self.ingest_seconds[i].snapshot().render(
                out,
                "stratadb_ingest_ack_seconds",
                "Ingest request latency until acknowledgement (committed at the configured durability).",
                &format!("endpoint=\"{ep}\""),
                i == 0,
            );
        }
        self.query_seconds.snapshot().render(
            out,
            "stratadb_query_seconds",
            "Query request latency (all outcomes).",
            "",
            true,
        );
        out.push_str("# HELP stratadb_queries_total Queries by outcome.\n# TYPE stratadb_queries_total counter\n");
        for (i, o) in OUTCOMES.iter().enumerate() {
            out.push_str(&format!(
                "stratadb_queries_total{{outcome=\"{o}\"}} {}\n",
                self.queries[i].load(Ordering::Relaxed)
            ));
        }
        for (name, help, v) in [
            (
                "query_rows_scanned_total",
                "Rows scanned by successful queries.",
                &self.rows_scanned,
            ),
            (
                "query_rows_matched_total",
                "Rows matching the filter of successful queries.",
                &self.rows_matched,
            ),
            (
                "query_segment_bytes_read_total",
                "Segment bytes read by successful queries.",
                &self.bytes_read,
            ),
            (
                "query_segments_scanned_total",
                "Segments scanned by successful queries.",
                &self.segments_scanned,
            ),
            (
                "query_segments_pruned_total",
                "Segments skipped by pruning in successful queries.",
                &self.segments_pruned,
            ),
        ] {
            out.push_str(&format!(
                "# HELP stratadb_{name} {help}\n# TYPE stratadb_{name} counter\nstratadb_{name} {}\n",
                v.load(Ordering::Relaxed)
            ));
        }
    }
}
