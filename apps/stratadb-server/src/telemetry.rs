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

/// Server-side INSERT stages (spec VIII.1.2), in pipeline order. Engine
/// stages (`EngineStats::insert_stages`) are rendered between `validate` and
/// `ack` in the same `strata_insert_stage_seconds` family.
#[derive(Debug, Clone, Copy)]
pub enum InsertStage {
    /// Token verification and permission check.
    Auth = 0,
    /// Body decompression, UTF-8 and JSON/protobuf framing.
    Decode = 1,
    /// Per-event schema mapping, validation and limits.
    Validate = 2,
    /// Response construction after the commit.
    Ack = 3,
}

const INSERT_STAGES: [&str; 4] = ["auth", "decode", "validate", "ack"];

/// SELECT stages (spec VIII.1.4, IX.1 `queue + plan + exec + send = total`).
#[derive(Debug, Clone, Copy)]
pub enum QueryStage {
    /// Waiting for an execution thread after admission.
    Queue = 0,
    Parse = 1,
    /// Bind + optimize (`plan_us`, STRATA-SYS-01-007).
    Plan = 2,
    /// Execution (`exec_us`).
    Exec = 3,
    /// Result serialization.
    Serialize = 4,
}

const QUERY_STAGES: [&str; 5] = ["queue", "parse", "plan", "exec", "serialize"];

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
    insert_stages: [Histogram; 4],
    query_stages: [Histogram; 5],
    /// Q-error of the scan's row estimate (STRATA-SYS-12-004).
    q_error: Histogram,
}

/// Buckets of the estimate Q-error (1 = exact).
pub const Q_ERROR_BUCKETS: &[f64] = &[
    1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0, 30.0, 100.0, 1000.0, 10000.0,
];

/// `max(est / actual, actual / est)`, each at least 1 (so empty scans and
/// zero estimates stay finite).
pub fn q_error(estimated: u64, actual: u64) -> f64 {
    let (e, a) = (estimated.max(1) as f64, actual.max(1) as f64);
    (e / a).max(a / e)
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            insert_stages: std::array::from_fn(|_| Histogram::stage()),
            query_stages: std::array::from_fn(|_| Histogram::stage()),
            ingest_seconds: [Histogram::latency(), Histogram::latency()],
            query_seconds: Histogram::latency(),
            queries: Default::default(),
            rows_scanned: AtomicU64::new(0),
            rows_matched: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            segments_scanned: AtomicU64::new(0),
            segments_pruned: AtomicU64::new(0),
            q_error: Histogram::with_bounds(Q_ERROR_BUCKETS),
        }
    }
}

impl ServerMetrics {
    pub fn insert_stage(&self, s: InsertStage, d: std::time::Duration) {
        self.insert_stages[s as usize].observe_duration(d);
    }

    pub fn query_stage(&self, s: QueryStage, d: std::time::Duration) {
        self.query_stages[s as usize].observe_duration(d);
    }

    /// Record a finished query's estimate Q-error.
    pub fn estimate(&self, q: f64) {
        self.q_error.observe(q);
    }

    /// Append `strata_insert_stage_seconds` (server stages around the
    /// engine's, in pipeline order) and `strata_query_stage_seconds`.
    pub fn render_stages(
        &self,
        out: &mut String,
        engine: &[(&'static str, stratadb_common::metrics::HistogramSnapshot)],
    ) {
        let name = "strata_insert_stage_seconds";
        let help = "INSERT pipeline stage latency (spec VIII.1.2).";
        let mut first = true;
        let mut emit = |label: &str, h: &stratadb_common::metrics::HistogramSnapshot| {
            h.render(out, name, help, &format!("stage=\"{label}\""), first);
            first = false;
        };
        for i in [
            InsertStage::Auth,
            InsertStage::Decode,
            InsertStage::Validate,
        ] {
            emit(
                INSERT_STAGES[i as usize],
                &self.insert_stages[i as usize].snapshot(),
            );
        }
        for (label, h) in engine {
            emit(label, h);
        }
        emit(
            INSERT_STAGES[InsertStage::Ack as usize],
            &self.insert_stages[InsertStage::Ack as usize].snapshot(),
        );
        for (i, label) in QUERY_STAGES.iter().enumerate() {
            self.query_stages[i].snapshot().render(
                out,
                "strata_query_stage_seconds",
                "SELECT stage latency (queue, parse, plan, exec, serialize; spec VIII.1.4).",
                &format!("stage=\"{label}\""),
                i == 0,
            );
        }
    }

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
        self.q_error.snapshot().render(
            out,
            "strata_query_estimate_q_error",
            "Q-error (max(est/actual, actual/est)) of the plan-time row estimate of query scans (STRATA-SYS-12-004).",
            "",
            true,
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q_error_is_symmetric_and_finite() {
        assert_eq!(q_error(100, 100), 1.0);
        assert_eq!(q_error(100, 25), 4.0);
        assert_eq!(q_error(25, 100), 4.0);
        assert_eq!(q_error(0, 0), 1.0);
        assert_eq!(q_error(0, 10), 10.0);
    }
}
