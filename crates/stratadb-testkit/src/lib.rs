//! Deterministic synthetic datasets (spec §27.3). All data is synthetic: no
//! real customer data, secrets, or PII. The same `(profile, seed)` always
//! yields byte-identical events, so benchmark runs are reproducible.

use std::collections::BTreeMap;

use stratadb_common::time::NANOS_PER_SEC;
use stratadb_common::TenantId;
use stratadb_schema::{Event, Severity, Value};

/// SplitMix64: tiny, fast, statistically adequate for workload generation
/// (not for cryptography).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (n > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n.max(1))) >> 64) as u64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }

    /// Approximately Zipf(1)-distributed index in `0..n` (rank 0 most frequent).
    pub fn zipf(&mut self, n: u64) -> u64 {
        // Inverse-CDF approximation for s = 1: rank ≈ n^u - 1.
        let u = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        (((n as f64 + 1.0).powf(u) - 1.0) as u64).min(n.saturating_sub(1))
    }

    pub fn hex(&mut self, chars: usize) -> String {
        let mut s = String::with_capacity(chars);
        while s.len() < chars {
            s.push_str(&format!("{:016x}", self.next_u64()));
        }
        s.truncate(chars);
        s
    }
}

/// Dataset profiles from spec §27.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Few services/hosts/regions, repeated message templates.
    AppLogs,
    /// Every event carries unique trace/request/user/session identifiers.
    HighCardinality,
    /// Deep, wide attribute maps.
    JsonHeavy,
    /// A fraction of events carry multi-kilobyte stack traces.
    StackTraces,
    /// Many tenants with Zipf-skewed volume.
    MultiTenant,
    /// Event times jitter backwards up to 10 minutes.
    OutOfOrder,
    /// Security audit events (actor, action, outcome, source IP).
    Audit,
}

impl Profile {
    pub const ALL: [Profile; 7] = [
        Profile::AppLogs,
        Profile::HighCardinality,
        Profile::JsonHeavy,
        Profile::StackTraces,
        Profile::MultiTenant,
        Profile::OutOfOrder,
        Profile::Audit,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Profile::AppLogs => "app-logs",
            Profile::HighCardinality => "high-cardinality",
            Profile::JsonHeavy => "json-heavy",
            Profile::StackTraces => "stack-traces",
            Profile::MultiTenant => "multi-tenant",
            Profile::OutOfOrder => "out-of-order",
            Profile::Audit => "audit",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == s)
    }
}

const SERVICES: [&str; 12] = [
    "payment",
    "auth",
    "search",
    "checkout",
    "catalog",
    "inventory",
    "shipping",
    "gateway",
    "billing",
    "notification",
    "profile",
    "recommendation",
];
const REGIONS: [&str; 6] = [
    "ap-south-1",
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "sa-east-1",
];
const ERRORS: [&str; 6] = [
    "TIMEOUT",
    "CONN_RESET",
    "BAD_GATEWAY",
    "DB_LOCK",
    "AUTH_EXPIRED",
    "QUOTA",
];

/// Deterministic event generator.
#[derive(Debug, Clone)]
pub struct Generator {
    rng: Rng,
    profile: Profile,
    /// Unix ns of the first event.
    pub start_ns: i64,
    /// Mean spacing between consecutive events.
    pub step_ns: i64,
    tenants: Vec<TenantId>,
    i: u64,
    /// Sampled trace / request IDs of the first tenant (for exact-lookup queries).
    pub sample_trace_ids: Vec<String>,
    pub sample_request_ids: Vec<String>,
}

impl Generator {
    pub fn new(profile: Profile, seed: u64, tenants: usize) -> Self {
        let tenants = (0..tenants.max(1))
            .filter_map(|t| TenantId::new(format!("tenant-{t}")).ok())
            .collect();
        Self {
            rng: Rng::new(seed),
            profile,
            start_ns: 1_795_132_800 * NANOS_PER_SEC, // 2026-11-20T00:00:00Z
            step_ns: NANOS_PER_SEC / 1000,
            tenants,
            i: 0,
            sample_trace_ids: Vec::new(),
            sample_request_ids: Vec::new(),
        }
    }

    pub fn tenants(&self) -> &[TenantId] {
        &self.tenants
    }

    /// Ingest time of event `i` (monotonic; independent of event-time disorder).
    pub fn ingest_time(&self, i: u64) -> i64 {
        self.start_ns + i as i64 * self.step_ns
    }

    /// Generate the next event.
    pub fn next_event(&mut self) -> Event {
        let i = self.i;
        self.i += 1;
        let r = &mut self.rng;
        let tenant = if self.profile == Profile::MultiTenant {
            self.tenants[r.zipf(self.tenants.len() as u64) as usize].clone()
        } else {
            self.tenants[(i % self.tenants.len() as u64) as usize].clone()
        };
        let ingest = self.start_ns + i as i64 * self.step_ns;
        let event_time = if self.profile == Profile::OutOfOrder {
            ingest - r.below(600) as i64 * NANOS_PER_SEC
        } else {
            ingest
        };
        let mut e = Event::new(tenant, event_time, ingest);
        let svc = SERVICES[r.zipf(SERVICES.len() as u64) as usize];
        e.service = Some(svc.to_owned());
        let roll = r.below(1000);
        e.severity = match roll {
            0..=4 => Severity::Fatal,
            5..=69 => Severity::Error,
            70..=169 => Severity::Warn,
            170..=899 => Severity::Info,
            _ => Severity::Debug,
        };
        let status: i64 = match e.severity {
            Severity::Error | Severity::Fatal => [500, 502, 503, 504][r.below(4) as usize],
            Severity::Warn => [404, 409, 429][r.below(3) as usize],
            _ => 200,
        };
        let region = REGIONS[r.below(REGIONS.len() as u64) as usize];
        let attrs = &mut e.attributes;
        attrs.insert("http.status_code".into(), Value::Int(status));
        attrs.insert("region".into(), Value::from(region));
        attrs.insert(
            "duration_ms".into(),
            Value::Float((r.below(5_000_000) as f64) / 1000.0),
        );
        e.resource_attributes.insert(
            "host".into(),
            Value::String(format!("{svc}-{}", r.below(20))),
        );
        e.resource_attributes.insert(
            "version".into(),
            Value::String(format!("1.{}.{}", r.below(5), r.below(20))),
        );
        let high_card = matches!(
            self.profile,
            Profile::HighCardinality | Profile::OutOfOrder | Profile::StackTraces
        );
        if high_card || r.chance(0.3) {
            let trace = r.hex(32);
            let req = format!("req_{}", r.hex(12));
            // Samples come from the first tenant so lookups target existing data.
            if e.tenant_id == self.tenants[0]
                && self.sample_trace_ids.len() < 1000
                && r.chance(0.01)
            {
                self.sample_trace_ids.push(trace.clone());
                self.sample_request_ids.push(req.clone());
            }
            e.span_id = Some(r.hex(16));
            e.trace_id = Some(trace);
            e.attributes.insert("request_id".into(), Value::String(req));
            e.attributes.insert(
                "user_id".into(),
                Value::String(format!("user_{}", r.below(1_000_000))),
            );
            e.attributes
                .insert("session_id".into(), Value::String(r.hex(20)));
        } else {
            e.attributes.insert(
                "user_id".into(),
                Value::String(format!("user_{}", r.zipf(5_000))),
            );
        }
        let body = if matches!(e.severity, Severity::Error | Severity::Fatal) {
            let code = ERRORS[r.below(ERRORS.len() as u64) as usize];
            e.attributes.insert("error.code".into(), Value::from(code));
            format!(
                "{svc}: request failed with {code} after {} ms (timeout={})",
                r.below(30_000),
                code == "TIMEOUT"
            )
        } else {
            format!("{svc}: handled request in {} ms", r.below(2_000))
        };
        e.body = Some(Value::String(body));
        match self.profile {
            Profile::JsonHeavy => {
                let mut nested = BTreeMap::new();
                for k in 0..20 {
                    nested.insert(format!("field_{k}"), Value::Int(r.below(1000) as i64));
                }
                nested.insert(
                    "tags".into(),
                    Value::List((0..5).map(|_| Value::String(r.hex(6))).collect()),
                );
                e.attributes.insert("payload".into(), Value::Map(nested));
                for k in 0..15 {
                    e.attributes
                        .insert(format!("extra.k{k}"), Value::String(r.hex(8)));
                }
            }
            Profile::StackTraces if r.chance(0.1) => {
                let frames: Vec<String> = (0..(20 + r.below(40)))
                    .map(|d| {
                        format!(
                            "    at com.example.{svc}.Handler{d}.invoke(Handler{d}.java:{})",
                            r.below(900)
                        )
                    })
                    .collect();
                e.attributes.insert(
                    "exception.stacktrace".into(),
                    Value::String(frames.join("\n")),
                );
            }
            Profile::Audit => {
                e.attributes.insert(
                    "actor".into(),
                    Value::String(format!("user_{}", r.zipf(500))),
                );
                e.attributes.insert(
                    "action".into(),
                    Value::from(
                        ["login", "logout", "read", "write", "delete", "grant"]
                            [r.below(6) as usize],
                    ),
                );
                e.attributes.insert(
                    "outcome".into(),
                    Value::from(if r.chance(0.95) { "success" } else { "denied" }),
                );
                e.attributes.insert(
                    "source_ip".into(),
                    Value::String(format!(
                        "10.{}.{}.{}",
                        r.below(256),
                        r.below(256),
                        r.below(256)
                    )),
                );
            }
            _ => {}
        }
        e
    }

    /// Generate `n` events.
    pub fn batch(&mut self, n: usize) -> Vec<Event> {
        (0..n).map(|_| self.next_event()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratadb_schema::validate::validate_event;
    use stratadb_schema::Limits;

    #[test]
    fn deterministic_and_valid_for_every_profile() {
        for p in Profile::ALL {
            let a = Generator::new(p, 42, 4).batch(500);
            let b = Generator::new(p, 42, 4).batch(500);
            assert_eq!(a, b, "{p:?} not deterministic");
            let c = Generator::new(p, 43, 4).batch(500);
            assert_ne!(a, c, "{p:?} ignores the seed");
            for e in &a {
                validate_event(e, &Limits::default()).unwrap_or_else(|err| panic!("{p:?}: {err}"));
            }
            assert_eq!(Profile::parse(p.name()), Some(p));
        }
    }

    #[test]
    fn distributions_are_plausible() {
        let evs = Generator::new(Profile::MultiTenant, 7, 10).batch(20_000);
        let t0 = evs
            .iter()
            .filter(|e| e.tenant_id.as_str() == "tenant-0")
            .count();
        let t9 = evs
            .iter()
            .filter(|e| e.tenant_id.as_str() == "tenant-9")
            .count();
        assert!(t0 > 3 * t9, "expected skew, got {t0} vs {t9}");
        let errors = evs.iter().filter(|e| e.severity == Severity::Error).count();
        assert!((800..1800).contains(&errors), "error rate off: {errors}");
        let mut r = Rng::new(1);
        assert!((0..10_000).all(|_| r.below(7) < 7));
    }
}
