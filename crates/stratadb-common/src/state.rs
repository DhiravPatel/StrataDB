//! System state machines (spec IX.22; STRATA-ARCH-22-001..008).
//!
//! Every long-lived entity has an explicit state machine: an enum generated
//! by [`state_machine!`], together with the transitions the spec allows.
//!
//! A [`Tracker`] follows one entity:
//! - **Time in state:** time spent in each state is recorded, when the state
//!   is left, in `strata_state_seconds{machine,state}`.
//! - **Entities per state:** `strata_state_entities{machine,state}` counts
//!   the entities in each state right now, so an entity stuck in a state is
//!   visible before it ever leaves.
//! - **Transitions:** each one is logged with its reason at `debug` level. A
//!   transition the spec does not allow is refused and counted in
//!   `strata_state_illegal_transitions_total`; debug builds panic so tests
//!   catch it.
//!
//! The transition tables are checked against the spec's tables by a test,
//! `state_tables_match_the_spec`.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::metrics::Histogram;

/// Buckets for time in a state: 10 µs to 1 day.
pub const STATE_BUCKETS: &[f64] = &[
    1e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0, 1800.0,
    3600.0, 21_600.0, 86_400.0,
];

/// Per-machine statistics.
#[derive(Debug)]
pub struct MachineStats {
    pub seconds: Vec<Histogram>,
    pub entities: Vec<AtomicI64>,
    pub illegal: AtomicU64,
}

impl MachineStats {
    fn new(states: usize) -> Self {
        Self {
            seconds: (0..states).map(|_| Histogram::new(STATE_BUCKETS)).collect(),
            entities: (0..states).map(|_| AtomicI64::new(0)).collect(),
            illegal: AtomicU64::new(0),
        }
    }
}

/// A state of one machine (implemented by [`state_machine!`]).
pub trait State: Copy + Eq + std::fmt::Debug + Send + Sync + 'static {
    /// Machine name as exported (`query_lifecycle`, ...).
    const MACHINE: &'static str;
    /// Every state, in declaration order.
    const ALL: &'static [Self];
    /// Every transition the spec allows.
    const TRANSITIONS: &'static [(Self, Self)];
    /// Spec name (`RUNNING`).
    fn name(self) -> &'static str;
    fn index(self) -> usize;
    fn stats() -> &'static MachineStats;

    fn allows(self, to: Self) -> bool {
        Self::TRANSITIONS.contains(&(self, to))
    }
}

/// Define a state enum and its transition table.
#[macro_export]
macro_rules! state_machine {
    (
        $(#[$m:meta])* $vis:vis enum $name:ident = $machine:literal {
            $($variant:ident = $spec:literal),+ $(,)?
        }
        transitions { $($from:ident -> $to:ident),* $(,)? }
    ) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $name { $($variant),+ }

        impl $crate::state::State for $name {
            const MACHINE: &'static str = $machine;
            const ALL: &'static [Self] = &[$($name::$variant),+];
            const TRANSITIONS: &'static [(Self, Self)] = &[$(($name::$from, $name::$to)),*];
            fn name(self) -> &'static str {
                match self { $($name::$variant => $spec),+ }
            }
            fn index(self) -> usize {
                // Declaration order is `ALL` order (no explicit discriminants).
                self as usize
            }
            fn stats() -> &'static $crate::state::MachineStats {
                static STATS: ::std::sync::OnceLock<$crate::state::MachineStats> = ::std::sync::OnceLock::new();
                STATS.get_or_init(|| $crate::state::register::<Self>())
            }
        }
    };
}

/// Exporters of every machine used so far (populated on first use).
#[allow(clippy::type_complexity)]
static REGISTRY: OnceLock<
    std::sync::Mutex<
        Vec<(
            &'static str,
            &'static [&'static str],
            fn() -> &'static MachineStats,
        )>,
    >,
> = OnceLock::new();

#[doc(hidden)]
pub fn register<S: State>() -> MachineStats {
    let names: &'static [&'static str] = Box::leak(
        S::ALL
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let reg = REGISTRY.get_or_init(Default::default);
    let mut r = reg.lock().unwrap_or_else(|e| e.into_inner());
    if !r.iter().any(|(m, _, _)| *m == S::MACHINE) {
        r.push((S::MACHINE, names, S::stats));
    }
    MachineStats::new(S::ALL.len())
}

/// Make a machine's series appear in exports before its first entity.
pub fn touch<S: State>() {
    let _ = S::stats();
}

/// Follows one entity through its states.
#[derive(Debug)]
pub struct Tracker<S: State> {
    state: S,
    since: Instant,
}

impl<S: State> Tracker<S> {
    /// An entity entering `initial` now.
    pub fn new(initial: S) -> Self {
        S::stats().entities[initial.index()].fetch_add(1, Ordering::Relaxed);
        Self {
            state: initial,
            since: Instant::now(),
        }
    }

    pub fn state(&self) -> S {
        self.state
    }

    /// Time in the current state so far.
    pub fn elapsed(&self) -> Duration {
        self.since.elapsed()
    }

    /// Move to `to` now. Returns false (and changes nothing) for a
    /// transition the spec does not allow.
    pub fn to(&mut self, to: S, reason: &str) -> bool {
        let now = Instant::now();
        let spent = now.saturating_duration_since(self.since);
        self.move_to(to, spent, now, reason)
    }

    /// Move to `to`, recording `spent` as the time in the current state
    /// (for phases measured elsewhere, e.g. by the storage engine).
    pub fn to_after(&mut self, to: S, spent: Duration, reason: &str) -> bool {
        self.move_to(to, spent, Instant::now(), reason)
    }

    fn move_to(&mut self, to: S, spent: Duration, now: Instant, reason: &str) -> bool {
        let st = S::stats();
        if !self.state.allows(to) {
            st.illegal.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                machine = S::MACHINE,
                from = self.state.name(),
                to = to.name(),
                reason,
                "illegal state transition (not in spec IX.22)"
            );
            debug_assert!(
                false,
                "illegal {} transition {:?} -> {:?} ({reason})",
                S::MACHINE,
                self.state,
                to
            );
            return false;
        }
        st.seconds[self.state.index()].observe_duration(spent);
        st.entities[self.state.index()].fetch_sub(1, Ordering::Relaxed);
        st.entities[to.index()].fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            machine = S::MACHINE,
            from = self.state.name(),
            to = to.name(),
            reason,
            spent_us = spent.as_micros() as u64,
            "state transition"
        );
        self.state = to;
        self.since = now;
        true
    }
}

impl<S: State> Drop for Tracker<S> {
    fn drop(&mut self) {
        S::stats().entities[self.state.index()].fetch_sub(1, Ordering::Relaxed);
    }
}

/// Prometheus text for every machine used so far.
pub fn render(out: &mut String) {
    use std::fmt::Write as _;
    let Some(reg) = REGISTRY.get() else {
        return;
    };
    let machines: Vec<_> = reg.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let _ = writeln!(out, "# HELP strata_state_seconds Time spent in each state of each system state machine (spec IX.22), recorded when the state is left.\n# TYPE strata_state_seconds histogram");
    for (m, names, stats) in &machines {
        let st = stats();
        for (i, n) in names.iter().enumerate() {
            st.seconds[i].snapshot().render(
                out,
                "strata_state_seconds",
                "",
                &format!("machine=\"{m}\",state=\"{n}\""),
                false,
            );
        }
    }
    let _ = writeln!(out, "# HELP strata_state_entities Entities currently in each state.\n# TYPE strata_state_entities gauge");
    for (m, names, stats) in &machines {
        let st = stats();
        for (i, n) in names.iter().enumerate() {
            let _ = writeln!(
                out,
                "strata_state_entities{{machine=\"{m}\",state=\"{n}\"}} {}",
                st.entities[i].load(Ordering::Relaxed).max(0)
            );
        }
    }
    let _ = writeln!(out, "# HELP strata_state_illegal_transitions_total Transitions refused because spec IX.22 does not allow them.\n# TYPE strata_state_illegal_transitions_total counter");
    for (m, _, stats) in &machines {
        let _ = writeln!(
            out,
            "strata_state_illegal_transitions_total{{machine=\"{m}\"}} {}",
            stats().illegal.load(Ordering::Relaxed)
        );
    }
}

// ---------------------------------------------------------------------------
// The machines of spec IX.22 (tables transcribed; checked by a test).

state_machine! {
    /// IX.22.1
    pub enum QueryState = "query_lifecycle" {
        Received = "RECEIVED", Parsed = "PARSED", Failed = "FAILED", Planned = "PLANNED",
        Queued = "QUEUED", Running = "RUNNING", Rejected = "REJECTED", Streaming = "STREAMING",
        Cancelling = "CANCELLING", Finished = "FINISHED", Cancelled = "CANCELLED",
    }
    transitions {
        Received -> Parsed, Received -> Failed, Parsed -> Planned, Parsed -> Failed,
        Planned -> Queued, Planned -> Running, Queued -> Running, Queued -> Rejected,
        Running -> Streaming, Running -> Cancelling, Streaming -> Finished,
        Streaming -> Cancelling, Running -> Failed, Cancelling -> Cancelled,
    }
}

state_machine! {
    /// IX.22.2
    pub enum InsertState = "insert_lifecycle" {
        Received = "RECEIVED", Decoded = "DECODED", Validated = "VALIDATED", Rejected = "REJECTED",
        Throttled = "THROTTLED", Logged = "LOGGED", Durable = "DURABLE", Failed = "FAILED",
        Visible = "VISIBLE", Acked = "ACKED", Flushed = "FLUSHED",
    }
    transitions {
        Received -> Decoded, Decoded -> Validated, Decoded -> Rejected, Validated -> Throttled,
        Throttled -> Validated, Validated -> Logged, Logged -> Durable, Logged -> Failed,
        Durable -> Visible, Visible -> Acked, Visible -> Flushed,
    }
}

state_machine! {
    /// IX.22.3
    pub enum WalSegmentState = "wal_segment" {
        Preallocated = "PREALLOCATED", Active = "ACTIVE", Sealed = "SEALED",
        Checkpointed = "CHECKPOINTED", Recycled = "RECYCLED", Deleted = "DELETED", Failed = "FAILED",
    }
    transitions {
        Preallocated -> Active, Active -> Sealed, Sealed -> Checkpointed, Checkpointed -> Recycled,
        Checkpointed -> Deleted, Recycled -> Preallocated, Active -> Failed,
    }
}

state_machine! {
    /// IX.22.4
    pub enum MergeState = "merge_task" {
        Planned = "PLANNED", Reserved = "RESERVED", Skipped = "SKIPPED", Running = "RUNNING",
        Paused = "PAUSED", Written = "WRITTEN", Committed = "COMMITTED", Cancelled = "CANCELLED",
        Failed = "FAILED",
    }
    transitions {
        Planned -> Reserved, Planned -> Skipped, Reserved -> Running, Running -> Paused,
        Paused -> Running, Running -> Written, Written -> Committed, Running -> Cancelled,
        Running -> Failed,
    }
}

state_machine! {
    /// IX.22.5 (defined for conformance; the HTTP API authenticates per
    /// request, so connections are not tracked yet).
    pub enum ConnectionState = "connection" {
        Accepted = "ACCEPTED", TlsHandshake = "TLS_HANDSHAKE", Hello = "HELLO",
        Authenticated = "AUTHENTICATED", Closed = "CLOSED", Idle = "IDLE", Busy = "BUSY",
        Draining = "DRAINING",
    }
    transitions {
        Accepted -> TlsHandshake, Accepted -> Hello, TlsHandshake -> Hello,
        Hello -> Authenticated, Hello -> Closed, Authenticated -> Idle, Idle -> Busy,
        Busy -> Idle, Idle -> Closed, Busy -> Draining, Draining -> Closed,
    }
}

state_machine! {
    /// IX.22.6
    pub enum TableState = "table" {
        Creating = "CREATING", Active = "ACTIVE", Altering = "ALTERING", ReadOnly = "READ_ONLY",
        Detached = "DETACHED", Dropping = "DROPPING", Dropped = "DROPPED",
    }
    transitions {
        Creating -> Active, Active -> Altering, Altering -> Active, Active -> ReadOnly,
        ReadOnly -> Active, Active -> Detached, Detached -> Active, Active -> Dropping,
        Dropping -> Dropped,
    }
}

state_machine! {
    /// IX.22.7
    pub enum ServerState = "server" {
        Starting = "STARTING", Recovering = "RECOVERING", Warming = "WARMING", Ready = "READY",
        Degraded = "DEGRADED", Draining = "DRAINING", Stopped = "STOPPED",
    }
    transitions {
        Starting -> Recovering, Recovering -> Warming, Warming -> Ready, Ready -> Degraded,
        Degraded -> Ready, Ready -> Draining, Draining -> Stopped,
    }
}

state_machine! {
    /// IX.22.8
    pub enum DeviceState = "device" {
        Unknown = "UNKNOWN", Healthy = "HEALTHY", Degraded = "DEGRADED", Failed = "FAILED",
    }
    transitions {
        Unknown -> Healthy, Healthy -> Degraded, Degraded -> Healthy, Healthy -> Failed,
        Degraded -> Failed, Failed -> Unknown,
    }
}

/// One machine as spec names: (machine, states, transitions).
pub type MachineTable = (
    &'static str,
    Vec<&'static str>,
    Vec<(&'static str, &'static str)>,
);

/// Every machine's table.
pub fn tables() -> Vec<MachineTable> {
    fn t<S: State>() -> MachineTable {
        (
            S::MACHINE,
            S::ALL.iter().map(|s| s.name()).collect(),
            S::TRANSITIONS
                .iter()
                .map(|(a, b)| (a.name(), b.name()))
                .collect(),
        )
    }
    vec![
        t::<QueryState>(),
        t::<InsertState>(),
        t::<WalSegmentState>(),
        t::<MergeState>(),
        t::<ConnectionState>(),
        t::<TableState>(),
        t::<ServerState>(),
        t::<DeviceState>(),
    ]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// Spec IX.22 tables, parsed from the master specification: the code's
    /// machines must equal them exactly.
    type Owned = (String, Vec<String>, Vec<(String, String)>);
    fn spec_tables() -> Vec<Owned> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/spec/StrataDB_Master_Engineering_Specification.md"
        );
        let text = std::fs::read_to_string(path).unwrap();
        let start = text.find("# IX.22 System state machines").unwrap();
        let end = start + text[start..].find("\n# IX.23").unwrap();
        let sec = &text[start..end];
        let mut out = Vec::new();
        for part in sec.split("\n## IX.22.").skip(1) {
            let states_line = part.lines().find(|l| l.starts_with("States:")).unwrap();
            let states: Vec<String> = states_line
                .trim_start_matches("States:")
                .split(',')
                .map(|s| s.trim().trim_end_matches('.').trim_matches('`').to_owned())
                .collect();
            let transitions: Vec<(String, String)> = part
                .lines()
                .filter(|l| l.starts_with("| `"))
                .map(|l| {
                    let cols: Vec<&str> = l.split('|').map(str::trim).collect();
                    (
                        cols[1].trim_matches('`').to_owned(),
                        cols[2].trim_matches('`').to_owned(),
                    )
                })
                .collect();
            let machine = part
                .lines()
                .find_map(|l| {
                    l.split("machine=\"")
                        .nth(1)
                        .map(|r| r.split('"').next().unwrap_or("").to_owned())
                })
                .unwrap();
            out.push((machine, states, transitions));
        }
        out
    }

    #[test]
    fn state_tables_match_the_spec() {
        let spec = spec_tables();
        let code = tables();
        assert_eq!(spec.len(), 8);
        assert_eq!(code.len(), spec.len());
        for ((sm, ss, st), (cm, cs, ct)) in spec.iter().zip(code.iter()) {
            assert_eq!(sm, cm);
            assert_eq!(
                ss.iter().map(String::as_str).collect::<Vec<_>>(),
                *cs,
                "{cm} states"
            );
            assert_eq!(
                st.iter()
                    .map(|(a, b)| (a.as_str(), b.as_str()))
                    .collect::<Vec<_>>(),
                *ct,
                "{cm} transitions"
            );
        }
    }

    #[test]
    fn tracker_records_time_counts_entities_and_refuses_illegal_moves() {
        let before = DeviceState::stats().seconds[DeviceState::Unknown.index()]
            .snapshot()
            .count;
        let mut t = Tracker::new(DeviceState::Unknown);
        assert_eq!(
            DeviceState::stats().entities[DeviceState::Unknown.index()].load(Ordering::Relaxed),
            1
        );
        assert!(t.to_after(
            DeviceState::Healthy,
            Duration::from_millis(5),
            "baseline measured"
        ));
        assert_eq!(t.state(), DeviceState::Healthy);
        let s = DeviceState::stats().seconds[DeviceState::Unknown.index()].snapshot();
        assert_eq!(s.count, before + 1);
        assert!((s.sum - 0.005).abs() < 1e-3 || s.sum >= 0.005);
        // Illegal in release builds: refused and counted (tests run debug,
        // where it panics, so check the table instead).
        assert!(!DeviceState::Healthy.allows(DeviceState::Unknown));
        drop(t);
        assert_eq!(
            DeviceState::stats().entities[DeviceState::Healthy.index()].load(Ordering::Relaxed),
            0
        );
        let mut out = String::new();
        render(&mut out);
        assert!(
            out.contains(
                "strata_state_seconds_bucket{machine=\"device\",state=\"UNKNOWN\",le=\"0.01\"}"
            ),
            "{out}"
        );
        assert!(
            out.contains("strata_state_entities{machine=\"device\",state=\"HEALTHY\"} 0"),
            "{out}"
        );
        assert!(
            out.contains("strata_state_illegal_transitions_total{machine=\"device\"} 0"),
            "{out}"
        );
    }

    #[test]
    fn index_is_the_position_in_all() {
        fn check<S: State>() {
            for (i, s) in S::ALL.iter().enumerate() {
                assert_eq!(s.index(), i, "{}", S::MACHINE);
            }
        }
        check::<QueryState>();
        check::<InsertState>();
        check::<WalSegmentState>();
        check::<MergeState>();
        check::<ConnectionState>();
        check::<TableState>();
        check::<ServerState>();
        check::<DeviceState>();
    }

    /// Cost of a query's lifecycle (create, 6 transitions, drop). Run with
    /// `cargo test --release -p stratadb-common state::tests::transition_cost
    /// -- --ignored --nocapture`.
    #[test]
    #[ignore = "timing measurement"]
    fn transition_cost() {
        let n = 1_000_000u32;
        let t = Instant::now();
        for _ in 0..n {
            let mut q = Tracker::new(QueryState::Received);
            q.to(QueryState::Parsed, "");
            q.to(QueryState::Planned, "");
            q.to(QueryState::Queued, "");
            q.to(QueryState::Running, "");
            q.to(QueryState::Streaming, "");
            q.to(QueryState::Finished, "");
        }
        let per = t.elapsed().as_nanos() as f64 / f64::from(n);
        let t2 = Instant::now();
        let mut x = Duration::ZERO;
        for _ in 0..n {
            x += Instant::now().elapsed();
        }
        let clock = t2.elapsed().as_nanos() as f64 / f64::from(n) / 2.0;
        let t3 = Instant::now();
        for i in 0..n {
            QueryState::stats().seconds[3].observe_duration(Duration::from_nanos(u64::from(i)));
        }
        let hist = t3.elapsed().as_nanos() as f64 / f64::from(n);
        let t4 = Instant::now();
        for _ in 0..n {
            tracing::debug!(
                machine = "x",
                from = "a",
                to = "b",
                reason = "",
                spent_us = 1u64,
                "state transition"
            );
        }
        let trace = t4.elapsed().as_nanos() as f64 / f64::from(n);
        println!("clock {clock:.1} ns, histogram {hist:.1} ns, tracing {trace:.1} ns ({x:?})");
        println!(
            "query lifecycle: {per:.0} ns ({:.1} ns per transition)",
            per / 6.0
        );
    }

    #[test]
    #[should_panic(expected = "illegal connection transition")]
    fn illegal_transitions_panic_in_debug_builds() {
        // Its own machine: counters are process-wide and tests run in
        // parallel.
        let mut t = Tracker::new(ConnectionState::Accepted);
        t.to(ConnectionState::Busy, "not allowed");
    }
}
