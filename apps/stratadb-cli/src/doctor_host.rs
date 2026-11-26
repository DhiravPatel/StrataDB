//! `strata doctor host`: host readiness checks (spec VIII.2,
//! STRATA-SYS-02-001…042).
//!
//! The checks *report* and never change the host: StrataDB does not modify
//! sysctls (STRATA-SYS-02-004); `packaging/sysctl.d/90-stratadb.conf` and the
//! `tuned` profile apply the baseline. Each check has a stable `id`, a
//! `status` of `pass`, `warn`, `fail`, `info` or `skip` (not applicable or
//! not readable here), the observed `value`, the `recommended` value and an
//! explanation. Any `fail` makes the command exit non-zero
//! (STRATA-SYS-02-041); `--json` prints the report with stable keys
//! (STRATA-SYS-02-042).
//!
//! All reads go through [`Host`], rooted at `/` in production and at a
//! fixture directory in tests. The checks target Linux production hosts; on
//! other systems most items are `skip`.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{json, Value as J};

/// Read access to the host's `/proc` and `/sys` (and the mount table).
#[derive(Debug, Clone)]
pub struct Host {
    root: PathBuf,
}

impl Host {
    pub fn real() -> Self {
        Self {
            root: PathBuf::from("/"),
        }
    }

    /// A host rooted at another directory (tests, inspecting a mounted
    /// image).
    pub fn at(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    fn read(&self, rel: &str) -> Option<String> {
        std::fs::read_to_string(self.root.join(rel.trim_start_matches('/')))
            .ok()
            .map(|s| s.trim().to_owned())
    }

    fn exists(&self, rel: &str) -> bool {
        self.root.join(rel.trim_start_matches('/')).exists()
    }

    fn list(&self, rel: &str) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(self.root.join(rel.trim_start_matches('/')))
            .map(|d| {
                d.filter_map(Result::ok)
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    fn sysctl(&self, name: &str) -> Option<String> {
        self.read(&format!("proc/sys/{}", name.replace('.', "/")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    Info,
    Skip,
    Warn,
    Fail,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Status::Pass => "PASS",
            Status::Info => "INFO",
            Status::Skip => "SKIP",
            Status::Warn => "WARN",
            Status::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub id: String,
    pub category: &'static str,
    pub status: Status,
    pub value: Option<String>,
    pub recommended: Option<String>,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct Report {
    pub header: Vec<(String, J)>,
    pub checks: Vec<Check>,
    /// Whether this is a Linux host (readiness is only assessed there).
    pub assessed: bool,
}

impl Report {
    fn add(
        &mut self,
        id: impl Into<String>,
        category: &'static str,
        status: Status,
        value: Option<String>,
        recommended: Option<&str>,
        detail: impl Into<String>,
    ) {
        self.checks.push(Check {
            id: id.into(),
            category,
            status,
            value,
            recommended: recommended.map(str::to_owned),
            detail: detail.into(),
        });
    }

    pub fn count(&self, s: Status) -> usize {
        self.checks.iter().filter(|c| c.status == s).count()
    }

    pub fn has_fail(&self) -> bool {
        self.count(Status::Fail) > 0
    }

    pub fn to_json(&self) -> J {
        let host: serde_json::Map<String, J> = self.header.iter().cloned().collect();
        json!({
            "report_version": 1,
            "host": host,
            "checks": self.checks,
            "summary": {
                "fail": self.count(Status::Fail),
                "warn": self.count(Status::Warn),
                "pass": self.count(Status::Pass),
                "info": self.count(Status::Info),
                "skip": self.count(Status::Skip),
                "assessed": self.assessed,
                "ready": self.assessed && !self.has_fail(),
            },
        })
    }

    /// The text contract of spec VIII.2.12.
    pub fn to_text(&self) -> String {
        let h = |k: &str| {
            self.header
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.as_str().map_or_else(|| v.to_string(), str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned())
        };
        let mut out = format!(
            "StrataDB host readiness report  (kernel {}, {}, {}, {} logical CPUs, {} NUMA node(s))\n",
            h("kernel"),
            h("arch"),
            h("cpu_model"),
            h("logical_cpus"),
            h("numa_nodes")
        );
        for c in &self.checks {
            let value = c
                .value
                .as_deref()
                .map(|v| format!("={v}"))
                .unwrap_or_default();
            let rec = match (&c.recommended, c.status) {
                (Some(r), Status::Warn | Status::Fail) => format!("  -> recommend {r}"),
                _ => String::new(),
            };
            out.push_str(&format!(
                "[{}] {}{}{}  ({})\n",
                c.status.label(),
                c.id,
                value,
                rec,
                c.detail
            ));
        }
        let (f, w) = (self.count(Status::Fail), self.count(Status::Warn));
        out.push_str(&format!(
            "Summary: {f} FAIL, {w} WARN. Production readiness: {}\n",
            if f > 0 {
                "NOT READY (fix FAIL items)"
            } else if !self.assessed {
                "NOT ASSESSED (the checks apply to Linux hosts)"
            } else if w > 0 {
                "READY with warnings"
            } else {
                "READY"
            }
        ));
        out
    }
}

/// One `sysctl` of the VIII.2.2 baseline.
struct Sysctl {
    name: &'static str,
    recommended: &'static str,
    reason: &'static str,
    /// Status for the observed value (`None`: unparsable → warn).
    judge: fn(&str, &Ctx) -> Option<Status>,
}

/// Facts some judgements need.
struct Ctx {
    mem_total_kb: Option<u64>,
}

fn num(v: &str) -> Option<u64> {
    v.split_whitespace().next()?.parse().ok()
}

fn at_least(min: u64) -> impl Fn(&str) -> Option<Status> {
    move |v| num(v).map(|n| if n >= min { Status::Pass } else { Status::Warn })
}

fn equals(want: u64) -> impl Fn(&str) -> Option<Status> {
    move |v| {
        num(v).map(|n| {
            if n == want {
                Status::Pass
            } else {
                Status::Warn
            }
        })
    }
}

/// The sysctl baseline of spec VIII.2.2 (and `packaging/sysctl.d`).
const SYSCTLS: &[Sysctl] = &[
    Sysctl {
        name: "vm.swappiness",
        recommended: "1",
        reason: "avoid swapping hot pools; keep swap only as an emergency",
        judge: |v, _| {
            num(v).map(|n| match n {
                0 | 1 => Status::Pass,
                2..=10 => Status::Warn,
                _ => Status::Fail,
            })
        },
    },
    Sysctl {
        name: "vm.dirty_background_bytes",
        recommended: "268435456",
        reason: "start writeback early; 0 means the ratio is used, which scales with RAM",
        judge: |v, _| {
            num(v).map(|n| {
                if n > 0 && n <= 268_435_456 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "vm.dirty_bytes",
        recommended: "1073741824",
        reason: "cap dirty data so fsync latency stays bounded",
        judge: |v, _| {
            num(v).map(|n| {
                if n > 0 && n <= 1_073_741_824 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "vm.dirty_expire_centisecs",
        recommended: "1000",
        reason: "write dirty pages within 10 s",
        judge: |v, _| {
            num(v).map(|n| {
                if n <= 1000 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "vm.dirty_writeback_centisecs",
        recommended: "100",
        reason: "writeback thread wakes every second",
        judge: |v, _| {
            num(v).map(|n| {
                if n > 0 && n <= 100 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "vm.max_map_count",
        recommended: "1048576",
        reason: "many mapped files",
        judge: |v, _| at_least(1_048_576)(v),
    },
    Sysctl {
        name: "vm.overcommit_memory",
        recommended: "0 (or 2 with an explicit ratio)",
        reason: "avoid OOM-killer surprises; the engine has its own accounting",
        judge: |v, _| num(v).map(|n| if n == 1 { Status::Warn } else { Status::Pass }),
    },
    Sysctl {
        name: "vm.zone_reclaim_mode",
        recommended: "0",
        reason: "do not reclaim the local NUMA node aggressively",
        judge: |v, _| equals(0)(v),
    },
    Sysctl {
        name: "vm.min_free_kbytes",
        recommended: ">= 1% of RAM (at most 4 GiB)",
        reason: "allow atomic allocations under pressure (NIC rings)",
        judge: |v, c| {
            let want = c.mem_total_kb? / 100;
            let want = want.min(4 * 1024 * 1024);
            num(v).map(|n| {
                if n >= want {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "fs.file-max",
        recommended: "4194304",
        reason: "many segment files and sockets",
        judge: |v, _| at_least(4_194_304)(v),
    },
    Sysctl {
        name: "fs.aio-max-nr",
        recommended: "1048576",
        reason: "fallback asynchronous I/O",
        judge: |v, _| at_least(1_048_576)(v),
    },
    Sysctl {
        name: "fs.nr_open",
        recommended: "4194304",
        reason: "per-process descriptor ceiling",
        judge: |v, _| at_least(4_194_304)(v),
    },
    Sysctl {
        name: "kernel.numa_balancing",
        recommended: "0",
        reason: "automatic page migration causes latency spikes",
        judge: |v, _| equals(0)(v),
    },
    Sysctl {
        name: "kernel.sched_autogroup_enabled",
        recommended: "0",
        reason: "server workloads: avoid autogroup fairness distortion",
        judge: |v, _| equals(0)(v),
    },
    Sysctl {
        name: "kernel.perf_event_paranoid",
        recommended: "1 (or 2 with CAP_PERFMON)",
        reason: "allow self-profiling with hardware counters",
        judge: |v, _| {
            v.trim().parse::<i64>().ok().map(|n| match n {
                i64::MIN..=1 => Status::Pass,
                2 => Status::Info,
                _ => Status::Warn,
            })
        },
    },
    Sysctl {
        name: "net.core.somaxconn",
        recommended: "65535",
        reason: "large accept backlog",
        judge: |v, _| at_least(65_535)(v),
    },
    Sysctl {
        name: "net.core.netdev_max_backlog",
        recommended: "250000",
        reason: "avoid drops at 25-100 GbE",
        judge: |v, _| at_least(250_000)(v),
    },
    Sysctl {
        name: "net.core.rmem_max",
        recommended: "268435456",
        reason: "large socket buffers for bulk transfer",
        judge: |v, _| at_least(268_435_456)(v),
    },
    Sysctl {
        name: "net.core.wmem_max",
        recommended: "268435456",
        reason: "large socket buffers for bulk transfer",
        judge: |v, _| at_least(268_435_456)(v),
    },
    Sysctl {
        name: "net.ipv4.tcp_rmem",
        recommended: "4096 1048576 268435456",
        reason: "TCP autotuning range",
        judge: |v, _| {
            v.split_whitespace().nth(2)?.parse::<u64>().ok().map(|m| {
                if m >= 268_435_456 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "net.ipv4.tcp_wmem",
        recommended: "4096 1048576 268435456",
        reason: "TCP autotuning range",
        judge: |v, _| {
            v.split_whitespace().nth(2)?.parse::<u64>().ok().map(|m| {
                if m >= 268_435_456 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "net.ipv4.tcp_congestion_control",
        recommended: "bbr (dctcp on ECN-enabled fabrics)",
        reason: "throughput and latency",
        judge: |v, _| {
            Some(if ["bbr", "dctcp"].contains(&v.trim()) {
                Status::Pass
            } else {
                Status::Warn
            })
        },
    },
    Sysctl {
        name: "net.core.default_qdisc",
        recommended: "fq",
        reason: "required for BBR pacing",
        judge: |v, _| {
            Some(if v.trim() == "fq" {
                Status::Pass
            } else {
                Status::Warn
            })
        },
    },
    Sysctl {
        name: "net.ipv4.tcp_slow_start_after_idle",
        recommended: "0",
        reason: "avoid congestion-window collapse on idle connections",
        judge: |v, _| equals(0)(v),
    },
    Sysctl {
        name: "net.ipv4.tcp_fastopen",
        recommended: "3",
        reason: "faster reconnect",
        judge: |v, _| {
            num(v).map(|n| {
                if n & 3 == 3 {
                    Status::Pass
                } else {
                    Status::Warn
                }
            })
        },
    },
    Sysctl {
        name: "net.ipv4.tcp_tw_reuse",
        recommended: "1",
        reason: "outbound connection reuse for inter-node RPC",
        judge: |v, _| equals(1)(v),
    },
    Sysctl {
        name: "net.ipv4.ip_local_port_range",
        recommended: "1024 65535",
        reason: "many outbound connections",
        judge: |v, _| {
            let mut it = v.split_whitespace().filter_map(|x| x.parse::<u64>().ok());
            let (lo, hi) = (it.next()?, it.next()?);
            Some(if lo <= 1024 && hi >= 65_535 {
                Status::Pass
            } else {
                Status::Warn
            })
        },
    },
];

/// The bracketed choice of a sysfs selector (`always [madvise] never`).
fn selected(v: &str) -> Option<&str> {
    let s = v.find('[')?;
    let e = v[s..].find(']')? + s;
    Some(&v[s + 1..e])
}

fn kernel_version(v: &str) -> Option<(u64, u64)> {
    let mut it = v
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty());
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

fn cpuinfo(host: &Host, key: &str) -> Option<String> {
    host.read("proc/cpuinfo")?.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == key).then(|| v.trim().to_owned())
    })
}

/// Mount point, filesystem type, source and options of the filesystem
/// holding `dir` (longest matching mount point in `/proc/self/mounts`).
fn mount_of(host: &Host, dir: &Path) -> Option<(String, String, String, String)> {
    let mounts = host
        .read("proc/self/mounts")
        .or_else(|| host.read("proc/mounts"))?;
    let d = dir.to_string_lossy();
    let mut best: Option<(String, String, String, String)> = None;
    for l in mounts.lines() {
        let f: Vec<&str> = l.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        let (src, mp, fs, opts) = (f[0], f[1], f[2], f[3]);
        let fits = mp == "/" || d == mp || d.starts_with(&format!("{}/", mp.trim_end_matches('/')));
        if fits && best.as_ref().is_none_or(|b| mp.len() > b.0.len()) {
            best = Some((
                mp.to_owned(),
                fs.to_owned(),
                src.to_owned(),
                opts.to_owned(),
            ));
        }
    }
    best
}

/// Block device of a mount source (`/dev/nvme0n1p2` → `nvme0n1`).
fn block_device(host: &Host, source: &str) -> Option<String> {
    let name = source.strip_prefix("/dev/")?.rsplit('/').next()?.to_owned();
    if !host.exists(&format!("sys/class/block/{name}")) {
        return None;
    }
    if host.exists(&format!("sys/class/block/{name}/partition")) {
        // Partition: strip the partition number (and `p` after a digit).
        let base = name.trim_end_matches(|c: char| c.is_ascii_digit());
        let base = match base.strip_suffix('p') {
            Some(b) if b.ends_with(|c: char| c.is_ascii_digit()) => b,
            _ => base,
        };
        return Some(base.to_owned());
    }
    Some(name)
}

/// Run all checks. `data_dir`: also check its filesystem and device.
pub fn run(host: &Host, data_dir: Option<&Path>) -> Report {
    let mut r = Report::default();
    let linux = host.exists("proc/sys/kernel/osrelease");
    r.assessed = linux;
    let kernel = host.read("proc/sys/kernel/osrelease").or_else(|| {
        (host.root == Path::new("/"))
            .then(crate::hostcheck::uname_release)
            .flatten()
    });
    let mem_total_kb = host.read("proc/meminfo").and_then(|m| {
        m.lines()
            .find_map(|l| l.strip_prefix("MemTotal:"))
            .and_then(num)
    });
    let numa = host
        .list("sys/devices/system/node")
        .iter()
        .filter(|n| n.starts_with("node") && n[4..].chars().all(|c| c.is_ascii_digit()))
        .count();
    r.header = vec![
        ("os".into(), json!(std::env::consts::OS)),
        (
            "kernel".into(),
            json!(kernel.clone().unwrap_or_else(|| "unknown".into())),
        ),
        ("arch".into(), json!(std::env::consts::ARCH)),
        (
            "cpu_model".into(),
            json!(cpuinfo(host, "model name").unwrap_or_else(
                || crate::hostcheck::cpu_model().unwrap_or_else(|| "unknown".into())
            )),
        ),
        (
            "logical_cpus".into(),
            json!(stratadb_common::threads::logical_cpus()),
        ),
        ("numa_nodes".into(), json!(numa.max(1))),
        ("memory_kb".into(), json!(mem_total_kb)),
    ];
    if !linux {
        r.add(
            "platform",
            "kernel",
            Status::Info,
            Some(std::env::consts::OS.to_owned()),
            Some("Linux >= 5.15"),
            "not Linux: production host checks apply to Linux; most items are skipped",
        );
    }

    // --- Kernel (STRATA-SYS-02-001/002)
    match (linux, kernel.as_deref().and_then(kernel_version)) {
        (true, Some(v)) if v < (5, 15) => r.add(
            "kernel",
            "kernel",
            Status::Fail,
            kernel.clone(),
            Some(">= 5.15 (6.6 LTS recommended)"),
            "below the minimum supported kernel",
        ),
        (true, Some(v)) if v < (6, 6) => r.add(
            "kernel",
            "kernel",
            Status::Warn,
            kernel.clone(),
            Some(">= 6.6 LTS"),
            "supported; 6.6 LTS or newer is recommended",
        ),
        (true, Some(_)) => r.add(
            "kernel",
            "kernel",
            Status::Pass,
            kernel.clone(),
            Some(">= 5.15"),
            "supported kernel",
        ),
        _ => r.add(
            "kernel",
            "kernel",
            Status::Skip,
            kernel.clone(),
            Some(">= 5.15"),
            "not a Linux kernel",
        ),
    }
    match host.sysctl("kernel.io_uring_disabled") {
        Some(v) => r.add("io_uring", "kernel", Status::Info, Some(v.clone()), None, if v == "0" { "available; this build uses the portable pread backend (ADR-0027)" } else { "restricted by kernel.io_uring_disabled; this build uses the portable pread backend (ADR-0027)" }),
        None => r.add("io_uring", "kernel", Status::Info, None, None, "this build uses the portable pread backend (ADR-0027)"),
    }

    // --- Memory management (STRATA-SYS-02-006)
    match host.read("sys/kernel/mm/transparent_hugepage/enabled") {
        Some(v) => {
            let s = selected(&v).unwrap_or(&v).to_owned();
            let st = match s.as_str() {
                "madvise" => Status::Pass,
                "never" => Status::Info,
                _ => Status::Warn,
            };
            r.add(
                "transparent_hugepage",
                "memory",
                st,
                Some(s),
                Some("madvise"),
                "'always' causes compaction stalls on the allocation path",
            );
        }
        None => r.add(
            "transparent_hugepage",
            "memory",
            Status::Skip,
            None,
            Some("madvise"),
            "not readable here",
        ),
    }
    if let Some(v) = host.read("sys/kernel/mm/transparent_hugepage/defrag") {
        let s = selected(&v).unwrap_or(&v).to_owned();
        let st = match s.as_str() {
            "defer+madvise" | "madvise" | "defer" | "never" => Status::Pass,
            _ => Status::Warn,
        };
        r.add(
            "transparent_hugepage.defrag",
            "memory",
            st,
            Some(s),
            Some("defer+madvise"),
            "synchronous defrag stalls allocations",
        );
    }
    match host.read("proc/swaps") {
        Some(s) => {
            let n = s.lines().skip(1).filter(|l| !l.trim().is_empty()).count();
            r.add(
                "swap",
                "memory",
                Status::Info,
                Some(format!("{n} device(s)")),
                None,
                "with swap enabled, keep vm.swappiness at 1",
            );
        }
        None => r.add(
            "swap",
            "memory",
            Status::Skip,
            None,
            None,
            "not readable here",
        ),
    }

    // --- sysctl baseline (VIII.2.2)
    let ctx = Ctx { mem_total_kb };
    for s in SYSCTLS {
        match host.sysctl(s.name) {
            Some(v) => {
                let st = (s.judge)(&v, &ctx).unwrap_or(Status::Warn);
                r.add(s.name, "sysctl", st, Some(v), Some(s.recommended), s.reason);
            }
            None => r.add(
                s.name,
                "sysctl",
                Status::Skip,
                None,
                Some(s.recommended),
                "not present on this host",
            ),
        }
    }
    for name in ["net.core.busy_poll", "net.core.busy_read"] {
        if let Some(v) = host.sysctl(name) {
            r.add(
                name,
                "sysctl",
                Status::Info,
                Some(v),
                Some("50 on dedicated hosts only"),
                "low-latency socket polling costs CPU",
            );
        }
    }

    // --- CPU (STRATA-SYS-02-010/012/013, 02-039)
    match host.read("sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") {
        Some(g) => r.add(
            "cpufreq.governor",
            "cpu",
            if g == "performance" {
                Status::Pass
            } else {
                Status::Warn
            },
            Some(g),
            Some("performance"),
            "frequency scaling adds latency under bursty load",
        ),
        None => r.add(
            "cpufreq.governor",
            "cpu",
            Status::Skip,
            None,
            Some("performance"),
            "no cpufreq (virtual machine or not Linux)",
        ),
    }
    match host.read("sys/devices/system/cpu/smt/control") {
        Some(v) => r.add(
            "smt",
            "cpu",
            Status::Info,
            Some(v),
            None,
            "recorded in benchmark manifests (STRATA-SYS-02-012)",
        ),
        None => r.add("smt", "cpu", Status::Skip, None, None, "not readable here"),
    }
    match host
        .read("sys/devices/system/cpu/intel_pstate/no_turbo")
        .map(|v| if v == "1" { "off" } else { "on" }.to_owned())
        .or_else(|| {
            host.read("sys/devices/system/cpu/cpufreq/boost")
                .map(|v| if v == "1" { "on" } else { "off" }.to_owned())
        }) {
        Some(v) => r.add(
            "turbo",
            "cpu",
            Status::Info,
            Some(v),
            None,
            "use identical turbo settings for comparisons (STRATA-SYS-02-013)",
        ),
        None => r.add(
            "turbo",
            "cpu",
            Status::Skip,
            None,
            None,
            "not readable here",
        ),
    }
    match host.read("sys/devices/system/clocksource/clocksource0/current_clocksource") {
        Some(c) => {
            let st = match c.as_str() {
                "tsc" | "arch_sys_counter" => Status::Pass,
                "hpet" | "acpi_pm" => Status::Warn,
                _ => Status::Info,
            };
            let invariant = cpuinfo(host, "flags")
                .is_some_and(|f| f.contains("constant_tsc") && f.contains("nonstop_tsc"));
            let detail = if c == "tsc" && invariant {
                "vDSO clock_gettime; invariant TSC".to_owned()
            } else if st == Status::Warn {
                "clock_gettime becomes a syscall (~0.5-1 us instead of ~20 ns)".to_owned()
            } else {
                "vDSO clock_gettime".to_owned()
            };
            r.add(
                "clocksource",
                "cpu",
                st,
                Some(c),
                Some("tsc (x86-64)"),
                detail,
            );
        }
        None => r.add(
            "clocksource",
            "cpu",
            Status::Skip,
            None,
            Some("tsc (x86-64)"),
            "not readable here",
        ),
    }

    // --- Resource limits (VIII.2.10)
    {
        use rustix::process::{getrlimit, Resource};
        let fmt = |v: Option<u64>| v.map_or_else(|| "unlimited".to_owned(), |n| n.to_string());
        let nofile = getrlimit(Resource::Nofile);
        let soft = nofile.current.unwrap_or(u64::MAX);
        r.add(
            "rlimit.nofile",
            "limits",
            if soft >= 1_048_576 { Status::Pass } else if soft >= 65_536 { Status::Warn } else { Status::Fail },
            Some(format!("{} (hard {})", fmt(nofile.current), fmt(nofile.maximum))),
            Some(">= 1048576"),
            "segment files, sockets, WAL; the server raises the soft limit to the hard limit at start (systemd: LimitNOFILE=1048576)",
        );
        let nproc = getrlimit(Resource::Nproc);
        r.add(
            "rlimit.nproc",
            "limits",
            if nproc.current.is_none_or(|n| n >= 65_536) {
                Status::Pass
            } else {
                Status::Warn
            },
            Some(fmt(nproc.current)),
            Some(">= 65536"),
            "thread pools (the engine itself uses few threads: 2 x CPUs + 32 at most)",
        );
        let core = getrlimit(Resource::Core);
        r.add(
            "rlimit.core",
            "limits",
            Status::Info,
            Some(fmt(core.current)),
            Some("0 in production"),
            "core dumps of large processes are slow and may contain data",
        );
    }

    // --- cgroup v2 (STRATA-SYS-02-027)
    let cg = host.read("proc/self/cgroup").and_then(|c| {
        c.lines()
            .find_map(|l| l.strip_prefix("0::").map(str::to_owned))
    });
    match cg {
        Some(path) => {
            let base = format!(
                "sys/fs/cgroup{}",
                if path == "/" { String::new() } else { path }
            );
            let get = |f: &str| host.read(&format!("{base}/{f}"));
            let cpu = get("cpu.max").unwrap_or_else(|| "max".into());
            let mem = get("memory.max").unwrap_or_else(|| "max".into());
            let high = get("memory.high").unwrap_or_else(|| "max".into());
            let set = get("cpuset.cpus.effective").unwrap_or_else(|| "-".into());
            r.add(
                "cgroup",
                "limits",
                Status::Info,
                Some(format!("cpu.max={cpu} memory.max={mem} memory.high={high} cpuset={set}")),
                None,
                "size scan_threads / net_threads / blocking_threads and the memory budget to these limits",
            );
        }
        None => r.add(
            "cgroup",
            "limits",
            Status::Skip,
            None,
            None,
            "no cgroup v2 hierarchy",
        ),
    }

    // --- Data directory (VIII.2.5, VIII.2.6)
    if let Some(dir) = data_dir {
        check_data_dir(host, dir, &mut r);
    }
    r
}

fn check_data_dir(host: &Host, dir: &Path, r: &mut Report) {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let Some((mp, fs, src, opts)) = mount_of(host, &dir) else {
        r.add(
            "data_dir.filesystem",
            "storage",
            Status::Skip,
            None,
            Some("xfs or ext4"),
            format!("{}: mount table not readable here", dir.display()),
        );
        return;
    };
    let id = |k: &str| format!("data_dir.{k}");
    let where_ = format!("{} on {mp} ({src})", dir.display());
    // STRATA-SYS-02-017/022
    let unsafe_fs = fs.starts_with("nfs")
        || fs == "cifs"
        || fs.starts_with("smb")
        || fs.starts_with("fuse")
        || fs == "overlay"
        || fs == "9p";
    let (st, detail) = match fs.as_str() {
        "xfs" => (Status::Pass, format!("{where_}: recommended")),
        "ext4" => (Status::Pass, format!("{where_}: supported (xfs recommended)")),
        _ if unsafe_fs => (Status::Fail, format!("{where_}: network/overlay/FUSE filesystems have unreliable fsync semantics and are rejected for data (STRATA-SYS-02-022)")),
        _ => (Status::Warn, format!("{where_}: not a supported production filesystem")),
    };
    r.add(
        id("filesystem"),
        "storage",
        st,
        Some(fs.clone()),
        Some("xfs (or ext4)"),
        detail,
    );
    // STRATA-SYS-02-018
    let o: Vec<&str> = opts.split(',').collect();
    if o.contains(&"nobarrier") {
        r.add(
            id("mount.barrier"),
            "storage",
            Status::Fail,
            Some("nobarrier".into()),
            Some("barriers on (default)"),
            "nobarrier breaks fsync durability",
        );
    }
    let atime = if o.contains(&"noatime") {
        Status::Pass
    } else {
        Status::Warn
    };
    r.add(
        id("mount.atime"),
        "storage",
        atime,
        Some(
            o.iter()
                .find(|x| x.contains("atime"))
                .map_or("atime", |s| s)
                .to_owned(),
        ),
        Some("noatime"),
        "access-time updates turn reads into writes",
    );
    if o.contains(&"discard") {
        r.add(
            id("mount.discard"),
            "storage",
            Status::Warn,
            Some("discard".into()),
            Some("periodic fstrim (weekly)"),
            "online discard causes latency spikes",
        );
    }
    if fs == "ext4" && o.contains(&"data=writeback") {
        r.add(
            id("mount.ext4_data"),
            "storage",
            Status::Warn,
            Some("data=writeback".into()),
            Some("data=ordered"),
            "writeback mode can expose stale data after a crash",
        );
    }
    // Block device (STRATA-SYS-02-014)
    let Some(dev) = block_device(host, &src) else {
        r.add(
            id("device"),
            "storage",
            Status::Skip,
            Some(src),
            None,
            "no local block device (virtual or network filesystem)",
        );
        return;
    };
    let q = |f: &str| host.read(&format!("sys/class/block/{dev}/queue/{f}"));
    let model = host
        .read(&format!("sys/class/block/{dev}/device/model"))
        .unwrap_or_else(|| "unknown".into());
    let rotational = q("rotational").as_deref() == Some("1");
    r.add(
        id("device"),
        "storage",
        if rotational {
            Status::Warn
        } else {
            Status::Pass
        },
        Some(format!("{dev} ({model})")),
        Some("NVMe/SSD"),
        if rotational {
            "rotational disk: random reads and fsync are slow"
        } else {
            "solid-state device"
        },
    );
    if let Some(s) = q("scheduler") {
        let sel = selected(&s).unwrap_or(&s).to_owned();
        let st = if rotational || sel == "none" {
            Status::Pass
        } else {
            Status::Warn
        };
        r.add(
            id("scheduler"),
            "storage",
            st,
            Some(sel),
            Some("none (NVMe)"),
            "NVMe has deep hardware queues; software scheduling adds latency",
        );
    }
    if let Some(ra) = q("read_ahead_kb") {
        let st = if num(&ra).is_some_and(|n| n <= 512) {
            Status::Pass
        } else {
            Status::Warn
        };
        r.add(
            id("read_ahead_kb"),
            "storage",
            st,
            Some(ra),
            Some("128"),
            "the engine reads exactly what it needs; large kernel read-ahead wastes bandwidth",
        );
    }
    match q("write_cache") {
        Some(wc) => {
            let volatile = wc == "write back";
            r.add(
                id("write_cache"),
                "storage",
                Status::Info,
                Some(wc),
                None,
                if volatile {
                    "volatile write cache: durability relies on fsync flushing it; power-loss protection (PLP): unknown (NVMe identify not read)"
                } else {
                    "write-through: no volatile cache"
                },
            );
        }
        None => r.add(
            id("write_cache"),
            "storage",
            Status::Skip,
            None,
            None,
            "not reported by this device",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(root: &Path, rel: &str, v: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, format!("{v}\n")).unwrap();
    }

    fn sysctl(root: &Path, name: &str, v: &str) {
        put(root, &format!("proc/sys/{}", name.replace('.', "/")), v);
    }

    /// A host following every recommendation, built from the shipped
    /// `packaging/sysctl.d/90-stratadb.conf` (so the file and the checks
    /// cannot drift apart).
    fn good_host(root: &Path) {
        let conf = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/sysctl.d/90-stratadb.conf"
        ))
        .unwrap();
        for l in conf.lines() {
            let l = l.trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            let (k, v) = l.split_once('=').unwrap();
            sysctl(root, k.trim(), v.trim());
        }
        sysctl(root, "kernel.osrelease", "6.6.32-generic");
        put(root, "proc/meminfo", "MemTotal:       16000000 kB");
        put(
            root,
            "sys/kernel/mm/transparent_hugepage/enabled",
            "always [madvise] never",
        );
        put(
            root,
            "sys/kernel/mm/transparent_hugepage/defrag",
            "always defer [defer+madvise] madvise never",
        );
        put(
            root,
            "sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            "performance",
        );
        put(
            root,
            "sys/devices/system/clocksource/clocksource0/current_clocksource",
            "tsc",
        );
        put(
            root,
            "proc/cpuinfo",
            "model name\t: Test CPU\nflags\t\t: fpu constant_tsc nonstop_tsc",
        );
        put(root, "sys/devices/system/node/node0/x", "");
        put(
            root,
            "proc/self/cgroup",
            "0::/system.slice/stratadb.service",
        );
        put(
            root,
            "sys/fs/cgroup/system.slice/stratadb.service/cpu.max",
            "400000 100000",
        );
        put(
            root,
            "sys/fs/cgroup/system.slice/stratadb.service/memory.max",
            "17179869184",
        );
        put(root, "proc/swaps", "Filename Type Size Used Priority");
        put(root, "proc/self/mounts", "/dev/nvme0n1p2 / ext4 rw,noatime 0 0\n/dev/nvme1n1 /data xfs rw,noatime,inode64,logbufs=8,logbsize=256k 0 0");
        put(root, "sys/class/block/nvme1n1/queue/rotational", "0");
        put(
            root,
            "sys/class/block/nvme1n1/queue/scheduler",
            "[none] mq-deadline",
        );
        put(root, "sys/class/block/nvme1n1/queue/read_ahead_kb", "128");
        put(
            root,
            "sys/class/block/nvme1n1/queue/write_cache",
            "write back",
        );
        put(root, "sys/class/block/nvme1n1/device/model", "Test NVMe");
    }

    fn status(r: &Report, id: &str) -> Status {
        r.checks
            .iter()
            .find(|c| c.id == id)
            .map(|c| c.status)
            .unwrap_or_else(|| panic!("no check {id}"))
    }

    #[test]
    fn recommended_host_has_no_warnings_or_failures() {
        let d = tempfile::tempdir().unwrap();
        good_host(d.path());
        let r = run(&Host::at(d.path()), Some(Path::new("/data/strata")));
        let bad: Vec<_> = r
            .checks
            .iter()
            .filter(|c| c.status >= Status::Warn && !c.id.starts_with("rlimit"))
            .collect();
        assert!(bad.is_empty(), "{bad:#?}");
        for s in SYSCTLS {
            assert_eq!(status(&r, s.name), Status::Pass, "{}", s.name);
        }
        assert_eq!(status(&r, "kernel"), Status::Pass);
        assert_eq!(status(&r, "data_dir.filesystem"), Status::Pass);
        assert_eq!(status(&r, "data_dir.device"), Status::Pass);
        assert_eq!(status(&r, "data_dir.write_cache"), Status::Info);
        let j = r.to_json();
        assert_eq!(j["host"]["kernel"], "6.6.32-generic");
        assert_eq!(j["host"]["cpu_model"], "Test CPU");
        assert_eq!(j["report_version"], 1);
        let text = r.to_text();
        assert!(
            text.starts_with("StrataDB host readiness report  (kernel 6.6.32-generic"),
            "{text}"
        );
        assert!(text.contains("[PASS] clocksource=tsc"), "{text}");
    }

    #[test]
    fn misconfigurations_warn_or_fail() {
        let d = tempfile::tempdir().unwrap();
        good_host(d.path());
        let root = d.path();
        sysctl(root, "vm.swappiness", "60");
        sysctl(root, "kernel.numa_balancing", "1");
        sysctl(root, "kernel.osrelease", "5.10.0");
        put(
            root,
            "sys/kernel/mm/transparent_hugepage/enabled",
            "[always] madvise never",
        );
        put(
            root,
            "sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            "powersave",
        );
        put(
            root,
            "sys/devices/system/clocksource/clocksource0/current_clocksource",
            "hpet",
        );
        put(
            root,
            "proc/self/mounts",
            "/dev/nvme1n1 /data ext4 rw,relatime,discard,nobarrier,data=writeback 0 0",
        );
        put(
            root,
            "sys/class/block/nvme1n1/queue/scheduler",
            "none [mq-deadline]",
        );
        let r = run(&Host::at(root), Some(Path::new("/data/x")));
        assert_eq!(status(&r, "vm.swappiness"), Status::Fail);
        assert_eq!(status(&r, "kernel.numa_balancing"), Status::Warn);
        assert_eq!(status(&r, "kernel"), Status::Fail);
        assert_eq!(status(&r, "transparent_hugepage"), Status::Warn);
        assert_eq!(status(&r, "cpufreq.governor"), Status::Warn);
        assert_eq!(status(&r, "clocksource"), Status::Warn);
        assert_eq!(status(&r, "data_dir.mount.barrier"), Status::Fail);
        assert_eq!(status(&r, "data_dir.mount.atime"), Status::Warn);
        assert_eq!(status(&r, "data_dir.mount.discard"), Status::Warn);
        assert_eq!(status(&r, "data_dir.mount.ext4_data"), Status::Warn);
        assert_eq!(status(&r, "data_dir.scheduler"), Status::Warn);
        assert!(r.has_fail());
        let text = r.to_text();
        assert!(
            text.contains("[FAIL] vm.swappiness=60  -> recommend 1"),
            "{text}"
        );
        assert!(
            text.contains("Production readiness: NOT READY (fix FAIL items)"),
            "{text}"
        );
        let j = r.to_json();
        assert_eq!(j["summary"]["ready"], false);
        assert!(j["summary"]["fail"].as_u64().unwrap() >= 3);
        // Stable keys on every check.
        for c in j["checks"].as_array().unwrap() {
            for k in ["id", "category", "status", "value", "recommended", "detail"] {
                assert!(c.get(k).is_some(), "{c}");
            }
        }
    }

    #[test]
    fn unsafe_filesystems_and_partitions() {
        let d = tempfile::tempdir().unwrap();
        good_host(d.path());
        let root = d.path();
        for fs in ["nfs4", "cifs", "fuse.s3fs", "overlay"] {
            put(
                root,
                "proc/self/mounts",
                &format!("server:/x /data {fs} rw,noatime 0 0"),
            );
            let r = run(&Host::at(root), Some(Path::new("/data/a")));
            assert_eq!(status(&r, "data_dir.filesystem"), Status::Fail, "{fs}");
            assert_eq!(status(&r, "data_dir.device"), Status::Skip, "{fs}");
        }
        put(
            root,
            "proc/self/mounts",
            "/dev/sda1 /data btrfs rw,noatime 0 0",
        );
        put(root, "sys/class/block/sda1/partition", "1");
        put(root, "sys/class/block/sda/queue/rotational", "1");
        put(
            root,
            "sys/class/block/sda/queue/scheduler",
            "[mq-deadline] none",
        );
        let r = run(&Host::at(root), Some(Path::new("/data/a")));
        assert_eq!(status(&r, "data_dir.filesystem"), Status::Warn);
        assert_eq!(status(&r, "data_dir.device"), Status::Warn, "rotational");
        assert_eq!(
            status(&r, "data_dir.scheduler"),
            Status::Pass,
            "scheduler fine on HDD"
        );
        // nvme partitions: nvme0n1p3 -> nvme0n1.
        put(root, "sys/class/block/nvme0n1p3/partition", "3");
        assert_eq!(
            block_device(&Host::at(root), "/dev/nvme0n1p3").as_deref(),
            Some("nvme0n1")
        );
        assert_eq!(
            block_device(&Host::at(root), "/dev/sda1").as_deref(),
            Some("sda")
        );
        assert_eq!(block_device(&Host::at(root), "tmpfs"), None);
    }

    #[test]
    fn non_linux_hosts_skip_and_parsers() {
        let d = tempfile::tempdir().unwrap();
        let r = run(&Host::at(d.path()), None);
        assert_eq!(status(&r, "platform"), Status::Info);
        assert!(!r.assessed);
        assert_eq!(r.to_json()["summary"]["ready"], false);
        assert!(r.to_text().contains("NOT ASSESSED"));
        assert_eq!(status(&r, "vm.swappiness"), Status::Skip);
        assert!(
            !r.has_fail()
                || r.checks
                    .iter()
                    .any(|c| c.id == "rlimit.nofile" && c.status == Status::Fail)
        );
        assert_eq!(selected("always [madvise] never"), Some("madvise"));
        assert_eq!(kernel_version("6.6.32-1-lts"), Some((6, 6)));
        assert_eq!(kernel_version("5.15.0"), Some((5, 15)));
    }
}
