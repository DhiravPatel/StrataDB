//! Run manifest (spec VIII.19, STRATA-SYS-19-004): everything needed to tell
//! whether two benchmark runs are comparable.
//!
//! Values that cannot be determined on this host are recorded as `"unknown"`
//! — never guessed. Linux exposes most host settings under `/proc` and
//! `/sys`; macOS exposes fewer (`sysctl`), and there is no portable source
//! for storage firmware, so those fields stay `unknown` off Linux.

use std::path::Path;
use std::process::Command;

use serde_json::{json, Map, Value as J};
use sha2::{Digest, Sha256};

const UNKNOWN: &str = "unknown";

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn cmd(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn or_unknown(v: Option<String>) -> J {
    J::String(v.unwrap_or_else(|| UNKNOWN.to_owned()))
}

/// `key: value` field of `/proc/cpuinfo` (first occurrence).
fn cpuinfo(key: &str) -> Option<String> {
    read("/proc/cpuinfo")?.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == key).then(|| v.trim().to_owned())
    })
}

pub fn cpu_model() -> Option<String> {
    cpuinfo("model name").or_else(|| cmd("sysctl", &["-n", "machdep.cpu.brand_string"]))
}

/// Linux sysctls that affect database performance (spec VIII.2), with their
/// current values. The "diff from baseline" is computed by `compare`.
const SYSCTLS: &[&str] = &[
    "vm/swappiness",
    "vm/dirty_ratio",
    "vm/dirty_background_ratio",
    "vm/max_map_count",
    "kernel/numa_balancing",
    "kernel/sched_autogroup_enabled",
    "fs/file-max",
    "net/core/somaxconn",
];

fn sysctls() -> J {
    let mut m = Map::new();
    for k in SYSCTLS {
        if let Some(v) = read(&format!("/proc/sys/{k}")) {
            m.insert(k.replace('/', "."), J::String(v));
        }
    }
    if m.is_empty() {
        J::String(UNKNOWN.to_owned())
    } else {
        J::Object(m)
    }
}

/// Mount point, filesystem type and options of the filesystem holding `dir`.
/// The mount point comes from `df -P` (correct across macOS firmlinks and
/// Linux bind mounts); type and options from `/proc/mounts` or `mount`.
fn filesystem(dir: &Path) -> J {
    let Some(mp) = cmd("df", &["-P", &dir.to_string_lossy()]).and_then(|out| {
        // Last column of the second line; mount points may contain spaces,
        // so take everything after the 5th field.
        let line = out.lines().nth(1)?.to_owned();
        let fields: Vec<&str> = line.split_whitespace().collect();
        (fields.len() >= 6).then(|| fields[5..].join(" "))
    }) else {
        return J::String(UNKNOWN.to_owned());
    };
    let mut found: Option<(String, String)> = None;
    if let Some(m) = read("/proc/mounts") {
        for l in m.lines() {
            let f: Vec<&str> = l.split_whitespace().collect();
            if f.len() >= 4 && f[1] == mp {
                found = Some((f[2].to_owned(), f[3].to_owned()));
            }
        }
    } else if let Some(m) = cmd("mount", &[]) {
        // macOS: `/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled)`
        for l in m.lines() {
            if let Some((_, rest)) = l.split_once(" on ") {
                if let Some((p, opts)) = rest.rsplit_once(" (") {
                    if p == mp {
                        let opts = opts.trim_end_matches(')').to_owned();
                        let fs = opts.split(',').next().unwrap_or(UNKNOWN).trim().to_owned();
                        found = Some((fs, opts));
                    }
                }
            }
        }
    }
    let (fs, opts) = found.unwrap_or_else(|| (UNKNOWN.to_owned(), UNKNOWN.to_owned()));
    json!({"mount_point": mp, "type": fs, "options": opts})
}

/// Block devices: model and firmware (Linux sysfs only).
fn storage() -> J {
    let Ok(entries) = std::fs::read_dir("/sys/block") else {
        return J::String(UNKNOWN.to_owned());
    };
    let mut devs: Vec<J> = Vec::new();
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with("loop") || name.starts_with("ram") || name.starts_with("zram") {
            continue;
        }
        let p = |f: &str| read(&format!("/sys/block/{name}/{f}"));
        devs.push(json!({
            "device": name,
            "model": or_unknown(p("device/model")),
            "firmware": or_unknown(p("device/firmware_rev").or_else(|| p("device/rev"))),
            "rotational": or_unknown(p("queue/rotational")),
            "write_cache": or_unknown(p("queue/write_cache")),
        }));
    }
    devs.sort_by_key(|d| d["device"].as_str().unwrap_or_default().to_owned());
    J::Array(devs)
}

/// Current load average (Linux `/proc/loadavg`, macOS `sysctl vm.loadavg`).
pub fn load_average() -> Option<String> {
    read("/proc/loadavg").or_else(|| cmd("sysctl", &["-n", "vm.loadavg"]))
}

fn turbo() -> Option<String> {
    if let Some(v) = read("/sys/devices/system/cpu/intel_pstate/no_turbo") {
        return Some(if v == "1" { "disabled" } else { "enabled" }.to_owned());
    }
    read("/sys/devices/system/cpu/cpufreq/boost")
        .map(|v| if v == "1" { "enabled" } else { "disabled" }.to_owned())
}

/// Hex SHA-256 of `bytes`.
pub fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn git(args: &[&str]) -> Option<String> {
    cmd("git", args)
}

/// Stable identity of the machine for regression baselines ("same runner"),
/// derived from hardware and OS facts only (no hostname or user data).
pub fn runner_fingerprint(m: &J) -> String {
    let h = &m["host"];
    let key = format!(
        "{}|{}|{}|{}|{}",
        h["cpu_model"], h["logical_cpus"], h["os"], h["arch"], h["memory_bytes"]
    );
    sha256(key.as_bytes())[..16].to_owned()
}

/// The manifest for a run with `dataset`, `scenario_bytes` and engine
/// `settings`; `data_dir` locates the filesystem under test.
pub fn collect(
    data_dir: &Path,
    hw_profile: &str,
    dataset: J,
    scenario_bytes: &[u8],
    settings: J,
) -> J {
    let sha = git(&["rev-parse", "HEAD"]);
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"]).is_some();
    let mut m = json!({
        "manifest_version": 1,
        "started_at": stratadb_common::time::format_rfc3339(stratadb_common::time::now_unix_nanos()),
        "hw_profile": hw_profile,
        "build": {
            "stratadb_version": env!("CARGO_PKG_VERSION"),
            "git_sha": or_unknown(sha),
            "git_dirty": dirty,
            "profile": env!("STRATA_BUILD_PROFILE"),
            "opt_level": env!("STRATA_BUILD_OPT_LEVEL"),
            "debug_assertions": cfg!(debug_assertions),
            "target": env!("STRATA_BUILD_TARGET"),
            "rustc": env!("STRATA_BUILD_RUSTC"),
            "allocator": "system",
        },
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "kernel": or_unknown(cmd("uname", &["-sr"])),
            "cpu_model": or_unknown(cpu_model()),
            "microcode": or_unknown(cpuinfo("microcode")),
            "logical_cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            "memory_bytes": crate::host_memory().map_or(J::String(UNKNOWN.to_owned()), J::from),
            "governor": or_unknown(read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")),
            "thp": or_unknown(read("/sys/kernel/mm/transparent_hugepage/enabled")),
            "smt": or_unknown(read("/sys/devices/system/cpu/smt/control")
                .or_else(|| cmd("sysctl", &["-n", "hw.optional.arm64"]).map(|_| "n/a (no SMT on Apple silicon)".to_owned()))),
            "turbo": or_unknown(turbo()),
            "sysctls": sysctls(),
            "storage": storage(),
            "filesystem": filesystem(data_dir),
            "load_average": or_unknown(load_average()),
        },
        "dataset": dataset,
        "scenario_sha256": sha256(scenario_bytes),
        "engine_settings": settings,
    });
    let fp = runner_fingerprint(&m);
    m["runner_fingerprint"] = J::String(fp);
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_has_every_required_field() {
        let dir = tempfile::tempdir().unwrap();
        let m = collect(
            dir.path(),
            "HW-REF-7",
            json!({"seed": 1}),
            b"x",
            json!({"durability": "async"}),
        );
        for path in [
            "/build/git_sha",
            "/build/profile",
            "/build/allocator",
            "/build/rustc",
            "/host/cpu_model",
            "/host/microcode",
            "/host/kernel",
            "/host/sysctls",
            "/host/governor",
            "/host/thp",
            "/host/smt",
            "/host/turbo",
            "/host/storage",
            "/host/filesystem",
            "/dataset/seed",
            "/scenario_sha256",
            "/engine_settings/durability",
            "/runner_fingerprint",
        ] {
            assert!(m.pointer(path).is_some(), "missing {path}: {m:#}");
        }
        assert_eq!(m["scenario_sha256"], sha256(b"x"));
        assert!(m["build"]["rustc"].as_str().unwrap().starts_with("rustc "));
        // Same machine → same fingerprint; it does not depend on the run.
        let m2 = collect(dir.path(), "HW-REF-7", json!({}), b"y", json!({}));
        assert_eq!(m["runner_fingerprint"], m2["runner_fingerprint"]);
        assert_eq!(
            sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
