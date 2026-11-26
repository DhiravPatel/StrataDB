//! Process-wide thread inventory (spec VIII.3, STRATA-SYS-03-014/015).
//!
//! Every long-lived engine thread registers itself with a role and its OS
//! thread name. The stable `sx-*` prefixes let `top -H`, `perf` and eBPF
//! tools attribute CPU. A thread records its own CPU time
//! (`CLOCK_THREAD_CPUTIME_ID`) at natural points, such as after a task or
//! when parking, because no portable safe API reads another thread's CPU
//! clock. Each entry therefore says how old its CPU figure is.
//!
//! The registry is bounded by the thread budget: entries exist only for
//! live registered threads and are removed when the thread exits (or when
//! the [`Registration`] is dropped).

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// Thread roles and their name prefixes (STRATA-SYS-03-015).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// The process main thread.
    Main,
    /// Network / async runtime threads (`sx-net-NN`).
    Net,
    /// Query scan workers (`sx-work-NN`).
    Work,
    /// Blocking request execution: commits that wait for fsync, query
    /// execution, admin operations (`sx-blk-NN`).
    Blk,
    /// Background flush, WAL sync, compaction, retention (`sx-maint`).
    Maint,
}

impl Role {
    pub const ALL: [Role; 5] = [Role::Main, Role::Net, Role::Work, Role::Blk, Role::Maint];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Main => "main",
            Role::Net => "net",
            Role::Work => "work",
            Role::Blk => "blk",
            Role::Maint => "maint",
        }
    }

    /// OS thread name for instance `n` (at most 15 bytes, the Linux limit).
    pub fn thread_name(self, n: usize) -> String {
        match self {
            Role::Main => "sx-main".to_owned(),
            Role::Maint => "sx-maint".to_owned(),
            Role::Net => format!("sx-net-{n:02}"),
            Role::Work => format!("sx-work-{n:02}"),
            Role::Blk => format!("sx-blk-{n:02}"),
        }
    }
}

/// Upper bound on engine threads (STRATA-SYS-03-014): `2 × logical_cpus + 32`.
pub fn thread_budget(logical_cpus: usize) -> usize {
    2 * logical_cpus.max(1) + 32
}

pub fn logical_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

#[derive(Debug)]
struct Entry {
    id: u64,
    name: String,
    role: Role,
    cpu_ns: AtomicU64,
    /// Nanoseconds since `epoch()` of the last CPU update.
    updated_ns: AtomicU64,
}

/// A live thread's registry entry; dropping it removes the entry.
#[derive(Debug)]
pub struct Registration {
    entry: Arc<Entry>,
}

/// Snapshot of one registered thread.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadInfo {
    pub name: String,
    pub role: Role,
    /// CPU time (user + system) as last recorded by the thread itself.
    pub cpu_seconds: f64,
    /// Age of `cpu_seconds`.
    pub cpu_age_ms: u64,
    /// Threads are not pinned to cores (no core affinity is set).
    pub affinity: &'static str,
}

fn registry() -> &'static Mutex<Vec<Arc<Entry>>> {
    static R: OnceLock<Mutex<Vec<Arc<Entry>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(Vec::new()))
}

fn epoch() -> Instant {
    static E: OnceLock<Instant> = OnceLock::new();
    *E.get_or_init(Instant::now)
}

fn now_ns() -> u64 {
    u64::try_from(epoch().elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Register the calling thread under `role`. Its name is the OS thread name
/// (set by `std::thread::Builder::name`), or the role's canonical name.
pub fn register(role: Role) -> Registration {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let name = std::thread::current()
        .name()
        .map_or_else(|| role.thread_name(0), str::to_owned);
    let entry = Arc::new(Entry {
        id: NEXT.fetch_add(1, Ordering::Relaxed),
        name,
        role,
        cpu_ns: AtomicU64::new(0),
        updated_ns: AtomicU64::new(now_ns()),
    });
    let r = Registration { entry };
    r.update_cpu();
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Arc::clone(&r.entry));
    r
}

impl Registration {
    /// Record this thread's CPU time. Must be called on the registered
    /// thread.
    pub fn update_cpu(&self) {
        let cpu = crate::cpu::thread_cpu_time();
        self.entry.cpu_ns.store(
            u64::try_from(cpu.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.entry.updated_ns.store(now_ns(), Ordering::Relaxed);
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let id = self.entry.id;
        registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|e| e.id != id);
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Registration>> = const { RefCell::new(None) };
}

/// Register the calling thread for its whole lifetime (the entry is removed
/// when the thread exits). For threads created by libraries, e.g. through
/// tokio's `on_thread_start`. A second call on the same thread is ignored.
pub fn register_current(role: Role) {
    CURRENT.with(|c| {
        let mut c = c.borrow_mut();
        if c.is_none() {
            *c = Some(register(role));
        }
    });
}

/// Record the calling thread's CPU time, if it was registered with
/// [`register_current`] (cheap: one clock read; e.g. tokio `on_thread_park`).
pub fn update_current_cpu() {
    CURRENT.with(|c| {
        if let Some(r) = c.borrow().as_ref() {
            r.update_cpu();
        }
    });
}

/// All registered live threads, sorted by role then name.
pub fn inventory() -> Vec<ThreadInfo> {
    let now = now_ns();
    let mut v: Vec<ThreadInfo> = registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|e| ThreadInfo {
            name: e.name.clone(),
            role: e.role,
            cpu_seconds: e.cpu_ns.load(Ordering::Relaxed) as f64 / 1e9,
            cpu_age_ms: now.saturating_sub(e.updated_ns.load(Ordering::Relaxed)) / 1_000_000,
            affinity: "unpinned",
        })
        .collect();
    v.sort_by(|a, b| (a.role, &a.name).cmp(&(b.role, &b.name)));
    v
}

/// OS threads of this process, where the OS exposes them without `unsafe`
/// (Linux `/proc/self/task`); `None` elsewhere.
pub fn os_thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|d| d.filter_map(Result::ok).count())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_fit_the_os_limit_and_budget_formula() {
        for r in Role::ALL {
            for n in [0, 7, 99, 255] {
                let name = r.thread_name(n);
                assert!(name.len() <= 15 && name.starts_with("sx-"), "{name}");
            }
        }
        assert_eq!(thread_budget(10), 52);
        assert_eq!(thread_budget(0), 34);
    }

    #[test]
    fn threads_register_update_and_disappear() {
        let mine = |name: &str| inventory().into_iter().find(|t| t.name == name);
        let h = std::thread::Builder::new()
            .name("sx-blk-77".into())
            .spawn(|| {
                register_current(Role::Blk);
                register_current(Role::Blk); // idempotent
                let t = crate::cpu::CpuTimer::start();
                let mut x = 0u64;
                while t.stop().thread_cpu < std::time::Duration::from_millis(15) {
                    x = x.wrapping_mul(31).wrapping_add(7);
                }
                std::hint::black_box(x);
                update_current_cpu();
                let me = inventory()
                    .into_iter()
                    .filter(|t| t.name == "sx-blk-77")
                    .collect::<Vec<_>>();
                assert_eq!(me.len(), 1, "registered once");
                assert_eq!(me[0].role, Role::Blk);
                assert!(me[0].cpu_seconds >= 0.015, "{:?}", me[0]);
                assert_eq!(me[0].affinity, "unpinned");
            })
            .unwrap();
        h.join().unwrap();
        // The thread exited: its entry is gone.
        assert!(mine("sx-blk-77").is_none());
        // Explicit registrations are removed on drop.
        let r = std::thread::Builder::new()
            .name("sx-maint".into())
            .spawn(|| {
                let reg = register(Role::Maint);
                assert!(inventory()
                    .iter()
                    .any(|t| t.name == "sx-maint" && t.role == Role::Maint));
                drop(reg);
                inventory().iter().any(|t| t.name == "sx-maint")
            })
            .unwrap()
            .join()
            .unwrap();
        assert!(!r);
        if cfg!(target_os = "linux") {
            assert!(os_thread_count().unwrap() >= 1);
        }
    }
}
