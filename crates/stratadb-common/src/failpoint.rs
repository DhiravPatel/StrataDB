//! Named failure-injection points for crash and error testing (ticket C-008).
//!
//! Compiled to no-ops unless the `failpoints` feature is enabled. Release
//! builds must never enable the feature.
//!
//! Configuration (feature enabled):
//! * Environment variable `STRATADB_FAILPOINTS`, read once on first use, e.g.
//!   `wal::after_write=abort@3;flush::after_segment_write=error`.
//! * Programmatically via [`set`] / [`clear_all`] for in-process tests.
//!
//! Actions: `abort` (immediate `std::process::abort`, simulating a process
//! crash with no destructors or buffered-write flushing), `exit` (immediate
//! `std::process::exit(86)` without unwinding — same crash semantics for
//! data already handed to the kernel, but no OS crash report), `error` (returns an
//! injected I/O error), `panic`, `off`. `@N` fires only on the N-th hit (1-based);
//! without it the action fires on every hit.

use crate::error::Result;

/// The action a failpoint performs when it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Abort,
    Exit,
    Error,
    Panic,
}

#[cfg(feature = "failpoints")]
mod imp {
    use super::Action;
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    #[derive(Debug, Clone, Copy)]
    struct Entry {
        action: Action,
        nth: Option<u64>,
        hits: u64,
    }

    fn registry() -> &'static Mutex<HashMap<String, Entry>> {
        static REG: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
        REG.get_or_init(|| {
            let mut map = HashMap::new();
            if let Ok(spec) = std::env::var("STRATADB_FAILPOINTS") {
                for item in spec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                    if let Some((name, action)) = parse_item(item) {
                        map.insert(name, action);
                    }
                }
            }
            Mutex::new(map)
        })
    }

    fn parse_action(s: &str) -> Option<(Option<Action>, Option<u64>)> {
        let (a, nth) = match s.split_once('@') {
            Some((a, n)) => (a, Some(n.parse().ok()?)),
            None => (s, None),
        };
        let action = match a {
            "abort" => Some(Action::Abort),
            "exit" => Some(Action::Exit),
            "error" => Some(Action::Error),
            "panic" => Some(Action::Panic),
            "off" => None,
            _ => return None,
        };
        Some((action, nth))
    }

    fn parse_item(item: &str) -> Option<(String, Entry)> {
        let (name, spec) = item.split_once('=')?;
        let (action, nth) = parse_action(spec.trim())?;
        Some((
            name.trim().to_owned(),
            Entry {
                action: action?,
                nth,
                hits: 0,
            },
        ))
    }

    pub fn set(name: &str, spec: &str) -> bool {
        let Some((action, nth)) = parse_action(spec) else {
            return false;
        };
        let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
        match action {
            Some(action) => {
                reg.insert(
                    name.to_owned(),
                    Entry {
                        action,
                        nth,
                        hits: 0,
                    },
                );
            }
            None => {
                reg.remove(name);
            }
        }
        true
    }

    pub fn clear_all() {
        registry().lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    pub fn eval(name: &str) -> Option<Action> {
        let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
        let entry = reg.get_mut(name)?;
        entry.hits += 1;
        match entry.nth {
            Some(n) if n != entry.hits => None,
            _ => Some(entry.action),
        }
    }
}

/// Count a hit on `name` and return the action if the failpoint fires now.
/// Callers that need custom behaviour (e.g. a torn write) use this directly.
#[inline]
pub fn eval(name: &str) -> Option<Action> {
    #[cfg(feature = "failpoints")]
    {
        imp::eval(name)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        None
    }
}

/// Evaluate failpoint `name`, performing its action if it fires.
#[inline]
pub fn hit(name: &str) -> Result<()> {
    match eval(name) {
        None => Ok(()),
        Some(action) => fire(name, action),
    }
}

/// Perform `action` for failpoint `name`.
pub fn fire(name: &str, action: Action) -> Result<()> {
    match action {
        Action::Abort => {
            eprintln!("failpoint {name}: abort");
            std::process::abort()
        }
        Action::Exit => {
            eprintln!("failpoint {name}: exit");
            std::process::exit(EXIT_CODE)
        }
        Action::Error => Err(crate::Error::io(
            format!("failpoint {name}"),
            std::io::Error::other("injected failure"),
        )),
        Action::Panic => panic!("failpoint {name}: panic"),
    }
}

/// Process exit status used by the `exit` action.
pub const EXIT_CODE: i32 = 86;

/// Configure a failpoint at runtime. Returns false if the spec is invalid or
/// the `failpoints` feature is disabled.
pub fn set(name: &str, spec: &str) -> bool {
    #[cfg(feature = "failpoints")]
    {
        imp::set(name, spec)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = (name, spec);
        false
    }
}

/// Remove all configured failpoints.
pub fn clear_all() {
    #[cfg(feature = "failpoints")]
    imp::clear_all();
}

/// Whether failpoints are compiled into this build.
pub const fn enabled() -> bool {
    cfg!(feature = "failpoints")
}
