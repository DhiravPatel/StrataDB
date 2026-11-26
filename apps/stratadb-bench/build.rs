//! Records build facts for the run manifest (spec VIII.19, STRATA-SYS-19-004).

use std::process::Command;

fn main() {
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| "unknown".to_owned());
    println!("cargo:rustc-env=STRATA_BUILD_PROFILE={}", env("PROFILE"));
    println!(
        "cargo:rustc-env=STRATA_BUILD_OPT_LEVEL={}",
        env("OPT_LEVEL")
    );
    println!("cargo:rustc-env=STRATA_BUILD_TARGET={}", env("TARGET"));
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let version = Command::new(rustc)
        .arg("-V")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=STRATA_BUILD_RUSTC={version}");
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=RUSTC");
}
