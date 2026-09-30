use sha2::{Digest, Sha256};
use std::process::Command;

fn main() {
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-path", "HEAD"])
        .output()
    {
        if output.status.success() {
            println!(
                "cargo:rerun-if-changed={}",
                String::from_utf8_lossy(&output.stdout).trim()
            );
        }
    }

    if let Ok(reference) = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()
    {
        if reference.status.success() {
            let reference = String::from_utf8_lossy(&reference.stdout).trim().to_owned();
            if let Ok(path) = Command::new("git")
                .args(["rev-parse", "--git-path", &reference])
                .output()
            {
                if path.status.success() {
                    println!(
                        "cargo:rerun-if-changed={}",
                        String::from_utf8_lossy(&path.stdout).trim()
                    );
                }
            }
        }
    }
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|revision| {
            revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        });
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|output| output.status.success() && !output.stdout.is_empty());
    println!(
        "cargo:rustc-env=PREFRONTAL_BUILD_GIT_SHA={}",
        revision.as_deref().unwrap_or("unknown")
    );
    println!("cargo:rustc-env=PREFRONTAL_BUILD_GIT_DIRTY={dirty}");
    let lock_digest = std::fs::read("../../Cargo.lock")
        .ok()
        .map(|contents| format!("{:x}", Sha256::digest(contents)));
    println!(
        "cargo:rustc-env=PREFRONTAL_BUILD_LOCK_DIGEST={}",
        lock_digest.as_deref().unwrap_or("unknown")
    );
}
