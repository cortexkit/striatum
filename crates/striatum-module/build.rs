use sha2::{Digest, Sha256};

fn main() {
    println!("cargo:rerun-if-env-changed=CK_BUILD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=CK_BUILD_GIT_DIRTY");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    let revision = std::env::var("CK_BUILD_GIT_SHA").ok().filter(|revision| {
        revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    // Missing or malformed dirty flags must not certify a revision as clean.
    let dirty = std::env::var("CK_BUILD_GIT_DIRTY").as_deref() != Ok("false");
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
