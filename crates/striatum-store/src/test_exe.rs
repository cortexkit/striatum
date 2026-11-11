//! Routing's test executable staging stays independent of prefrontal helpers.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub(crate) fn current_test_exe() -> std::io::Result<PathBuf> {
    static EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();
    if let Some(executable) = EXECUTABLE.get() {
        return Ok(executable.clone());
    }
    let source = std::env::current_exe()?;
    Ok(EXECUTABLE
        .get_or_init(|| {
            stage_if_mounted(source, |source| {
                cortexkit_test_support::ckdev_binary(source)
            })
        })
        .clone())
}

fn stage_if_mounted(source: PathBuf, stage: impl FnOnce(&Path) -> PathBuf) -> PathBuf {
    if !is_under_mounted_volume(&source) {
        return source;
    }
    let staged = stage(&source);
    assert!(
        !is_under_mounted_volume(&staged),
        "refusing to execute a routing test program from a mounted volume: {}",
        staged.display()
    );
    staged
}

// Resolve symlinks so boot-volume aliases are unchanged and indirect paths to
// removable drives are staged, matching the shared test-support policy.
fn is_under_mounted_volume(path: &Path) -> bool {
    let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut components = resolved.components();
    matches!(components.next(), Some(std::path::Component::RootDir))
        && components.next().is_some_and(|component| {
            component
                .as_os_str()
                .to_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("Volumes"))
        })
}

#[test]
fn mounted_test_exe_is_staged_internally() {
    let source = PathBuf::from("/Volumes/synthetic-test-volume/test-program");
    let internal = PathBuf::from("/internal-test-cache/ckdev-routing-test");
    let staged = stage_if_mounted(source.clone(), |path| {
        assert_eq!(path, source);
        internal.clone()
    });
    assert_eq!(staged, internal);
    assert!(!is_under_mounted_volume(&staged));
}

#[test]
fn boot_volume_test_exe_is_unchanged() {
    let source = PathBuf::from("/internal-test-build/test-program");
    assert_eq!(
        stage_if_mounted(source.clone(), |_| panic!(
            "must not copy a boot-volume test"
        )),
        source
    );
}
