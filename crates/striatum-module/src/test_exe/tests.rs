//! Stage self-executing tests before launching them from removable build volumes.

pub(crate) fn current_test_exe() -> std::io::Result<std::path::PathBuf> {
    Ok(cortexkit_test_support::stage_test_binary(
        &std::env::current_exe()?,
    ))
}
