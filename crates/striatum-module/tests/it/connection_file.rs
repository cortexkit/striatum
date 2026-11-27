use std::path::Path;

pub(super) fn write_private(path: &Path, bytes: &[u8]) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[cfg(windows)]
    {
        use std::io::Write;

        subc_os::windows_acl::create_private_file(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }
}
