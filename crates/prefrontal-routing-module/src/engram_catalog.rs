use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const CATALOG_FILE_NAME: &str = "engram-catalog.json";
const CATALOG_BYTES: &[u8] = include_bytes!("../engram-catalog.json");

/// Write the engram catalog at startup; file I/O failures must not prevent routing from booting.
pub fn write_startup_catalog() {
    let directory = crate::config_overrides::default_store_path()
        .and_then(|store| store.parent().map(Path::to_path_buf));
    let result = directory
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "routing data home is unavailable"))
        .and_then(|directory| write_catalog_at(&directory, CATALOG_BYTES));
    if let Err(error) = result {
        eprintln!("[prefrontal-routing] WARN: could not write engram catalog: {error}");
    }
}

fn write_catalog_at(directory: &Path, bytes: &[u8]) -> io::Result<bool> {
    std::fs::create_dir_all(directory)?;
    let metadata = std::fs::symlink_metadata(directory)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "routing data path is not a real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }

    let destination = directory.join(CATALOG_FILE_NAME);
    match std::fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => {
            if std::fs::read(&destination)? == bytes {
                return Ok(false);
            }
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(
            ".{CATALOG_FILE_NAME}.tmp-{}-{sequence}",
            std::process::id()
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };

    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &destination)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map(|()| true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const EXPECTED_CATALOG: &str = r#"{
  "schema_version": 1,
  "module_id": "prefrontal-routing",
  "entries": [
    {
      "entry_id": "prefrontal-routing/route-state-store",
      "class": "portable",
      "mechanism": "page-db",
      "path": "store.db",
      "writer_interaction": "read-transaction-live"
    },
    {
      "entry_id": "prefrontal-routing/engram-catalog",
      "class": "excluded",
      "mechanism": "filetree",
      "path": "engram-catalog.json",
      "reason": "The module regenerates this catalog from its bundled crate source at startup."
    }
  ]
}
"#;

    fn entries(bytes: &[u8]) -> Vec<(String, String)> {
        let catalog: Value = serde_json::from_slice(bytes).expect("valid catalog JSON");
        catalog["entries"]
            .as_array()
            .expect("entries array")
            .iter()
            .map(|entry| {
                (
                    entry["entry_id"].as_str().expect("entry id").to_owned(),
                    entry["path"].as_str().expect("entry path").to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn catalog_bytes_are_pinned() {
        assert_eq!(CATALOG_BYTES, EXPECTED_CATALOG.as_bytes());
    }

    #[test]
    fn committed_catalog_passes_engram_catalog_validation_contract() {
        let catalog: Value = serde_json::from_slice(CATALOG_BYTES).expect("catalog JSON");
        assert_eq!(catalog["schema_version"], 1);
        assert_eq!(catalog["module_id"], "prefrontal-routing");
        let entries = catalog["entries"].as_array().expect("entries array");
        let mut ids = std::collections::HashSet::new();
        for entry in entries {
            let id = entry["entry_id"].as_str().expect("entry id");
            assert!(ids.insert(id), "duplicate entry id {id}");
            let class = entry["class"].as_str().expect("entry class");
            assert!(matches!(class, "portable" | "device-local" | "excluded"));
            let mechanism = entry["mechanism"].as_str().expect("mechanism");
            assert!(matches!(mechanism, "whole-db" | "page-db" | "filetree"));
            let path = entry["path"].as_str().expect("entry path");
            assert!(!path.is_empty() && !Path::new(path).is_absolute());
            assert!(path.split('/').all(|part| !matches!(part, "" | "." | "..")));
            assert!(!path.contains('\\') && !path.contains(':'));
            if class == "excluded" {
                assert!(entry["reason"]
                    .as_str()
                    .is_some_and(|reason| !reason.trim().is_empty()));
            }
            if class == "portable" && mechanism == "page-db" {
                assert_eq!(entry["writer_interaction"], "read-transaction-live");
            }
        }
    }

    #[test]
    fn catalog_database_path_matches_the_route_store_opener() {
        let path = entries(CATALOG_BYTES)
            .into_iter()
            .find_map(|(entry_id, path)| {
                (entry_id == "prefrontal-routing/route-state-store").then_some(path)
            })
            .expect("route-state entry");
        let opened_path = crate::config_overrides::default_store_path().expect("store path");
        assert_eq!(opened_path.file_name().unwrap().to_string_lossy(), path);
        assert_eq!(path, "store.db");
    }

    #[test]
    fn second_startup_preserves_committed_bytes_without_rewriting_the_file() {
        let directory = prefrontal_core_store::scratch_root().join(format!(
            "routing-engram-catalog-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let committed = include_bytes!("../engram-catalog.json");
        assert_eq!(CATALOG_BYTES, committed);
        assert!(write_catalog_at(&directory, CATALOG_BYTES).expect("first catalog write"));
        let destination = directory.join(CATALOG_FILE_NAME);
        assert_eq!(
            std::fs::read(&destination).expect("read first catalog"),
            committed
        );
        let before = std::fs::metadata(&destination).expect("stat first catalog");
        let rewrote = write_catalog_at(&directory, committed).expect("second catalog write");
        assert_eq!(
            std::fs::read(&destination).expect("read second catalog"),
            committed
        );
        let after = std::fs::metadata(&destination).expect("stat second catalog");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(before.ino(), after.ino(), "identical catalog was replaced");
        }
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
        assert!(!rewrote, "identical catalog reported a write");
        std::fs::remove_dir_all(directory).expect("remove catalog test directory");
    }
}
