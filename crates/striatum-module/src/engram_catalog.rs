use crate::{ModuleError, SharedStore};
use cortexkit_store_types::{StorageBackend, StorageDescriptor};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use striatum_store::SqliteRouteStateStore;

const CATALOG_FILE_NAME: &str = "engram-catalog.json";
const CATALOG_BYTES: &[u8] = include_bytes!("../engram-catalog.json");

/// Publish backup sources only after both SQLite files are ready. Engram
/// skips absent sources for the whole capture, so opening them later is too late.
pub(crate) fn open_route_state_store(
    descriptor: &StorageDescriptor,
) -> Result<SharedStore, ModuleError> {
    let store = SqliteRouteStateStore::open(descriptor)
        .map_err(|error| ModuleError::Message(format!("open route-state store: {error}")))?;
    write_startup_catalog(descriptor);
    Ok(Arc::new(store))
}

fn write_startup_catalog(descriptor: &StorageDescriptor) {
    let StorageBackend::Sqlite { path } = &descriptor.backend else {
        return;
    };
    let state = Path::new(path);
    // The catalogue uses fixed relative filenames. Do not publish it beside
    // an in-memory or differently named store that it would not describe.
    if state.file_name().and_then(|name| name.to_str()) != Some("store.db") {
        return;
    }
    let directory = state
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let result = if state.is_file() && directory.join("history.db").is_file() {
        write_catalog_at(directory, CATALOG_BYTES)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "state or history backup source is missing",
        ))
    };
    if let Err(error) = result {
        eprintln!("[striatum] WARN: could not write engram catalog: {error}");
    }
}

fn write_catalog_at(directory: &Path, bytes: &[u8]) -> io::Result<bool> {
    #[cfg(unix)]
    std::fs::create_dir_all(directory)?;
    #[cfg(windows)]
    subc_os::windows_acl::create_private_dir(directory)?;
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
        #[cfg(unix)]
        let opened = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
        };
        #[cfg(windows)]
        let opened = subc_os::windows_acl::create_private_file(&path);
        match opened {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };

    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        #[cfg(unix)]
        let published = std::fs::rename(&temporary, &destination);
        #[cfg(windows)]
        // Windows has no directory fsync; use the library's write-through move.
        let published = subc_os::durable_replace(&temporary, &destination).map_err(io::Error::from);
        published
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
  "module_id": "striatum",
  "entries": [
    {
      "entry_id": "striatum/store",
      "class": "portable",
      "mechanism": "page-db",
      "path": "store.db",
      "writer_interaction": "read-transaction-live",
      "exclude_tables": [
        "model_route_decisions",
        "account_observation",
        "account_decision",
        "route_winner_baseline",
        "fusiform_catalog_cursor",
        "routing_operator_quality_state",
        "routing_operator_quality"
      ]
    },
    {
      "entry_id": "striatum/history",
      "class": "portable",
      "mechanism": "page-db",
      "path": "history.db",
      "writer_interaction": "read-transaction-live",
      "exclude_tables": ["quota_reading_log"]
    },
    {
      "entry_id": "striatum/engram-catalog",
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

    fn excluded_tables(entry: &Value) -> Vec<String> {
        entry["exclude_tables"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|table| table.as_str().expect("excluded table name").to_owned())
            .collect()
    }

    #[test]
    fn catalog_bytes_are_pinned() {
        assert_eq!(CATALOG_BYTES, EXPECTED_CATALOG.as_bytes());
    }

    // Engram requires each entry id to start with its module id. The coordinated
    // store move starts a new backup stream with a full baseline; old backups
    // remain restorable under the retired entry id until retention expires.
    #[test]
    fn store_entry_uses_the_registered_module_id() {
        assert!(entries(CATALOG_BYTES)
            .iter()
            .any(|(id, path)| id == "striatum/store" && path == "store.db"));
    }

    #[test]
    fn catalog_captures_state_before_history() {
        // Engram reads one file at a time. If the mover copies a pending row
        // to history and deletes it from state between captures, both captures
        // include the row. In the opposite order, neither would include it.
        let ids: Vec<_> = entries(CATALOG_BYTES)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            ids,
            [
                "striatum/store",
                "striatum/history",
                "striatum/engram-catalog"
            ]
        );
    }

    // Unix-only: script/split-state-history.sh upgrades stores from before history moved into history.db, and no Windows install is that old.
    #[cfg(unix)]
    #[test]
    fn catalog_excluded_tables_exist_in_the_migrated_routing_store_schema() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-engram-catalog-schema");
        let database = scratch.path().join("store.db");
        let descriptor = striatum_store::dev_descriptor(
            "routing-engram-catalog-schema",
            database.to_string_lossy().into_owned(),
        );
        drop(
            SqliteRouteStateStore::open_at_version_for_test(&descriptor, 97)
                .expect("open pre-split store"),
        );
        // A fresh state schema retains empty legacy tables. Check the split
        // layout too, or a quota log exclusion against state would falsely pass.
        striatum_store::run_split_for_test(database.to_str().unwrap(), 1_800_000_000_000);
        let store = open_route_state_store(&descriptor).expect("open store after migrations");
        assert_catalog_excluded_tables(scratch.path());
        drop(store);
    }

    #[cfg(windows)]
    #[test]
    fn catalog_excluded_tables_exist_in_a_fresh_split_store() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-engram-catalog-schema");
        let descriptor = striatum_store::dev_descriptor(
            "routing-engram-catalog-schema",
            scratch.join("store.db").to_string_lossy(),
        );
        let _store = open_route_state_store(&descriptor).expect("open fresh split store");
        assert_catalog_excluded_tables(scratch.path());
    }

    fn assert_catalog_excluded_tables(directory: &Path) {
        let catalog: Value = serde_json::from_slice(CATALOG_BYTES).expect("valid catalog JSON");
        let entries = catalog["entries"].as_array().expect("entries array");
        for entry in entries
            .iter()
            .filter(|entry| entry["mechanism"] == "page-db")
        {
            let path = directory.join(entry["path"].as_str().unwrap());
            let mut actual = excluded_tables(entry);
            check_exclusions(&path, &actual);
            let expected = match entry["entry_id"].as_str().unwrap() {
                "striatum/store" => striatum_store::backup::BACKUP_EXCLUDABLE_TABLES,
                "striatum/history" => striatum_store::backup::BACKUP_HISTORY_EXCLUDABLE_TABLES,
                id => panic!("unexpected database backup entry {id}"),
            };
            let mut expected: Vec<_> = expected
                .iter()
                .map(|(table, _)| (*table).to_owned())
                .collect();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
        }
    }

    fn check_exclusions(path: &Path, excluded: &[String]) {
        let connection =
            rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .expect("open migrated backup source read-only");
        for table in excluded {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
                    [table],
                    |row| row.get(0),
                )
                .expect("query migrated schema");
            assert!(
                exists,
                "{} excludes missing routing table {table}",
                path.display()
            );
        }

        let tables: Vec<String> = connection
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type = 'table' \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .expect("prepare migrated tables query")
            .query_map([], |row| row.get(0))
            .expect("query migrated tables")
            .collect::<rusqlite::Result<_>>()
            .expect("read migrated table names");
        for table in tables {
            if excluded.contains(&table) {
                continue;
            }
            let parents: Vec<String> = connection
                .prepare("SELECT \"table\" FROM pragma_foreign_key_list(?1)")
                .expect("prepare foreign key query")
                .query_map([&table], |row| row.get(0))
                .expect("query foreign keys")
                .collect::<rusqlite::Result<_>>()
                .expect("read foreign keys");
            for parent in parents {
                assert!(
                    !excluded.contains(&parent),
                    "{}: retained routing table {table} references excluded table {parent}",
                    path.display()
                );
            }
        }
    }

    #[test]
    fn committed_catalog_passes_engram_catalog_validation_contract() {
        let catalog: Value = serde_json::from_slice(CATALOG_BYTES).expect("catalog JSON");
        assert_eq!(catalog["schema_version"], 1);
        assert_eq!(catalog["module_id"], "striatum");
        assert_eq!(catalog["module_id"], crate::DEFAULT_MODULE_ID);
        let entries = catalog["entries"].as_array().expect("entries array");
        let mut ids = std::collections::HashSet::new();
        for entry in entries {
            let id = entry["entry_id"].as_str().expect("entry id");
            assert!(
                id.starts_with("striatum/"),
                "entry must belong to striatum: {id}"
            );
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
            .find_map(|(entry_id, path)| (entry_id == "striatum/store").then_some(path))
            .expect("route-state entry");
        let opened_path = crate::config_overrides::default_store_path().expect("store path");
        assert_eq!(opened_path.file_name().unwrap().to_string_lossy(), path);
        assert!(opened_path.ends_with("cortexkit/striatum/store.db"));
        assert_eq!(path, "store.db");
    }

    #[test]
    fn fresh_start_opens_history_before_publishing_catalog() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-engram-catalog-startup");
        let state = scratch.path().join("store.db");
        let history = scratch.path().join("history.db");
        let catalog = scratch.path().join(CATALOG_FILE_NAME);
        assert!(!state.exists() && !history.exists() && !catalog.exists());

        let descriptor = striatum_store::dev_descriptor(
            "routing-engram-catalog-startup",
            state.to_str().unwrap(),
        );
        let _store = open_route_state_store(&descriptor).expect("fresh startup");
        // Publication itself refuses missing sources, so the catalog cannot
        // appear unless history was already created when startup wrote it.
        assert_eq!(std::fs::read(&catalog).unwrap(), CATALOG_BYTES);
        check_exclusions(&history, &["quota_reading_log".into()]);
    }

    #[test]
    fn second_startup_preserves_committed_bytes_without_rewriting_the_file() {
        let directory = cortexkit_test_support::scratch_root().join(format!(
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
        #[cfg(windows)]
        let catalog_handle = {
            use std::os::windows::fs::OpenOptionsExt;
            // Without FILE_SHARE_DELETE, replacing the live name would fail.
            std::fs::OpenOptions::new()
                .read(true)
                .share_mode(1 | 2) // FILE_SHARE_READ | FILE_SHARE_WRITE
                .open(&destination)
                .unwrap()
        };
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
        #[cfg(windows)]
        drop(catalog_handle);
        std::fs::remove_dir_all(directory).expect("remove catalog test directory");
    }

    #[cfg(windows)]
    #[test]
    fn catalog_and_directory_have_owner_only_acls() {
        let scratch = cortexkit_test_support::ScratchDir::new("routing-catalog-acl");
        let directory = scratch.join("catalog");
        assert!(write_catalog_at(&directory, b"original").unwrap());
        assert!(write_catalog_at(&directory, CATALOG_BYTES).unwrap());
        let file = std::fs::File::open(directory.join(CATALOG_FILE_NAME)).unwrap();
        subc_os::windows_acl::verify_private_dir(&directory).unwrap();
        subc_os::windows_acl::verify_owner_only(&file).unwrap();
        assert_eq!(
            std::fs::read(directory.join(CATALOG_FILE_NAME)).unwrap(),
            CATALOG_BYTES
        );
    }
}
