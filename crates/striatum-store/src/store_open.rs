use super::*;

impl SqliteRouteStateStore {
    pub(super) fn open_with_migrations(
        descriptor: &StorageDescriptor,
        migrations: &[Migration],
    ) -> RouteStateResult<Self> {
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            return Err(err("route state requires a sqlite descriptor"));
        };
        let memory = path.is_empty()
            || path == ":memory:"
            || path == "file::memory:"
            || path.starts_with("file::memory:?");
        let split = migrations
            .iter()
            .any(|migration| migration.version == history::STATE_MIGRATION.version);
        if split && !memory {
            history::refuse_unsplit(path)?;
        }
        let store = open_sqlite(descriptor).map_err(err)?;
        migrate_route_state(&store, migrations)?;
        validate_family_spellings(&store)?;
        // Changing temp_store discards existing TEMP objects, so configure it
        // before installing the catalog write counter and its TEMP triggers.
        store
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)?;
                conn.pragma_update(None, "journal_size_limit", WAL_SIZE_CAP_BYTES)?;
                conn.pragma_update(None, "synchronous", "NORMAL")?;
                // Savepoint statement journals otherwise create and delete a
                // temp file per account command once they spill out of memory.
                conn.pragma_update(None, "temp_store", "MEMORY")
            })
            .map_err(err)?;
        // A failure here costs only the cache: every fingerprint read finds the
        // tracking missing, computes the value fresh and tries to install it again.
        if let Err(error) = store.with_conn(install_catalog_change_tracking) {
            tracing::warn!(%error, "catalog change tracking not installed at open; fingerprints are computed fresh until it is");
        }
        // Hold each checkout until the next is configured, filling the lazy pool.
        // Move this to cortexkit-store 0.4's open_sqlite_with_init hook when upgrading,
        // so replacements after failed rollbacks get the larger cache too. A rare
        // replacement keeps SQLite's default cache; configuring every checkout is
        // more expensive than the larger cache can save.
        fn prewarm(store: &SqliteStore, count: usize) -> Result<(), cortexkit_store::StoreError> {
            if count == 0 {
                return Ok(());
            }
            let mut nested = Ok(());
            store.with_read(|conn| {
                conn.pragma_update(None, "cache_size", READER_CACHE_SIZE_KIB)?;
                nested = prewarm(store, count - 1);
                Ok(())
            })?;
            nested
        }
        let archive = if split {
            Some(history::History::open(path, memory, &descriptor.module_id)?)
        } else {
            None
        };
        prewarm(
            &store,
            if memory {
                1
            } else {
                cortexkit_store::SqliteOpenOptions::default().read_pool_size
            },
        )
        .map_err(err)?;
        let mut result = Self::from_inner(connection_store::ConnectionStore::live(store));
        result.history = archive;
        if !memory {
            let maintenance = Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            .map_err(err)?;
            maintenance
                .busy_timeout(WAL_TRUNCATE_WAIT_BOUND)
                .map_err(err)?;
            maintenance
                .pragma_update(None, "synchronous", "NORMAL")
                .map_err(err)?;
            result.maintenance = Some(Mutex::new(maintenance));
        }
        Ok(result)
    }

    pub(super) fn from_inner(store: connection_store::ConnectionStore) -> Self {
        Self {
            maintenance: None,
            history: None,
            store: std::sync::Arc::new(store),
            account_writer: std::sync::OnceLock::new(),
            account_state: std::sync::Arc::new(std::sync::OnceLock::new()),
            account_state_retry: Mutex::new(None),
            account_state_retries: AtomicU64::new(0),
            call_counts: Default::default(),
            class_vectors: Mutex::new(HashMap::new()),
            usage_generation: AtomicU64::new(0),
            reported_score_conflicts: Mutex::new(HashSet::new()),
            catalog_fingerprint: Mutex::new(None),
            resolved_facts: Mutex::new(None),
            selection_catalog: Mutex::new(None),
            catalog_facts: Mutex::new(None),
            catalog_cache_identity: AtomicU64::new(next_catalog_cache_identity()),
            #[cfg(any(test, feature = "test-support"))]
            account_write_transactions: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }
}
