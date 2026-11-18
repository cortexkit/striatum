//! Restore validation and the first-open binding of the state and archive files.
use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) id: String,
    pub(crate) generation: i64,
}

fn has_table(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |r| r.get(0),
    )
}

pub(crate) fn version(conn: &Connection, namespace: &str) -> rusqlite::Result<u32> {
    if !has_table(conn, "cortexkit_schema_version")? {
        return Ok(0);
    }
    conn.query_row(
        "SELECT coalesce(max(version),0) FROM main.cortexkit_schema_version WHERE namespace=?1",
        [namespace],
        |r| r.get(0),
    )
}

fn binding(conn: &Connection, table: &str) -> rusqlite::Result<Option<Binding>> {
    if !has_table(conn, table)? {
        return Ok(None);
    }
    conn.query_row(
        &format!("SELECT split_identity, move_generation FROM main.{table} WHERE singleton=1"),
        [],
        |r| {
            Ok(Binding {
                id: r.get(0)?,
                generation: r.get(1)?,
            })
        },
    )
    .optional()
    .and_then(|row| {
        if row
            .as_ref()
            .is_some_and(|value| value.id.len() != 32 || value.generation < 0)
        {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            Ok(row)
        }
    })
}

pub(crate) fn state_binding(
    conn: &Connection,
    path: &str,
    recorded: u32,
) -> RouteStateResult<Option<Binding>> {
    let incomplete = || {
        err(format!("{path}: route_state version {recorded} has incomplete history_layout; restore a matching, supported split pair"))
    };
    let value = binding(conn, "history_layout").map_err(|_| incomplete())?;
    if value.is_none() && recorded >= STATE_MIGRATION.version {
        return Err(incomplete());
    }
    Ok(value)
}

pub(crate) fn moved_table_with_rows(conn: &Connection) -> rusqlite::Result<Option<&'static str>> {
    for table in TABLES.lines() {
        if has_table(conn, table)?
            && conn.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM main.{table})"),
                [],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Ok(Some(table));
        }
    }
    Ok(None)
}

pub(crate) fn refuse_moved_rows(conn: &Connection) -> RouteStateResult<()> {
    if let Some(table) = moved_table_with_rows(conn).map_err(err)? {
        return Err(err(format!("unsplit state database has rows in {table}; stop routing and run script/split-state-history.sh on an offline copy before opening it")));
    }
    Ok(())
}

fn id(value: Option<&Binding>) -> &str {
    value.map_or("none", |binding| binding.id.as_str())
}

fn validate_archive(
    conn: &Connection,
    path: &Path,
    state: Option<&Binding>,
) -> RouteStateResult<()> {
    let recorded = version(conn, NAMESPACE).map_err(err)?;
    let incomplete = || {
        err(format!("{}: route_history version {recorded} has incomplete history_binding; restore a matching, supported split pair", path.display()))
    };
    if recorded >= 1 && !has_table(conn, "history_binding").map_err(err)? {
        return Err(incomplete());
    }
    let archive = binding(conn, "history_binding").map_err(|_| incomplete())?;
    if let (Some(state), Some(archive)) = (state, archive.as_ref()) {
        if state.id == archive.id {
            if archive.generation < state.generation {
                return Err(err(format!("{}: stale history archive: state generation {}, history generation {} (split identity {})", path.display(), state.generation, archive.generation, state.id)));
            }
            return Ok(());
        }
    }
    // A failed first open can leave a bound but empty archive. Reusing it is
    // safe only when state has never been bound and no history was committed.
    if state.is_none()
        && archive.as_ref().is_none_or(|value| value.generation == 0)
        && archive_has_no_rows(conn).map_err(err)?
    {
        return Ok(());
    }
    Err(err(format!("{}: incompatible history archive: state identity {}, history identity {}; restore a matching pair", path.display(), id(state), id(archive.as_ref()))))
}

fn archive_has_no_rows(conn: &Connection) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare("SELECT name FROM main.sqlite_schema WHERE type='table'
        AND name NOT GLOB 'sqlite_*' AND name NOT IN ('history_binding','cortexkit_schema_version')")?;
    for name in statement.query_map([], |r| r.get::<_, String>(0))? {
        let table = name?.replace('"', "\"\"");
        if conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM main.\"{table}\")"),
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn inspect_files(path: &str) -> RouteStateResult<()> {
    let state = Path::new(path);
    let archive = path_for(state);
    let binding = if state.exists() {
        let connection = open_before_write(state)?;
        let recorded = version(&connection, ROUTE_STATE_NAMESPACE).map_err(err)?;
        let binding = state_binding(&connection, path, recorded)?;
        refuse_moved_rows(&connection)?;
        binding
    } else {
        None
    };
    if archive.is_file() {
        validate_archive(&open_before_write(&archive)?, &archive, binding.as_ref())?;
    } else if binding.is_some() {
        return Err(err(format!("split routing store is missing {}; restore history.db (the matching archive) before starting routing: state identity {}, history identity none", archive.display(), id(binding.as_ref()))));
    }
    Ok(())
}

fn open_before_write(path: &Path) -> RouteStateResult<Connection> {
    let sidecar = |suffix| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    };
    let rollback = sidecar("-journal");
    if rollback.exists() {
        return Err(err(format!(
            "refusing rollback journal {}; recover the store before opening it",
            rollback.display()
        )));
    }
    if sidecar("-wal").exists() {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)
    } else {
        open_offline(path).map_err(err)
    }
}

impl History {
    pub(crate) fn bind(&self, path: &Path, state: Option<&Binding>) -> RouteStateResult<String> {
        self.writer
            .with_conn(|conn| {
                validate_archive(conn, path, state)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                if let Some(state) = state {
                    return Ok(state.id.clone());
                }
                conn.query_row(
                    "INSERT INTO history_binding VALUES (1, lower(hex(randomblob(16))), 0)
                 ON CONFLICT(singleton) DO UPDATE SET split_identity=excluded.split_identity
                 RETURNING split_identity",
                    [],
                    |r| r.get(0),
                )
            })
            .map_err(err)
    }
}

pub(crate) fn migrate_split(
    store: &SqliteStore,
    migrations: &[Migration],
    identity: String,
) -> RouteStateResult<()> {
    store
        .with_conn(|conn| {
            conn.create_scalar_function(
                "history_split_identity",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
                move |_| Ok(identity.clone()),
            )
        })
        .map_err(err)?;
    let outcome = migrate_route_state(store, migrations);
    let cleanup = store.with_conn(|conn| conn.remove_function("history_split_identity", 0));
    outcome?;
    cleanup.map_err(err)
}
