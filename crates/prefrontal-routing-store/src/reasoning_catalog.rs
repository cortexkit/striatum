//! The reasoning-effort values the catalog publishes per model, read for
//! operator variant overrides. Fusiform's `capability.reasoning_options` is the
//! authority: Broca admits a variant only when it is one of those values.

use rusqlite::{Connection, OpenFlags};
use std::collections::BTreeMap;
use std::path::Path;

/// What the catalog last said about one model's reasoning efforts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedEfforts {
    /// The row has not been read from the catalog since the store began
    /// recording reasoning options, so nothing is known yet.
    Unread,
    /// The effort values the catalog publishes; empty when it publishes none.
    Published(Vec<String>),
}

/// Published reasoning efforts keyed by catalog `provider/model`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasoningCatalog {
    pub models: BTreeMap<String, PublishedEfforts>,
}

impl ReasoningCatalog {
    /// The model's entry, or `None` when the catalog has no row for it.
    pub fn get(&self, provider_id: &str, model_id: &str) -> Option<&PublishedEfforts> {
        self.models.get(&format!("{provider_id}/{model_id}"))
    }

    /// Every catalog model under `provider_id`, in key order.
    pub fn models_of<'a>(
        &'a self,
        provider_id: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a PublishedEfforts)> + 'a {
        self.models.iter().filter_map(move |(key, efforts)| {
            key.split_once('/')
                .filter(|(provider, _)| *provider == provider_id)
                .map(|(_, model)| (model, efforts))
        })
    }
}

pub(crate) fn read_reasoning_catalog(
    connection: &Connection,
) -> rusqlite::Result<ReasoningCatalog> {
    let mut statement = connection.prepare(
        "SELECT provider_id, model_id, reasoning_efforts_json FROM model_facts
         ORDER BY provider_id, model_id, route_key",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut catalog = ReasoningCatalog::default();
    for row in rows {
        let (provider_id, model_id, efforts_json) = row?;
        let efforts = match efforts_json {
            None => PublishedEfforts::Unread,
            Some(json) => PublishedEfforts::Published(
                serde_json::from_str::<Vec<String>>(&json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
            ),
        };
        // Every variant row of one model carries the same catalog value; a read
        // row wins over a row the catalog has not written yet.
        let entry = catalog
            .models
            .entry(format!("{provider_id}/{model_id}"))
            .or_insert(PublishedEfforts::Unread);
        if matches!(entry, PublishedEfforts::Unread) {
            *entry = efforts;
        }
    }
    Ok(catalog)
}

/// Read the published reasoning efforts from a routing store file without
/// taking its writer lease or migrating it, so a running module is undisturbed.
pub fn read_reasoning_catalog_offline(path: &Path) -> Result<ReasoningCatalog, String> {
    if !path.exists() {
        return Err(format!("{} does not exist", path.display()));
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("opening {} read-only: {error}", path.display()))?;
    read_reasoning_catalog(&connection)
        .map_err(|error| format!("reading reasoning options from {}: {error}", path.display()))
}
