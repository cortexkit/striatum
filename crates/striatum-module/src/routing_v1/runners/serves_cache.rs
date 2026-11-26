//! Last good, complete lists are retained independently for each harness.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use cortexkit_role_llm_runner::serves::{
    decode_serves, ServesEntry, ServesError, ServesLogin, ServesModels,
};
use serde_json::Value;

use super::describe_cache::LAST_GOOD_MS;

#[derive(Clone, Debug, Default)]
pub(crate) struct RunnerServes {
    last_answer: Option<Arc<Value>>,
    lists: BTreeMap<Option<String>, List>,
}

#[derive(Clone, Debug)]
struct List {
    at_ms: i64,
    entries: Arc<[IndexedEntry]>,
    in_last_answer: bool,
}

#[derive(Clone, Debug)]
pub(super) struct IndexedEntry {
    pub(super) provider: String,
    pub(super) keyless: bool,
    all: bool,
    ids: HashSet<String>,
}

impl From<ServesEntry> for IndexedEntry {
    fn from(entry: ServesEntry) -> Self {
        let (all, ids) = match entry.models {
            ServesModels::All => (true, entry.except.unwrap_or_default()),
            ServesModels::List(ids) => (false, ids),
        };
        Self {
            provider: entry.provider,
            keyless: entry.login == Some(ServesLogin::None),
            all,
            ids: ids.into_iter().collect(),
        }
    }
}

impl IndexedEntry {
    /// The caller has already resolved this model against the catalog. For an
    /// all-models entry the indexed IDs are exclusions, not inclusions.
    pub(super) fn placement(&self, catalog_model: &str) -> Option<bool> {
        (self.all != self.ids.contains(catalog_model)).then_some(self.keyless)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ListStatus {
    Missing,
    Expired,
    Ready,
}

impl RunnerServes {
    pub(super) fn apply(
        &mut self,
        raw: &Value,
        declared: Option<&[String]>,
        now: i64,
    ) -> Result<(), ServesError> {
        if self.last_answer.as_deref() == Some(raw) {
            // The same answer renews only the lists it actually carried.
            for list in self.lists.values_mut().filter(|list| list.in_last_answer) {
                list.at_ms = now;
            }
        } else {
            // Validate the entire answer before publishing any part of it.
            let serves = decode_serves(raw, declared)?;
            for list in self.lists.values_mut() {
                list.in_last_answer = false;
            }
            let lists = serves
                .harnesses
                .into_iter()
                .flatten()
                .map(|(name, entries)| (Some(name), entries))
                .chain(serves.entries.map(|entries| (None, entries)));
            for (harness, entries) in lists {
                self.lists.insert(
                    harness,
                    List {
                        at_ms: now,
                        entries: entries.into_iter().map(IndexedEntry::from).collect(),
                        in_last_answer: true,
                    },
                );
            }
            self.last_answer = Some(Arc::new(raw.clone()));
        }
        Ok(())
    }

    pub(super) fn lists(&self, now: i64) -> impl Iterator<Item = (Option<&str>, &[IndexedEntry])> {
        self.lists.iter().filter_map(move |(harness, list)| {
            fresh(list, now).then_some((harness.as_deref(), list.entries.as_ref()))
        })
    }

    pub(super) fn status(&self, harness: Option<&str>, now: i64) -> ListStatus {
        match self
            .lists
            .iter()
            .find(|(name, _)| name.as_deref() == harness)
        {
            Some((_, list)) if fresh(list, now) => ListStatus::Ready,
            Some(_) => ListStatus::Expired,
            None => ListStatus::Missing,
        }
    }

    pub(super) fn includes(
        &self,
        harness: Option<&str>,
        provider: &str,
        model: &str,
        now: i64,
    ) -> bool {
        self.lists(now)
            .find(|(name, _)| *name == harness)
            .is_some_and(|(_, entries)| {
                entries
                    .iter()
                    .any(|entry| entry.provider == provider && entry.placement(model).is_some())
            })
    }
}

fn fresh(list: &List, now: i64) -> bool {
    now.saturating_sub(list.at_ms) < LAST_GOOD_MS
}

#[cfg(test)]
mod tests;
