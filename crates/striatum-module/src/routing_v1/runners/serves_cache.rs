//! Last good, complete lists are retained independently for each harness.

use std::collections::BTreeMap;
use std::sync::Arc;

use cortexkit_role_llm_runner::serves::{decode_serves, ServesEntry, ServesError};
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
    entries: Arc<[ServesEntry]>,
    in_last_answer: bool,
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
                        entries: entries.into(),
                        in_last_answer: true,
                    },
                );
            }
            self.last_answer = Some(Arc::new(raw.clone()));
        }
        Ok(())
    }

    pub(super) fn lists(&self, now: i64) -> impl Iterator<Item = (Option<&str>, &[ServesEntry])> {
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
}

fn fresh(list: &List, now: i64) -> bool {
    now.saturating_sub(list.at_ms) < LAST_GOOD_MS
}

#[cfg(test)]
mod tests;
