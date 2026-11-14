//! The bounded, account-local projection used to count runs and reservations.
//! Admission bodies remain authoritative in `AdmissionState`; entries here
//! retain only the identity and deadline needed by a cap count.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use striatum_store::AccountAdmissionRow;

use super::admissions::{AdmissionState, Effect, RowKey, ShardedMap};

const STARTED_WINDOW_MS: i64 = 30 * 60 * 1_000;
type AccountKey = (String, String);

/// A model contributes either to the shared account count or its own count.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "cap resolution will supply the count scope")
)]
pub(crate) enum InflightScope<'a> {
    Account,
    AccountModel(&'a str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InflightEntry {
    principal: String,
    conversation_hash: String,
    model: String,
    run_id: Option<String>,
    renewal: bool,
    until_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InflightLedger {
    accounts: ShardedMap<AccountKey, Arc<BTreeMap<RowKey, InflightEntry>>>,
}

impl InflightLedger {
    pub(crate) fn from_rows<'a>(
        rows: impl IntoIterator<Item = &'a AccountAdmissionRow>,
        now: i64,
    ) -> Self {
        // Build each account once; repeatedly touching a growing account while
        // loading would turn a large set of live rows into a quadratic scan.
        let mut accounts: BTreeMap<AccountKey, BTreeMap<RowKey, InflightEntry>> = BTreeMap::new();
        for row in rows {
            if let Some(entry) = Self::entry(row, now) {
                accounts
                    .entry((row.provider.clone(), row.account_id.clone()))
                    .or_default()
                    .insert((row.principal.clone(), row.admission_id.clone()), entry);
            }
        }
        let mut ledger = Self::default();
        for (account, entries) in accounts {
            ledger.accounts.insert(account, Arc::new(entries));
        }
        ledger
    }

    /// Lapse does not change a durable lifecycle state. Removing old entries
    /// on the next command bounds the index without manufacturing expirations.
    pub(crate) fn touch(&mut self, provider: &str, account: &str, now: i64) {
        let key = (provider.to_owned(), account.to_owned());
        let Some(entries) = self.accounts.get_mut(&key) else {
            return;
        };
        Arc::make_mut(entries).retain(|_, entry| now < entry.until_ms);
        if entries.is_empty() {
            self.accounts.remove(&key);
        }
    }

    pub(crate) fn remove(&mut self, provider: &str, account: &str, admission: &RowKey, now: i64) {
        self.touch(provider, account, now);
        let key = (provider.to_owned(), account.to_owned());
        if let Some(entries) = self.accounts.get_mut(&key) {
            Arc::make_mut(entries).remove(admission);
            if entries.is_empty() {
                self.accounts.remove(&key);
            }
        }
    }

    fn entry(row: &AccountAdmissionRow, now: i64) -> Option<InflightEntry> {
        let until_ms = match row.state.as_str() {
            "reserved" => Some(row.admission_valid_until),
            "started" => row
                .last_observed_at
                .into_iter()
                .chain(row.started_at)
                .max()
                .map(|at| at.saturating_add(STARTED_WINDOW_MS)),
            _ => None,
        };
        let until_ms = until_ms.filter(|until| now < *until)?;
        Some(InflightEntry {
            principal: row.principal.clone(),
            conversation_hash: row.conversation_hash.clone(),
            model: row.model.clone(),
            run_id: row.run_id.clone(),
            renewal: row.request_mode == "renew",
            until_ms,
        })
    }

    pub(crate) fn update(&mut self, row: &AccountAdmissionRow, now: i64) {
        let admission = (row.principal.clone(), row.admission_id.clone());
        self.remove(&row.provider, &row.account_id, &admission, now);
        let Some(entry) = Self::entry(row, now) else {
            return;
        };
        let key = (row.provider.clone(), row.account_id.clone());
        if !self.accounts.contains_key(&key) {
            self.accounts.insert(key.clone(), Arc::new(BTreeMap::new()));
        }
        Arc::make_mut(self.accounts.get_mut(&key).expect("inserted above"))
            .insert(admission, entry);
    }

    /// Lifecycle effects include closed rows after their bodies leave memory;
    /// observation effects refer to bodies still in the admission index. Using
    /// the same effects as persistence keeps the admission state and in-flight
    /// ledger consistent in the writer's uncommitted working copy.
    pub(crate) fn apply(&mut self, admissions: &AdmissionState, now: i64) {
        for effect in &admissions.pending {
            match effect {
                Effect::Admission { row, .. } => self.update(row, now),
                Effect::Observed { admission, .. } => {
                    if let Some(open) = admissions.open.get(admission) {
                        self.update(&open.row, now);
                    }
                }
                Effect::Unchanged { admission, .. } => {
                    if let Some(open) = admissions.open.get(admission) {
                        self.touch(&open.row.provider, &open.row.account_id, now);
                    }
                }
                _ => {}
            }
        }
    }

    /// Reads visit only this account's entries, never admission bodies or SQL.
    /// Filtering deadlines on read makes an idle account lapse without a write.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "cap enforcement will consume ledger counts")
    )]
    pub(crate) fn count(
        &self,
        provider: &str,
        account: &str,
        scope: InflightScope<'_>,
        now: i64,
    ) -> usize {
        let Some(entries) = self
            .accounts
            .get(&(provider.to_owned(), account.to_owned()))
        else {
            return 0;
        };
        let mut runs = BTreeSet::new();
        let mut conversations = BTreeSet::new();
        let mut renewals = BTreeSet::new();
        let mut ungrouped = 0;
        for entry in entries.values().filter(|entry| {
            now < entry.until_ms
                && match scope {
                    InflightScope::Account => true,
                    InflightScope::AccountModel(model) => entry.model == model,
                }
        }) {
            let conversation = (&entry.principal, &entry.conversation_hash);
            if entry.renewal {
                renewals.insert(conversation);
            } else {
                conversations.insert(conversation);
                if let Some(run) = &entry.run_id {
                    runs.insert((&entry.principal, run));
                } else {
                    ungrouped += 1;
                }
            }
        }
        ungrouped + runs.len() + renewals.difference(&conversations).count()
    }

    #[cfg(test)]
    pub(super) fn entry_count(&self, provider: &str, account: &str) -> usize {
        self.accounts
            .get(&(provider.to_owned(), account.to_owned()))
            .map_or(0, |entries| entries.len())
    }
}
