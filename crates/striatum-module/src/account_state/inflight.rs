//! The bounded, account-local projection used to count runs and reservations.
//! Admission bodies remain authoritative in `AdmissionState`; entries here
//! retain only the identity and deadline needed by a cap count.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use striatum_store::{AccountAdmissionRow, ContinuationSourceRow, PrunedContinuation};

use super::admissions::{AdmissionState, Effect, ShardedMap};

const STARTED_WINDOW_MS: i64 = 30 * 60 * 1_000;
type AccountKey = (String, String);
// Unlike historical admission indexes, inflight identities are bounded by the
// active runs and reservations, so their text stays owned rather than interned.
type RowKey = (String, String);

#[cfg(test)]
#[path = "inflight_sharing_tests.rs"]
mod sharing_tests;

/// A model contributes either to the shared account count or its own count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    started: bool,
    started_at: Option<i64>,
    until_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InflightLedger {
    // A command copies an account bucket, not its immutable keys or payloads.
    accounts: ShardedMap<AccountKey, Arc<BTreeMap<Arc<RowKey>, Arc<InflightEntry>>>>,
    /// Only offers retain source identities, not historical admission rows.
    /// Used evidence must survive a source closing while another row of the
    /// run is still started, so it is independent of the open-admission index.
    sources: ShardedMap<RowKey, Arc<ContinuationSourceRow>>,
    successions: ShardedMap<RowKey, Arc<BTreeMap<String, ContinuationSourceRow>>>,
}

impl InflightLedger {
    pub(crate) fn from_admissions(
        admissions: &AdmissionState,
        sources: Vec<ContinuationSourceRow>,
        now: i64,
    ) -> Self {
        let mut ledger = Self::default();
        for source in sources {
            if source.used_at_ms.is_some() {
                ledger.record_succession(source);
            } else {
                ledger.cache_source(source);
            }
        }
        // Build each account once; repeatedly touching a growing account while
        // loading would turn a large set of live rows into a quadratic scan.
        let mut accounts: BTreeMap<AccountKey, BTreeMap<Arc<RowKey>, Arc<InflightEntry>>> =
            BTreeMap::new();
        for (_, open) in admissions.open.iter() {
            let row = &open.row;
            if ledger.succeeded(row) {
                continue;
            }
            if let Some(entry) = Self::entry(row, now) {
                accounts
                    .entry((row.provider.to_string(), row.account_id.to_string()))
                    .or_default()
                    .insert(
                        Arc::new((row.principal.as_str().into(), row.admission_id.clone())),
                        entry,
                    );
            }
        }
        for (account, entries) in accounts {
            ledger.accounts.insert(account, Arc::new(entries));
        }
        ledger
    }

    pub(crate) fn cache_source(&mut self, source: ContinuationSourceRow) {
        self.sources.insert(
            (
                source.principal.as_str().into(),
                source.selection_id.clone(),
            ),
            Arc::new(source),
        );
    }

    fn record_succession(&mut self, source: ContinuationSourceRow) {
        let run: RowKey = (source.principal.as_str().into(), source.run_id.clone());
        if !self.successions.contains_key(&run) {
            self.successions
                .insert(run.clone(), Arc::new(BTreeMap::new()));
        }
        Arc::make_mut(self.successions.get_mut(&run).expect("inserted above"))
            .insert(source.selection_id.clone(), source);
    }

    fn succeeded<T: AsRef<str>, H: std::fmt::Display>(
        &self,
        row: &AccountAdmissionRow<T, H>,
    ) -> bool {
        row.state.as_ref() == "started"
            && row.run_id.as_ref().is_some_and(|run| {
                self.successions
                    .get(&(row.principal.as_ref().into(), run.clone()))
                    .is_some_and(|offers| {
                        offers.values().any(|offer| {
                            offer.provider == row.provider.as_ref()
                                && offer.account_id == row.account_id.as_ref()
                                && (offer.left_admission_id == row.admission_id
                                    || (row.request_mode.as_ref() != "renew"
                                        && row
                                            .started_at
                                            .is_some_and(|at| Some(at) < offer.used_at_ms)))
                        })
                    })
            })
    }

    pub(crate) fn prune_continuations(&mut self, deleted: &[PrunedContinuation]) {
        for row in deleted {
            self.sources
                .remove(&(row.principal.as_str().into(), row.selection_id.clone()));
            let run: RowKey = (row.principal.as_str().into(), row.run_id.clone());
            if let Some(offers) = self.successions.get_mut(&run) {
                Arc::make_mut(offers).remove(&row.selection_id);
                if offers.is_empty() {
                    self.successions.remove(&run);
                }
            }
        }
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

    /// Sweep only cached runs, not admission history. Collect expired keys in
    /// one walk so a Tick copies and changes only affected account shards.
    pub(crate) fn expire_lapsed(&mut self, now: i64) -> usize {
        let expired: Vec<_> = self
            .accounts
            .iter()
            .filter_map(|(account, entries)| {
                let keys: Vec<_> = entries
                    .iter()
                    .filter(|(_, entry)| now >= entry.until_ms)
                    .map(|(key, _)| key.clone())
                    .collect();
                (!keys.is_empty()).then(|| (account.clone(), keys))
            })
            .collect();
        let mut removed = 0;
        for (account, keys) in expired {
            removed += keys.len();
            let entries = Arc::make_mut(
                self.accounts
                    .get_mut(&account)
                    .expect("collected account still exists"),
            );
            for key in keys {
                entries.remove(&key);
            }
            if entries.is_empty() {
                self.accounts.remove(&account);
            }
        }
        removed
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

    fn entry<T: AsRef<str>, H: std::fmt::Display>(
        row: &AccountAdmissionRow<T, H>,
        now: i64,
    ) -> Option<Arc<InflightEntry>> {
        let until_ms = match row.state.as_ref() {
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
        Some(Arc::new(InflightEntry {
            principal: row.principal.as_ref().into(),
            conversation_hash: row.conversation_hash.to_string(),
            model: row.model.as_ref().to_owned(),
            run_id: row.run_id.clone(),
            renewal: row.request_mode.as_ref() == "renew",
            started: row.state.as_ref() == "started",
            started_at: row.started_at,
            until_ms,
        }))
    }

    pub(crate) fn update<T: AsRef<str>, H: std::fmt::Display>(
        &mut self,
        row: &AccountAdmissionRow<T, H>,
        now: i64,
    ) {
        let admission: RowKey = (row.principal.as_ref().into(), row.admission_id.clone());
        self.remove(
            row.provider.as_ref(),
            row.account_id.as_ref(),
            &admission,
            now,
        );
        if self.succeeded(row) {
            return;
        }
        let Some(entry) = Self::entry(row, now) else {
            return;
        };
        let key = (
            row.provider.as_ref().to_owned(),
            row.account_id.as_ref().to_owned(),
        );
        if !self.accounts.contains_key(&key) {
            self.accounts.insert(key.clone(), Arc::new(BTreeMap::new()));
        }
        Arc::make_mut(self.accounts.get_mut(&key).expect("inserted above"))
            .insert(Arc::new(admission), entry);
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
                Effect::Offer {
                    selection, used_at, ..
                } => {
                    let key = (selection.0.to_string(), selection.1.clone());
                    let Some(source) = self.sources.remove(&key) else {
                        continue;
                    };
                    let Some(used_at) = used_at else {
                        continue;
                    };
                    let mut offer = (*source).clone();
                    offer.used_at_ms = Some(*used_at);
                    self.record_succession(offer.clone());
                    self.touch(&offer.provider, &offer.account_id, now);
                    let account = (offer.provider.clone(), offer.account_id.clone());
                    if let Some(entries) = self.accounts.get_mut(&account) {
                        // The explicitly left row is succeeded even at the same
                        // millisecond. Other rows started then are new holds.
                        Arc::make_mut(entries).retain(|key, entry| {
                            let (_, id) = key.as_ref();
                            !(entry.principal == offer.principal
                                && entry.run_id.as_deref() == Some(&offer.run_id)
                                && entry.started
                                && (id == &offer.left_admission_id
                                    || (!entry.renewal
                                        && entry.started_at.is_some_and(|at| at < *used_at))))
                        });
                        if entries.is_empty() {
                            self.accounts.remove(&account);
                        }
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
        self.count_matching(provider, account, now, |entry| match scope {
            InflightScope::Account => true,
            InflightScope::AccountModel(model) => entry.model == model,
        })
    }

    pub(crate) fn count_for_model(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        now: i64,
    ) -> usize {
        let model = crate::account_selection::normalize(model);
        let count = caps.cap_count(provider, account, &model);
        self.count_for_scope(caps, provider, account, count, now)
    }

    pub(crate) fn conversation(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        principal: &str,
        conversation: &str,
        now: i64,
    ) -> bool {
        let model = crate::account_selection::normalize(model);
        let scope = caps.cap_count(provider, account, &model);
        self.accounts
            .get(&(provider.to_owned(), account.to_owned()))
            .is_some_and(|entries| {
                entries.values().any(|entry| {
                    now < entry.until_ms
                        && entry.principal == principal
                        && entry.conversation_hash == conversation
                        && caps.cap_count(provider, account, &entry.model) == scope
                })
            })
    }

    pub(crate) fn count_for_scope(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        scope: InflightScope<'_>,
        now: i64,
    ) -> usize {
        self.count_matching(provider, account, now, |entry| {
            caps.cap_count(provider, account, &entry.model) == scope
        })
    }

    /// The earliest instant after `now` at which this model's cap count is
    /// below `cap` with no further command, or `None` when no entry in it closes
    /// soon enough to free a slot. Counts are evaluated at each entry deadline
    /// rather than subtracting one per closing row: a renewal beside its
    /// conversation's run, or a second row of one run, closes without
    /// removing a hold, and an over-cap count must fall more than once.
    pub(crate) fn next_free_at(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        model: &str,
        cap: u64,
        now: i64,
    ) -> Option<i64> {
        let model = crate::account_selection::normalize(model);
        let scope = caps.cap_count(provider, account, &model);
        let entries = self
            .accounts
            .get(&(provider.to_owned(), account.to_owned()))?;
        let mut deadlines: Vec<i64> = entries
            .values()
            .filter(|entry| {
                now < entry.until_ms && caps.cap_count(provider, account, &entry.model) == scope
            })
            .map(|entry| entry.until_ms)
            .collect();
        deadlines.sort_unstable();
        deadlines.dedup();
        deadlines
            .into_iter()
            .find(|at| (self.count_for_scope(caps, provider, account, scope, *at) as u64) < cap)
    }

    /// The instants after `now` at which an entry of this cap count closes,
    /// the only instants at which its count can fall with no further command.
    pub(crate) fn deadlines(
        &self,
        caps: &crate::concurrency_caps::ResolvedCaps,
        provider: &str,
        account: &str,
        scope: InflightScope<'_>,
        now: i64,
    ) -> Vec<i64> {
        self.accounts
            .get(&(provider.to_owned(), account.to_owned()))
            .map_or_else(Vec::new, |entries| {
                entries
                    .values()
                    .filter(|entry| {
                        now < entry.until_ms
                            && caps.cap_count(provider, account, &entry.model) == scope
                    })
                    .map(|entry| entry.until_ms)
                    .collect()
            })
    }

    /// Include live models absent from today's catalog, so a catalog withdrawal
    /// cannot hide a run that still consumes capacity.
    pub(crate) fn active_models(&self, now: i64) -> BTreeMap<AccountKey, BTreeSet<String>> {
        self.accounts
            .iter()
            .map(|(key, entries)| {
                let models = entries
                    .values()
                    .filter(|entry| now < entry.until_ms)
                    .map(|entry| entry.model.clone())
                    .collect();
                (key.clone(), models)
            })
            .collect()
    }

    fn count_matching(
        &self,
        provider: &str,
        account: &str,
        now: i64,
        includes: impl Fn(&InflightEntry) -> bool,
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
        for entry in entries
            .values()
            .filter(|entry| now < entry.until_ms && includes(entry))
        {
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

    #[cfg(test)]
    pub(super) fn report_expiry_cost(&self, now: i64, iterations: usize) {
        assert!(iterations > 0);
        let accounts = self.accounts.iter().count();
        let entries = self
            .accounts
            .iter()
            .map(|(_, entries)| entries.len())
            .sum::<usize>();
        assert!(
            entries > 0,
            "the request mix produced no live entries to measure"
        );
        for (case, at, expected) in [("live", now, 0), ("lapsed", i64::MAX, entries)] {
            let mut elapsed = std::time::Duration::ZERO;
            for _ in 0..iterations {
                let mut copy = self.clone();
                let started = std::time::Instant::now();
                let removed = std::hint::black_box(copy.expire_lapsed(at));
                elapsed += started.elapsed();
                assert_eq!(
                    removed, expected,
                    "{case} measurement did not exercise its intended population"
                );
                std::hint::black_box(copy);
            }
            println!("inflight_tick_expiry case={case} cached_entries={entries} accounts={accounts} iterations={iterations} removed_per_iteration={expected} average_us={:.3}",elapsed.as_secs_f64()*1_000_000.0/iterations as f64);
        }
    }
}
