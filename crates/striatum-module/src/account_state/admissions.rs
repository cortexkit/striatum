//! In-memory admission, affinity, demand and continuation state.
//!
//! Every writer of `account_admission`, `account_affinity` and
//! `account_continue` applies the same change here, in the same writer command
//! as its SQL. Request paths do not read this state yet; until they do, the
//! load-equivalence tests prove that maintaining it in step with SQL leaves it
//! equal to a fresh load.
//!
//! Only bounded data is kept: open admissions, one demand entry per charged
//! unreleased admission, every affinity row, and small derived indexes for
//! the historical SQL predicates (newest admission per conversation, started
//! rows on the current pin, and the continuation chains of open originals).
//! Finished and released admission bodies stay in SQL.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AdmissionGroupRows, ContinuationStateRow,
    LatestAdmissionRow, PrunedAdmission, PrunedContinuation, ReservationRef,
};

const SHARDS: usize = 64;

/// A map whose clone copies one pointer and whose first write after a clone
/// copies one shard of 64, so a savepoint fork never copies all rows.
#[derive(Clone)]
pub(crate) struct ShardedMap<K, V> {
    shards: Arc<Vec<Arc<BTreeMap<K, V>>>>,
    len: usize,
}

impl<K, V> Default for ShardedMap<K, V> {
    fn default() -> Self {
        Self {
            shards: Arc::new((0..SHARDS).map(|_| Arc::new(BTreeMap::new())).collect()),
            len: 0,
        }
    }
}

fn shard_of<K: Hash>(key: &K) -> usize {
    // A fixed-key hasher, so two maps built independently place a key in the
    // same shard.
    #[allow(deprecated)]
    let mut hasher = std::hash::SipHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % SHARDS as u64) as usize
}

impl<K: Ord + Hash + Clone, V: Clone> ShardedMap<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        self.shards[shard_of(key)].get(key)
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    fn shard_mut(&mut self, key: &K) -> &mut BTreeMap<K, V> {
        let shard = shard_of(key);
        Arc::make_mut(&mut Arc::make_mut(&mut self.shards)[shard])
    }

    /// Copies a shard only when the key is present.
    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.contains_key(key) {
            return None;
        }
        self.shard_mut(key).get_mut(key)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self.shard_mut(&key).insert(key, value);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        if !self.contains_key(key) {
            return None;
        }
        let removed = self.shard_mut(key).remove(key);
        self.len -= 1;
        removed
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.shards.iter().flat_map(|shard| shard.iter())
    }

    /// Whether `other` shares every shard with this map, so a fork copied no rows.
    #[cfg(test)]
    pub(crate) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shards, &other.shards)
    }
}

impl<K: Ord + Hash + Clone, V: Clone + PartialEq> PartialEq for ShardedMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<K: Ord + Hash + Clone, V: Clone + Eq> Eq for ShardedMap<K, V> {}

impl<K: Ord + Hash + Clone + std::fmt::Debug, V: Clone + std::fmt::Debug> std::fmt::Debug
    for ShardedMap<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.iter().collect::<BTreeMap<_, _>>())
            .finish()
    }
}

/// (principal, admission id) or, for continuations, (principal, selection id).
pub(crate) type RowKey = (String, String);
/// (principal, conversation hash, provider).
pub(crate) type ConversationKey = (String, String, String);

fn key(a: &str, b: &str) -> RowKey {
    (a.to_owned(), b.to_owned())
}

fn conversation(principal: &str, hash: &str, provider: &str) -> ConversationKey {
    (principal.to_owned(), hash.to_owned(), provider.to_owned())
}

/// An admission in `reserved`, `started` or `expired`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OpenAdmission {
    pub(crate) row: AccountAdmissionRow,
    pub(crate) input_size_class: Option<u32>,
    pub(crate) model_decision_id: Option<String>,
    pub(crate) rowid: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Demand {
    pub(crate) state: String,
    pub(crate) request_bytes: u64,
}

/// Demand of one (provider, account), in `ACCOUNT_DEMAND_SQL` order.
pub(crate) type AccountDemand = BTreeMap<(i64, String, String), Demand>;

/// An offer that is neither used nor released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Offer {
    pub(crate) rowid: i64,
    pub(crate) run_id: String,
    pub(crate) admission_id: String,
    pub(crate) previous_decision_id: String,
    pub(crate) decision_id: String,
    pub(crate) reservation_id: String,
    pub(crate) valid_until_ms: i64,
    pub(crate) reservation: Option<ReservationRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChainReservation {
    pub(crate) reservation_id: String,
    pub(crate) previous_decision_id: String,
    pub(crate) decision_id: String,
    pub(crate) used: bool,
    pub(crate) reservation: Option<ReservationRef>,
}

/// Every offer row of one original admission that is still open, grouped by
/// run and keyed by rowid, plus the newest used row across its runs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Chain {
    pub(crate) runs: BTreeMap<String, BTreeMap<i64, ChainReservation>>,
    pub(crate) latest_used: Option<(i64, String)>,
}

impl Chain {
    fn used_successors(&self, run: &str, previous: &str) -> bool {
        self.runs.get(run).is_some_and(|rows| {
            rows.values()
                .any(|row| row.used && row.previous_decision_id == previous)
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AdmissionState {
    pub(crate) open: ShardedMap<RowKey, Arc<OpenAdmission>>,
    /// Keyed by (provider, account id).
    pub(crate) demand: ShardedMap<(String, String), Arc<AccountDemand>>,
    pub(crate) latest: ShardedMap<ConversationKey, Arc<LatestAdmissionRow>>,
    pub(crate) affinity: ShardedMap<ConversationKey, Arc<AccountAffinityRow>>,
    pub(crate) affinity_by_pin: ShardedMap<String, Arc<BTreeSet<ConversationKey>>>,
    /// Started rows on each conversation's current pin and generation; absent
    /// means none.
    pub(crate) pin_started: ShardedMap<ConversationKey, u64>,
    /// Open admissions in `started`, per pin id; absent means none.
    pub(crate) started_by_pin: ShardedMap<String, u64>,
    /// Keyed by (principal, selection id).
    pub(crate) offers: ShardedMap<RowKey, Arc<Offer>>,
    pub(crate) offer_by_decision: ShardedMap<String, RowKey>,
    /// Keyed by the original (principal, admission id).
    pub(crate) chains: ShardedMap<RowKey, Arc<Chain>>,
    /// Originals that closed or were deleted in the current command. Their
    /// chains are dropped when the command settles, because finishing a run
    /// closes the original before its continuations are finished.
    pub(crate) closing: Vec<RowKey>,
}

/// The index state an admission row contributes, before and after a change.
struct Contribution<'a> {
    principal: &'a str,
    hash: &'a str,
    provider: &'a str,
    pin_id: &'a str,
    pin_generation: u64,
    started: bool,
}

impl AdmissionState {
    pub(crate) fn from_rows(rows: AdmissionGroupRows) -> Self {
        let mut state = Self::default();
        for row in rows.open {
            let open = OpenAdmission {
                input_size_class: row.input_size_class,
                model_decision_id: row.model_decision_id,
                rowid: row.rowid,
                row: row.row,
            };
            if open.row.state == "started" {
                state.add_started_pin(&open.row.pin_id, 1);
            }
            state.open.insert(
                key(&open.row.principal, &open.row.admission_id),
                Arc::new(open),
            );
        }
        for row in rows.latest {
            state.latest.insert(
                conversation(&row.principal, &row.conversation_hash, &row.provider),
                Arc::new(row),
            );
        }
        for row in rows.demand {
            let account = (row.provider, row.account_id);
            let mut entries = state
                .demand
                .remove(&account)
                .map(|entries| (*entries).clone())
                .unwrap_or_default();
            entries.insert(
                (row.committed_at, row.principal, row.admission_id),
                Demand {
                    state: row.state,
                    request_bytes: row.request_bytes,
                },
            );
            state.demand.insert(account, Arc::new(entries));
        }
        for row in rows.affinities {
            state.index_pin(&row.pin_id, conversation_of(&row));
            state.affinity.insert(conversation_of(&row), Arc::new(row));
        }
        for row in rows.pin_started {
            if row.started > 0 {
                state.pin_started.insert(
                    conversation(&row.principal, &row.conversation_hash, &row.provider),
                    row.started,
                );
            }
        }
        for row in rows.offers {
            state.insert_offer(&row);
        }
        for row in rows.chains {
            let original = key(&row.principal, &row.admission_id);
            let mut chain = state
                .chains
                .remove(&original)
                .map(|chain| (*chain).clone())
                .unwrap_or_default();
            if row.used_at_ms.is_some() {
                chain.latest_used = Some((row.rowid, row.reservation_id.clone()));
            }
            chain.runs.entry(row.run_id.clone()).or_default().insert(
                row.rowid,
                ChainReservation {
                    reservation_id: row.reservation_id,
                    previous_decision_id: row.previous_decision_id,
                    decision_id: row.decision_id,
                    used: row.used_at_ms.is_some(),
                    reservation: row.reservation,
                },
            );
            state.chains.insert(original, Arc::new(chain));
        }
        state
    }

    fn insert_offer(&mut self, row: &ContinuationStateRow) {
        let selection = key(&row.principal, &row.selection_id);
        self.offer_by_decision
            .insert(row.decision_id.clone(), selection.clone());
        self.offers.insert(
            selection,
            Arc::new(Offer {
                rowid: row.rowid,
                run_id: row.run_id.clone(),
                admission_id: row.admission_id.clone(),
                previous_decision_id: row.previous_decision_id.clone(),
                decision_id: row.decision_id.clone(),
                reservation_id: row.reservation_id.clone(),
                valid_until_ms: row.valid_until_ms,
                reservation: row.reservation.clone(),
            }),
        );
    }

    fn index_pin(&mut self, pin_id: &str, conversation: ConversationKey) {
        let mut keys = self
            .affinity_by_pin
            .remove(&pin_id.to_owned())
            .map(|keys| (*keys).clone())
            .unwrap_or_default();
        keys.insert(conversation);
        self.affinity_by_pin.insert(pin_id.to_owned(), Arc::new(keys));
    }

    fn unindex_pin(&mut self, pin_id: &str, conversation: &ConversationKey) {
        let pin = pin_id.to_owned();
        let Some(keys) = self.affinity_by_pin.remove(&pin) else {
            return;
        };
        let mut keys = (*keys).clone();
        keys.remove(conversation);
        if !keys.is_empty() {
            self.affinity_by_pin.insert(pin, Arc::new(keys));
        }
    }

    fn add_count<K: Ord + Hash + Clone>(map: &mut ShardedMap<K, u64>, key: K, delta: i64) {
        let current = map.get(&key).copied().unwrap_or(0) as i64;
        let next = current + delta;
        debug_assert!(next >= 0, "derived count below zero");
        if next <= 0 {
            map.remove(&key);
        } else {
            map.insert(key, next as u64);
        }
    }

    fn add_started_pin(&mut self, pin_id: &str, delta: i64) {
        Self::add_count(&mut self.started_by_pin, pin_id.to_owned(), delta);
    }

    /// A started row counts toward `pin_has_started` only while it names the
    /// conversation's current pin and generation.
    fn add_pin_started(&mut self, row: &Contribution<'_>, delta: i64) {
        let conversation = conversation(row.principal, row.hash, row.provider);
        let current = self.affinity.get(&conversation).is_some_and(|affinity| {
            affinity.pin_id == row.pin_id && affinity.pin_generation == row.pin_generation
        });
        if current && row.started {
            Self::add_count(&mut self.pin_started, conversation, delta);
        }
    }

    fn set_demand(
        &mut self,
        provider: &str,
        account_id: &str,
        entry: (i64, String, String),
        demand: Option<Demand>,
    ) {
        let account = key(provider, account_id);
        let mut entries = self
            .demand
            .get(&account)
            .map(|entries| (**entries).clone())
            .unwrap_or_default();
        match demand {
            Some(demand) => {
                entries.insert(entry, demand);
            }
            None => {
                entries.remove(&entry);
            }
        }
        if entries.is_empty() {
            self.demand.remove(&account);
        } else {
            self.demand.insert(account, Arc::new(entries));
        }
    }

    /// Mirror a state change in demand and in the conversation's newest row.
    fn restate(&mut self, row: &AccountAdmissionRow) {
        if row.charged {
            let entry = (
                row.committed_at,
                row.principal.clone(),
                row.admission_id.clone(),
            );
            let demand = (row.state != "released").then(|| Demand {
                state: row.state.clone(),
                request_bytes: row.request_bytes,
            });
            self.set_demand(&row.provider, &row.account_id, entry, demand);
        }
        let conversation = conversation(&row.principal, &row.conversation_hash, &row.provider);
        if let Some(latest) = self.latest.get_mut(&conversation) {
            if latest.admission_id == row.admission_id {
                Arc::make_mut(latest).state = row.state.clone();
            }
        }
    }

    pub(crate) fn insert_admission(&mut self, row: &AccountAdmissionRow, rowid: i64) {
        let candidate = LatestAdmissionRow {
            principal: row.principal.clone(),
            conversation_hash: row.conversation_hash.clone(),
            provider: row.provider.clone(),
            admission_id: row.admission_id.clone(),
            committed_at: row.committed_at,
            rowid,
            pin_id: row.pin_id.clone(),
            pin_generation: row.pin_generation,
            state: row.state.clone(),
            admission_valid_until: row.admission_valid_until,
            lease_valid_until: row.lease_valid_until,
            account_id: row.account_id.clone(),
        };
        let conversation = conversation(&row.principal, &row.conversation_hash, &row.provider);
        let newer = self.latest.get(&conversation).is_none_or(|latest| {
            (candidate.committed_at, candidate.rowid) > (latest.committed_at, latest.rowid)
        });
        if newer {
            self.latest.insert(conversation, Arc::new(candidate));
        }
        if row.charged && row.state != "released" {
            self.set_demand(
                &row.provider,
                &row.account_id,
                (
                    row.committed_at,
                    row.principal.clone(),
                    row.admission_id.clone(),
                ),
                Some(Demand {
                    state: row.state.clone(),
                    request_bytes: row.request_bytes,
                }),
            );
        }
        self.add_pin_started(
            &Contribution {
                principal: &row.principal,
                hash: &row.conversation_hash,
                provider: &row.provider,
                pin_id: &row.pin_id,
                pin_generation: row.pin_generation,
                started: row.started_at.is_some(),
            },
            1,
        );
        if matches!(row.state.as_str(), "reserved" | "started" | "expired") {
            if row.state == "started" {
                self.add_started_pin(&row.pin_id, 1);
            }
            self.open.insert(
                key(&row.principal, &row.admission_id),
                Arc::new(OpenAdmission {
                    row: row.clone(),
                    input_size_class: None,
                    model_decision_id: None,
                    rowid,
                }),
            );
        }
    }

    pub(crate) fn set_probe_scope(
        &mut self,
        principal: &str,
        admission_id: &str,
        size: u32,
        decision: Option<&str>,
    ) {
        if let Some(open) = self.open.get_mut(&key(principal, admission_id)) {
            let open = Arc::make_mut(open);
            open.input_size_class = Some(size);
            open.model_decision_id = decision.map(str::to_owned);
        }
    }

    /// Apply `change` to an open admission whose state is one of `from`.
    /// Returns whether it applied, as the SQL `UPDATE ... WHERE state IN`
    /// does.
    fn transition(
        &mut self,
        principal: &str,
        admission_id: &str,
        from: &[&str],
        change: impl FnOnce(&mut AccountAdmissionRow),
    ) -> bool {
        let id = key(principal, admission_id);
        let Some(open) = self.open.get(&id) else {
            return false;
        };
        if !from.contains(&open.row.state.as_str()) {
            return false;
        }
        let mut row = open.row.clone();
        let was_started = row.started_at.is_some();
        let was_state_started = row.state == "started";
        change(&mut row);
        if was_state_started != (row.state == "started") {
            self.add_started_pin(&row.pin_id, if was_state_started { -1 } else { 1 });
        }
        if !was_started && row.started_at.is_some() {
            self.add_pin_started(
                &Contribution {
                    principal: &row.principal,
                    hash: &row.conversation_hash,
                    provider: &row.provider,
                    pin_id: &row.pin_id,
                    pin_generation: row.pin_generation,
                    started: true,
                },
                1,
            );
        }
        self.restate(&row);
        if matches!(row.state.as_str(), "released" | "finished") {
            self.open.remove(&id);
            self.closing.push(id);
        } else if let Some(open) = self.open.get_mut(&id) {
            Arc::make_mut(open).row = row;
        }
        true
    }

    pub(crate) fn expire(&mut self, principal: &str, admission_id: &str) -> bool {
        self.transition(principal, admission_id, &["reserved"], |row| {
            row.state = "expired".into();
        })
    }

    pub(crate) fn start(
        &mut self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        at: i64,
        late: bool,
    ) -> bool {
        self.transition(principal, admission_id, &["reserved", "expired"], |row| {
            row.state = "started".into();
            row.run_id = Some(run_id.to_owned());
            row.started_at = Some(at);
            row.started_late = late;
        })
    }

    pub(crate) fn release(&mut self, principal: &str, admission_id: &str, at: i64) -> bool {
        self.transition(principal, admission_id, &["reserved", "expired"], |row| {
            row.state = "released".into();
            row.released_at = Some(at);
        })
    }

    pub(crate) fn finish(&mut self, principal: &str, admission_id: &str, at: i64) -> bool {
        self.transition(
            principal,
            admission_id,
            &["reserved", "expired", "started"],
            |row| {
                row.state = "finished".into();
                row.finished_at = Some(at);
            },
        )
    }

    pub(crate) fn put_affinity(
        &mut self,
        row: &AccountAffinityRow,
        expected: Option<(&str, u64)>,
    ) -> bool {
        let conversation = conversation_of(row);
        let current = self.affinity.get(&conversation).cloned();
        let applies = match (&current, expected) {
            (None, None) => true,
            (Some(current), Some((pin, generation))) => {
                current.pin_id == pin && current.pin_generation == generation
            }
            _ => false,
        };
        if !applies {
            return false;
        }
        if let Some(current) = &current {
            if current.pin_id != row.pin_id || current.pin_generation != row.pin_generation {
                // A newly issued pin generation has no admissions yet, so
                // its started count starts at zero.
                self.pin_started.remove(&conversation);
            }
            self.unindex_pin(&current.pin_id, &conversation);
        } else {
            self.pin_started.remove(&conversation);
        }
        self.index_pin(&row.pin_id, conversation.clone());
        self.affinity.insert(conversation, Arc::new(row.clone()));
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn touch_affinity(
        &mut self,
        principal: &str,
        hash: &str,
        provider: &str,
        expected: (&str, u64),
        last_used_at: i64,
        expires_at: i64,
        lease_valid_until: Option<i64>,
    ) -> bool {
        let Some(row) = self.affinity.get_mut(&conversation(principal, hash, provider)) else {
            return false;
        };
        if row.pin_id != expected.0 || row.pin_generation != expected.1 {
            return false;
        }
        let row = Arc::make_mut(row);
        row.last_used_at = last_used_at;
        row.expires_at = expires_at;
        if lease_valid_until.is_some() {
            row.lease_valid_until = lease_valid_until;
        }
        true
    }

    pub(crate) fn end_lease(&mut self, pin_id: &str, at: i64) -> bool {
        let Some(keys) = self.affinity_by_pin.get(&pin_id.to_owned()).cloned() else {
            return false;
        };
        let mut changed = false;
        for conversation in keys.iter() {
            if let Some(row) = self.affinity.get_mut(conversation) {
                if row.lease_valid_until.is_none_or(|until| until > at) {
                    Arc::make_mut(row).lease_valid_until = Some(at);
                    changed = true;
                }
            }
        }
        changed
    }

    fn reservation_ref(&self, principal: &str, reservation_id: &str) -> Option<ReservationRef> {
        self.open
            .get(&key(principal, reservation_id))
            .map(|open| ReservationRef {
                pin_id: open.row.pin_id.clone(),
                account_id: open.row.account_id.clone(),
                credential_id: open.row.credential_id.clone(),
            })
    }

    /// `offer` is (decision id, reservation id, deadline); a continuation
    /// without an offer has nothing the indexes need.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_continuation(
        &mut self,
        principal: &str,
        selection_id: &str,
        run_id: &str,
        admission_id: &str,
        previous: &str,
        offer: Option<(&str, &str, i64)>,
        rowid: i64,
    ) {
        let Some((decision, reservation, valid_until)) = offer else {
            return;
        };
        let reservation_ref = self.reservation_ref(principal, reservation);
        let row = ContinuationStateRow {
            rowid,
            principal: principal.to_owned(),
            selection_id: selection_id.to_owned(),
            run_id: run_id.to_owned(),
            admission_id: admission_id.to_owned(),
            previous_decision_id: previous.to_owned(),
            decision_id: decision.to_owned(),
            reservation_id: reservation.to_owned(),
            valid_until_ms: valid_until,
            used_at_ms: None,
            released_at_ms: None,
            reservation: reservation_ref.clone(),
        };
        self.insert_offer(&row);
        let original = key(principal, admission_id);
        if self.open.contains_key(&original) {
            let mut chain = self
                .chains
                .get(&original)
                .map(|chain| (**chain).clone())
                .unwrap_or_default();
            chain.runs.entry(run_id.to_owned()).or_default().insert(
                rowid,
                ChainReservation {
                    reservation_id: reservation.to_owned(),
                    previous_decision_id: previous.to_owned(),
                    decision_id: decision.to_owned(),
                    used: false,
                    reservation: reservation_ref,
                },
            );
            self.chains.insert(original, Arc::new(chain));
        }
    }

    fn remove_offer(&mut self, selection: &RowKey) -> Option<Arc<Offer>> {
        let offer = self.offers.remove(selection)?;
        self.offer_by_decision.remove(&offer.decision_id);
        Some(offer)
    }

    /// Offers past their deadline release their reserved admission, then
    /// stop being offers, in the order of the two SQL updates.
    pub(crate) fn release_expired_continuations(&mut self, now: i64) {
        let mut expired: Vec<(RowKey, Arc<Offer>)> = self
            .offers
            .iter()
            .filter(|(_, offer)| offer.valid_until_ms <= now)
            .map(|(selection, offer)| (selection.clone(), offer.clone()))
            .collect();
        expired.sort_by_key(|(_, offer)| offer.rowid);
        for (selection, offer) in &expired {
            self.transition(&selection.0, &offer.reservation_id, &["reserved"], |row| {
                row.state = "released".into();
                row.released_at = Some(now);
            });
        }
        for (selection, _) in &expired {
            self.remove_offer(selection);
        }
    }

    fn mark_chain_used(&mut self, principal: &str, offer: &Offer) {
        let original = key(principal, &offer.admission_id);
        if let Some(chain) = self.chains.get_mut(&original) {
            let chain = Arc::make_mut(chain);
            if let Some(row) = chain
                .runs
                .get_mut(&offer.run_id)
                .and_then(|rows| rows.get_mut(&offer.rowid))
            {
                row.used = true;
            }
            if chain
                .latest_used
                .as_ref()
                .is_none_or(|(rowid, _)| *rowid < offer.rowid)
            {
                chain.latest_used = Some((offer.rowid, offer.reservation_id.clone()));
            }
        }
    }

    pub(crate) fn use_continuation(&mut self, decision: &str, now: i64) {
        self.release_expired_continuations(now);
        // The used row identifies the run and predecessor whose competing
        // offers are released.
        let mut used = None;
        if let Some(selection) = self.offer_by_decision.get(&decision.to_owned()).cloned() {
            if let Some(offer) = self.remove_offer(&selection) {
                self.transition(&selection.0, &offer.reservation_id, &["reserved"], |row| {
                    row.state = "started".into();
                    row.started_at = Some(now);
                    row.run_id = Some(offer.run_id.clone());
                });
                self.mark_chain_used(&selection.0, &offer);
                used = Some((selection.0.clone(), (*offer).clone()));
            }
        } else {
            used = self.used_row(decision);
        }
        let Some((principal, used)) = used else {
            return;
        };
        let mut siblings: Vec<Arc<Offer>> = self
            .offers
            .iter()
            .filter(|((owner, _), offer)| {
                *owner == principal
                    && offer.admission_id == used.admission_id
                    && offer.run_id == used.run_id
                    && offer.previous_decision_id == used.previous_decision_id
            })
            .map(|(_, offer)| offer.clone())
            .collect();
        siblings.sort_by_key(|offer| offer.rowid);
        for sibling in siblings {
            self.release_continued_reservation(
                &principal,
                &sibling.reservation_id,
                sibling.reservation.clone(),
                now,
            );
        }
    }

    /// An already used offer of a live chain, by decision id.
    fn used_row(&self, decision: &str) -> Option<(String, Offer)> {
        self.chains.iter().find_map(|((principal, original), chain)| {
            chain.runs.iter().find_map(|(run, rows)| {
                rows.iter().find_map(|(rowid, row)| {
                    (row.used && row.decision_id == decision).then(|| {
                        (
                            principal.clone(),
                            Offer {
                                rowid: *rowid,
                                run_id: run.clone(),
                                admission_id: original.clone(),
                                previous_decision_id: row.previous_decision_id.clone(),
                                decision_id: row.decision_id.clone(),
                                reservation_id: row.reservation_id.clone(),
                                valid_until_ms: 0,
                                reservation: row.reservation.clone(),
                            },
                        )
                    })
                })
            })
        })
    }

    /// `reservation` is the reserved admission's identity as last known, for
    /// a reservation that is no longer open.
    fn release_continued_reservation(
        &mut self,
        principal: &str,
        reservation_id: &str,
        reservation: Option<ReservationRef>,
        now: i64,
    ) {
        let pin = self
            .reservation_ref(principal, reservation_id)
            .or(reservation)
            .map(|reservation| reservation.pin_id);
        if let Some(pin) = pin {
            self.release(principal, reservation_id, now);
            // Sibling reservations can share a pin with the successor that started.
            if !self.started_by_pin.contains_key(&pin) {
                self.end_lease(&pin, now);
            }
        }
        let released: Vec<RowKey> = self
            .offers
            .iter()
            .filter(|((owner, _), offer)| {
                owner == principal && offer.reservation_id == reservation_id
            })
            .map(|(selection, _)| selection.clone())
            .collect();
        for selection in released {
            self.remove_offer(&selection);
        }
    }

    pub(crate) fn finish_continuations(
        &mut self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        now: i64,
    ) {
        let Some(rows) = self
            .chains
            .get(&key(principal, admission_id))
            .and_then(|chain| chain.runs.get(run_id).cloned())
        else {
            return;
        };
        for row in rows.values() {
            if row.used {
                let pin = self
                    .reservation_ref(principal, &row.reservation_id)
                    .or(row.reservation.clone())
                    .map(|reservation| reservation.pin_id);
                if let Some(pin) = pin {
                    self.finish(principal, &row.reservation_id, now);
                    self.end_lease(&pin, now);
                }
            } else {
                self.release_continued_reservation(
                    principal,
                    &row.reservation_id,
                    row.reservation.clone(),
                    now,
                );
            }
        }
    }

    /// The offer `use_observed_continuation` would use, mirroring its SQL.
    pub(crate) fn observed_continuation(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        account: &str,
        credential: Option<&str>,
        now: i64,
    ) -> Option<String> {
        let original = self.open.get(&key(principal, admission_id))?;
        if original.row.state != "started" || original.row.run_id.as_deref() != Some(run_id) {
            return None;
        }
        let chain = self.chains.get(&key(principal, admission_id));
        self.offers
            .iter()
            .filter(|((owner, _), offer)| {
                owner == principal
                    && offer.admission_id == admission_id
                    && offer.run_id == run_id
                    && offer.valid_until_ms > now
            })
            .filter(|(_, offer)| {
                self.reservation_ref(principal, &offer.reservation_id)
                    .or(offer.reservation.clone())
                    .is_some_and(|reservation| {
                        reservation.account_id == account
                            && credential.is_none_or(|credential| {
                                reservation.credential_id == credential
                            })
                    })
            })
            .filter(|(_, offer)| {
                !chain.is_some_and(|chain| {
                    chain.used_successors(run_id, &offer.previous_decision_id)
                })
            })
            .max_by_key(|(_, offer)| offer.rowid)
            .map(|(_, offer)| offer.decision_id.clone())
    }

    /// Drop the rows retention deleted. Returns the conversations whose newest
    /// admission was deleted; the caller must recheck them with a keyed read.
    pub(crate) fn prune_admissions(&mut self, deleted: &[PrunedAdmission]) -> Vec<ConversationKey> {
        let mut recheck = Vec::new();
        for row in deleted {
            let id = key(&row.principal, &row.admission_id);
            if let Some(open) = self.open.remove(&id) {
                if open.row.state == "started" {
                    self.add_started_pin(&open.row.pin_id, -1);
                }
                self.closing.push(id.clone());
            }
            if row.charged && row.state != "released" {
                self.set_demand(
                    &row.provider,
                    &row.account_id,
                    (
                        row.committed_at,
                        row.principal.clone(),
                        row.admission_id.clone(),
                    ),
                    None,
                );
            }
            self.add_pin_started(
                &Contribution {
                    principal: &row.principal,
                    hash: &row.conversation_hash,
                    provider: &row.provider,
                    pin_id: &row.pin_id,
                    pin_generation: row.pin_generation,
                    started: row.started,
                },
                -1,
            );
            let conversation =
                conversation(&row.principal, &row.conversation_hash, &row.provider);
            if self
                .latest
                .get(&conversation)
                .is_some_and(|latest| latest.rowid == row.rowid)
                && !recheck.contains(&conversation)
            {
                recheck.push(conversation);
            }
            self.forget_reservation(&row.principal, &row.admission_id);
        }
        recheck
    }

    /// A deleted reservation row no longer joins its offers.
    fn forget_reservation(&mut self, principal: &str, reservation_id: &str) {
        let offers: Vec<RowKey> = self
            .offers
            .iter()
            .filter(|((owner, _), offer)| {
                owner == principal
                    && offer.reservation_id == reservation_id
                    && offer.reservation.is_some()
            })
            .map(|(selection, _)| selection.clone())
            .collect();
        for selection in offers {
            if let Some(offer) = self.offers.get_mut(&selection) {
                Arc::make_mut(offer).reservation = None;
            }
        }
        let chains: Vec<RowKey> = self
            .chains
            .iter()
            .filter(|((owner, _), chain)| {
                owner == principal
                    && chain.runs.values().flat_map(|rows| rows.values()).any(|row| {
                        row.reservation_id == reservation_id && row.reservation.is_some()
                    })
            })
            .map(|(original, _)| original.clone())
            .collect();
        for original in chains {
            if let Some(chain) = self.chains.get_mut(&original) {
                for row in Arc::make_mut(chain)
                    .runs
                    .values_mut()
                    .flat_map(|rows| rows.values_mut())
                {
                    if row.reservation_id == reservation_id {
                        row.reservation = None;
                    }
                }
            }
        }
    }

    pub(crate) fn set_latest(
        &mut self,
        conversation: ConversationKey,
        latest: Option<LatestAdmissionRow>,
    ) {
        match latest {
            Some(latest) => {
                self.latest.insert(conversation, Arc::new(latest));
            }
            None => {
                self.latest.remove(&conversation);
            }
        }
    }

    pub(crate) fn prune_affinities(&mut self, deleted: &[ConversationKey]) {
        for conversation in deleted {
            if let Some(row) = self.affinity.remove(conversation) {
                self.unindex_pin(&row.pin_id, conversation);
            }
            self.pin_started.remove(conversation);
        }
    }

    /// Drop deleted continuation rows. Returns originals whose newest used
    /// row was deleted; the caller must recheck them with a keyed read.
    pub(crate) fn prune_continuations(&mut self, deleted: &[PrunedContinuation]) -> Vec<RowKey> {
        let mut recheck = Vec::new();
        for row in deleted {
            if self
                .offers
                .get(&key(&row.principal, &row.selection_id))
                .is_some_and(|offer| offer.rowid == row.rowid)
            {
                self.remove_offer(&key(&row.principal, &row.selection_id));
            }
            let original = key(&row.principal, &row.admission_id);
            let Some(chain) = self.chains.get_mut(&original) else {
                continue;
            };
            let chain = Arc::make_mut(chain);
            if let Some(rows) = chain.runs.get_mut(&row.run_id) {
                rows.remove(&row.rowid);
                if rows.is_empty() {
                    chain.runs.remove(&row.run_id);
                }
            }
            if chain
                .latest_used
                .as_ref()
                .is_some_and(|(rowid, _)| *rowid == row.rowid)
                && !recheck.contains(&original)
            {
                recheck.push(original.clone());
            }
            if chain.runs.is_empty() {
                self.chains.remove(&original);
            }
        }
        recheck
    }

    pub(crate) fn set_latest_used(&mut self, original: &RowKey, latest: Option<(i64, String)>) {
        if let Some(chain) = self.chains.get_mut(original) {
            Arc::make_mut(chain).latest_used = latest;
        }
    }

    /// End-of-command cleanup: a chain lives only while its original is open.
    pub(crate) fn settle(&mut self) {
        for original in std::mem::take(&mut self.closing) {
            if !self.open.contains_key(&original) {
                self.chains.remove(&original);
            }
        }
    }
}

fn conversation_of(row: &AccountAffinityRow) -> ConversationKey {
    conversation(&row.principal, &row.conversation_hash, &row.provider)
}
