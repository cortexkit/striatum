//! In-memory admission, affinity, demand and continuation state.
//!
//! Every writer of `account_admission`, `account_affinity` and
//! `account_continue` applies the same change here, in the same writer command
//! as its SQL. Request paths read this state instead of those tables; the
//! continuation transitions are decided here and persisted as keyed writes
//! (see [`Effect`]). The load-equivalence tests prove the state stays equal
//! to a fresh load, and that every read answers what its SQL query would.
//!
//! Only bounded data is kept: open admissions, one demand entry per charged
//! unreleased admission, every affinity row, and small derived indexes for
//! the historical SQL predicates (newest admission per conversation, started
//! rows on the current pin, and the continuation chains of open originals).
//! Finished and released admission bodies stay in SQL.

use super::pin_members::PinMembers;
use super::shared_text::SharedText;
use super::{compact_rows, conversation_hash::ConversationHash};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDemandRow, AdmissionGroupRows,
    AdmissionScanRow, ContinuationStateRow, LatestAdmissionRow, PrunedAdmission,
    PrunedContinuation, ReservationRef,
};

/// Shards are grouped [`GROUP`] to a group, [`GROUP`] groups to a map. At the
/// live size (about 200,000 conversations) a shard holds about 50 entries.
const GROUP: usize = 64;
const SHARDS: usize = GROUP * GROUP;

/// A hash map keyed with [`KeyHasher`], for the loader's grouping and the shards.
type FastMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<KeyHasher>>;
type Shard<K, V> = FastMap<K, V>;
type Group<K, V> = Vec<Arc<Shard<K, V>>>;

/// A map whose clone copies one pointer. Its first write after a clone copies
/// the 64 group pointers, the 64 shard pointers of one group and one shard of
/// about 50 entries, so a savepoint fork never copies all rows, a command
/// copies a few dozen rows per shard it writes, and a 256-row retention batch
/// copies at most 256 small shards.
#[derive(Clone)]
pub(crate) struct ShardedMap<K, V> {
    groups: Arc<Vec<Arc<Group<K, V>>>>,
    len: usize,
}

impl<K, V> Default for ShardedMap<K, V> {
    fn default() -> Self {
        // Empty shards and groups share one value until their first write.
        let group: Arc<Group<K, V>> = Arc::new(vec![Arc::new(Shard::default()); GROUP]);
        Self {
            groups: Arc::new(vec![group; GROUP]),
            len: 0,
        }
    }
}

/// FNV-1a with a final bit mix: fixed keys, so two maps built independently
/// place a key in the same shard, and far cheaper than SipHash for the short
/// string keys these maps hold. The mix spreads every input bit over the
/// whole hash, which plain FNV-1a does not do for its low bits.
#[derive(Default)]
pub(crate) struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        let mut x = self.0 ^ 0xcbf2_9ce4_8422_2325;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^ (x >> 33)
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

/// The shard index takes bits 40-51 of the hash. A shard's own table uses
/// the low bits to place a key and the top seven as its tag, so keys that
/// share a shard still spread within it.
fn shard_of<K: Hash>(key: &K) -> usize {
    let mut hasher = KeyHasher::default();
    key.hash(&mut hasher);
    ((hasher.finish() >> 40) % SHARDS as u64) as usize
}

impl<K: Eq + Hash + Clone, V: Clone> ShardedMap<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        let shard = shard_of(key);
        self.groups[shard / GROUP][shard % GROUP].get(key)
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    fn shard_mut(&mut self, key: &K) -> &mut Shard<K, V> {
        let shard = shard_of(key);
        let group = Arc::make_mut(&mut Arc::make_mut(&mut self.groups)[shard / GROUP]);
        Arc::make_mut(&mut group[shard % GROUP])
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
        let shard = self.shard_mut(key);
        let removed = shard.remove(key);
        // Keep deletion-heavy shards near their live size, with hysteresis so
        // alternating insertions and removals do not repeatedly resize them.
        if shard.is_empty() || shard.len() < shard.capacity() / 4 {
            shard.shrink_to_fit();
        }
        self.len -= 1;
        removed
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        // An empty map skips its 4,096 empty shards.
        let groups = if self.len == 0 { 0 } else { GROUP };
        self.groups[..groups]
            .iter()
            .flat_map(|group| group.iter())
            .flat_map(|shard| shard.iter())
    }

    /// Whether `other` shares every shard with this map, so a fork copied no rows.
    #[cfg(test)]
    pub(crate) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.groups, &other.groups)
    }
}

impl<K: Eq + Hash + Clone, V: Clone + PartialEq> PartialEq for ShardedMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<K: Eq + Hash + Clone, V: Clone + Eq> Eq for ShardedMap<K, V> {}

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
pub(crate) type RowKey = (SharedText, String);
/// (principal, conversation hash, provider).
pub(crate) type ConversationKey = (SharedText, ConversationHash, SharedText);

fn key(a: &str, b: &str) -> RowKey {
    (a.into(), b.to_owned())
}

fn conversation(
    principal: &str,
    hash: impl Into<ConversationHash>,
    provider: &str,
) -> ConversationKey {
    (principal.into(), hash.into(), provider.into())
}

/// An admission in `reserved`, `started` or `expired`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OpenAdmission {
    pub(crate) row: compact_rows::Admission,
    pub(crate) input_size_class: Option<u32>,
    pub(crate) model_decision_id: Option<String>,
    pub(crate) rowid: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Demand {
    pub(crate) state: SharedText,
    pub(crate) request_bytes: u64,
}

/// Commit-time span of one demand bucket. An account can hold tens of
/// thousands of demand entries; bucketing by hour makes a write copy only the
/// bucket pointers and one hour's entries, never the whole account.
const DEMAND_BUCKET_MS: i64 = 3_600_000;

/// One demand entry's key: (commit time, principal, admission id), the order
/// of `ACCOUNT_DEMAND_SQL`.
pub(crate) type DemandKey = (i64, SharedText, String);

/// Demand of one (provider, account), in `ACCOUNT_DEMAND_SQL` order: hour
/// buckets of entries, so iterating buckets then entries keeps that order.
/// Buckets are never empty, so equal entries mean equal structure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AccountDemand {
    buckets: BTreeMap<i64, Arc<BTreeMap<DemandKey, Demand>>>,
    len: usize,
}

impl AccountDemand {
    #[cfg(all(test, feature = "heap-measure"))]
    pub(crate) fn visit_heap_strings<'a>(&'a self, mut visit: impl FnMut(&'a str)) {
        for entries in self.buckets.values() {
            for ((_, principal, admission), demand) in entries.iter() {
                visit(principal);
                visit(admission);
                visit(&demand.state);
            }
        }
    }

    fn bucket(committed_at: i64) -> i64 {
        committed_at.div_euclid(DEMAND_BUCKET_MS)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn insert(&mut self, key: DemandKey, demand: Demand) {
        let bucket = Arc::make_mut(self.buckets.entry(Self::bucket(key.0)).or_default());
        if bucket.insert(key, demand).is_none() {
            self.len += 1;
        }
    }

    fn remove(&mut self, key: &DemandKey) {
        let bucket = Self::bucket(key.0);
        let Some(entries) = self.buckets.get_mut(&bucket) else {
            return;
        };
        if !entries.contains_key(key) {
            return;
        }
        let entries = Arc::make_mut(entries);
        entries.remove(key);
        self.len -= 1;
        if entries.is_empty() {
            self.buckets.remove(&bucket);
        }
    }

    /// Entries committed after `floor`, in order.
    fn after(&self, floor: i64) -> impl Iterator<Item = (&DemandKey, &Demand)> {
        // A bucket ending at or before the floor holds nothing newer.
        self.buckets
            .range(Self::bucket(floor)..)
            .flat_map(|(_, entries)| entries.iter())
            .filter(move |(key, _)| key.0 > floor)
    }
}

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
    /// The offer was released unused (`released_at_ms` is set). A released
    /// offer no longer names its run's current reservation.
    pub(crate) released: bool,
    pub(crate) reservation: Option<ReservationRef>,
}

/// A SQL write the state decided. The continuation transitions are computed
/// on the working state, then persisted as keyed writes, so deciding them
/// never reads the tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Write the admission's lifecycle columns; it was in state `from`.
    Admission {
        row: AccountAdmissionRow,
        from: String,
    },
    /// Advance only observation time, including for a closed original row.
    Observed {
        admission: RowKey,
        previous: Option<i64>,
        at: i64,
    },
    /// End one conversation's lease. The write requires the pin, generation
    /// and lease end the state saw, as `end_lease`'s SQL requires its pin.
    LeaseEnded {
        conversation: ConversationKey,
        pin_id: String,
        pin_generation: u64,
        previous: Option<i64>,
        at: i64,
    },
    /// A transition the state found not applicable: the admission is not
    /// open in one of `from`. SQL must agree.
    Unchanged {
        admission: RowKey,
        from: Vec<String>,
    },
    /// An offer was used or released.
    Offer {
        selection: RowKey,
        used_at: Option<i64>,
        released_at: Option<i64>,
    },
}

/// How an offer stops being an offer.
#[derive(Clone, Copy)]
enum Settle {
    Used(i64),
    Released(i64),
    /// Retention deleted the row: nothing to persist.
    Deleted,
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
    open_by_pin: ShardedMap<(String, u64), u64>,
    /// Keyed by (provider, account id).
    pub(crate) demand: ShardedMap<(SharedText, SharedText), Arc<AccountDemand>>,
    pub(crate) latest: ShardedMap<ConversationKey, Arc<compact_rows::Latest>>,
    pub(crate) affinity: ShardedMap<ConversationKey, Arc<compact_rows::Affinity>>,
    pub(crate) affinity_by_pin: ShardedMap<String, Arc<PinMembers>>,
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
    /// The bound `signed_out` observations of each open admission, keyed by
    /// the observation's (principal, admission id). A continuation excludes
    /// the credentials its run reported signed out; it reads them here, not
    /// from `account_observation`. Dropped when the admission closes.
    pub(crate) signed_out: BTreeMap<RowKey, BTreeSet<SignedOut>>,
    /// Writes decided since the command last drained them; empty whenever a
    /// state is published.
    pub(crate) pending: Vec<Effect>,
}

/// What a bound `signed_out` observation says, read out of its stored body
/// the way `AccountTx::signed_out_credentials` reads it with `json_extract`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SignedOut {
    /// The body is not JSON. The SQL read fails on such a row.
    Unreadable,
    Row {
        /// `$.run_id` when it is a string; any other value never equals a run.
        run_id: Option<String>,
        /// `$.exhausted.credential_id`: a string, or an object or array as
        /// its JSON text. `None` (missing, null, a number or a boolean) is a
        /// value the SQL read cannot return as a credential, so it fails too.
        credential_id: Option<String>,
    },
}

impl SignedOut {
    pub(crate) fn parse(observation_json: &str) -> Self {
        let Ok(body) = serde_json::from_str::<serde_json::Value>(observation_json) else {
            return Self::Unreadable;
        };
        let credential_id = match body.get("exhausted").and_then(|e| e.get("credential_id")) {
            Some(serde_json::Value::String(text)) => Some(text.clone()),
            Some(value @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => {
                Some(value.to_string())
            }
            _ => None,
        };
        Self::Row {
            run_id: body
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            credential_id,
        }
    }
}

/// The index state an admission row contributes, before and after a change.
struct Contribution<'a> {
    principal: &'a str,
    hash: ConversationHash,
    provider: &'a str,
    pin_id: &'a str,
    pin_generation: u64,
    started: bool,
}

impl AdmissionState {
    pub(crate) fn from_rows(rows: AdmissionGroupRows) -> Self {
        let AdmissionGroupRows {
            open,
            admissions,
            affinities,
            offers,
            chains,
            continuation_sources: _,
            signed_out,
        } = rows;
        let mut state = Self::default();
        for row in open {
            let open = OpenAdmission {
                input_size_class: row.input_size_class,
                model_decision_id: row.model_decision_id,
                rowid: row.rowid,
                row: compact_rows::admission(row.row),
            };
            if open.row.state == "started" {
                state.add_started_pin(&open.row.pin_id, 1);
            }
            state.add_open_pin(&open.row.pin_id, open.row.pin_generation, 1);
            state.open.insert(
                key(&open.row.principal, &open.row.admission_id),
                Arc::new(open),
            );
        }
        // The affinity indexes and the pass over every admission row are
        // independent, so they are built on two threads; a live-sized load is
        // about 200,000 rows of each. Started rows are matched to their
        // conversation's current pin afterwards.
        let ((affinity, affinity_by_pin), (conversations, demand)) = std::thread::scope(|scope| {
            let affinity = scope.spawn(move || Self::affinity_indexes(affinities));
            let scanned = Self::scan_admissions(admissions);
            (affinity.join().expect("affinity index build"), scanned)
        });
        state.affinity = affinity;
        state.affinity_by_pin = affinity_by_pin;
        state.demand = demand;
        for (conversation, (newest, started)) in conversations {
            // A started row counts toward `pin_has_started` only while it
            // names its conversation's current pin and generation.
            let count = state.affinity.get(&conversation).map_or(0, |affinity| {
                started
                    .iter()
                    .filter(|(pin, generation)| {
                        *pin == affinity.pin_id && *generation == affinity.pin_generation
                    })
                    .count() as u64
            });
            if count > 0 {
                state.pin_started.insert(conversation.clone(), count);
            }
            state.latest.insert(conversation, Arc::new(newest));
        }
        state.load_continuations(offers, chains);
        for row in signed_out {
            state.record_signed_out(&row.principal, &row.admission_id, &row.observation_json);
        }
        state
    }

    #[allow(clippy::type_complexity)]
    fn affinity_indexes(
        affinities: Vec<AccountAffinityRow>,
    ) -> (
        ShardedMap<ConversationKey, Arc<compact_rows::Affinity>>,
        ShardedMap<String, Arc<PinMembers>>,
    ) {
        let mut by_pin: FastMap<String, PinMembers> =
            FastMap::with_capacity_and_hasher(affinities.len(), Default::default());
        let mut affinity = ShardedMap::default();
        for row in affinities {
            let conversation = conversation_of(&row);
            match by_pin.get_mut(&row.pin_id) {
                Some(conversations) => {
                    conversations.insert(conversation.clone());
                }
                None => {
                    let mut members = PinMembers::default();
                    members.insert(conversation.clone());
                    by_pin.insert(row.pin_id.clone(), members);
                }
            }
            affinity.insert(conversation, Arc::new(compact_rows::affinity(row)));
        }
        let mut affinity_by_pin = ShardedMap::default();
        for (pin, conversations) in by_pin {
            affinity_by_pin.insert(pin, Arc::new(conversations));
        }
        (affinity, affinity_by_pin)
    }

    /// One pass over every admission row: per conversation, its newest row
    /// and the (pin, generation) of each of its started rows; and the charged
    /// demand. Indexes are built in plain maps and wrapped once, so loading
    /// is linear in the rows read.
    #[allow(clippy::type_complexity)]
    fn scan_admissions(
        admissions: Vec<AdmissionScanRow>,
    ) -> (
        FastMap<ConversationKey, (compact_rows::Latest, Vec<(String, u64)>)>,
        ShardedMap<(SharedText, SharedText), Arc<AccountDemand>>,
    ) {
        let mut conversations: FastMap<
            ConversationKey,
            (compact_rows::Latest, Vec<(String, u64)>),
        > = FastMap::default();
        let mut demand: HashMap<SharedText, HashMap<SharedText, AccountDemand>> = HashMap::new();
        for scanned in admissions {
            let row = compact_rows::latest(scanned.latest);
            let conversation = conversation(&row.principal, &row.conversation_hash, &row.provider);
            let started = scanned
                .started
                .then(|| (row.pin_id.clone(), row.pin_generation));
            if scanned.charged && row.state != "released" {
                // Look the account up by reference; its key strings are
                // copied only for the first row of each account.
                if !demand.contains_key(&row.provider) {
                    demand.insert(row.provider.clone(), HashMap::new());
                }
                let accounts = demand.get_mut(&row.provider).expect("inserted above");
                if !accounts.contains_key(&row.account_id) {
                    accounts.insert(row.account_id.clone(), AccountDemand::default());
                }
                accounts
                    .get_mut(&row.account_id)
                    .expect("inserted above")
                    .insert(
                        (
                            row.committed_at,
                            row.principal.clone(),
                            row.admission_id.clone(),
                        ),
                        Demand {
                            state: row.state.clone(),
                            request_bytes: scanned.request_bytes,
                        },
                    );
            }
            match conversations.entry(conversation) {
                Entry::Occupied(mut entry) => {
                    let (newest, pins) = entry.get_mut();
                    pins.extend(started);
                    if (row.committed_at, row.rowid) > (newest.committed_at, newest.rowid) {
                        *newest = row;
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert((row, started.into_iter().collect()));
                }
            }
        }
        let mut charged = ShardedMap::default();
        for (provider, accounts) in demand {
            for (account, entries) in accounts {
                charged.insert((provider.clone(), account), Arc::new(entries));
            }
        }
        (conversations, charged)
    }

    /// Open offers and the offer chains of open originals.
    fn load_continuations(
        &mut self,
        offers: Vec<ContinuationStateRow>,
        chains: Vec<ContinuationStateRow>,
    ) {
        for row in offers {
            self.insert_offer(&row);
        }
        let mut grouped: BTreeMap<RowKey, Chain> = BTreeMap::new();
        for row in chains {
            let chain = grouped
                .entry(key(&row.principal, &row.admission_id))
                .or_default();
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
                    released: row.released_at_ms.is_some(),
                    reservation: row.reservation,
                },
            );
        }
        for (original, chain) in grouped {
            self.chains.insert(original, Arc::new(chain));
        }
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
        let pin = pin_id.to_owned();
        match self.affinity_by_pin.get_mut(&pin) {
            Some(keys) => {
                Arc::make_mut(keys).insert(conversation);
            }
            None => {
                let mut members = PinMembers::default();
                members.insert(conversation);
                self.affinity_by_pin.insert(pin, Arc::new(members));
            }
        }
    }

    fn unindex_pin(&mut self, pin_id: &str, conversation: &ConversationKey) {
        let pin = pin_id.to_owned();
        let Some(keys) = self.affinity_by_pin.get_mut(&pin) else {
            return;
        };
        let keys = Arc::make_mut(keys);
        keys.remove(conversation);
        if keys.is_empty() {
            self.affinity_by_pin.remove(&pin);
        }
    }

    fn add_count<K: Eq + Hash + Clone>(map: &mut ShardedMap<K, u64>, key: K, delta: i64) {
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

    fn add_open_pin(&mut self, pin: &str, generation: u64, delta: i64) {
        Self::add_count(&mut self.open_by_pin, (pin.to_owned(), generation), delta);
    }

    pub(crate) fn pin_has_open_admissions(&self, pin: &str, generation: u64) -> bool {
        self.open_by_pin.contains_key(&(pin.to_owned(), generation))
    }

    /// A started row counts toward `pin_has_started` only while it names the
    /// conversation's current pin and generation.
    fn add_pin_started(&mut self, row: &Contribution<'_>, delta: i64) {
        let conversation = conversation(row.principal, &row.hash, row.provider);
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
        entry: DemandKey,
        demand: Option<Demand>,
    ) {
        // Copy on write: an account shared with the published snapshot copies
        // its bucket pointers and one bucket, once per command.
        let account = (provider.into(), account_id.into());
        match demand {
            Some(demand) => match self.demand.get_mut(&account) {
                Some(entries) => Arc::make_mut(entries).insert(entry, demand),
                None => {
                    let mut entries = AccountDemand::default();
                    entries.insert(entry, demand);
                    self.demand.insert(account, Arc::new(entries));
                }
            },
            None => {
                let Some(entries) = self.demand.get_mut(&account) else {
                    return;
                };
                let entries = Arc::make_mut(entries);
                entries.remove(&entry);
                if entries.len() == 0 {
                    self.demand.remove(&account);
                }
            }
        }
    }

    /// Mirror a state change in demand and in the conversation's newest row.
    fn restate(&mut self, row: &compact_rows::Admission) {
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
            self.latest
                .insert(conversation, Arc::new(compact_rows::latest(candidate)));
        }
        if row.charged && row.state != "released" {
            self.set_demand(
                &row.provider,
                &row.account_id,
                (
                    row.committed_at,
                    row.principal.as_str().into(),
                    row.admission_id.clone(),
                ),
                Some(Demand {
                    state: row.state.as_str().into(),
                    request_bytes: row.request_bytes,
                }),
            );
        }
        self.add_pin_started(
            &Contribution {
                principal: &row.principal,
                hash: (&row.conversation_hash).into(),
                provider: &row.provider,
                pin_id: &row.pin_id,
                pin_generation: row.pin_generation,
                started: row.started_at.is_some(),
            },
            1,
        );
        if matches!(row.state.as_str(), "reserved" | "started" | "expired") {
            self.add_open_pin(&row.pin_id, row.pin_generation, 1);
            if row.state == "started" {
                self.add_started_pin(&row.pin_id, 1);
            }
            self.open.insert(
                key(&row.principal, &row.admission_id),
                Arc::new(OpenAdmission {
                    row: compact_rows::admission(row.clone()),
                    input_size_class: None,
                    model_decision_id: None,
                    rowid,
                }),
            );
        }
    }

    /// Closed rows are not retained in `open`, but late start evidence restores
    /// their demand and contributes to the current pin's history. Do not reopen
    /// the admission or count it as an active run.
    pub(crate) fn record_terminal_start(&mut self, row: &AccountAdmissionRow) {
        self.set_demand(
            &row.provider,
            &row.account_id,
            (
                row.committed_at,
                row.principal.clone().into(),
                row.admission_id.clone().into(),
            ),
            Some(Demand {
                state: row.state.clone().into(),
                request_bytes: row.request_bytes,
            }),
        );
        self.add_pin_started(
            &Contribution {
                principal: &row.principal,
                hash: (&row.conversation_hash).into(),
                provider: &row.provider,
                pin_id: &row.pin_id,
                pin_generation: row.pin_generation,
                started: true,
            },
            1,
        );
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
        change: impl FnOnce(&mut compact_rows::Admission),
    ) -> bool {
        let id = key(principal, admission_id);
        if !self
            .open
            .get(&id)
            .is_some_and(|open| from.contains(&open.row.state.as_str()))
        {
            // Persisted only where the state decides; a mirrored writer has
            // already compared SQL's own result and discards this.
            self.pending.push(Effect::Unchanged {
                admission: id,
                from: from.iter().map(|state| (*state).to_owned()).collect(),
            });
            return false;
        }
        let open = self.open.get(&id).expect("checked above");
        let mut row = open.row.clone();
        let from = row.state.to_string();
        let was_charged = row.charged;
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
                    hash: (&row.conversation_hash).into(),
                    provider: &row.provider,
                    pin_id: &row.pin_id,
                    pin_generation: row.pin_generation,
                    started: true,
                },
                1,
            );
        }
        if was_charged && !row.charged {
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
        self.restate(&row);
        self.pending.push(Effect::Admission {
            row: compact_rows::expand_admission(&row),
            from,
        });
        if matches!(row.state.as_str(), "released" | "finished") {
            self.open.remove(&id);
            self.add_open_pin(&row.pin_id, row.pin_generation, -1);
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
        run_id: Option<&str>,
        at: i64,
        late: bool,
    ) -> bool {
        self.transition(principal, admission_id, &["reserved", "expired"], |row| {
            row.state = "started".into();
            row.run_id = run_id.map(str::to_owned);
            row.started_at = Some(at);
            row.last_observed_at = Some(row.last_observed_at.unwrap_or(at).max(at));
            row.started_late = late;
        })
    }

    pub(crate) fn release(&mut self, principal: &str, admission_id: &str, at: i64) -> bool {
        self.transition(principal, admission_id, &["reserved", "expired"], |row| {
            row.state = "released".into();
            row.released_at = Some(at);
        })
    }

    pub(crate) fn observe_admission(&mut self, original: &AccountAdmissionRow, at: i64) {
        let original_key = key(&original.principal, &original.admission_id);
        let (previous, started) = self
            .open
            .get(&original_key)
            .map_or((original.last_observed_at, original.started_at), |open| {
                (open.row.last_observed_at, open.row.started_at)
            });
        let mut rows = vec![(original_key.clone(), previous, started)];
        if let Some(chain) = self.chains.get(&original_key) {
            rows.extend(
                chain
                    .runs
                    .values()
                    .flat_map(|offers| offers.values())
                    .filter_map(|offer| {
                        let id = key(&original.principal, &offer.reservation_id);
                        let open = self.open.get(&id)?;
                        (open.row.state == "started").then_some((
                            id,
                            open.row.last_observed_at,
                            open.row.started_at,
                        ))
                    }),
            );
        }
        for (admission, previous, started) in rows {
            let latest = previous
                .into_iter()
                .chain(started)
                .chain([at])
                .max()
                .unwrap();
            self.pending.push(Effect::Observed {
                admission: admission.clone(),
                previous,
                at: latest,
            });
            if let Some(open) = self.open.get_mut(&admission) {
                Arc::make_mut(open).row.last_observed_at = Some(latest);
            }
        }
    }

    pub(crate) fn finish(&mut self, principal: &str, admission_id: &str, at: i64) -> bool {
        self.transition(
            principal,
            admission_id,
            &["reserved", "expired", "started"],
            |row| {
                row.state = "finished".into();
                row.finished_at = Some(at);
                if row.started_at.is_none() && row.run_id.is_none() {
                    row.charged = false;
                }
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
        self.affinity
            .insert(conversation, Arc::new(compact_rows::affinity(row.clone())));
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
        let Some(row) = self
            .affinity
            .get_mut(&conversation(principal, hash, provider))
        else {
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
                    let effect = Effect::LeaseEnded {
                        conversation: conversation.clone(),
                        pin_id: row.pin_id.clone(),
                        pin_generation: row.pin_generation,
                        previous: row.lease_valid_until,
                        at,
                    };
                    Arc::make_mut(row).lease_valid_until = Some(at);
                    changed = true;
                    self.pending.push(effect);
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
                account_id: open.row.account_id.to_string(),
                credential_id: open.row.credential_id.to_string(),
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
                    released: false,
                    reservation: reservation_ref,
                },
            );
            self.chains.insert(original, Arc::new(chain));
        }
    }

    fn remove_offer(&mut self, selection: &RowKey, settle: Settle) -> Option<Arc<Offer>> {
        let offer = self.offers.remove(selection)?;
        self.offer_by_decision.remove(&offer.decision_id);
        let (used_at, released_at) = match settle {
            Settle::Used(at) => (Some(at), None),
            Settle::Released(at) => (None, Some(at)),
            Settle::Deleted => return Some(offer),
        };
        if released_at.is_some() {
            let original = key(&selection.0, &offer.admission_id);
            if let Some(chain) = self.chains.get_mut(&original) {
                if let Some(row) = Arc::make_mut(chain)
                    .runs
                    .get_mut(&offer.run_id)
                    .and_then(|rows| rows.get_mut(&offer.rowid))
                {
                    row.released = true;
                }
            }
        }
        self.pending.push(Effect::Offer {
            selection: selection.clone(),
            used_at,
            released_at,
        });
        Some(offer)
    }

    /// Offers past their deadline release their reserved admission, then
    /// stop being offers, in the order of the two SQL updates. Each release
    /// is recorded at the offer's own deadline, as the SQL records it.
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
                row.released_at = Some(offer.valid_until_ms);
            });
        }
        for (selection, offer) in &expired {
            self.remove_offer(selection, Settle::Released(offer.valid_until_ms));
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
            if let Some(offer) = self.remove_offer(&selection, Settle::Used(now)) {
                self.transition(&selection.0, &offer.reservation_id, &["reserved"], |row| {
                    row.state = "started".into();
                    row.started_at = Some(now);
                    row.last_observed_at = Some(row.last_observed_at.unwrap_or(now).max(now));
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
    fn used_row(&self, decision: &str) -> Option<(SharedText, Offer)> {
        self.chains
            .iter()
            .find_map(|((principal, original), chain)| {
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
            self.remove_offer(&selection, Settle::Released(now));
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
                            && credential
                                .is_none_or(|credential| reservation.credential_id == credential)
                    })
            })
            .filter(|(_, offer)| {
                !chain
                    .is_some_and(|chain| chain.used_successors(run_id, &offer.previous_decision_id))
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
                self.add_open_pin(&open.row.pin_id, open.row.pin_generation, -1);
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
                        row.principal.as_str().into(),
                        row.admission_id.clone(),
                    ),
                    None,
                );
            }
            self.add_pin_started(
                &Contribution {
                    principal: &row.principal,
                    hash: (&row.conversation_hash).into(),
                    provider: &row.provider,
                    pin_id: &row.pin_id,
                    pin_generation: row.pin_generation,
                    started: row.started,
                },
                -1,
            );
            let conversation = conversation(&row.principal, &row.conversation_hash, &row.provider);
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
                    && chain
                        .runs
                        .values()
                        .flat_map(|rows| rows.values())
                        .any(|row| {
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
                self.latest
                    .insert(conversation, Arc::new(compact_rows::latest(latest)));
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
                self.remove_offer(&key(&row.principal, &row.selection_id), Settle::Deleted);
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

    /// End-of-command cleanup: a chain and the sign-in exclusions live only
    /// while their admission is open.
    pub(crate) fn settle(&mut self) {
        for original in std::mem::take(&mut self.closing) {
            if !self.open.contains_key(&original) {
                self.chains.remove(&original);
                self.signed_out.remove(&original);
            }
        }
    }

    /// Keep a bound `signed_out` observation if it names an open admission.
    /// The loader passes every stored one, a command each one it binds.
    pub(crate) fn record_signed_out(
        &mut self,
        principal: &str,
        admission_id: &str,
        observation_json: &str,
    ) {
        let admission = key(principal, admission_id);
        if self.open.contains_key(&admission) {
            self.signed_out
                .entry(admission)
                .or_default()
                .insert(SignedOut::parse(observation_json));
        }
    }
}

// Request-path reads. Each answers exactly what the named SQL query answers
// over the rows this state mirrors.
impl AdmissionState {
    pub(crate) fn open_admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> Option<&OpenAdmission> {
        self.open
            .get(&key(principal, admission_id))
            .map(Arc::as_ref)
    }

    /// `AccountTx::signed_out_credentials` over an open admission: the
    /// distinct credentials its bound `signed_out` observations name for
    /// `run_id`. An error where that SQL read fails on a row. Only an open
    /// admission can continue, so only open ones are held.
    pub(crate) fn signed_out_credentials(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
    ) -> Result<Vec<String>, String> {
        let mut credentials = BTreeSet::new();
        for row in self
            .signed_out
            .get(&key(principal, admission_id))
            .into_iter()
            .flatten()
        {
            match row {
                SignedOut::Unreadable => {
                    return Err(format!(
                        "signed-out observation of admission {admission_id} is not JSON"
                    ))
                }
                SignedOut::Row {
                    run_id: Some(run),
                    credential_id,
                } if run == run_id => match credential_id {
                    Some(credential) => {
                        credentials.insert(credential.clone());
                    }
                    None => {
                        return Err(format!(
                            "signed-out observation of admission {admission_id} names no \
                             credential"
                        ))
                    }
                },
                SignedOut::Row { .. } => {}
            }
        }
        Ok(credentials.into_iter().collect())
    }

    pub(crate) fn affinity(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
    ) -> Option<&compact_rows::Affinity> {
        self.affinity
            .get(&conversation(principal, hash, provider))
            .map(Arc::as_ref)
    }

    /// `ACCOUNT_DEMAND_SQL`: charged, unreleased admissions of one account
    /// committed after `committed_after`, in (commit time, principal,
    /// admission id) order.
    pub(crate) fn demand(
        &self,
        provider: &str,
        account_id: &str,
        committed_after: Option<i64>,
    ) -> Vec<AccountDemandRow> {
        let floor = committed_after.unwrap_or(i64::MIN);
        self.demand
            .get(&(provider.into(), account_id.into()))
            .into_iter()
            .flat_map(|entries| entries.after(floor))
            .map(|((committed_at, _, _), demand)| AccountDemandRow {
                account_id: account_id.to_owned(),
                state: demand.state.to_string(),
                committed_at: *committed_at,
                request_bytes: demand.request_bytes,
            })
            .collect()
    }

    /// `pin_has_started`: whether an admission on `affinity`'s pin and
    /// generation has started. The index counts started rows on each
    /// conversation's current pin, so another pin has none here.
    pub(crate) fn pin_has_started(&self, affinity: &AccountAffinityRow) -> bool {
        let conversation = conversation_of(affinity);
        let current = self.affinity.get(&conversation).is_some_and(|current| {
            current.pin_id == affinity.pin_id && current.pin_generation == affinity.pin_generation
        });
        current && self.pin_started.contains_key(&conversation)
    }

    /// `parent_used_account`: the account of the conversation's newest
    /// admission while it holds the conversation's current pin, until that pin
    /// lapses (the admission deadline before start, the lease deadline after).
    pub(crate) fn parent_used_account(
        &self,
        principal: &str,
        hash: &str,
        provider: &str,
        now: i64,
    ) -> Option<String> {
        let conversation = conversation(principal, hash, provider);
        let latest = self.latest.get(&conversation)?;
        let affinity = self.affinity.get(&conversation)?;
        if affinity.pin_id != latest.pin_id || affinity.pin_generation != latest.pin_generation {
            return None;
        }
        let holds = (latest.state == "reserved" && latest.admission_valid_until > now)
            || (latest.state == "started" && latest.lease_valid_until > now);
        holds.then(|| latest.account_id.to_string())
    }

    /// `has_expired_continuations`: an unused, unreleased offer past its deadline.
    pub(crate) fn has_expired_offers(&self, now: i64) -> bool {
        self.offers
            .iter()
            .any(|(_, offer)| offer.valid_until_ms <= now)
    }

    /// `continued_reservation`: the reservation of the run's offer that
    /// `decision` names, unless that offer was released.
    pub(crate) fn continued_reservation(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        decision: &str,
    ) -> Option<String> {
        self.chains
            .get(&key(principal, admission_id))?
            .runs
            .get(run_id)?
            .values()
            .find(|row| row.decision_id == decision && !row.released)
            .map(|row| row.reservation_id.clone())
    }

    /// `continuation_has_used_successor`.
    pub(crate) fn has_used_successor(
        &self,
        principal: &str,
        admission_id: &str,
        run_id: &str,
        previous: &str,
    ) -> bool {
        self.chains
            .get(&key(principal, admission_id))
            .is_some_and(|chain| chain.used_successors(run_id, previous))
    }

    /// The reservation id of the original's newest used offer.
    pub(crate) fn latest_used_reservation(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> Option<String> {
        self.chains
            .get(&key(principal, admission_id))?
            .latest_used
            .as_ref()
            .map(|(_, reservation)| reservation.clone())
    }

    /// Of the probe-authorization `owners`, the admissions whose probe
    /// `EXPIRED_ADMISSION_PROBES` releases: an admission owner that names an
    /// unstarted `reserved` or `expired` admission past its deadline. Closed
    /// admissions never qualify, so the open rows answer it exactly.
    pub(crate) fn expired_probe_admissions(
        &self,
        owners: Vec<String>,
        now: i64,
    ) -> Vec<(String, String, String)> {
        owners
            .into_iter()
            .filter_map(|owner| {
                let parts: Vec<String> = serde_json::from_str(&owner).ok()?;
                let [kind, principal, admission_id] = parts.as_slice() else {
                    return None;
                };
                if kind != "account_admission"
                    || owner != serde_json::json!([kind, principal, admission_id]).to_string()
                {
                    return None;
                }
                let open = self.open_admission(principal, admission_id)?;
                let expired = matches!(open.row.state.as_str(), "reserved" | "expired")
                    && open.row.started_at.is_none()
                    && open.row.admission_valid_until <= now;
                expired.then(|| (principal.clone(), admission_id.clone(), owner.clone()))
            })
            .collect()
    }
}

fn conversation_of(row: &AccountAffinityRow) -> ConversationKey {
    conversation(&row.principal, &row.conversation_hash, &row.provider)
}

#[cfg(test)]
#[path = "sharded_map_tests.rs"]
mod sharded_map_tests;
