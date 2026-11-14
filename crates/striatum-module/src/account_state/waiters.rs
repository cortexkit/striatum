//! Conversations refused `concurrency_full` at `account_for`. A free slot is
//! held back from a lower-ranked request while a higher-ranked caller that was
//! refused is still expected back, so the caller told to retry is not beaten
//! to the slot by one that merely arrived first.
//!
//! Waiters live in the account state only. They are never written to SQL: a
//! restart forgets them, and callers simply ask again at their retry time.
//! Like the rest of the state they change only inside account-writer
//! commands, so a rolled-back command leaves none and a batch's waiters are
//! published only when its transaction commits.

use std::collections::BTreeMap;
use std::sync::Arc;

use cortexkit_role_routing::declarations::ConsumerPriority;

/// How long a waiter outlives the retry time its refusal gave. A caller that
/// has not come back by then is assumed gone, and the slot it was kept for is
/// open to everyone again.
pub(crate) const WAIT_GRACE_MS: i64 = 30_000;

/// One cap count a waiter could take a slot on.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CapCount {
    pub(crate) provider: String,
    pub(crate) account: String,
    /// The model of a per-model count; `None` for the account's shared count.
    pub(crate) model: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Waiter {
    pub(crate) class: ConsumerPriority,
    /// Advanced by the writer for each new waiter, so two refusals in one
    /// millisecond keep the order of their commands.
    pub(crate) order: u64,
    /// When the refusal told the caller to come back.
    pub(crate) retry_at_ms: i64,
    /// The counts of the latest refusal: the slots this caller could take.
    pub(crate) counts: Vec<CapCount>,
}

impl Waiter {
    pub(crate) fn ends_at(&self) -> i64 {
        self.retry_at_ms.saturating_add(WAIT_GRACE_MS)
    }

    pub(crate) fn live(&self, now: i64) -> bool {
        now < self.ends_at()
    }
}

/// A conversation: principal and conversation hash.
pub(crate) type Conversation = (String, String);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Waiters {
    next_order: u64,
    /// Behind an `Arc` so forking the state for each writer command does not
    /// copy the map.
    by_conversation: Arc<BTreeMap<Conversation, Waiter>>,
}

/// Higher classes are served first.
pub(crate) fn rank(class: ConsumerPriority) -> u8 {
    match class {
        ConsumerPriority::Interactive => 3,
        ConsumerPriority::Blocking => 2,
        ConsumerPriority::Worker => 1,
        ConsumerPriority::Background => 0,
    }
}

impl Waiters {
    /// The conversation's waiter, while it lasts.
    pub(crate) fn get(&self, conversation: &Conversation, now: i64) -> Option<&Waiter> {
        self.by_conversation
            .get(conversation)
            .filter(|waiter| waiter.live(now))
    }

    pub(crate) fn live(&self, now: i64) -> impl Iterator<Item = (&Conversation, &Waiter)> {
        self.by_conversation
            .iter()
            .filter(move |(_, waiter)| waiter.live(now))
    }

    /// The live waiters ranked ahead of a request of `class` from
    /// `conversation`: other conversations of a higher class, and those of
    /// its class with an earlier order. Without a waiter of its own the
    /// request ranks behind every waiter of its class, and its own waiter is
    /// never ahead of it, whatever class that waiter recorded.
    pub(crate) fn ahead(
        &self,
        conversation: &Conversation,
        class: ConsumerPriority,
        now: i64,
    ) -> Vec<&Waiter> {
        let own = self
            .get(conversation, now)
            .map_or(u64::MAX, |waiter| waiter.order);
        let own_rank = rank(class);
        self.live(now)
            .filter(|(other, waiter)| {
                *other != conversation
                    && (rank(waiter.class) > own_rank
                        || (rank(waiter.class) == own_rank && waiter.order < own))
            })
            .map(|(_, waiter)| waiter)
            .collect()
    }

    /// Record a computed refusal. A conversation that is still waiting keeps
    /// its order and takes the class, counts and retry time of this refusal.
    pub(crate) fn record(
        &mut self,
        conversation: Conversation,
        class: ConsumerPriority,
        retry_at_ms: i64,
        counts: Vec<CapCount>,
        now: i64,
    ) {
        let order = match self.get(&conversation, now) {
            Some(waiter) => waiter.order,
            None => {
                let order = self.next_order;
                self.next_order += 1;
                order
            }
        };
        let map = Arc::make_mut(&mut self.by_conversation);
        // Ended waiters are dropped here, so the map holds only conversations
        // refused in roughly the last minute.
        map.retain(|_, waiter| waiter.live(now));
        map.insert(
            conversation,
            Waiter {
                class,
                order,
                retry_at_ms,
                counts,
            },
        );
    }

    /// A continue `hold` tells the caller to come back at its `until_ms`,
    /// which may be earlier than the refusal's own reset.
    pub(crate) fn retime(&mut self, conversation: &Conversation, retry_at_ms: i64, now: i64) {
        if self.get(conversation, now).is_none() {
            return;
        }
        if let Some(waiter) = Arc::make_mut(&mut self.by_conversation).get_mut(conversation) {
            waiter.retry_at_ms = retry_at_ms;
        }
    }

    /// The conversation took a slot on `count`. That ends its wait only when
    /// it was waiting on that count.
    pub(crate) fn took(&mut self, conversation: &Conversation, count: &CapCount) {
        if self
            .by_conversation
            .get(conversation)
            .is_some_and(|waiter| waiter.counts.contains(count))
        {
            Arc::make_mut(&mut self.by_conversation).remove(conversation);
        }
    }
}

/// The most of `waiters` that can each hold a distinct free slot on one of
/// their counts at once, given `free` slots per count. This is a maximum
/// matching, not a first-fit hand-out: one waiter's choice never strands
/// another that has fewer counts to choose from.
pub(crate) fn matched(waiters: &[&Waiter], free: &BTreeMap<CapCount, usize>) -> usize {
    // A count never needs more units than there are waiters.
    let units: Vec<&CapCount> = free
        .iter()
        .flat_map(|(count, n)| std::iter::repeat_n(count, (*n).min(waiters.len())))
        .collect();
    let mut holder: Vec<Option<usize>> = vec![None; units.len()];
    let mut size = 0;
    for waiter in 0..waiters.len() {
        let mut seen = vec![false; units.len()];
        if augment(waiter, waiters, &units, &mut holder, &mut seen) {
            size += 1;
        }
    }
    size
}

fn augment(
    waiter: usize,
    waiters: &[&Waiter],
    units: &[&CapCount],
    holder: &mut [Option<usize>],
    seen: &mut [bool],
) -> bool {
    for unit in 0..units.len() {
        if seen[unit] || !waiters[waiter].counts.contains(units[unit]) {
            continue;
        }
        seen[unit] = true;
        if holder[unit].is_none_or(|other| augment(other, waiters, units, holder, seen)) {
            holder[unit] = Some(waiter);
            return true;
        }
    }
    false
}
