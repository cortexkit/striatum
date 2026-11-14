//! Waiter bookkeeping and the matching that decides whether a free slot is
//! needed by the waiters ranked ahead of a request.

use std::collections::BTreeMap;

use cortexkit_role_routing::declarations::ConsumerPriority::{self, *};

use super::waiters::{matched, CapCount, Waiter, Waiters, WAIT_GRACE_MS};

const T: i64 = 1_000_000;

fn count(account: &str) -> CapCount {
    CapCount {
        provider: "p".into(),
        account: account.into(),
        model: None,
    }
}

fn conversation(name: &str) -> (String, String) {
    ("principal".into(), name.into())
}

fn waiter(counts: &[&str]) -> Waiter {
    Waiter {
        class: Worker,
        order: 0,
        retry_at_ms: T,
        counts: counts.iter().map(|account| count(account)).collect(),
    }
}

fn orders(waiters: &[&Waiter]) -> Vec<u64> {
    let mut orders: Vec<_> = waiters.iter().map(|waiter| waiter.order).collect();
    orders.sort_unstable();
    orders
}

#[test]
fn matching_counts_waiters_that_can_hold_distinct_slots_together() {
    let both = waiter(&["a", "b"]);
    let only_a = waiter(&["a"]);
    let free = BTreeMap::from([(count("a"), 1), (count("b"), 1)]);
    // First fit would give `both` the slot on A and strand `only_a`.
    assert_eq!(matched(&[&both, &only_a], &free), 2);
    let free = BTreeMap::from([(count("a"), 1), (count("b"), 0)]);
    assert_eq!(matched(&[&both, &only_a], &free), 1);
    // A count with room for many still gives each waiter one slot.
    let free = BTreeMap::from([(count("a"), 5)]);
    assert_eq!(matched(&[&only_a, &only_a.clone()], &free), 2);
    assert_eq!(matched(&[], &free), 0);
}

#[test]
fn ahead_ranks_by_class_then_order_and_never_counts_its_own_conversation() {
    let mut waiters = Waiters::default();
    let record = |waiters: &mut Waiters, name: &str, class: ConsumerPriority| {
        waiters.record(conversation(name), class, T, vec![count("a")], T)
    };
    record(&mut waiters, "first-worker", Worker);
    record(&mut waiters, "blocking", Blocking);
    record(&mut waiters, "second-worker", Worker);
    record(&mut waiters, "background", Background);
    // A request with no waiter of its own ranks behind every waiter of its class.
    assert_eq!(
        orders(&waiters.ahead(&conversation("new"), Worker, T)),
        [0, 1, 2]
    );
    // A waiter keeps its place among its class.
    assert_eq!(
        orders(&waiters.ahead(&conversation("second-worker"), Worker, T)),
        [0, 1]
    );
    assert_eq!(
        orders(&waiters.ahead(&conversation("first-worker"), Worker, T)),
        [1]
    );
    // Its own waiter is never ahead, whatever class that waiter recorded.
    assert!(waiters
        .ahead(&conversation("blocking"), Background, T)
        .iter()
        .all(|waiter| waiter.class != Blocking));
    assert_eq!(
        orders(&waiters.ahead(&conversation("new"), Interactive, T)),
        Vec::<u64>::new()
    );
}

#[test]
fn a_later_refusal_keeps_the_order_and_replaces_the_rest_until_the_wait_ends() {
    let mut waiters = Waiters::default();
    waiters.record(conversation("x"), Blocking, T, vec![count("a")], T);
    waiters.record(conversation("y"), Worker, T, vec![count("a")], T);
    waiters.record(
        conversation("x"),
        Worker,
        T + 5_000,
        vec![count("b")],
        T + 1,
    );
    let x = waiters.get(&conversation("x"), T + 1).unwrap().clone();
    assert_eq!(
        x,
        Waiter {
            class: Worker,
            order: 0,
            retry_at_ms: T + 5_000,
            counts: vec![count("b")],
        }
    );
    // A slot on a count it does not wait on leaves it waiting.
    waiters.took(&conversation("x"), &count("a"));
    assert!(waiters.get(&conversation("x"), T + 1).is_some());
    waiters.took(&conversation("x"), &count("b"));
    assert!(waiters.get(&conversation("x"), T + 1).is_none());
    // A wait ends 30 s after its retry time; a refusal after that is a new
    // waiter with a new order.
    let y_end = T + WAIT_GRACE_MS;
    assert!(waiters.get(&conversation("y"), y_end - 1).is_some());
    assert!(waiters.get(&conversation("y"), y_end).is_none());
    waiters.record(
        conversation("y"),
        Worker,
        y_end + 1,
        vec![count("a")],
        y_end,
    );
    assert_eq!(waiters.get(&conversation("y"), y_end).unwrap().order, 2);
    // Retiming moves the end of a live wait and revives no ended one.
    waiters.retime(&conversation("y"), y_end + 10, y_end);
    assert_eq!(
        waiters.get(&conversation("y"), y_end).unwrap().ends_at(),
        y_end + 10 + WAIT_GRACE_MS
    );
    waiters.retime(&conversation("x"), y_end + 10, y_end);
    assert!(waiters.get(&conversation("x"), y_end).is_none());
}
