//! Deterministic fixtures from the non-secret live-store column statistics.
//! Correlations not present in those statistics (charge flags, pin matches,
//! observation kinds, and commit-time distribution) are explicit below.

use super::*;
use crate::memory_measurements::{operation, profiler, release};
use striatum_store::{
    AccountAdmissionRow, AccountAffinityRow, AdmissionGroupRows, AdmissionScanRow,
    AdmissionStateRow, LatestAdmissionRow,
};

const ADMISSIONS: usize = 245_763;
const OPEN: usize = 30_981;
const CONVERSATIONS: usize = 124_291;
const OPEN_CONVERSATIONS: usize = 22_586;
const AFFINITIES: usize = 125_873;
const NOW: i64 = 1_800_000_000_000;

fn text(tag: char, id: usize, len: usize) -> String {
    let prefix = format!("{tag}{id:x}");
    assert!(prefix.len() <= len);
    let mut value = String::with_capacity(len);
    value.push_str(&prefix);
    value.extend(std::iter::repeat_n('x', len - prefix.len()));
    value
}

/// Unique identifiers with the requested rounded mean and one maximum-length
/// value; the remainder differ by at most one byte.
fn unique(tag: char, i: usize, n: usize, mean: f64, max: usize) -> String {
    let total = (mean * n as f64).round() as usize;
    let rest = total - max;
    let len = if i == 0 {
        max
    } else {
        rest / (n - 1) + usize::from(i <= rest % (n - 1))
    };
    text(tag, i, len)
}

/// A small repeated vocabulary shared across the open and historical rows.
/// Every vocabulary member occurs, while two endpoint values carry the weight.
fn repeated(
    tag: char,
    i: usize,
    n: usize,
    distinct: usize,
    mean: f64,
    low: usize,
    max: usize,
) -> String {
    let slot = if i < distinct {
        i
    } else {
        let fraction = (mean - low as f64) / (max - low) as f64;
        if (i * 7919 % n) as f64 / (n as f64) < fraction {
            distinct - 1
        } else {
            i % (distinct - 1)
        }
    };
    text(tag, slot, if slot == distinct - 1 { max } else { low })
}

fn json_reply(i: usize, n: usize, mean: f64) -> String {
    let len = unique('j', i, n, mean, 763).len();
    let mut value = serde_json::json!({
        "admission_id": format!("admission-{i}"), "state": "reserved",
        "provider": "anthropic", "model": "claude-opus-4-6", "admitted": true,
        "account_id": "00000000-0000-0000-0000-000000000001", "padding": ""
    });
    let base = value.to_string().len();
    value["padding"] = serde_json::Value::String("x".repeat(len - base));
    let mut value = value.to_string();
    // SQLite returns an owned copy of the text, not the JSON serializer's
    // spare output capacity. Match that loader allocation shape as well.
    value.shrink_to_fit();
    assert_eq!(value.len(), len);
    assert_eq!(value.capacity(), len);
    value
}

fn conversation(i: usize) -> usize {
    if i < OPEN_CONVERSATIONS {
        if i < 2_000 {
            101_622 + i
        } else {
            i - 2_000
        }
    } else if i < 27_900 {
        let j = i - 22_586;
        if j < 535 {
            101_622 + j
        } else {
            (j - 535) % 20_586
        }
    } else if i < 30_947 {
        (i - 27_900) % 488
    } else if i < OPEN {
        i - 30_947
    } else {
        let j = i - OPEN;
        if j < CONVERSATIONS {
            j
        } else if j < CONVERSATIONS + 18_667 {
            j - CONVERSATIONS
        } else {
            let k = j - CONVERSATIONS - 18_667;
            if k < 68_633 {
                101_622 + k % (CONVERSATIONS - 101_622)
            } else {
                (k - 68_633) % 101_622
            }
        }
    }
}

fn conversation_column(
    tag: char,
    open_mean: f64,
    all_mean: f64,
    low: usize,
    max: usize,
    open_distinct: usize,
    all_distinct: usize,
) -> Vec<String> {
    let mut weights = vec![0_usize; CONVERSATIONS];
    let mut open_weights = vec![0_usize; CONVERSATIONS];
    for i in 0..ADMISSIONS {
        weights[conversation(i)] += 1;
    }
    for i in 0..OPEN {
        open_weights[conversation(i)] += 1;
    }
    let mut high_open =
        ((open_mean - low as f64) * OPEN as f64 / (max - low) as f64).round() as i64;
    let mut high_all =
        ((all_mean - low as f64) * ADMISSIONS as f64 / (max - low) as f64).round() as i64;
    let mut high = vec![false; CONVERSATIONS];
    let mut order: Vec<_> = (0..CONVERSATIONS).collect();
    order.sort_by_key(|c| (*c < 101_622, *c));
    for &c in &order {
        if open_weights[c] > 0 && high_open > 0 {
            high[c] = true;
            high_open -= open_weights[c] as i64;
            high_all -= weights[c] as i64;
        }
    }
    for &c in &order {
        if open_weights[c] == 0 && high_all > 0 {
            high[c] = true;
            high_all -= weights[c] as i64;
        }
    }
    (0..CONVERSATIONS)
        .map(|c| {
            let distinct = if open_weights[c] > 0 {
                open_distinct
            } else {
                all_distinct
            };
            let high = high[c];
            text(
                tag,
                if high { 0 } else { 1 + c % (distinct - 1) },
                if high { max } else { low },
            )
        })
        .collect()
}

pub(super) fn rows() -> AdmissionGroupRows {
    let mut rows = AdmissionGroupRows::default();
    let principals: Vec<_> = (0..CONVERSATIONS)
        .map(|c| {
            if c >= 101_622 {
                text('p', 2, 25)
            } else if c == 1 {
                text('p', 1, 15)
            } else {
                text('p', 0, 14)
            }
        })
        .collect();
    let providers = conversation_column('v', 6.6, 7.3, 6, 21, 9, 11);
    let mut old_pin = 0;
    for i in 0..ADMISSIONS {
        let open = i < OPEN;
        let (index, n) = if open {
            (i, OPEN)
        } else {
            (i - OPEN, ADMISSIONS - OPEN)
        };
        let closed_mean = |all: f64, open: f64| {
            (all * ADMISSIONS as f64 - open * OPEN as f64) / (ADMISSIONS - OPEN) as f64
        };
        let c = conversation(i);
        let state = if i < 27_900 {
            "reserved"
        } else if i < 30_947 {
            "started"
        } else if open {
            "expired"
        } else if i + 1 == ADMISSIONS {
            "released"
        } else {
            "finished"
        };
        let admission_id = text(
            'a',
            i,
            unique(
                'a',
                index,
                n,
                if open { 30.4 } else { closed_mean(32.4, 30.4) },
                114,
            )
            .len(),
        );
        let current_pin = if c < 124_762 { c } else { c - 124_762 };
        let matched = if open {
            state == "started"
        } else {
            i - OPEN < 101_622 || (i - OPEN >= CONVERSATIONS && i - OPEN < CONVERSATIONS + 18_667)
        };
        let pin_id = text(
            'n',
            if matched {
                current_pin
            } else if open {
                i % 22_651
            } else {
                let mut pin = 101_622 + old_pin % 23_452;
                old_pin += 1;
                if pin == current_pin {
                    pin = 101_622 + (old_pin % 23_452);
                }
                pin
            },
            28,
        );
        let provider_slot = usize::from_str_radix(&providers[c][1..2], 16).unwrap();
        let account = if provider_slot == 0 {
            if c % 3 == 0 {
                22
            } else {
                (c / 11) % 2
            }
        } else {
            2 * provider_slot + (c / 11) % 2
        };
        let account_id = text('b', account, if account < 15 { 36 } else { 35 });
        let committed_at = NOW - 29 * 86_400_000 + i as i64 * (29 * 86_400_000 / ADMISSIONS as i64);
        let latest = LatestAdmissionRow {
            principal: principals[c].clone(),
            conversation_hash: format!("{c:064x}"),
            provider: providers[c].clone(),
            admission_id: admission_id.clone(),
            committed_at,
            rowid: i as i64 + 1,
            pin_id: pin_id.clone(),
            pin_generation: 1,
            state: state.into(),
            admission_valid_until: committed_at + 60_000,
            lease_valid_until: committed_at + 120_000,
            account_id: account_id.clone(),
        };
        rows.admissions.push(AdmissionScanRow {
            latest: latest.clone(),
            charged: i >= 38,
            request_bytes: 100_000,
            started: state == "started" || state == "finished",
        });
        if open {
            rows.open.push(AdmissionStateRow {
                row: AccountAdmissionRow {
                    principal: latest.principal,
                    admission_id,
                    request_fingerprint: text('f', i % 22_604, 64),
                    request_mode: if i % 2 == 0 { "admit" } else { "renew" }.into(),
                    decision_id: text('d', i, 28),
                    conversation_hash: latest.conversation_hash,
                    provider: latest.provider,
                    model: repeated('m', i, OPEN, 24, 11.2, 10, 29),
                    account_id,
                    credential_id: repeated('c', i, OPEN, 19, 19.9, 17, 38),
                    pin_id,
                    pin_generation: 1,
                    state: state.into(),
                    charged: i >= 38,
                    request_bytes: 100_000,
                    request_bytes_estimated: false,
                    committed_at,
                    admission_valid_until: committed_at + 60_000,
                    lease_valid_until: committed_at + 120_000,
                    reply_json: json_reply(i, OPEN, 635.0),
                    run_id: (state == "started")
                        .then(|| repeated('r', i - 27_900, 3047, 537, 28.3, 28, 30)),
                    started_at: (state == "started").then_some(committed_at + 1),
                    started_late: false,
                    released_at: None,
                    finished_at: None,
                    last_observed_at: (state == "started").then_some(committed_at + 1),
                },
                input_size_class: (i < 14_665).then_some(1),
                model_decision_id: (i < 12_597).then(|| text('z', i % 10_671, 16)),
                rowid: i as i64 + 1,
            });
        }
    }
    for i in 0..AFFINITIES {
        let c = i % 125_868;
        rows.affinities.push(AccountAffinityRow {
            principal: if c < 101_622 {
                principals[c].clone()
            } else if c < 102_766 {
                text('p', 2, 25)
            } else {
                text('p', 0, 14)
            },
            conversation_hash: format!("{c:064x}"),
            provider: if i >= 125_868 {
                text('v', 0, 21)
            } else if c < 101_622 {
                providers[c].clone()
            } else if c < 102_461 {
                text('v', 0, 21)
            } else if i < 125_868 {
                text('v', 1 + i % 10, 6)
            } else {
                text('v', 0, 21)
            },
            affinity_model: repeated('m', i, AFFINITIES, 35, 10.5, 10, 29),
            pin_id: text('n', i % 124_762, 28),
            pin_generation: 1,
            account_id: repeated('b', i, AFFINITIES, 23, 35.8, 35, 36),
            credential_id: repeated('c', i, AFFINITIES, 23, 17.9, 17, 38),
            credential_binding_revision: 1,
            policy_version: 1,
            initial_demand_bytes: 100_000,
            created_at: NOW - 86_400_000,
            last_used_at: NOW - 1000,
            expires_at: NOW + 60_000,
            lease_valid_until: Some(NOW + 120_000),
        });
    }
    for i in 0..7 {
        let original = &rows.open[i].row;
        let offer = striatum_store::ContinuationStateRow {
            rowid: i as i64 + 1,
            principal: original.principal.clone(),
            selection_id: text('s', i, 28),
            run_id: text('r', i, 28),
            admission_id: original.admission_id.clone(),
            previous_decision_id: original.decision_id.clone(),
            decision_id: text('d', ADMISSIONS + i, 28),
            reservation_id: rows.open[10 + i].row.admission_id.clone(),
            valid_until_ms: NOW + 60_000,
            used_at_ms: None,
            released_at_ms: (i >= 3).then_some(NOW - 1000),
            reservation: None,
        };
        if i < 3 {
            rows.offers.push(offer.clone());
        }
        rows.chains.push(offer);
    }
    rows
}

#[test]
#[ignore = "live-shaped heap profile; run alone"]
fn live_shaped_account_heap() {
    let _profiler = profiler("account-state");
    let input = operation("admission_input_rows", rows);
    print_shape(&input);
    let admissions = operation("AdmissionState::from_rows", || {
        AdmissionState::from_rows(input)
    });
    assert_eq!(admissions.open.len(), OPEN);
    assert_eq!(admissions.latest.len(), CONVERSATIONS);
    assert_eq!(admissions.affinity.len(), AFFINITIES);
    assert_eq!(admissions.pin_started.len(), 101_622);
    assert_eq!(admissions.started_by_pin.len(), 488);
    assert_eq!(admissions.affinity_by_pin.len(), 124_762);
    assert_eq!(admissions.demand.len(), 23);
    assert_eq!(
        admissions
            .demand
            .iter()
            .map(|(_, v)| v.len())
            .sum::<usize>(),
        245_724
    );
    assert_eq!(
        admissions.pin_started.iter().map(|(_, v)| v).sum::<u64>(),
        123_336
    );
    println!(
        "COUNTS open={} latest={} affinity={} demand={} pin_started={} started_by_pin={}",
        admissions.open.len(),
        admissions.latest.len(),
        admissions.affinity.len(),
        admissions
            .demand
            .iter()
            .map(|(_, v)| v.len())
            .sum::<usize>(),
        admissions.pin_started.len(),
        admissions.started_by_pin.len()
    );
    let state = AccountState {
        inflight: InflightLedger::from_admissions(&admissions, Vec::new(), NOW),
        admissions,
        ..AccountState::default()
    };
    let fork = operation("AccountState::fork", || state.fork());
    release("AccountState::fork", fork);
    let second = operation("independent_full_admission_load", || {
        AdmissionState::from_rows(rows())
    });
    assert_eq!(state.admissions, second);
    release("independent_full_admission_copy", second);
    string_inventory(&state.admissions);
    let AccountState {
        admissions,
        inflight,
        blocks,
        selection_salt,
        waiters,
        ..
    } = state;
    release("inflight", inflight);
    release("blocks_empty", blocks);
    release("selection_salt_empty", selection_salt);
    release("waiters_empty", waiters);
    let AdmissionState {
        open,
        demand,
        latest,
        affinity,
        affinity_by_pin,
        pin_started,
        started_by_pin,
        offers,
        offer_by_decision,
        chains,
        closing,
        signed_out,
        pending,
    } = admissions;
    release("open", open);
    release("demand", demand);
    release("latest", latest);
    release("affinity", affinity);
    release("affinity_by_pin", affinity_by_pin);
    release("pin_started", pin_started);
    release("started_by_pin", started_by_pin);
    release("offers", offers);
    release("offer_by_decision", offer_by_decision);
    release("chains", chains);
    release("closing", closing);
    release("signed_out", signed_out);
    release("pending", pending);
    release("text_pool", super::shared_text::take_heap_pool());
}

fn string_inventory<'a>(state: &'a AdmissionState) {
    use std::collections::BTreeMap;
    let mut strings: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    let mut bytes = 0_usize;
    let mut count = 0_usize;
    let mut add = |value: &'a str| {
        bytes += value.len();
        count += 1;
        let entry = strings.entry(value).or_default();
        entry.0 = entry.0.max(value.len());
        entry.1 += 1;
    };
    let mut conversation = |key: &'a admissions::ConversationKey| {
        add(&key.0);
        if let Some(text) = key.1.heap_text() {
            add(text);
        }
        add(&key.2);
    };
    for (key, _) in state.latest.iter() {
        conversation(key);
    }
    for (key, _) in state.affinity.iter() {
        conversation(key);
    }
    for (key, _) in state.pin_started.iter() {
        conversation(key);
    }
    for (_, keys) in state.affinity_by_pin.iter() {
        for key in keys.iter() {
            conversation(key);
        }
    }
    drop(conversation);
    for (key, row) in state.open.iter() {
        add(&key.0);
        add(&key.1);
        if let Some(value) = &row.model_decision_id {
            add(value);
        }
        let row = &row.row;
        for value in [
            row.principal.as_str(),
            &row.admission_id,
            &row.request_fingerprint,
            &row.request_mode,
            &row.decision_id,
            &row.provider,
            &row.model,
            &row.account_id,
            &row.credential_id,
            &row.pin_id,
            &row.state,
            &row.reply_json,
        ] {
            add(value);
        }
        if let Some(value) = &row.run_id {
            add(value);
        }
    }
    for (_, row) in state.latest.iter() {
        for value in [
            row.principal.as_str(),
            &row.provider,
            &row.admission_id,
            &row.pin_id,
            &row.state,
            &row.account_id,
        ] {
            add(value);
        }
    }
    for (_, row) in state.affinity.iter() {
        for value in [
            row.principal.as_str(),
            &row.provider,
            &row.affinity_model,
            &row.pin_id,
            &row.account_id,
            &row.credential_id,
        ] {
            add(value);
        }
    }
    for (key, rows) in state.demand.iter() {
        add(&key.0);
        add(&key.1);
        rows.visit_heap_strings(|s| add(s));
    }
    for (pin, _) in state.affinity_by_pin.iter() {
        add(pin);
    }
    for (pin, _) in state.started_by_pin.iter() {
        add(pin);
    }
    drop(add);
    let unique_bytes: usize = strings.values().map(|(bytes, _)| bytes).sum();
    let hash_count: usize = strings
        .iter()
        .filter(|(s, _)| s.starts_with('h') && s.len() == 64)
        .map(|(_, (_, count))| count)
        .sum();
    let replies: usize = state
        .open
        .iter()
        .map(|(_, r)| r.row.reply_json.capacity())
        .sum();
    println!("STRINGS scoped_capacity_bytes={bytes} strings={count} distinct={} distinct_capacity_bytes={unique_bytes} duplicate_capacity_bytes={} duplicate_blocks={} hash_occurrences={hash_count} hash_payload_bytes={} open_reply_capacity_bytes={replies}",strings.len(),bytes-unique_bytes,count-strings.len(),hash_count*64);
}

fn print_column<'a>(name: &str, values: impl Iterator<Item = &'a String>) {
    let mut distinct = std::collections::BTreeSet::new();
    let mut bytes = 0_usize;
    let mut max = 0_usize;
    let mut n = 0_usize;
    for value in values {
        n += 1;
        bytes += value.len();
        max = max.max(value.len());
        distinct.insert(value.as_str());
    }
    println!(
        "SHAPE column={name} count={n} bytes={bytes} mean={:.4} max={max} distinct={}",
        bytes as f64 / n as f64,
        distinct.len()
    );
}

fn print_shape(rows: &AdmissionGroupRows) {
    macro_rules! open {
        ($field:ident) => {
            print_column(
                concat!("open.", stringify!($field)),
                rows.open.iter().map(|r| &r.row.$field),
            );
        };
    }
    macro_rules! all {
        ($field:ident) => {
            print_column(
                concat!("all_scan.", stringify!($field)),
                rows.admissions.iter().map(|r| &r.latest.$field),
            );
        };
    }
    macro_rules! affinity {
        ($field:ident) => {
            print_column(
                concat!("affinity.", stringify!($field)),
                rows.affinities.iter().map(|r| &r.$field),
            );
        };
    }
    open!(principal);
    open!(admission_id);
    open!(provider);
    open!(model);
    open!(account_id);
    open!(credential_id);
    open!(pin_id);
    open!(reply_json);
    all!(principal);
    all!(admission_id);
    all!(conversation_hash);
    all!(provider);
    all!(account_id);
    all!(pin_id);
    affinity!(principal);
    affinity!(provider);
    affinity!(affinity_model);
    affinity!(account_id);
    affinity!(credential_id);
    affinity!(pin_id);
}
