//! Enforced concurrency caps on continue mode. A continuation offer reserves a
//! row on the replacement account, so it is checked against that account's cap
//! like any request that adds a hold, in the writer command that makes it.
//! Every scenario runs under an injected clock.

use super::*;
use crate::account_state::InflightScope;

const T: u64 = 1_790_683_200_000;
const A: &str = "account-1";
const B: &str = "account-2";
const RESERVED_CEILING_MS: u64 = 30_000;

/// The continue fixture's credentials for A and B only, so B is the one
/// replacement a run leaving A can be offered.
fn a_and_b() -> Vec<cortexkit_role_routing_conformance::ContinueCredential> {
    fixture::continue_credentials()
        .into_iter()
        .filter(|credential| [A, B].contains(&credential.account.account_id.as_str()))
        .collect()
}

fn credential_on(account: &str) -> String {
    a_and_b()
        .into_iter()
        .find(|credential| credential.account.account_id == account)
        .unwrap()
        .account
        .credential_id
}

/// `n` runs, each under its own selection, admitted and started on A, then A
/// and B listed. Returns a continue request per run that leaves A at the
/// current clock. The runs' admissions name no candidates, because a
/// continuation inherits the original request and would otherwise be kept off
/// B.
async fn runs_on_a(n: usize, scope: Option<&str>) -> (KitSubject, Route, Route, Vec<Value>) {
    let subject = KitSubject {
        observe_blocks: Mutex::new(false),
        media: Mutex::new(Default::default()),
        harness: Mutex::new(Harness::new("continue")),
    };
    subject
        .configure_consumers(&fixture::consumers())
        .await
        .unwrap();
    let mut catalog = fixture::continue_catalog();
    catalog.accounts.retain(|account| account.account_id == A);
    subject.configure_catalog(&catalog).await.unwrap();
    let harness = subject.harness();
    let consumer = harness.bind(reserved(fixture::CONSUMER), scope).await;
    let mut runner = None;
    let mut runs = Vec::new();
    for i in 0..n {
        let selected = result(consumer.call("route.select", &json!({"consumer":fixture::SUMMARY,
            "pin":fixture::HOLD_FIRST,"requirements":{"iq":50},"task_ref":format!("continued-task-{i}")})).await);
        if runner.is_none() {
            runner = Some(
                harness
                    .bind(
                        reserved(selected["runner"]["module"].as_str().unwrap()),
                        scope,
                    )
                    .await,
            );
        }
        let runner = runner.as_ref().unwrap();
        let admitted = result(runner.call("route.account_for", &json!({"provider":fixture::SUBSCRIPTION_PROVIDER,
            "model":"model-c","decision_id":selected["decision_id"],"admission_id":format!("admission-{i}"),
            "conversation_key":format!("conversation-{i}"),"initial_input_bytes":65536})).await);
        assert_eq!(admitted["account"]["account_id"], A, "{admitted}");
        result(runner.call("route.account_observe", &json!({"event_id":format!("started-{i}"),
            "kind":"admission_started","admission_id":format!("admission-{i}"),"run_id":format!("run-{i}")})).await);
        runs.push(json!({"mode":"continue","run_id":format!("run-{i}"),"admission_id":format!("admission-{i}"),
            "conversation_key":format!("conversation-{i}"),"previous_decision_id":selected["decision_id"],
            "selection_id":format!("selection-{i}"),
            "exhausted":{"credential_id":admitted["account"]["credential_id"],"account":A,
                "limit_window":"five_hour","observed_at_ms":account_selection::now_ms(),
                "resets_at_ms":account_selection::now_ms()+300_000}}));
    }
    subject
        .configure_continue_credentials(&a_and_b())
        .await
        .unwrap();
    (subject, consumer, runner.unwrap(), runs)
}

fn caps(harness: &Harness, limit: u64, enforced: bool) {
    harness.edit_config(|config| {
        config["concurrency"] = json!({"enforcement_enabled":enforced,
            "providers":{fixture::SUBSCRIPTION_PROVIDER:{"limit":limit}}});
    });
}

/// `n` new conversations placed on `account` alone, started when `started`.
async fn fill(runner: &Route, account: &str, n: usize, started: bool) {
    for i in 0..n {
        let id = format!("fill-{account}-{i}");
        let body = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
            "admission_id":id,"conversation_key":id,
            "candidates":{"view_digest":format!("{account}-only"),"accounts":[{
                "account_id":account,"credential_id":credential_on(account)}]}});
        let admitted = result(runner.call("route.account_for", &body).await);
        assert_eq!(admitted["account"]["account_id"], account, "{admitted}");
        if started {
            result(
                runner
                    .call(
                        "route.account_observe",
                        &json!({"event_id":format!("{id}-started"),
                "kind":"admission_started","admission_id":id,"run_id":format!("{id}-run")}),
                    )
                    .await,
            );
        }
    }
}

fn count(harness: &Harness, account: &str) -> usize {
    crate::account_state::snapshot(harness.store())
        .unwrap()
        .inflight
        .count(
            fixture::SUBSCRIPTION_PROVIDER,
            account,
            InflightScope::Account,
            account_selection::now_ms(),
        )
}

/// The concurrency lines logged since the last call: would-refuse lines with
/// enforcement off, refusal lines (marked `refused`) with it on.
fn pressure() -> Vec<Value> {
    crate::take_operator_infos()
        .into_iter()
        .filter_map(|line| {
            line.strip_prefix("routing account concurrency would refuse ")
                .map(|body| serde_json::from_str(body).unwrap())
        })
        .collect()
}

fn query(harness: &Harness, sql: &str) -> Vec<Vec<rusqlite::types::Value>> {
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let mut statement = conn.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns).map(|column| row.get(column)).collect()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// What a concurrency refusal must leave alone: admission rows, timed holds,
/// the conversation's affinity, and every fence of the replacement account.
fn untouched_by_refusal(harness: &Harness) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    let b = credential_on(B);
    [
        "SELECT * FROM account_admission ORDER BY principal, admission_id".to_owned(),
        "SELECT * FROM account_hold".into(),
        "SELECT * FROM account_affinity ORDER BY principal, conversation_hash, provider".into(),
        "SELECT * FROM account_pin ORDER BY pin_id".into(),
        "SELECT * FROM routing_v1_admission ORDER BY 1, 2".into(),
        format!("SELECT * FROM account_fence WHERE account_id = '{B}'"),
        format!("SELECT * FROM account_route_fence WHERE credential_id = '{b}'"),
        format!("SELECT * FROM account_non_quota_route_fence WHERE credential_id = '{b}'"),
        format!("SELECT * FROM fence_recheck WHERE account_id = '{B}' OR credential_id = '{b}'"),
    ]
    .iter()
    .map(|sql| query(harness, sql))
    .collect()
}

/// The exhaustion evidence a continue request records for the account it
/// leaves, without generated ids so two stores can be compared.
fn exhaustion_evidence(harness: &Harness) -> Vec<Vec<rusqlite::types::Value>> {
    let mut rows: Vec<_> = [
        "SELECT 'account_fence', provider, account_id, scope_json, fence_json, source,
            observed_at, created_at, cleared_at FROM account_fence",
        "SELECT 'route_fence', provider, credential_id, model, until_ms, recorded_at,
            last_reported_at, wall_class FROM account_route_fence",
        "SELECT 'non_quota_route_fence', provider, credential_id, model, until_ms,
            recorded_at, last_reported_at, wall_kind FROM account_non_quota_route_fence",
        "SELECT 'recheck', kind, provider, account_id, credential_id, model, evidence_at,
            quota, next_recheck_at, reset_check_at, cleared_at FROM fence_recheck",
    ]
    .into_iter()
    .flat_map(|sql| query(harness, sql))
    .collect();
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

fn decisions(harness: &Harness) -> usize {
    harness
        .store()
        .account_decisions("serve", 1_000)
        .unwrap()
        .len()
}

fn assert_concurrency_hold(answer: &Value, until_ms: u64) {
    assert_eq!(answer["kind"], "hold", "{answer}");
    assert_eq!(answer["routing_code"], "concurrency_full", "{answer}");
    assert_eq!(answer["until_ms"], until_ms, "{answer}");
}

#[tokio::test]
async fn enforced_continue_gives_one_free_slot_to_one_run_and_holds_the_others() {
    let _clock = crate::InjectedClock::set(T);
    let (subject, _, runner, runs) = runs_on_a(3, None).await;
    let harness = subject.harness();
    caps(&harness, 3, true);
    fill(&runner, B, 2, true).await;
    assert_eq!((count(&harness, A), count(&harness, B)), (3, 2));
    pressure();
    let mut offers = 0;
    let mut holds = 0;
    for run in &runs {
        let answer = result(runner.call("route.account_for", run).await);
        match answer["kind"].as_str() {
            Some("offer") => {
                assert_eq!(answer["account"], B, "{answer}");
                offers += 1;
            }
            // B's other holds are runs just started, and the offer's
            // reservation outlives the ceiling, so no slot is known to free
            // sooner than the ceiling.
            Some("hold") => {
                assert_concurrency_hold(&answer, T + RESERVED_CEILING_MS);
                holds += 1;
            }
            _ => panic!("{answer}"),
        }
        // The offer's reservation holds the slot from the command that makes
        // it, so a later run finds B full.
        assert!(count(&harness, B) <= 3, "B exceeded its cap after {run}");
    }
    assert_eq!((offers, holds), (1, 2));
    assert_eq!(count(&harness, B), 3);
    let lines = pressure();
    assert_eq!(lines.len(), 2, "{lines:?}");
    for line in &lines {
        assert_eq!(line["refused"], true, "{line}");
        // The account the runs are leaving is never one of the counts judged.
        assert_eq!(
            line["accounts"],
            json!([{"account_id":B,"count":3,"cap":3}]),
            "{line}"
        );
    }
}

#[tokio::test]
async fn enforced_continue_refusal_keeps_exhaustion_evidence_and_replays_without_allocating() {
    let mut evidence = Vec::new();
    for enforced in [false, true] {
        // B's six holds are runs started half a minute before the request.
        let setup = crate::InjectedClock::set(T - 30_000);
        let (subject, _, runner, runs) = runs_on_a(1, None).await;
        let harness = subject.harness();
        caps(&harness, 6, enforced);
        fill(&runner, B, 6, true).await;
        drop(setup);
        let at_t = crate::InjectedClock::set(T);
        let mut request = runs[0].clone();
        request["exhausted"]["observed_at_ms"] = json!(T);
        request["exhausted"]["resets_at_ms"] = json!(T + 300_000);
        let before = untouched_by_refusal(&harness);
        let decisions_before = decisions(&harness);
        pressure();
        let answer = result(runner.call("route.account_for", &request).await);
        evidence.push(exhaustion_evidence(&harness));
        if !enforced {
            // With enforcement off the request is answered an offer over the
            // cap and logs only the would-refuse line.
            assert_eq!(answer["kind"], "offer", "{answer}");
            assert_eq!(answer["account"], B, "{answer}");
            assert_eq!(count(&harness, B), 7);
            let lines = pressure();
            assert_eq!(lines.len(), 1, "{lines:?}");
            assert!(lines[0].get("refused").is_none(), "{}", lines[0]);
            continue;
        }
        assert_concurrency_hold(&answer, T + RESERVED_CEILING_MS);
        assert_eq!(count(&harness, B), 6);
        assert_eq!(
            untouched_by_refusal(&harness),
            before,
            "a concurrency refusal adds no admission, hold, fence or affinity change"
        );
        assert_eq!(decisions(&harness), decisions_before + 1);
        let refusal = &harness.store().account_decisions("serve", 1).unwrap()[0];
        assert_eq!(
            (refusal.outcome.as_str(), refusal.code.as_str()),
            ("refuse", "concurrency_full")
        );
        // The hold's until_ms is the allocator's reset_at_ms: B is the only
        // compatible replacement, so no other wake time is earlier.
        let explained: Value = serde_json::from_str(&refusal.decision_json).unwrap();
        assert_eq!(explained["outcome"]["reset_at_ms"], answer["until_ms"]);
        let lines = pressure();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0]["refused"], true, "{}", lines[0]);

        // The stored answer replays under its selection_id after its until_ms
        // has passed: no allocation, no decision row, no refusal line.
        drop(at_t);
        let later = T + 40_000;
        let _later = crate::InjectedClock::set(later);
        let replayed = result(runner.call("route.account_for", &request).await);
        assert_eq!(replayed, answer);
        assert!(pressure().is_empty());
        assert_eq!(decisions(&harness), decisions_before + 1);

        // A new selection_id asks again and is refused like any request.
        request["selection_id"] = json!("selection-again");
        let again = result(runner.call("route.account_for", &request).await);
        assert_concurrency_hold(&again, later + RESERVED_CEILING_MS);
        assert_eq!(decisions(&harness), decisions_before + 2);
        let lines = pressure();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0]["refused"], true, "{}", lines[0]);
        assert_eq!(count(&harness, B), 6);
    }
    // A's exhaustion evidence is written the same whether or not B had room,
    // and none of it names B.
    assert!(!evidence[1].is_empty());
    let b = [B.to_owned(), credential_on(B)].map(rusqlite::types::Value::Text);
    assert!(
        evidence[1]
            .iter()
            .flatten()
            .all(|column| !b.contains(column)),
        "{:?}",
        evidence[1]
    );
    assert_eq!(evidence[0], evidence[1]);
}

#[tokio::test]
async fn enforced_continue_from_a_full_account_takes_room_on_the_replacement() {
    let _clock = crate::InjectedClock::set(T);
    let (subject, _, runner, runs) = runs_on_a(1, None).await;
    let harness = subject.harness();
    caps(&harness, 6, true);
    fill(&runner, A, 5, true).await;
    assert_eq!((count(&harness, A), count(&harness, B)), (6, 0));
    let offer = result(runner.call("route.account_for", &runs[0]).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_eq!(offer["account"], B, "{offer}");
    // While the offer is live its run counts on both accounts.
    assert_eq!((count(&harness, A), count(&harness, B)), (6, 1));
}

#[tokio::test]
async fn enforced_head_call_continue_is_offered_a_full_replacement_over_its_cap() {
    for head in [false, true] {
        let _clock = crate::InjectedClock::set(T);
        // A route with a head scope gives the continue request the head-call
        // fact, regardless of which authorization path allowed the runner.
        let (subject, consumer, runner, runs) = runs_on_a(1, head.then_some("head")).await;
        let harness = subject.harness();
        caps(&harness, 6, true);
        fill(&runner, B, 6, true).await;
        assert_eq!(count(&harness, B), 6);
        pressure();
        let answer = result(runner.call("route.account_for", &runs[0]).await);
        if !head {
            assert_concurrency_hold(&answer, T + RESERVED_CEILING_MS);
            assert_eq!(count(&harness, B), 6);
            assert_eq!(pressure().len(), 1);
            continue;
        }
        assert_eq!(answer["kind"], "offer", "{answer}");
        assert_eq!(answer["account"], B, "{answer}");
        assert_eq!(
            count(&harness, B),
            7,
            "the offer's reservation is the overshoot"
        );
        assert!(pressure().is_empty());
        // Using the offer moves the run's hold to B: B's count stays, A's falls.
        result(
            consumer
                .call(
                    "route.report",
                    &json!({"decision_id":answer["decision_id"],
            "outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}}),
                )
                .await,
        );
        assert_eq!((count(&harness, A), count(&harness, B)), (0, 7));
    }
}
