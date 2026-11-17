//! A run's account hits its weekly limit and the runner asks to continue on
//! another account. On the live store that continue held the account writer
//! for 3.2 s: it looked up the run's sign-in exclusions in
//! `account_observation`, whose only usable index is by principal, so the
//! lookup read every observation of the continuing principal (about 266,000
//! rows). The exclusions now live in the account state.
//!
//! The ignored test measures one block observation and one continue on a
//! store as large as the live one, timing every statement the writer runs.
//! The other test proves on the statement trace that a continue reads no
//! observation rows.

use super::*;

/// The live store's sizes on 16 Nov: admissions, retained conversations
/// (each with one pin), observations, and the observations of the principal
/// that sends continues.
const LIVE_ADMISSIONS: i64 = 240_483;
const LIVE_AFFINITIES: i64 = 126_440;
const LIVE_OBSERVATIONS: i64 = 454_077;
const LIVE_PRINCIPAL_OBSERVATIONS: i64 = 266_253;

fn env_or(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Copy the paused run's own admission, pin and affinity into a closed
/// history of `admissions` admissions over `affinities` idle conversations of
/// the same principal, provider, model and account, and add `observations`
/// bound observations of about 510 bytes each, `own` of them from the run's
/// principal, as the live store holds.
fn seed_history(
    store: &striatum_store::SqliteRouteStateStore,
    admissions: i64,
    affinities: i64,
    observations: i64,
    own: i64,
) {
    store
        .with_writer_for_test(|conn| {
            conn.execute_batch("PRAGMA foreign_keys=OFF")?;
            conn.execute(
                "INSERT INTO account_pin (pin_id, principal, conversation_hash, provider,
                   pin_generation, affinity_model, account_id, credential_id, pin_mode,
                   previous_pin_id, decision_id, created_at)
                 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?1-1)
                 SELECT 'history-pin-'||n.i, p.principal, 'history-conversation-'||n.i,
                   p.provider, 1, p.affinity_model, p.account_id, p.credential_id, p.pin_mode,
                   NULL, 'history-decision-'||n.i, p.created_at-n.i*1000
                 FROM n, account_pin p
                 WHERE p.pin_id=(SELECT pin_id FROM account_admission WHERE admission_id='admission')",
                [affinities],
            )?;
            conn.execute(
                "INSERT INTO account_affinity (principal, conversation_hash, provider,
                   affinity_model, pin_id, pin_generation, account_id, credential_id,
                   credential_binding_revision, policy_version, initial_demand_bytes, created_at,
                   last_used_at, expires_at, lease_valid_until)
                 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?1-1)
                 SELECT f.principal, 'history-conversation-'||n.i, f.provider, f.affinity_model,
                   'history-pin-'||n.i, 1, f.account_id, f.credential_id,
                   f.credential_binding_revision, f.policy_version, f.initial_demand_bytes,
                   f.created_at-n.i*1000, f.last_used_at-n.i*1000, f.expires_at, NULL
                 FROM n, account_affinity f
                 WHERE f.pin_id=(SELECT pin_id FROM account_admission WHERE admission_id='admission')",
                [affinities],
            )?;
            conn.execute(
                "INSERT INTO account_admission (principal, admission_id, request_fingerprint,
                   request_mode, decision_id, conversation_hash, provider, model, account_id,
                   credential_id, pin_id, pin_generation, state, charged, request_bytes,
                   request_bytes_estimated, committed_at, admission_valid_until,
                   lease_valid_until, reply_json, run_id, started_at, started_late, released_at,
                   finished_at, input_size_class, model_decision_id, last_observed_at)
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?1)
                 SELECT a.principal, 'history-'||n.i, a.request_fingerprint, a.request_mode,
                   'history-decision-'||n.i, 'history-conversation-'||(n.i%?2), a.provider,
                   a.model, a.account_id, a.credential_id, 'history-pin-'||(n.i%?2), 1,
                   'finished', a.charged, a.request_bytes, a.request_bytes_estimated,
                   a.committed_at-n.i*1000, a.admission_valid_until-n.i*1000,
                   a.lease_valid_until-n.i*1000, a.reply_json, 'history-run-'||n.i,
                   a.committed_at-n.i*1000, 0, NULL, a.committed_at-n.i*1000+500,
                   a.input_size_class, 'history-model-decision-'||n.i,
                   a.committed_at-n.i*1000+500
                 FROM n, account_admission a WHERE a.admission_id='admission'",
                [admissions, affinities],
            )?;
            conn.execute(
                "INSERT INTO account_observation (observation_id, principal, runner, event_id,
                   kind, admission_id, bound, observation_json, effects_json, received_at)
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?1)
                 SELECT 'history-observation-'||n.i,
                   CASE WHEN n.i<=?2 THEN a.principal ELSE 'reserved:history-other' END,
                   'runner', 'history-event-'||n.i,
                   CASE n.i%6 WHEN 0 THEN 'admission_started' WHEN 1 THEN 'run_finished'
                     ELSE 'attempt' END,
                   'history-'||(n.i%?3+1), 1,
                   json_object('event_id','history-event-'||n.i,'run_id','history-run-'||n.i,
                     'pad',hex(randomblob(200))),
                   '[]', a.committed_at-n.i*1000
                 FROM n, account_admission a WHERE a.admission_id='admission'",
                [observations, own, admissions],
            )?;
            conn.execute_batch("PRAGMA foreign_keys=ON")
        })
        .unwrap();
    store.reload_account_state_for_test().unwrap();
}

/// Print every distinct statement in `runs` whose query plan scans a table
/// instead of searching an index: its cost grows with that table.
fn print_scans(store: &striatum_store::SqliteRouteStateStore, runs: &[(String, u64)]) {
    let distinct: std::collections::BTreeSet<&str> =
        runs.iter().map(|(sql, _)| sql.as_str()).collect();
    for sql in distinct {
        let scans: Vec<_> = plan(store, sql)
            .into_iter()
            .filter(|step| step.starts_with("SCAN") && !step.contains("CONSTANT ROW"))
            .collect();
        if !scans.is_empty() {
            let text: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
            eprintln!("  scan {scans:?}: {}", &text[..text.len().min(300)]);
        }
    }
}

/// SQLite's query plan for `sql` with every parameter unbound; empty for a
/// statement that has none (transaction control, pragmas).
fn plan(store: &striatum_store::SqliteRouteStateStore, sql: &str) -> Vec<String> {
    let trimmed = sql.trim_start();
    if [
        "BEGIN",
        "COMMIT",
        "SAVEPOINT",
        "RELEASE",
        "ROLLBACK",
        "PRAGMA",
    ]
    .iter()
    .any(|word| trimmed.starts_with(word))
    {
        return Vec::new();
    }
    store
        .with_writer_for_test(|conn| {
            let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
            let unbound = vec![rusqlite::types::Value::Null; plan.parameter_count()];
            let steps = plan
                .query_map(rusqlite::params_from_iter(unbound), |row| {
                    row.get::<_, String>(3)
                })?
                .collect::<rusqlite::Result<Vec<_>>>();
            steps
        })
        .unwrap()
}

/// Print the statements of one request, slowest first, with the writer's
/// hold time for it.
fn report(label: &str, runs: &[(String, u64)], held_ms: f64) {
    let total: u64 = runs.iter().map(|(_, ns)| ns).sum();
    let mut by_text: std::collections::BTreeMap<&str, (u64, usize)> = Default::default();
    for (sql, ns) in runs {
        let entry = by_text.entry(sql).or_default();
        entry.0 += ns;
        entry.1 += 1;
    }
    let mut slowest: Vec<_> = by_text.into_iter().collect();
    slowest.sort_by(|a, b| b.1 .0.cmp(&a.1 .0));
    eprintln!(
        "{label}: writer held {held_ms:.1} ms; {} statements, {:.1} ms in SQLite",
        runs.len(),
        total as f64 / 1e6
    );
    for (sql, (ns, count)) in slowest.iter().take(5) {
        let text: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        eprintln!(
            "  {:>9.1} ms x{count}: {}",
            *ns as f64 / 1e6,
            &text[..text.len().min(240)]
        );
    }
}

// Pin the complete keyed effect read. A prefix exemption could also accept a
// statement that scans observation history after losing its id predicate.
const OBSERVATION_EFFECT_READ: &str = "SELECT json_extract(e.value,'$.provider'),json_extract(e.value,'$.account_id'),MIN(json_extract(e.value,'$.retry_at_ms'),o.received_at+3600000),
                o.received_at,json_array(json_array(o.observation_id,e.key) || '',e.value || '')
            FROM account_observation o,json_each(CASE WHEN json_valid(?2) THEN ?2 ELSE '[]' END) e
            WHERE o.observation_id=?1 AND o.kind='transient_backoff' AND o.bound=1 AND e.type='object'
                AND json_extract(e.value,'$.effect')='transient_backoff'
                AND json_type(e.value,'$.provider')='text' AND json_type(e.value,'$.account_id')='text'
                AND json_type(e.value,'$.retry_at_ms') IN ('integer','real')";

fn keyed_effect_read(sql: &str) -> bool {
    sql == OBSERVATION_EFFECT_READ
}

/// Statements selecting observation rows, except the complete keyed effect
/// lookup that the continuation's observation write needs.
fn observation_reads(runs: &[(String, u64)]) -> Vec<String> {
    runs.iter()
        .map(|(sql, _)| sql)
        .filter(|sql| {
            striatum_store::group_reads::reads_tables(sql, &["account_observation"])
                && !keyed_effect_read(sql)
        })
        .cloned()
        .collect()
}

fn writer_held_ms(store: &striatum_store::SqliteRouteStateStore, before: u64) -> f64 {
    (store.writer_hold_ns_for_test() - before) as f64 / 1e6
}

/// Call `method` and return its result with the writer hold it caused.
async fn timed(
    store: &striatum_store::SqliteRouteStateStore,
    route: &Route,
    method: &str,
    params: Value,
) -> (Value, f64) {
    let before = store.writer_hold_ns_for_test();
    let reply = route.call(method, &params).await;
    (reply.1["result"].clone(), writer_held_ms(store, before))
}

#[tokio::test]
#[ignore = "live-sized writer-hold measurement; run in release with --ignored --nocapture"]
async fn live_sized_block_and_continue_writer_hold() {
    let (subject, consumer, runner, mut request) = paused().await;
    let store = subject.harness().store().clone();
    seed_history(
        &store,
        env_or("STRIATUM_CONTINUE_ADMISSIONS", LIVE_ADMISSIONS),
        env_or("STRIATUM_CONTINUE_AFFINITIES", LIVE_AFFINITIES),
        env_or("STRIATUM_CONTINUE_OBSERVATIONS", LIVE_OBSERVATIONS),
        env_or(
            "STRIATUM_CONTINUE_PRINCIPAL_OBSERVATIONS",
            LIVE_PRINCIPAL_OBSERVATIONS,
        ),
    );
    let profile = store.profile_writer_statements_for_test().unwrap();
    let loads = store.account_state_loads();
    let now = account_selection::now_ms();
    request["exhausted"]["limit_window"] = json!("seven_day");
    request["exhausted"]["resets_at_ms"] = json!(now + 7 * 86_400_000);
    profile.take();
    let held = store.writer_hold_ns_for_test();
    result(
        runner
            .call(
                "route.account_observe",
                &json!({"event_id":"weekly-limit","kind":"account_limit_exhausted",
                    "admission_id":"admission","run_id":"run",
                    "served_account_id":request["exhausted"]["account"],
                    "credential_id":request["exhausted"]["credential_id"],
                    "limit_window":"seven_day",
                    "resets_at_ms":now + 7 * 86_400_000,"observed_at_ms":now}),
            )
            .await,
    );
    let held = writer_held_ms(&store, held);
    let runs = profile.take();
    report("block observation", &runs, held);
    print_scans(&store, &runs);
    // Drop the plan statements `print_scans` ran on the writer.
    profile.take();
    let held = store.writer_hold_ns_for_test();
    let offer = result(runner.call("route.account_for", &request).await);
    let held = writer_held_ms(&store, held);
    assert_eq!(offer["kind"], "offer", "{offer}");
    let runs = profile.take();
    report("continue", &runs, held);
    print_scans(&store, &runs);
    eprintln!(
        "continue observation reads: {:#?}",
        observation_reads(&runs)
    );
    // A mix of the requests routing serves all day: selections, admissions,
    // run lifecycle observations and reports. A full state load during it
    // would hold the writer for seconds; count them and the longest hold.
    let mut held = Vec::new();
    for i in 0..20 {
        let (selected, ms) = timed(
            &store,
            &consumer,
            "route.select",
            json!({"consumer":fixture::SUMMARY,"pin":fixture::HOLD_FIRST,
                "requirements":{"iq":50},"task_ref":format!("mix-task-{i}")}),
        )
        .await;
        held.push(ms);
        let id = format!("mix-{i}");
        let (admitted, ms) = timed(
            &store,
            &runner,
            "route.account_for",
            json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                "decision_id":selected["decision_id"],"admission_id":id,
                "conversation_key":id,"initial_input_bytes":4096}),
        )
        .await;
        held.push(ms);
        for (kind, event) in [
            ("admission_started", "started"),
            ("attempt", "attempt"),
            ("run_finished", "finished"),
        ] {
            let (_, ms) = timed(
                &store,
                &runner,
                "route.account_observe",
                json!({"event_id":format!("{id}-{event}"),"kind":kind,"admission_id":id,
                    "run_id":format!("{id}-run"),
                    "served_account_id":admitted["account"]["account_id"],
                    "credential_id":admitted["account"]["credential_id"],
                    "attempt_outcome":{"status":"served"}}),
            )
            .await;
            held.push(ms);
        }
        let (_, ms) = timed(
            &store,
            &consumer,
            "route.report",
            json!({"decision_id":selected["decision_id"],"outcome":"completed",
                "usage":{"input_tokens":100,"output_tokens":20}}),
        )
        .await;
        held.push(ms);
    }
    let (calls, longest) = (held.len(), held.iter().copied().fold(0.0_f64, f64::max));
    eprintln!(
        "after the block, the continue and {calls} mixed calls: {} state loads since the \
         fixture loaded, longest writer hold of a mixed call {longest:.1} ms",
        store.account_state_loads() - loads
    );
}

/// A continue answers its run's sign-in exclusions from the account state,
/// for a quota limit and for a sign-in loss alike. No statement either one
/// runs on the writer selects observation rows, apart from the
/// observation-effects write, whose plan is a primary-key search. The state
/// equals a fresh load after each step.
#[tokio::test]
async fn a_continue_reads_no_observation_rows() {
    let (subject, _consumer, runner, request) = paused().await;
    let store = subject.harness().store().clone();
    let profile = store.profile_writer_statements_for_test().unwrap();
    let loads = store.account_state_loads();
    profile.take();
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    let quota = profile.take();
    crate::account_state::assert_published_equals_fresh_load(&store, "after a quota continue");
    // Drop the load statements the comparison ran on the writer.
    profile.take();
    // The run pauses again on the offered account, now for a sign-in loss.
    let mut next = request.clone();
    next["selection_id"] = json!("signed-out-pause");
    next["previous_decision_id"] = offer["decision_id"].clone();
    next["exhausted"] = json!({"credential_id":offer["credential_id"],"account":offer["account"],
        "limit_window":"signed_out","observed_at_ms":account_selection::now_ms()});
    let answer = result(runner.call("route.account_for", &next).await);
    assert!(
        matches!(answer["kind"].as_str(), Some("offer" | "hold")),
        "{answer}"
    );
    let signed_out = profile.take();
    crate::account_state::assert_published_equals_fresh_load(&store, "after a sign-in continue");
    assert_eq!(
        store.account_state_loads(),
        loads,
        "neither continue may reload the account state"
    );
    let mut effect_reads = 0;
    for (label, runs) in [
        ("quota continue", &quota),
        ("sign-in continue", &signed_out),
    ] {
        assert!(
            !runs.is_empty(),
            "{label}: the trace saw no writer statements"
        );
        let reads = observation_reads(runs);
        assert!(
            reads.is_empty(),
            "{label} read account_observation: {reads:#?}"
        );
        for (sql, _) in runs.iter().filter(|(sql, _)| keyed_effect_read(sql)) {
            effect_reads += 1;
            let steps = plan(&store, sql);
            assert!(
                steps
                    .iter()
                    .any(|step| step.starts_with("SEARCH o USING INDEX")
                        && step.contains("(observation_id=?)")),
                "{label}: the observation-effects write must find its row by key: {steps:?}"
            );
        }
    }
    assert!(
        effect_reads > 0,
        "the continue mix must execute the keyed observation-effect read"
    );
}

#[tokio::test]
async fn observation_effect_read_allowance_rejects_the_same_select_without_its_id_predicate() {
    let (subject, _, _, _) = paused().await;
    let keyed = OBSERVATION_EFFECT_READ.to_string();
    let id_predicate = "o.observation_id=?1 AND ";
    assert_eq!(keyed.matches(id_predicate).count(), 1);
    let scan = keyed.replacen(id_predicate, "", 1);
    assert!(observation_reads(&[(keyed.clone(), 1)]).is_empty());
    assert_eq!(observation_reads(&[(scan.clone(), 1)]), vec![scan.clone()]);
    let indexed = plan(subject.harness().store(), &keyed);
    assert!(
        indexed
            .iter()
            .any(|step| step.starts_with("SEARCH o USING INDEX")
                && step.contains("(observation_id=?)")),
        "{indexed:?}"
    );
    let unkeyed = plan(subject.harness().store(), &scan);
    assert!(
        unkeyed.iter().any(|step| step.starts_with("SCAN o")),
        "{unkeyed:?}"
    );
}
