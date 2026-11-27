use super::*;

const BACKOFF_AT: u64 = 1_794_961_270_112;
const RETRY_AT: u64 = 1_794_961_290_112;
const PROBE_AT: u64 = 1_794_961_292_240;
const START_AT: u64 = 1_794_961_292_299;
const FINISH_AT: u64 = 1_794_961_295_341;
const HOUR: u64 = 3_600_000;
const KEY: &str = "apikey:google";
const FIRST: &str = "aa568941-reproduction/1";
const PROBE: &str = "83a3362e-reproduction/1";
const MODEL: &str = "gemini-4-hydrogen";

fn google() -> Arc<Harness> {
    let mut declaration = engine(MODEL, false);
    declaration["provider"] = json!("google");
    let harness = setup(vec![declaration]);
    let mut fact = crate::tests::fact("google", MODEL);
    fact.auth_methods = Some(vec!["apikey".into()]);
    let facts = [fact];
    harness.install_facts(&facts);
    super::super::account_classes_tests::catalog_fact(&harness, &facts[0]);
    let mut credential = key(KEY);
    credential.provider_ids = vec!["google".into()];
    credential.serves = vec!["google".into()];
    let mut credentials = media_credentials(vec![credential]);
    credentials.refresh_serving_methods(&Default::default(), &facts, BACKOFF_AT as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials);
    harness
}

async fn admit(route: &Route, id: &str, decision: Option<&Value>) -> (FrameType, Value) {
    let mut params = json!({
        "caller":{"runner":"colliculus"}, "provider":"google", "model":MODEL,
        "admission_id":id, "conversation_key":id, "hold_wait_max_s":0
    });
    if let Some(decision) = decision {
        params["decision_id"] = decision.clone();
    }
    route.call("route.account_for", &params).await
}

async fn observe(route: &Route, id: &str, kind: &str, at: u64, extra: Value) -> Value {
    let mut params = json!({"admission_id":id,"caller":{"runner":"colliculus"},
        "event_id":format!("{id}/{kind}/{at}"),"kind":kind,"observed_at_ms":at});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (frame, reply) = route.call("route.account_observe", &params).await;
    assert_eq!(frame, FrameType::Response, "{reply}");
    let reply = &reply["result"]["observation"];
    assert_eq!(reply["bound"], true, "{reply}");
    reply.clone()
}

fn origin(harness: &Harness) -> striatum_store::FenceRecheck {
    harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|origin| origin.kind == "non_quota_route")
        .unwrap()
}

fn admission_row(harness: &Harness, id: &str) -> striatum_store::AccountAdmissionRow {
    harness
        .store()
        .account_read(|tx| tx.admission("reserved:colliculus", id))
        .unwrap()
        .unwrap()
}

async fn backoff(route: &Route, id: &str, at: u64, retry_at: u64) {
    let reply = observe(
        route,
        id,
        "account_limit_exhausted",
        at,
        json!({"retry_at_ms":retry_at, "limit_window":"credit"}),
    )
    .await;
    assert_eq!(reply["effects"][0]["effect"], "fence", "{reply}");
}

async fn claim(route: &Route, harness: &Harness) -> striatum_store::FenceRecheck {
    let first = admit(route, FIRST, None).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(first.1["result"]["account"]["credential_id"], KEY);
    backoff(route, FIRST, BACKOFF_AT, RETRY_AT).await;
    let _clock = crate::InjectedClock::set(PROBE_AT);
    assert_probe(admit(route, PROBE, None).await);
    let block = origin(harness);
    assert_eq!(block.last_claimed_at, Some(PROBE_AT as i64));
    assert_eq!(block.next_recheck_at, (PROBE_AT + HOUR) as i64);
    block
}

fn assert_probe((frame, reply): (FrameType, Value)) {
    assert_eq!(frame, FrameType::Response, "{reply}");
    assert_eq!(
        reply["result"]["reason"]["code"], "credential_fence_probe",
        "{reply}"
    );
}

async fn finish(route: &Route, at: u64) -> Value {
    observe(route, PROBE, "run_finished", at, json!({})).await
}

#[tokio::test]
async fn colliculus_bare_finish_hands_back_one_recovery_slot() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    let original = claim(&route, &harness).await;
    {
        let _clock = crate::InjectedClock::set(START_AT);
        observe(&route, PROBE, "admission_started", START_AT, json!({})).await;
    }
    let _clock = crate::InjectedClock::set(FINISH_AT);
    let answer = finish(&route, FINISH_AT).await;
    assert_eq!(answer["effects"][0]["effect"], "admission_finished");
    assert_eq!(answer["effects"][1]["effect"], "lease_ended");
    let row = admission_row(&harness, PROBE);
    assert_eq!(row.state, "finished");
    assert_eq!(row.run_id, None);
    assert_eq!(row.finished_at, Some(FINISH_AT as i64));
    assert_probe(admit(&route, "next-test", None).await);
    let next = origin(&harness);
    assert_eq!(next.evidence, original.evidence);
    assert_eq!(next.last_claimed_at, Some(FINISH_AT as i64));
    assert_eq!(next.next_recheck_at, (FINISH_AT + HOUR) as i64);

    let duplicate = finish(&route, FINISH_AT).await;
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["id"], answer["id"]);
    assert_refused(admit(&route, "second-next-test", None).await);
    let after_replay = origin(&harness);
    assert_eq!(after_replay.evidence, next.evidence);
    assert_eq!(after_replay.last_claimed_at, next.last_claimed_at);
    assert_eq!(after_replay.next_recheck_at, next.next_recheck_at);
}

fn assert_refused((frame, reply): (FrameType, Value)) {
    assert_eq!(frame, FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "account_pool_exhausted", "{reply}");
}

#[tokio::test]
async fn colliculus_old_finish_leaves_newer_claim_held() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    let original = claim(&route, &harness).await;
    {
        let _clock = crate::InjectedClock::set(START_AT);
        observe(
            &route,
            PROBE,
            "admission_started",
            START_AT,
            json!({"run_id":"old-run"}),
        )
        .await;
    }
    let at = PROBE_AT + HOUR;
    let _clock = crate::InjectedClock::set(at);
    assert_probe(admit(&route, "newer-test", None).await);
    let held = origin(&harness);
    assert_eq!(held.last_claimed_at, Some(at as i64));
    assert_eq!(held.evidence, original.evidence);
    finish(&route, at).await;
    assert_refused(admit(&route, "third-test", None).await);
    assert_eq!(origin(&harness).next_recheck_at, held.next_recheck_at);

    observe(&route, "newer-test", "run_finished", at, json!({})).await;
    assert_probe(admit(&route, "after-newer-finish", None).await);
}

#[tokio::test]
async fn colliculus_finish_does_not_release_newer_backoff() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    let original = claim(&route, &harness).await;
    let newer_at = START_AT + 500;
    let retry_at = FINISH_AT + 20_000;
    {
        let _clock = crate::InjectedClock::set(newer_at);
        backoff(&route, PROBE, newer_at, retry_at).await;
    }
    let _clock = crate::InjectedClock::set(FINISH_AT);
    finish(&route, FINISH_AT).await;
    assert_refused(admit(&route, "too-early", None).await);
    let block = origin(&harness);
    assert_ne!(block.evidence, original.evidence);
    assert_eq!(block.evidence_at, newer_at as i64);
    assert_eq!(block.next_recheck_at, retry_at as i64);

    let _clock = crate::InjectedClock::set(retry_at);
    assert_probe(admit(&route, "new-evidence-test", None).await);
    observe(
        &route,
        "new-evidence-test",
        "run_finished",
        retry_at,
        json!({}),
    )
    .await;
    assert_probe(admit(&route, "after-new-evidence-finish", None).await);
    assert_eq!(origin(&harness).evidence, block.evidence);
}

#[tokio::test]
async fn colliculus_decision_probe_heals_on_success_report_after_finish() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    let first = admit(&route, FIRST, None).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    backoff(&route, FIRST, BACKOFF_AT, RETRY_AT).await;
    let decision = {
        let _clock = crate::InjectedClock::set(PROBE_AT);
        let decision = selected(&route, &request()).await["decision_id"].clone();
        assert_probe(admit(&route, PROBE, Some(&decision)).await);
        let scope = harness
            .store()
            .account_read(|tx| {
                tx.size_admission_for_model_decision(
                    decision.as_str().unwrap(),
                    "google",
                    KEY,
                    MODEL,
                )
            })
            .unwrap();
        assert_eq!(
            scope.map(|(_, committed_at)| committed_at),
            Some(PROBE_AT as i64)
        );
        decision
    };
    let held = origin(&harness);
    {
        let _clock = crate::InjectedClock::set(START_AT);
        observe(
            &route,
            PROBE,
            "admission_started",
            START_AT,
            json!({"run_id":"reported-run"}),
        )
        .await;
    }
    let _clock = crate::InjectedClock::set(FINISH_AT);
    finish(&route, FINISH_AT).await;
    assert_eq!(admission_row(&harness, PROBE).state, "finished");
    assert_refused(admit(&route, "while-waiting-for-report", None).await);
    assert_eq!(origin(&harness).next_recheck_at, held.next_recheck_at);

    let (frame, reply) = route
        .call(
            "route.report",
            &json!({
                "decision_id":decision, "outcome":"completed", "credential_id":KEY,
                "attempt_outcome":{"status":"served"}, "usage":{"run_id":"reported-run"}
            }),
        )
        .await;
    assert_eq!(frame, FrameType::Response, "{reply}");
    assert_eq!(reply["result"]["outcome"], "completed", "{reply}");
    assert_no_backoff(&harness);
    assert_ordinary(admit(&route, "after-success-report", None).await);
}

fn assert_no_backoff(harness: &Harness) {
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "non_quota_route"));
}

fn assert_ordinary((frame, reply): (FrameType, Value)) {
    assert_eq!(frame, FrameType::Response, "{reply}");
    assert_eq!(reply["result"]["account"]["credential_id"], KEY, "{reply}");
    assert_ne!(
        reply["result"]["reason"]["code"], "credential_fence_probe",
        "{reply}"
    );
}

#[tokio::test]
async fn colliculus_typed_served_attempt_heals_before_finish() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    claim(&route, &harness).await;
    let _clock = crate::InjectedClock::set(FINISH_AT);
    let (frame, reply) = route
        .call(
            "route.account_observe",
            &json!({
                "kind":"attempt", "attempt_outcome":{"status":"served"}, "admission_id":PROBE,
                "served_account_id":KEY, "credential_id":KEY, "caller":{"runner":"colliculus"},
                "event_id":format!("{PROBE}/attempt/{FINISH_AT}")
            }),
        )
        .await;
    assert_eq!(frame, FrameType::Response, "{reply}");
    assert_eq!(
        reply["result"]["observation"]["effects"][0]["effect"], "block_cleared",
        "{reply}"
    );
    finish(&route, FINISH_AT).await;
    assert_no_backoff(&harness);
    assert_ordinary(admit(&route, "after-served-attempt", None).await);
}

#[tokio::test]
async fn colliculus_started_without_run_id_is_not_an_expired_reservation() {
    let _clock = crate::InjectedClock::set(BACKOFF_AT);
    let harness = google();
    let route = harness.bind(reserved("colliculus"), None).await;
    let held = claim(&route, &harness).await;
    {
        let _clock = crate::InjectedClock::set(START_AT);
        let started = observe(&route, PROBE, "admission_started", START_AT, json!({})).await;
        assert_eq!(
            started["effects"][0]["effect"], "admission_started",
            "{started}"
        );
    }
    let row = admission_row(&harness, PROBE);
    assert_eq!(row.state, "started");
    assert_eq!(row.run_id, None);
    assert_eq!(row.started_at, Some(START_AT as i64));
    assert_eq!(row.last_observed_at, row.started_at);
    assert!(!row.started_late);

    let _clock = crate::InjectedClock::set(row.admission_valid_until as u64 + 1);
    let inflight = &crate::account_state::snapshot(harness.store())
        .unwrap()
        .inflight;
    let count = |at| {
        inflight.count(
            "google",
            KEY,
            crate::account_state::InflightScope::Account,
            at,
        )
    };
    let started_deadline = START_AT as i64 + 30 * 60_000;
    assert_eq!(count(row.admission_valid_until), 1);
    assert_eq!(count(started_deadline - 1), 1);
    assert_eq!(count(started_deadline), 0);
    assert_refused(admit(&route, "after-reservation-deadline", None).await);
    assert_eq!(admission_row(&harness, PROBE).state, "started");
    assert_eq!(origin(&harness).next_recheck_at, held.next_recheck_at);
}

#[tokio::test]
async fn colliculus_later_start_does_not_name_an_anonymously_started_run() {
    for finish_first in [false, true] {
        let _clock = crate::InjectedClock::set(BACKOFF_AT);
        let harness = google();
        let route = harness.bind(reserved("colliculus"), None).await;
        claim(&route, &harness).await;
        let first_start = if finish_first {
            let _clock = crate::InjectedClock::set(FINISH_AT);
            finish(&route, FINISH_AT).await;
            FINISH_AT + 1
        } else {
            START_AT
        };
        for (at, extra) in [
            (first_start, json!({})),
            (first_start + 1, json!({"run_id":"later-run"})),
            (first_start + 2, json!({})),
        ] {
            let _clock = crate::InjectedClock::set(at);
            observe(&route, PROBE, "admission_started", at, extra).await;
        }
        let row = admission_row(&harness, PROBE);
        assert_eq!(row.state, if finish_first { "finished" } else { "started" });
        assert_eq!(row.run_id, None);
        assert_eq!(row.started_at, Some(first_start as i64));
        assert_eq!(row.last_observed_at, row.started_at);
    }
}
