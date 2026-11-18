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
    params.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    let (frame, reply) = route.call("route.account_observe", &params).await;
    assert_eq!(frame, FrameType::Response, "{reply}");
    let reply = &reply["result"]["observation"];
    assert_eq!(reply["bound"], true, "{reply}");
    reply.clone()
}

fn origin(harness: &Harness) -> striatum_store::FenceRecheck {
    harness.store().fence_rechecks().unwrap().into_iter()
        .find(|origin| origin.kind == "observation").unwrap()
}

fn admission_row(harness: &Harness, id: &str) -> striatum_store::AccountAdmissionRow {
    harness.store().account_read(|tx| tx.admission("reserved:colliculus", id)).unwrap().unwrap()
}

async fn backoff(route: &Route, id: &str, at: u64, retry_at: u64) {
    let reply = observe(route, id, "transient_backoff", at, json!({"retry_at_ms":retry_at})).await;
    assert_eq!(reply["effects"][0]["effect"], "transient_backoff", "{reply}");
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
    assert_eq!(reply["result"]["reason"]["code"], "credential_fence_probe", "{reply}");
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
    assert_eq!(origin(&harness).next_recheck_at, next.next_recheck_at);
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
    claim(&route, &harness).await;
    {
        let _clock = crate::InjectedClock::set(START_AT);
        observe(&route, PROBE, "admission_started", START_AT, json!({"run_id":"old-run"})).await;
    }
    let at = PROBE_AT + HOUR;
    let _clock = crate::InjectedClock::set(at);
    assert_probe(admit(&route, "newer-test", None).await);
    let held = origin(&harness);
    assert_eq!(held.last_claimed_at, Some(at as i64));
    finish(&route, at).await;
    assert_refused(admit(&route, "third-test", None).await);
    assert_eq!(origin(&harness).next_recheck_at, held.next_recheck_at);
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
}
