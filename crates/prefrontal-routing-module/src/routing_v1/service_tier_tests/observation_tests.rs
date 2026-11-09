use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use subc_protocol::{Flags, Frame, Priority};
use subc_transport::{authenticate_server, read_frame, write_frame};

use super::*;

enum DescribeReply {
    Answer(Value),
    Plans(Value),
    PlansWithAstro(Value),
    PlansStall,
    OpenRefused,
    DescribeRefused,
    Stall,
}

struct DescribePeer {
    dir: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl DescribePeer {
    async fn start(reply: DescribeReply, credentials: &mut BrocaCredentials) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "routing-tier-describe-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let path = dir.join("connection.json");
        let key = [7_u8; 32];
        let daemon = [0_u8; 16];
        std::fs::write(&path, serde_json::to_vec(&json!({
            "schema":1,"endpoints":[{"host":"127.0.0.1","port":listener.local_addr().unwrap().port()}],
            "key":key.to_vec(),"daemon_id":daemon.to_vec(),"pid":std::process::id(),"daemon_ver":"test"
        })).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        credentials.connection_file_path = Some(path);
        let task = tokio::spawn(async move {
            if let DescribeReply::PlansWithAstro(prices) = &reply {
                parallel_economics_peer(listener, key, daemon, prices.clone()).await;
                return;
            }
            let (mut stream, _) = listener.accept().await.unwrap();
            authenticate_server(&mut stream, &key, &daemon, "test", Duration::from_secs(5))
                .await
                .unwrap();
            let open = read_frame(&mut stream).await.unwrap().unwrap();
            let body: Value = serde_json::from_slice(&open.body).unwrap();
            assert_eq!(body["op"], "route.open");
            let target = match &reply {
                DescribeReply::Plans(_) | DescribeReply::PlansStall => {
                    json!({"kind":"tool_provider", "module_id":"fusiform"})
                }
                _ => json!({"kind":"management_surface", "module_id":"broca"}),
            };
            assert_eq!(body["target"], target);
            if matches!(reply, DescribeReply::OpenRefused) {
                respond(
                    &mut stream,
                    &open,
                    FrameType::Error,
                    json!({
                        "code":"target_unavailable","message":"[TODO(user): runner draining]"
                    }),
                )
                .await;
                return;
            }
            respond(
                &mut stream,
                &open,
                FrameType::Response,
                json!({
                    "op":"route.open","route_channel":7,"route_epoch":3
                }),
            )
            .await;
            let request = read_frame(&mut stream).await.unwrap().unwrap();
            assert_eq!(request.header.channel, 7);
            let expected = match &reply {
                DescribeReply::Plans(_) | DescribeReply::PlansStall => {
                    json!({"name":"plan.prices", "arguments":{}})
                }
                _ => json!({"method":"role.describe", "params":{}}),
            };
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap(),
                expected
            );
            match reply {
                DescribeReply::Plans(answer) => {
                    // Keep the load in flight while both admissions reach it.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    respond(
                        &mut stream,
                        &request,
                        FrameType::Response,
                        json!({"result":answer}),
                    )
                    .await;
                    assert!(
                        tokio::time::timeout(Duration::from_millis(100), listener.accept())
                            .await
                            .is_err(),
                        "concurrent admissions opened duplicate plan-price connections"
                    );
                }
                DescribeReply::Answer(answer) => {
                    respond(
                        &mut stream,
                        &request,
                        FrameType::Response,
                        json!({"result":answer}),
                    )
                    .await;
                }
                DescribeReply::DescribeRefused => {
                    respond(
                        &mut stream,
                        &request,
                        FrameType::Error,
                        json!({
                            "code":"unavailable","message":"[TODO(user): describe unavailable]"
                        }),
                    )
                    .await;
                }
                DescribeReply::Stall | DescribeReply::PlansStall => {
                    let _ = read_frame(&mut stream).await;
                }
                DescribeReply::OpenRefused | DescribeReply::PlansWithAstro(_) => unreachable!(),
            }
        });
        Self {
            dir,
            task: Some(task),
        }
    }

    async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}

impl Drop for DescribePeer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn respond(stream: &mut tokio::net::TcpStream, request: &Frame, ty: FrameType, body: Value) {
    let frame = Frame::build(
        ty,
        Flags::new(false, Priority::Interactive, false),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
}

async fn parallel_economics_peer(
    listener: tokio::net::TcpListener,
    key: [u8; 32],
    daemon: [u8; 16],
    prices: Value,
) {
    let plans_queried = Arc::new(tokio::sync::Notify::new());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let (mut stream, _) = listener.accept().await.unwrap();
        let plans_queried = plans_queried.clone();
        let prices = prices.clone();
        tasks.spawn(async move {
            authenticate_server(&mut stream, &key, &daemon, "test", Duration::from_secs(5))
                .await
                .unwrap();
            let open = read_frame(&mut stream).await.unwrap().unwrap();
            let body: Value = serde_json::from_slice(&open.body).unwrap();
            assert_eq!(body["op"], "route.open");
            respond(
                &mut stream,
                &open,
                FrameType::Response,
                json!({"route_channel":7,"route_epoch":3}),
            )
            .await;
            let request = read_frame(&mut stream).await.unwrap().unwrap();
            let query: Value = serde_json::from_slice(&request.body).unwrap();
            let result = match body["target"]["module_id"].as_str().unwrap() {
                "fusiform" => {
                    assert_eq!(query, json!({"name":"plan.prices","arguments":{}}));
                    plans_queried.notify_one();
                    prices
                }
                "astrocyte" => {
                    assert_eq!(query["method"], "spend.multiplier.report");
                    // A serial Astro-first refresh cannot get past this reply.
                    plans_queried.notified().await;
                    Value::Null
                }
                other => panic!("unexpected economics target: {other}"),
            };
            respond(
                &mut stream,
                &request,
                FrameType::Response,
                json!({"result":result}),
            )
            .await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

fn live_describe() -> Value {
    let reply: Value = serde_json::from_slice(include_bytes!(
        "../../../fixtures/contracts/broca-service-tiers-v1/role-describe-live.json"
    ))
    .unwrap();
    reply["result"].clone()
}

fn assert_candidate_detail(reply: &Value, request: &Value, reason: &str) {
    let detail = &reply["detail"];
    assert_eq!(detail["code"], "account_selection_unavailable", "{reply}");
    assert_eq!(detail["decision_id"], request["decision_id"], "{reply}");
    assert_eq!(
        detail["exclusions"],
        json!([
            {"account_id":"account-0","reason":reason},
            {"account_id":"account-1","reason":reason},
            {"account_id":"account-2","reason":reason},
        ]),
        "{reply}"
    );
}

async fn bound_request(harness: &Arc<Harness>, id: &str, legacy: bool) -> Value {
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select_sol("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = legacy_broca_request(id);
    if !legacy {
        request.as_object_mut().unwrap().remove("caller");
    }
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    request["service_tier"] = json!("fast");
    request["hold_wait_max_s"] = json!(0);
    request["candidates"]["accounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "account_id":"account-2", "credential_id":"chatgpt:2"
        }));
    request
}

#[tokio::test]
async fn empty_view_fast_admission_describes_synchronously_after_routing_restart() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        for bound in [false, true] {
            let (harness, mut credentials) = legacy_broca_setup();
            let mut request = bound_request(&harness, "cold-fast", legacy).await;
            if !bound {
                request.as_object_mut().unwrap().remove("decision_id");
            }
            credentials.runner_service_tiers.clear();
            credentials.runner_routed_accounts.clear();
            credentials.runner_describe_observed.clear();
            let peer =
                DescribePeer::start(DescribeReply::Answer(live_describe()), &mut credentials).await;
            harness.set_credentials(credentials);
            let runner = harness.bind(reserved("broca"), None).await;
            let admitted = runner.call("route.account_for", &request).await;
            assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
            assert_eq!(admitted.1["result"]["service_tier_chosen"], "fast");
            assert!(admitted.1["result"]["account"]["credential_id"]
                .as_str()
                .unwrap()
                .starts_with("chatgpt:"));
            peer.finish().await;
            assert_eq!(runner.call("route.account_for", &request).await, admitted);
        }
    }
}

#[tokio::test]
async fn unknown_view_does_not_block_standard_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        for bound in [false, true] {
            let (harness, mut credentials) = legacy_broca_setup();
            let mut request = legacy_broca_request("unknown-optional-tier");
            if !legacy {
                request.as_object_mut().unwrap().remove("caller");
            }
            if bound {
                let owner = harness.bind(reserved("tier-owner"), None).await;
                let selected = owner.call("route.select", &select_sol("standard")).await;
                assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
                request["decision_id"] = selected.1["result"]["decision_id"].clone();
            }
            request["service_tier"] = json!("standard");
            credentials.runner_service_tiers.clear();
            credentials.runner_describe_observed.clear();
            harness.set_credentials(credentials);
            let runner = harness.bind(reserved("broca"), None).await;
            let admitted = runner.call("route.account_for", &request).await;
            assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
            assert_eq!(admitted.1["result"]["service_tier_chosen"], "standard");
        }
    }
}

#[tokio::test]
async fn failed_describe_fast_admission_is_timed_transient_without_tier_downgrade() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        for bound in [false, true] {
            for failure in [
                DescribeReply::OpenRefused,
                DescribeReply::DescribeRefused,
                DescribeReply::Stall,
            ] {
                let (harness, mut credentials) = legacy_broca_setup();
                let mut request = bound_request(&harness, "unknown-fast", legacy).await;
                if !bound {
                    request.as_object_mut().unwrap().remove("decision_id");
                }
                credentials.runner_service_tiers.clear();
                credentials.runner_describe_observed.clear();
                let peer = DescribePeer::start(failure, &mut credentials).await;
                harness.set_credentials(credentials);
                let runner = harness.bind(reserved("broca"), None).await;
                let refused = tokio::time::timeout(
                    Duration::from_secs(3),
                    runner.call("route.account_for", &request),
                )
                .await
                .unwrap();
                assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
                assert_eq!(refused.1["code"], "account_selection_unavailable");
                assert_eq!(
                    refused.1["detail"]["retry_after_ms"], 3_000,
                    "{}",
                    refused.1
                );
                assert_eq!(refused.1["detail"]["retry_after_s"], 3);
                assert_candidate_detail(
                    &refused.1,
                    &request,
                    "[TODO(user): runner tiers not yet read after routing started]",
                );
                assert!(!refused.1["message"]
                    .as_str()
                    .unwrap()
                    .contains("No account can serve"));
                assert!(harness
                    .store()
                    .account_decisions("serve", 10)
                    .unwrap()
                    .is_empty());
                assert!(harness
                    .store()
                    .v1_admission("reserved:broca", "unknown-fast")
                    .unwrap()
                    .is_none());
                peer.finish().await;
            }
        }
    }
}

#[tokio::test]
async fn startup_account_tier_inputs_are_retryable_until_loaded() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        for bound in [true, false] {
            for (input, reason) in [
                (
                    "plans",
                    "[TODO(user): plan prices not yet read after routing started]",
                ),
                (
                    "catalog",
                    "[TODO(user): model tiers not yet read after routing started]",
                ),
                (
                    "listing",
                    "[TODO(user): credential list not yet read after routing started]",
                ),
            ] {
                let (harness, mut credentials) = legacy_broca_setup();
                let mut request = bound_request(&harness, "cold-account-inputs", legacy).await;
                if !bound {
                    request.as_object_mut().unwrap().remove("decision_id");
                }
                let loaded = credentials.clone();
                match input {
                    "plans" => credentials.plan_prices = Value::Null,
                    "catalog" => credentials.billing_catalog = Default::default(),
                    "listing" => {
                        credentials = BrocaCredentials::default();
                        credentials.runner_declarations = loaded.runner_declarations.clone();
                        credentials.runner_service_tiers = loaded.runner_service_tiers.clone();
                        credentials.billing_catalog = loaded.billing_catalog.clone();
                        credentials.plan_prices = loaded.plan_prices.clone();
                    }
                    _ => unreachable!(),
                }
                harness.set_credentials(credentials);
                let runner = harness.bind(reserved("broca"), None).await;
                let refused = runner.call("route.account_for", &request).await;
                assert_eq!(refused.0, FrameType::Error, "{input}: {}", refused.1);
                assert_eq!(refused.1["code"], "account_selection_unavailable");
                assert_eq!(
                    refused.1["detail"]["retry_after_s"].as_u64(),
                    Some(3),
                    "{input}: {}",
                    refused.1
                );
                assert_candidate_detail(&refused.1, &request, reason);
                assert!(harness
                    .store()
                    .account_decisions("serve", 10)
                    .unwrap()
                    .is_empty());
                harness.set_credentials(loaded);
                let admitted = runner.call("route.account_for", &request).await;
                assert_eq!(admitted.0, FrameType::Response, "{input}: {}", admitted.1);
                assert_eq!(admitted.1["result"]["service_tier_chosen"], "fast");
            }
        }
    }
}

#[tokio::test]
async fn draining_runner_failed_fresh_describe_open_and_catalog_gap_keep_fast_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = legacy_broca_setup();
    let policies = serde_json::from_value::<Vec<crate::config_policy::RunnerPolicy>>(
        json!([{"module":"broca","via_vault":true}]),
    )
    .unwrap();
    let mut cache = runners::DescribeCache::default();
    cache.catalog(
        &json!([{"module_id":"broca","nonce_verified":true}]),
        &policies,
        &mut credentials,
        NOW as i64,
    );
    cache.reply(
        "broca",
        Some(&live_describe()),
        &mut credentials,
        NOW as i64,
    );
    harness.set_credentials(credentials.clone());
    let request = bound_request(&harness, "draining-fast", true).await;
    let peer = DescribePeer::start(DescribeReply::OpenRefused, &mut credentials).await;
    let path = credentials.connection_file_path.as_ref().unwrap();
    let consumer = subc_client_rs::SubcConsumer::connect(path, Default::default())
        .await
        .unwrap();
    let reply = runners::fetch_describe(&consumer, path, "broca").await;
    assert!(
        reply.is_none(),
        "a failed route open is not a successful describe"
    );
    cache.reply("broca", reply.as_ref(), &mut credentials, NOW as i64 + 1);
    let absent = cache.catalog(&json!([]), &policies, &mut credentials, NOW as i64 + 2);
    assert!(absent.is_empty());
    harness.set_credentials(credentials.clone());
    let runner = harness.bind(reserved("broca"), None).await;
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_eq!(admitted.1["result"]["service_tier_chosen"], "fast");
    peer.finish().await;
    assert_eq!(credentials.runner_describe_observed["broca"], NOW as i64);
    cache.missed_all(&mut credentials, NOW as i64 + 300_000);
    assert!(!credentials.runner_service_tiers.contains_key("broca"));
    assert!(!credentials.runner_routed_accounts.contains_key("broca"));
}

#[tokio::test]
async fn current_positive_withdrawal_refuses_bound_fast_without_a_retry_or_step_down() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (harness, mut credentials) = legacy_broca_setup();
        let request = bound_request(&harness, "withdrawn-fast", legacy).await;
        runners::DescribeCache::default().reply(
            "broca",
            Some(&json!({"capabilities":[]})),
            &mut credentials,
            NOW as i64,
        );
        harness.set_credentials(credentials);
        let runner = harness.bind(reserved("broca"), None).await;
        let refused = runner.call("route.account_for", &request).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_selection_unavailable");
        assert!(refused.1["detail"]["retry_after_s"].is_null());
        assert_candidate_detail(&refused.1, &request, "bound_service_tier_unavailable");
        assert!(
            refused.1["detail"].get("retry_after_ms").is_none(),
            "{}",
            refused.1
        );
        assert!(harness
            .store()
            .account_decisions("serve", 10)
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn concurrent_bound_fast_admissions_share_the_on_demand_account_input_load() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (harness, mut credentials) = legacy_broca_setup();
        let first = bound_request(&harness, "loading-first", legacy).await;
        let mut second = first.clone();
        second["admission_id"] = json!("loading-second");
        let prices = credentials.plan_prices.clone();
        credentials.plan_prices = Value::Null;
        let reply = DescribeReply::Plans(prices);
        // The peer accepts only one connection; a duplicate load cannot finish.
        let peer = DescribePeer::start(reply, &mut credentials).await;
        harness.set_credentials(credentials);
        let runner = harness.bind(reserved("broca"), None).await;
        let other = harness.bind(reserved("broca"), None).await;
        let (first, second) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                runner.call("route.account_for", &first),
                other.call("route.account_for", &second)
            )
        })
        .await
        .unwrap();
        for admitted in [first, second] {
            assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
            assert_eq!(admitted.1["result"]["service_tier_chosen"], "fast");
        }
        peer.finish().await;
    }
}

#[tokio::test]
async fn stalled_on_demand_plan_prices_refuse_with_integer_retry_seconds() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (harness, mut credentials) = legacy_broca_setup();
        let request = bound_request(&harness, "stalled-plans", legacy).await;
        credentials.plan_prices = Value::Null;
        let peer = DescribePeer::start(DescribeReply::PlansStall, &mut credentials).await;
        harness.set_credentials(credentials);
        let runner = harness.bind(reserved("broca"), None).await;
        let refused = tokio::time::timeout(
            Duration::from_secs(3),
            runner.call("route.account_for", &request),
        )
        .await
        .unwrap();
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "account_selection_unavailable");
        assert_eq!(refused.1["detail"]["retry_after_s"].as_u64(), Some(3));
        assert_candidate_detail(
            &refused.1,
            &request,
            "[TODO(user): plan prices not yet read after routing started]",
        );
        peer.finish().await;
    }
}

#[tokio::test]
async fn startup_plan_prices_do_not_wait_for_the_astro_spend_report() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, mut credentials) = legacy_broca_setup();
    let prices = credentials.plan_prices.clone();
    credentials.plan_prices = Value::Null;
    let peer = DescribePeer::start(
        DescribeReply::PlansWithAstro(prices.clone()),
        &mut credentials,
    )
    .await;
    let path = credentials.connection_file_path.clone().unwrap();
    let credentials = Arc::new(std::sync::Mutex::new(credentials));
    let quota = Arc::new(std::sync::Mutex::new(
        crate::account_quota::AccountQuotaState::default(),
    ));
    tokio::time::timeout(
        Duration::from_secs(1),
        crate::subscription_economics::refresh(
            &path,
            path.parent().unwrap(),
            &quota,
            None,
            &credentials,
        ),
    )
    .await
    .unwrap();
    assert_eq!(credentials.lock().unwrap().plan_prices, prices);
    peer.finish().await;
}
