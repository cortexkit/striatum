use super::*;

fn modules(mark: Option<bool>) -> Value {
    let mut runner = json!({
        "module_id":fixture::RUNNER_ONE, "nonce_verified":true,
        "roles":[], "control_ops":[],
        "incarnation":"daemon-a", "spawn":7,
        "role_declarations":{"routing/v1":{"runner":{
            "harnesses":["fixture"]
        }}}
    });
    if let Some(mark) = mark {
        runner["first_party"] = json!(mark);
    }
    json!([runner])
}

fn target() -> Value {
    json!({"first_party":true,"kind":"spawn","incarnation":"daemon-a",
        "spawn":7,"authority_epoch":2,"approved_declarations_digest":"sha256:approved"})
}

async fn open_peer(target: Value, settings: bool, should_send: bool) {
    use std::time::Duration;
    use subc_transport::{authenticate_server, read_frame, write_frame};
    let harness = setup("first-party-open");
    if settings {
        harness.edit_config(|config| config["runners"] = json!([{"module":fixture::RUNNER_ONE}]));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let path = harness.dir.join("connection.json");
    let key = [7_u8; 32];
    let daemon = [0_u8; 16];
    std::fs::write(&path, serde_json::to_vec(&json!({
        "schema":1,"endpoints":[{"host":"127.0.0.1","port":listener.local_addr().unwrap().port()}],
        "key":key.to_vec(),"daemon_id":daemon.to_vec(),"pid":std::process::id(),"daemon_ver":"0.40.0"
    })).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    async fn respond(stream: &mut tokio::net::TcpStream, request: &Frame, body: Value) {
        let frame = Frame::build(
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(stream, &frame).await.unwrap();
    }
    let mut peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        authenticate_server(&mut stream, &key, &daemon, "0.40.0", Duration::from_secs(2))
            .await
            .unwrap();
        let request = crate::test_daemon::read_application_frame(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["op"],
            "catalog.list"
        );
        respond(
            &mut stream,
            &request,
            json!({"op":"catalog.list","generation":1,
            "modules":modules(Some(true)),"subc_ops":[]}),
        )
        .await;
        let request = crate::test_daemon::read_application_frame(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["op"],
            "route.open"
        );
        respond(
            &mut stream,
            &request,
            json!({"op":"route.open","route_channel":7,
            "route_epoch":3,"target":target}),
        )
        .await;
        if should_send {
            let request = read_frame(&mut stream).await.unwrap().unwrap();
            assert_eq!(request.header.ty, FrameType::Request);
            assert_eq!(request.header.channel, 7);
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap()["method"],
                "role.describe"
            );
            respond(&mut stream, &request, json!({"result":{"capabilities":[]}})).await;
        } else {
            // A rejected open may close its route but must send no application
            // request, even before the catalog is read again on a fresh connection.
            let next_connection = listener.accept();
            tokio::pin!(next_connection);
            let mut stream_open = true;
            let mut replacement = loop {
                tokio::select! {
                    next = &mut next_connection => break next.unwrap().0,
                    frame = read_frame(&mut stream), if stream_open => {
                        match frame.unwrap() {
                            Some(frame) => assert_ne!(frame.header.ty, FrameType::Request,
                                "mismatched target received application bytes"),
                            None => stream_open = false,
                        }
                    }
                }
            };
            authenticate_server(
                &mut replacement,
                &key,
                &daemon,
                "0.40.0",
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            let request = crate::test_daemon::read_application_frame(&mut replacement)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap()["op"],
                "catalog.list",
                "target rejection must immediately re-read the catalog"
            );
            respond(
                &mut replacement,
                &request,
                json!({"op":"catalog.list","generation":2,
                "modules":[],"subc_ops":[]}),
            )
            .await;
        }
    });
    let poller = tokio::spawn(super::super::runners::refresh_loop(
        path,
        harness.credentials(),
        harness.config_path.lock().unwrap().clone(),
    ));
    let result = tokio::time::timeout(Duration::from_secs(5), &mut peer).await;
    poller.abort();
    let _ = poller.await;
    if result.is_err() {
        peer.abort();
        let _ = peer.await;
    }
    result
        .expect("runner target check or catalog re-read did not complete")
        .unwrap();
}

#[tokio::test]
async fn route_open_missing_target_is_never_sent_and_rereads_catalog() {
    open_peer(Value::Null, false, false).await;
}

#[tokio::test]
async fn route_open_unmarked_target_is_never_sent_and_rereads_catalog() {
    let mut target = target();
    target["first_party"] = json!(false);
    open_peer(target, false, false).await;
}

#[tokio::test]
async fn route_open_different_incarnation_is_never_sent_and_rereads_catalog() {
    let mut target = target();
    target["incarnation"] = json!("daemon-b");
    open_peer(target, false, false).await;
}

#[tokio::test]
async fn route_open_different_spawn_is_never_sent_and_rereads_catalog() {
    let mut target = target();
    target["spawn"] = json!(8);
    open_peer(target, false, false).await;
}

#[tokio::test]
async fn route_open_non_spawn_is_never_sent_and_rereads_catalog() {
    let mut target = target();
    target["kind"] = json!("prefix");
    open_peer(target, false, false).await;
}

#[tokio::test]
async fn route_open_matching_target_sends_and_settings_need_no_mark() {
    open_peer(target(), false, true).await;
    open_peer(Value::Null, true, true).await;
}

fn catalog(harness: &Harness, modules: &Value) {
    let shared = harness.credentials();
    let mut credentials = shared.lock().unwrap();
    let queried = super::super::runners::DescribeCache::default().catalog(
        modules,
        &harness.operator_config().v1_runners(),
        &mut credentials,
        crate::now_ms() as i64,
    );
    if queried.contains(fixture::RUNNER_ONE)
        && credentials
            .runner_declarations
            .contains_key(fixture::RUNNER_ONE)
    {
        publish_fixture_providers(
            &mut credentials,
            fixture::RUNNER_ONE,
            &[fixture::PRICED_PROVIDER, fixture::SUBSCRIPTION_PROVIDER],
            false,
        );
    }
}

fn setup(label: &str) -> Arc<Harness> {
    let harness = Harness::new(label);
    install_catalog(&harness, &fixture::catalog());
    harness.edit_config(|config| {
        config["runners"] = json!([]);
        config["consumers"] = json!({"first-party-test":{"owner":"test-owner"}});
    });
    harness
}

async fn select(harness: &Arc<Harness>) -> (FrameType, Value) {
    harness
        .bind(reserved("test-owner"), None)
        .await
        .call(
            "route.select",
            &json!({
                "consumer":"first-party-test", "requirements":{"iq":0}
            }),
        )
        .await
}

#[tokio::test]
async fn first_party_runner_without_settings_gets_selections_and_account_trust() {
    let harness = setup("first-party-enroll");
    catalog(&harness, &modules(Some(true)));
    let reply = select(&harness).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["runner"]["module"], fixture::RUNNER_ONE);
    let mut marked_without_nonce = modules(Some(true));
    marked_without_nonce[0]["nonce_verified"] = json!(false);
    catalog(&harness, &marked_without_nonce);
    assert_eq!(select(&harness).await.0, FrameType::Response);
    let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
    let admission = runner
        .call(
            "route.account_for",
            &json!({
                "provider":"example-provider", "model":"model-a",
                "conversation_key":"first-party", "admission_id":"first-party"
            }),
        )
        .await;
    assert_ne!(
        admission.1["code"], "account_not_authorized",
        "{admission:?}"
    );
    catalog(&harness, &json!([]));
    assert_eq!(select(&harness).await.0, FrameType::Error);
}

#[tokio::test]
async fn unmarked_and_third_party_runners_require_settings_and_settings_win() {
    let harness = setup("third-party-enroll");
    for mark in [None, Some(false)] {
        catalog(&harness, &modules(mark));
        assert_eq!(select(&harness).await.0, FrameType::Error);
    }
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":fixture::RUNNER_ONE}]);
    });
    assert_eq!(select(&harness).await.0, FrameType::Response);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":fixture::RUNNER_ONE,"routes":[]}]);
    });
    catalog(&harness, &modules(Some(true)));
    assert_eq!(
        select(&harness).await.0,
        FrameType::Error,
        "automatic enrollment must not reopen a settings placement with no routes"
    );
    let shared = harness.credentials();
    let credentials = shared.lock().unwrap();
    let registry = super::super::runners::Registry::build(
        harness.operator_config().as_ref(),
        &credentials,
        &harness.store().get_facts().unwrap(),
        crate::now_ms(),
    );
    assert_eq!(
        registry.entries.len(),
        1,
        "settings must not produce a second automatic placement"
    );
}

#[tokio::test]
async fn mark_off_enrollment_bytes_and_every_consumer_what_if_picks_are_unchanged() {
    let harness = setup("first-party-pick-parity");
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":fixture::RUNNER_ONE}]);
        config["consumers"] = json!({
            "head":{"owner":"test-owner"}, "mason":{"owner":"test-owner"},
            "sidekick":{"owner":"test-owner"}, "codemode":{"owner":"test-owner"},
            "flow":{"owner":"test-owner"}, "mc_summary":{"owner":"test-owner"}
        });
    });
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let names = harness.operator_config().v1_consumers();
    let mut baseline = None;
    for mark in [None, Some(false), Some(true)] {
        catalog(&harness, &modules(mark));
        let bytes = {
            let shared = harness.credentials();
            let credentials = shared.lock().unwrap();
            let registry = super::super::runners::Registry::build(
                harness.operator_config().as_ref(),
                &credentials,
                &harness.store().get_facts().unwrap(),
                crate::now_ms(),
            );
            serde_json::to_vec(&registry.entries).unwrap()
        };
        let mut picks = Vec::new();
        for name in &names {
            let reply = operator
                .call("route.what_if", &json!({"consumer":name,"iqs":[0,50,100]}))
                .await;
            assert_eq!(reply.0, FrameType::Response, "{reply:?}");
            for result in reply.1["result"]["results"].as_array().unwrap() {
                assert!(
                    !result["pick"].is_null(),
                    "parity must compare actual picks: {reply:?}"
                );
                picks.push(result["pick"].clone());
            }
        }
        let observed = (bytes, picks);
        if let Some(baseline) = &baseline {
            assert_eq!(
                &observed, baseline,
                "mark {mark:?} changed an explicitly enrolled fixture"
            );
        } else {
            baseline = Some(observed);
        }
    }
}
