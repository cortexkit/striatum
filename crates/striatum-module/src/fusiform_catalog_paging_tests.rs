fn paged_request(token: Option<&str>) -> Value {
    let mut request = json!({
        "name": "catalog.get",
        "arguments": {"include_retired": true, "paged": true}
    });
    if let Some(token) = token {
        request["arguments"]["page_token"] = json!(token);
    }
    request
}

fn paged_payload(version: i64, instant: i64, provider: &str, next: Option<&str>) -> Value {
    let key = format!("{provider}/m");
    let mut payload = json!({
        "source": "models.dev", "catalog_version": version, "resolved_at_ms": instant,
        "models": {&key: {"existence": "present", "rate.output": {"state": "stated_zero"}}},
        "curated": [{"model": &key, "auth_methods": ["apikey"]}]
    });
    if let Some(next) = next {
        payload["next_page_token"] = json!(next);
    }
    payload
}

async fn paged_wire(
    script: Vec<(Value, FrameType, Value)>,
) -> (FusiformCatalogConsumer, tokio::task::JoinHandle<usize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    let peer = tokio::spawn(async move {
        let count = script.len();
        for (expected, ty, payload) in script {
            let frame = read_frame(&mut server).await.unwrap().unwrap();
            assert_eq!(frame.header.ty, FrameType::Request);
            let request: Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(request, expected);
            let response = Frame::build(
                ty,
                Flags::new(false, Priority::Interactive, false),
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                serde_json::to_vec(&payload).unwrap(),
            )
            .unwrap();
            write_frame(&mut server, &response).await.unwrap();
        }
        let goodbye = read_frame(&mut server).await.unwrap().unwrap();
        assert_eq!(goodbye.header.ty, FrameType::Goodbye);
        count
    });
    (
        FusiformCatalogConsumer {
            stream,
            credentials: None,
            route_channel: 1,
            route_epoch: 1,
            next_corr: 1,
        },
        peer,
    )
}

async fn paged_finish(peer: tokio::task::JoinHandle<usize>, expected: usize) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap(),
        expected
    );
}

#[test]
fn paged_golden_request_and_response_bytes_are_pinned() {
    let bytes = include_bytes!("../fixtures/catalog-paged.json");
    let golden: Value = serde_json::from_slice(bytes).unwrap();
    let digest = format!("{:x}", Sha256::digest(bytes));
    assert_eq!(
        digest,
        "0db77b1acf1c92e7f856c96106c738dbdb7925e58a7989e210971b4bb0eceb46"
    );
    let request = CatalogGetRequest {
        name: CATALOG_GET,
        arguments: CatalogGetArguments {
            paged: true,
            at_ms: Some(1000),
            ..Default::default()
        },
    };
    // Canonical compact JSON compares the wire bytes without depending on key order.
    assert_eq!(
        serde_json::to_vec(&serde_json::to_value(request).unwrap()).unwrap(),
        serde_json::to_vec(&golden["request"]).unwrap()
    );
    let page: CatalogPage = serde_json::from_value(golden["response"].clone()).unwrap();
    assert_eq!(page.next_page_token.as_deref(), Some("v1:0:1000:000-page"));
    assert_eq!(
        serde_json::to_vec(&serde_json::to_value(&page).unwrap()).unwrap(),
        serde_json::to_vec(&golden["response"]).unwrap()
    );
    let snapshot = decode_catalog_get(&serde_json::to_vec(&page).unwrap()).unwrap();
    assert_eq!(snapshot.cursor.resolved_at_ms, 1000);
    assert_eq!(snapshot.facts[0].provider_id, "000-page");
}

#[tokio::test]
async fn paged_three_pages_apply_together_with_retired_models_and_auth_policies() {
    use striatum_core::{FactSource, RouteStateStore as _};
    let rig = Rig::new("paged-three");
    let store = std::sync::Arc::new(rig.store());
    for provider in ["alpha", "beta", "zulu"] {
        store
            .upsert_fact(&crate::tests::fact(provider, "m"), FactSource::Seed)
            .unwrap();
    }
    let mut retired = paged_payload(7, 700, "beta", Some("opaque::second:do-not-parse"));
    retired["models"]["beta/m"]["existence"] = json!("absent");
    let mut last = paged_payload(7, 700, "zulu", None);
    // Login-method disclosures on later pages must survive assembly just like
    // curated disclosures, or page boundaries would change reachability.
    last["login_methods"] = last.as_object_mut().unwrap().remove("curated").unwrap();
    let (mut client, peer) = paged_wire(vec![
        (
            paged_request(None),
            FrameType::Response,
            paged_payload(7, 700, "alpha", Some("opaque:first:+/%")),
        ),
        (
            paged_request(Some("opaque:first:+/%")),
            FrameType::Response,
            json!({"result": retired}),
        ),
        (
            paged_request(Some("opaque::second:do-not-parse")),
            FrameType::Response,
            last,
        ),
    ])
    .await;
    let credentials = Arc::new(Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    client.credentials = Some(Arc::clone(&credentials));
    let health = Arc::new(Mutex::new(CatalogReachHealth::new(100)));
    bootstrap_cycle(&mut client, &store, &health, None, None)
        .await
        .unwrap();
    paged_finish(peer, 3).await;
    assert_eq!(
        store.fusiform_catalog_cursor().unwrap(),
        Some(FusiformCatalogCursor {
            catalog_version: 7,
            resolved_at_ms: 700,
        })
    );
    let cached = credentials
        .lock()
        .unwrap()
        .catalog_snapshot
        .clone()
        .unwrap();
    assert_eq!(cached.facts.len(), 3);
    assert_eq!(cached.rates.len(), 3);
    for fact in &cached.facts {
        assert_eq!(fact.auth_methods, Some(vec!["apikey".into()]));
        assert_eq!(fact.cost_per_mtok_out, Some(0.0));
    }
    assert!(
        !cached
            .facts
            .iter()
            .find(|f| f.provider_id == "beta")
            .unwrap()
            .present
    );
    let rows = store.get_catalog_facts().unwrap();
    assert!(
        rows.iter().all(|row| row.fact.provider_id != "beta"),
        "an absent route with no recent decision is pruned after the full snapshot"
    );
    assert!(store
        .get_facts()
        .unwrap()
        .iter()
        .all(|f| f.provider_id != "beta"));
}

#[tokio::test]
async fn paged_quality_disclosures_merge_across_the_read_and_apply() {
    let mut first = paged_payload(7, 700, "alpha", Some("first"));
    first["quality_scores"] = json!({"graded_by": "operator", "graded_at_ms": 100});
    let middle = paged_payload(7, 700, "beta", Some("second"));
    let mut last = paged_payload(7, 700, "zulu", None);
    last["quality_scores"] = json!({"graded_by": "operator", "graded_at_ms": 200});
    last["models"]["zulu/m"]["quality.elo"] = json!(1400.0);
    last["models"]["zulu/m"]["quality.eq"] = json!(120.0);
    let (mut client, peer) = paged_wire(vec![
        (paged_request(None), FrameType::Response, first),
        (paged_request(Some("first")), FrameType::Response, middle),
        (paged_request(Some("second")), FrameType::Response, last),
    ])
    .await;
    let snapshot = client.catalog_get().await.unwrap();
    client.close().await.unwrap();
    paged_finish(peer, 3).await;
    assert_eq!(snapshot.quality_scores.as_ref().unwrap().graded_at_ms, 200);
    assert_eq!(snapshot.facts.len(), 3);
    let rig = Rig::new("paged-quality");
    let store = rig.store();
    apply_mapped_catalog(&store, &snapshot, None, Some("zulu")).unwrap();
    assert!(store.operator_quality().unwrap().contains_key("zulu/m"));
}

#[tokio::test]
async fn paged_catalog_changed_discards_partial_pages_and_restarts_from_page_one() {
    for (ty, refusal) in [
        (
            FrameType::Error,
            json!({"code": "catalog_changed", "message": "changed"}),
        ),
        (
            FrameType::Response,
            json!({"result": {"error": {"code": "catalog_changed", "message": "changed"}}}),
        ),
    ] {
        let (mut client, peer) = paged_wire(vec![
            (
                paged_request(None),
                FrameType::Response,
                paged_payload(1, 100, "discard", Some("old")),
            ),
            (paged_request(Some("old")), ty, refusal),
            (
                paged_request(None),
                FrameType::Response,
                paged_payload(2, 200, "alpha", Some("new")),
            ),
            (
                paged_request(Some("new")),
                FrameType::Response,
                paged_payload(2, 200, "beta", None),
            ),
        ])
        .await;
        let snapshot = client.catalog_get().await.unwrap();
        client.close().await.unwrap();
        paged_finish(peer, 4).await;
        assert_eq!(
            snapshot.cursor,
            FusiformCatalogCursor {
                catalog_version: 2,
                resolved_at_ms: 200
            }
        );
        assert_eq!(
            snapshot
                .facts
                .iter()
                .map(|f| f.provider_id.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(
            snapshot
                .rates
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["alpha/m", "beta/m"]
        );
    }
}

#[tokio::test]
async fn paged_restart_limit_preserves_the_previous_store_and_cached_snapshot() {
    use striatum_core::{FactSource, RouteStateStore as _};
    let rig = Rig::new("paged-limit");
    let store = std::sync::Arc::new(rig.store());
    store
        .upsert_fact(&crate::tests::fact("alpha", "m"), FactSource::Seed)
        .unwrap();
    let initial = Arc::new(
        decode_catalog_get(&serde_json::to_vec(&paged_payload(0, 10, "alpha", None)).unwrap())
            .unwrap(),
    );
    store
        .apply_fusiform_catalog(&initial.facts, initial.cursor)
        .unwrap();
    let credentials = Arc::new(Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    credentials.lock().unwrap().catalog_snapshot = Some(Arc::clone(&initial));
    let mut script = Vec::new();
    // Four attempts are the first read plus three restarts, not a page-count limit.
    for version in 1..=4 {
        let token = format!("token-{version}");
        let mut partial = paged_payload(version, version * 100, "alpha", Some(&token));
        partial["models"]["alpha/m"]["existence"] = json!("absent");
        script.push((paged_request(None), FrameType::Response, partial));
        script.push((
            paged_request(Some(&token)),
            FrameType::Error,
            json!({"error": {"code": "catalog_changed", "message": "changed"}}),
        ));
    }
    let (mut client, peer) = paged_wire(script).await;
    client.credentials = Some(Arc::clone(&credentials));
    let health = Arc::new(Mutex::new(CatalogReachHealth::new(100)));
    let reason = bootstrap_cycle(&mut client, &store, &health, None, None)
        .await
        .unwrap_err();
    assert!(
        matches!(&reason, FusiformCatalogError::Refused {code, ..} if code == "catalog_changed")
    );
    assert_eq!(
        reason.to_string(),
        "catalog_changed: whole-catalog read exhausted 3 restarts"
    );
    client.close().await.unwrap();
    paged_finish(peer, 8).await;
    assert_eq!(
        store.fusiform_catalog_cursor().unwrap(),
        Some(initial.cursor)
    );
    let row = store
        .get_catalog_facts()
        .unwrap()
        .into_iter()
        .find(|r| r.fact.provider_id == "alpha")
        .unwrap();
    assert_eq!(row.fusiform_present, Some(true));
    assert!(store
        .get_facts()
        .unwrap()
        .iter()
        .any(|f| f.provider_id == "alpha"));
    assert!(Arc::ptr_eq(
        &credentials
            .lock()
            .unwrap()
            .catalog_snapshot
            .clone()
            .unwrap(),
        &initial
    ));
}

#[tokio::test]
async fn paged_other_refusals_fail_without_restart_and_only_codes_trigger_restart() {
    for code in ["bad_request", "no_coverage", "unavailable", "future_code"] {
        let (mut client, peer) = paged_wire(vec![
            (
                paged_request(None),
                FrameType::Response,
                paged_payload(1, 100, "alpha", Some("next")),
            ),
            (
                paged_request(Some("next")),
                FrameType::Error,
                json!({"code": code, "message": "catalog_changed"}),
            ),
        ])
        .await;
        let reason = client.catalog_get().await.unwrap_err();
        assert!(
            matches!(reason, FusiformCatalogError::Refused {code: actual, ..} if actual == code)
        );
        client.close().await.unwrap();
        paged_finish(peer, 2).await;
    }
    let (mut client, peer) = paged_wire(vec![(
        paged_request(None),
        FrameType::Error,
        json!({"code": "catalog_changed", "message": "first page refused"}),
    )])
    .await;
    assert!(
        matches!(client.catalog_get().await.unwrap_err(), FusiformCatalogError::Refused {code, ..} if code == "catalog_changed")
    );
    client.close().await.unwrap();
    paged_finish(peer, 1).await;
}

#[tokio::test]
async fn paged_inconsistent_identity_repeated_tokens_and_malformed_final_pages_fail() {
    for field in [
        "source",
        "catalog_version",
        "resolved_at_ms",
        "next_page_token",
        "models",
    ] {
        let mut last = paged_payload(1, 100, "beta", None);
        last[field] = match field {
            "source" => json!("another-source"),
            "catalog_version" => json!(2),
            "resolved_at_ms" => json!(101),
            "next_page_token" => json!("next"),
            "models" => json!(["not an object"]),
            _ => unreachable!(),
        };
        let (mut client, peer) = paged_wire(vec![
            (
                paged_request(None),
                FrameType::Response,
                paged_payload(1, 100, "alpha", Some("next")),
            ),
            (paged_request(Some("next")), FrameType::Response, last),
        ])
        .await;
        let reason = client.catalog_get().await.unwrap_err().to_string();
        assert!(
            reason.contains(match field {
                "next_page_token" => "repeated a page token",
                "models" => "invalid type",
                _ => "page identity changed",
            }),
            "{field}: {reason}"
        );
        client.close().await.unwrap();
        paged_finish(peer, 2).await;
    }
}

#[tokio::test]
async fn paged_final_page_with_an_unreadable_entry_still_applies_the_rest() {
    let mut last = paged_payload(1, 100, "beta", None);
    last["models"] = json!({"beta/m": {"existence": "unexpected"}});
    let (mut client, peer) = paged_wire(vec![
        (
            paged_request(None),
            FrameType::Response,
            paged_payload(1, 100, "alpha", Some("next")),
        ),
        (paged_request(Some("next")), FrameType::Response, last),
    ])
    .await;
    let snapshot = client.catalog_get().await.unwrap();
    assert_eq!(
        snapshot
            .facts
            .iter()
            .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
            .collect::<Vec<_>>(),
        vec!["alpha/m"]
    );
    assert_eq!(
        snapshot.retained,
        BTreeSet::from([("beta".to_string(), "m".to_string())])
    );
    client.close().await.unwrap();
    paged_finish(peer, 2).await;
}

#[test]
fn paged_merge_concatenates_every_disclosure() {
    let mut pages = Vec::new();
    for provider in ["alpha", "beta", "zulu"] {
        let mut value = paged_payload(1, 100, provider, None);
        value["withheld"] = json!([{
            "provider_id": provider, "model_id": "m", "fact_key": "model.family", "corrections": []
        }]);
        value["uncertain"] = json!([{
            "provider_id": provider, "model_id": "m", "fact_key": "rate.output",
            "superseded_after_ms": 90, "superseded_by_ms": 110
        }]);
        value["aliased"] = json!([{
            "model": format!("{provider}/m"), "target_provider_id": provider,
            "target_model_id": "target", "source_ref": "curated source"
        }]);
        pages.push(value);
    }
    let mut whole: CatalogPage = serde_json::from_value(pages[0].clone()).unwrap();
    for page in &pages[1..] {
        whole
            .append(serde_json::from_value(page.clone()).unwrap())
            .unwrap();
    }
    let merged = serde_json::to_value(whole).unwrap();
    for field in ["withheld", "uncertain", "curated", "aliased"] {
        assert_eq!(
            merged[field],
            json!([pages[0][field][0], pages[1][field][0], pages[2][field][0]]),
            "{field}"
        );
    }
    assert_eq!(merged["models"].as_object().unwrap().len(), 3);
}
