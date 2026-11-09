fn credentials(listed: bool, snapshot: Option<i64>) -> SharedBrocaCredentials {
    let mut credentials = BrocaCredentials::default();
    if listed {
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "catalog-targets-fixture".into(),
                credentials: [
                    ("resolved", "anthropic-platform", vec!["anthropic"]),
                    (
                        "ambiguous",
                        "ambiguous-platform",
                        vec!["anthropic", "openai"],
                    ),
                    ("unknown", "unknown-platform", vec!["no-catalog-vendor"]),
                    ("native", "anthropic", vec!["anthropic"]),
                ]
                .into_iter()
                .map(
                    |(id, provider, serves)| cortexkit_claustrum_client::ListedCredential {
                        id: id.into(),
                        categories: vec!["llm-provider".into()],
                        credential_type: "apikey".into(),
                        auth_method: Some("apikey".into()),
                        state: "active".into(),
                        provider_ids: vec![provider.into()],
                        serves: serves.into_iter().map(str::to_string).collect(),
                        ..Default::default()
                    },
                )
                .collect(),
                ..Default::default()
            },
            now_ms(),
        );
    }
    if let Some(version) = snapshot {
        credentials.catalog_snapshot = Some(catalog(version, true));
    }
    Arc::new(Mutex::new(credentials))
}

fn catalog(version: i64, anthropic: bool) -> Arc<fusiform_catalog::CatalogSnapshot> {
    let mut models = json!({"openai/gpt-fixture": {"existence": "present"}});
    if anthropic {
        models["anthropic/claude-opus-5-5"] = json!({"existence": "present"});
    }
    Arc::new(
        fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": version,
                "resolved_at_ms": 123,
                "models": models,
            }))
            .unwrap(),
        )
        .unwrap(),
    )
}

async fn request(
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    principal: &str,
    params: serde_json::Value,
) -> Frame {
    static CHANNEL: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(43000);
    let channel = CHANNEL.fetch_add(1, Ordering::Relaxed);
    route_principals()
        .lock()
        .unwrap()
        .insert(channel, (7, principal.into()));
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        7,
        42,
        serde_json::to_vec(&json!({
            "method": "route.catalog_targets",
            "params": params,
            "principal": "reserved:occipital",
        }))
        .unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    handle_select_request(
        frame,
        7,
        &tx,
        &Arc::new(Mutex::new(QuotaCache::default())),
        store,
        &Arc::new(Mutex::new(None)),
        Path::new("/nonexistent/catalog-targets-routing.jsonc"),
        Path::new("/nonexistent/catalog-targets-operator.jsonc"),
        credentials,
    )
    .await
    .unwrap();
    let response = rx.recv().await.unwrap();
    route_principals().lock().unwrap().remove(&channel);
    assert_eq!(response.header.channel, channel);
    assert_eq!(response.header.epoch, 7);
    assert_eq!(response.header.corr, 42);
    response
}

fn body(frame: &Frame) -> serde_json::Value {
    serde_json::from_slice(&frame.body).unwrap()
}

#[tokio::test]
async fn registered_query_serves_snapshot_resolution() {
    let manifest = manifest(DEFAULT_MODULE_ID);
    let operation = manifest
        .provides
        .iter()
        .find_map(|role| match role {
            ProviderRole::ManagementSurface { operations, .. } => operations
                .iter()
                .find(|operation| operation.name == "route.catalog_targets"),
            _ => None,
        })
        .expect("route.catalog_targets must be registered in the manifest");
    assert_eq!(operation.kind, ManagementOperationKind::Query);
    let rig = Rig::new("catalog-targets-snapshot");
    let store: SharedStore = Arc::new(rig.open_store());
    // A separately updated durable row must not change the snapshot's provider set.
    store
        .upsert_fact(
            &fact("anthropic-platform", "store-only"),
            FactSource::Override,
        )
        .unwrap();
    let credentials = credentials(true, Some(41));
    let response = request(&store, &credentials, "reserved:occipital", json!({})).await;
    assert_eq!(
        response.header.ty,
        FrameType::Response,
        "{}",
        body(&response)
    );
    assert_eq!(
        body(&response),
        json!({"result": {
            "targets": {"anthropic-platform": "anthropic"},
            "catalog_version": 41,
        }})
    );

    credentials.lock().unwrap().catalog_snapshot = Some(catalog(42, false));
    let next = request(&store, &credentials, "reserved:occipital", json!({})).await;
    assert_eq!(next.header.ty, FrameType::Response, "{}", body(&next));
    assert_eq!(
        body(&next),
        json!({"result": {
            "targets": {"ambiguous-platform": "openai"},
            "catalog_version": 42,
        }})
    );
}

#[tokio::test]
async fn admission_uses_daemon_principal_not_request_claim() {
    let rig = Rig::new("catalog-targets-admission");
    let store: SharedStore = Arc::new(rig.open_store());
    let credentials = credentials(true, Some(41));
    for principal in ["reserved:occipital", "direct", "reserved:callosum"] {
        let response = request(&store, &credentials, principal, json!({})).await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            body(&response)
        );
    }
    for principal in [
        "reserved:broca",
        "reserved:routing",
        "occipital",
        "unverified",
        "unattested",
    ] {
        let response = request(&store, &credentials, principal, json!({})).await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body = body(&response);
        assert_eq!(body["code"], "not_permitted", "{body}");
        assert!(
            body["message"].as_str().unwrap().contains(principal),
            "{body}"
        );
    }
}

#[tokio::test]
async fn warming_refuses_until_both_inputs_exist() {
    let rig = Rig::new("catalog-targets-warming");
    let store: SharedStore = Arc::new(rig.open_store());
    for credentials in [
        credentials(false, None),
        credentials(true, None),
        credentials(false, Some(41)),
    ] {
        let response = request(&store, &credentials, "reserved:occipital", json!({})).await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body = body(&response);
        assert_eq!(body["code"], "routing_not_ready", "{body}");
        assert_eq!(body["detail"]["retryable"], true, "{body}");
        assert_eq!(body["detail"]["retry_after_ms"], 1000, "{body}");
        assert!(body.get("result").is_none(), "{body}");
    }
    let credentials = credentials(false, Some(41));
    credentials.lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "known-empty-listing".into(),
            ..Default::default()
        },
        now_ms(),
    );
    let response = request(&store, &credentials, "reserved:occipital", json!({})).await;
    assert_eq!(response.header.ty, FrameType::Response);
    assert_eq!(
        body(&response),
        json!({"result": {"targets": {}, "catalog_version": 41}})
    );
}

#[tokio::test]
async fn params_must_be_an_empty_object() {
    let rig = Rig::new("catalog-targets-params");
    let store: SharedStore = Arc::new(rig.open_store());
    let credentials = credentials(true, Some(41));
    for params in [
        json!(null),
        json!([]),
        json!({"provider": "anthropic-platform"}),
    ] {
        let response = request(&store, &credentials, "reserved:occipital", params).await;
        assert_eq!(response.header.ty, FrameType::Error);
        assert_eq!(body(&response)["code"], "invalid_request");
    }
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    use sha2::{Digest, Sha256};

    let rig = Rig::new("catalog-targets-vectors");
    let store: SharedStore = Arc::new(rig.open_store());
    let resolved = request(
        &store,
        &credentials(true, Some(41)),
        "reserved:occipital",
        json!({}),
    )
    .await;
    assert_eq!(resolved.header.ty, FrameType::Response);
    let warming = request(
        &store,
        &credentials(false, None),
        "reserved:occipital",
        json!({}),
    )
    .await;
    assert_eq!(warming.header.ty, FrameType::Error);
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-catalog-targets-v1");
    let update = std::env::var_os("UPDATE_ROUTE_CATALOG_TARGETS_VECTORS").is_some();
    let mut sums = String::new();
    for (name, bytes) in [
        ("resolved.json", resolved.body),
        ("warming.json", warming.body),
    ] {
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        let path = directory.join(name);
        if update {
            std::fs::write(&path, &bytes).unwrap();
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "{name}");
    }
    let path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&path, &sums).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), sums);
}
