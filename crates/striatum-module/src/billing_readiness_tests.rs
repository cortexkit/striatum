fn cold_credentials() -> crate::SharedBrocaCredentials {
    Arc::new(Mutex::new(crate::BrocaCredentials::default()))
}

fn pricing_request(method: &str) -> Value {
    json!({
        "method": method,
        "params": {
            "taskId": "pricing-startup-test",
            "requirements": {"iq": 30, "eq": 0},
            "availableModels": ["meta/muse-spark-1.3-contributor"],
            "selection": "n_pick",
            "panelSpec": {"size": 1, "iqFloor": 0, "diversity": "none", "class": "AUDIT"}
        }
    })
}


#[tokio::test]
async fn legacy_panel_past_first_plane_load_bound_refuses_retryably_without_a_decision() {
    let rig = Rig::new("pricing-startup-timeout");
    let store: crate::SharedStore = Arc::new(rig.store());
    let credentials = cold_credentials();
    // Current route.select has its own v1 readiness test. The legacy panel
    // also waits three seconds for the first billing catalog before refusing.
    for method in [crate::ROUTE_SELECT_PANEL_OP] {
        let started = std::time::Instant::now();
        let response = crate::tests::run_route_wire_request_with_paths(
            &store,
            pricing_request(method),
            Path::new("/nonexistent/striatum.jsonc"),
            &credentials,
        )
        .await;
        assert!(started.elapsed() >= Duration::from_secs(3));
        assert_eq!(response.header.ty, subc_protocol::FrameType::Error);
        let error: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error["code"], "routing_not_ready", "{error}");
        assert_eq!(error["detail"]["retryable"], true);
        assert_eq!(error["detail"]["retry_after_ms"], 1000);
    }
    use striatum_core::RouteStateStore as _;
    assert!(store
        .get_decisions_for_task("pricing-startup-test")
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn pricing_health_is_not_ready_until_first_plane_publication() {
    let credentials = cold_credentials();
    credentials.lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "healthy-listing".into(),
            credentials: vec![muse_credential("oauth", true)],
            ..Default::default()
        },
        crate::now_ms(),
    );
    let before = serde_json::to_value(
        crate::tests::health_report_for_with_credentials(
            crate::QuotaCache::default(),
            &credentials,
        )
        .await,
    )
    .unwrap();
    assert_eq!(before["status"], "degraded", "{before}");
    assert!(before["detail"]
        .as_str()
        .unwrap()
        .contains("pricing not ready"));
    assert_eq!(before["metrics"]["pricingReady"], false);
    credentials
        .lock()
        .unwrap()
        .publish_billing_catalog(PlaneCatalog::ordinary_test_catalog(&[]));
    let after = serde_json::to_value(
        crate::tests::health_report_for_with_credentials(
            crate::QuotaCache::default(),
            &credentials,
        )
        .await,
    )
    .unwrap();
    assert_eq!(after["status"], "ok", "{after}");
    assert_eq!(after["metrics"]["pricingReady"], true);
}

#[tokio::test]
async fn first_plane_publication_wakes_all_pending_pricing_waiters() {
    let credentials = cold_credentials();
    let waiters = async {
        tokio::join!(
            crate::broca_credentials::wait_for_billing_catalog(
                &credentials,
                Duration::from_secs(1)
            ),
            crate::broca_credentials::wait_for_billing_catalog(
                &credentials,
                Duration::from_secs(1)
            ),
        )
    };
    tokio::pin!(waiters);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut waiters)
            .await
            .is_err()
    );
    credentials
        .lock()
        .unwrap()
        .publish_billing_catalog(PlaneCatalog::ordinary_test_catalog(&[]));
    assert_eq!(waiters.await, (true, true));
    assert!(
        crate::broca_credentials::wait_for_billing_catalog(&credentials, Duration::from_millis(1))
            .await
    );
}
