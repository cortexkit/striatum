struct HarnessRig {
    rig: Rig,
    store: SharedStore,
    config: PathBuf,
}

impl HarnessRig {
    fn new(mapping: serde_json::Value) -> Self {
        let rig = Rig::new("harness-selection");
        let store = Arc::new(rig.open_store());
        rusqlite::Connection::open(rig.dir.join("store.db"))
            .unwrap()
            .execute("DELETE FROM model_facts", [])
            .unwrap();
        let mut cheap = fact("opencode", "deepseek-v4.1-flash-free");
        cheap.cost_per_m_tok_out = Some(0.0);
        cheap.family = Some("deepseek".into());
        let mut paid = fact("anthropic", "claude-opus-5-5");
        paid.family = Some("claude".into());
        for row in [cheap, paid] {
            store.upsert_fact(&row, FactSource::Override).unwrap();
        }
        let config = rig.dir.join("prefrontal-routing.jsonc");
        std::fs::write(
            &config,
            json!({"model_routing": {"substrates": mapping, "models": {
                "opencode/deepseek-v4.1-flash-free": {"cost_per_mtok_out": 0},
                "anthropic/claude-opus-5-5": {"cost_per_mtok_out": 4.5}
            }}})
            .to_string(),
        )
        .unwrap();
        Self { rig, store, config }
    }

    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
        credentials: &SharedBrocaCredentials,
    ) -> serde_json::Value {
        let _guard = config_overrides::multiplier_map_guard();
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_select_request(
            frame,
            1,
            &tx,
            &Arc::new(Mutex::new(QuotaCache::default())),
            &self.store,
            &Arc::new(Mutex::new(None)),
            &self.config,
            &self.config,
            credentials,
            &empty_opencode_auth(),
        )
        .await
        .unwrap();
        serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap()
    }

    async fn select(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut params = json!({"consumer": "mason", "targetAgent": "mason", "requirements": {"iq": 30, "eq": 0}});
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let mut credentials =
            BrocaCredentials::api_test_credentials(&self.store.get_facts().unwrap());
        credentials.apply_listing(
            &prefrontal_ckcred_client::ScopedCredentialListing {
                credentials: vec![prefrontal_ckcred_client::ListedCredential {
                    id: "apikey:anthropic:fixture".into(),
                    credential_type: "apikey".into(),
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    provider_ids: vec!["anthropic".into()],
                    ..Default::default()
                }],
                view: "fixture".into(),
                ..Default::default()
            },
            now_ms(),
        );
        self.call(ROUTE_SELECT_OP, params, &Arc::new(Mutex::new(credentials)))
            .await
    }
}

const CHEAP: &str = "opencode/deepseek-v4.1-flash-free";
const PAID: &str = "anthropic/claude-opus-5-5";

fn rejection(body: &serde_json::Value, key: &str) -> serde_json::Value {
    body["result"]["selected"]["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["routeKey"] == key)
        .unwrap_or_else(|| panic!("missing rejection for {key}: {body}"))
        .clone()
}

#[tokio::test]
async fn launch_availability_is_per_mapped_harness() {
    let rig = HarnessRig::new(json!({"all": {"opencode": "opencode"}}));
    let host = rig
        .select(json!({"harness": "opencode", "availableModels": [CHEAP]}))
        .await;
    assert_eq!(host["result"]["selected"]["routeKey"], CHEAP, "{host}");
    assert_eq!(host["result"]["selected"]["harness"], "opencode");
    assert_eq!(
        host["result"]["resolvedInput"]["availableModels"],
        json!([CHEAP])
    );
    for extra in [
        json!({}),
        json!({"harness": "pi", "availableModels": [CHEAP]}),
        json!({"harness": "opencode"}),
    ] {
        let broca = rig.select(extra).await;
        assert_eq!(broca["result"]["selected"]["routeKey"], PAID, "{broca}");
        assert_eq!(broca["result"]["selected"]["harness"], "broca");
        assert_eq!(rejection(&broca, CHEAP)["reason"], "not_available");
        assert_eq!(
            rejection(&broca, CHEAP)["detail"],
            "harness opencode does not serve this route for this request"
        );
    }
    let unmapped = HarnessRig::new(json!({}));
    let result = unmapped
        .select(json!({"harness": "opencode", "availableModels": [CHEAP], "substrate": "opencode"}))
        .await;
    assert_eq!(result["result"]["selected"]["routeKey"], PAID, "{result}");
    let fallback = unmapped
        .call(
            ROUTE_SELECT_OP,
            json!({"targetAgent": "mason", "requirements": {"iq": 30, "eq": 0}}),
            &empty_broca_credentials(),
        )
        .await;
    assert_eq!(fallback["result"]["selected"]["harness"], "broca");
}

#[tokio::test]
async fn workload_is_validated_and_derived_from_target_agent() {
    let rig = HarnessRig::new(
        json!({"sidekick.librarian": {"default": "opencode"}, "sidekick": {"default": "broca"}}),
    );
    let librarian = rig.select(json!({"consumer": "sidekick", "targetAgent": "librarian", "harness": "opencode", "availableModels": [CHEAP]})).await;
    assert_eq!(
        librarian["result"]["selected"]["harness"], "opencode",
        "{librarian}"
    );
    let explore = rig.select(json!({"consumer": "sidekick", "targetAgent": "explore", "harness": "opencode", "availableModels": [CHEAP]})).await;
    assert_eq!(
        explore["result"]["selected"]["harness"], "broca",
        "{explore}"
    );
    let explicit = rig.select(json!({"consumer": "sidekick", "targetAgent": "explore", "workload": "sidekick.librarian", "harness": "opencode", "availableModels": [CHEAP]})).await;
    assert_eq!(
        explicit["result"]["selected"]["harness"], "opencode",
        "{explicit}"
    );
    let connection = rusqlite::Connection::open(rig.rig.dir.join("store.db")).unwrap();
    let count = || {
        connection
            .query_row("SELECT count(*) FROM model_route_decisions", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    let before = count();
    for extra in [
        json!({"workload": "unknown"}),
        json!({"requiredHarness": "claude-code"}),
        json!({"workload": 3}),
        json!({"executor": "opencode"}),
    ] {
        let invalid = rig.select(extra).await;
        assert_eq!(invalid["code"], "invalid_request", "{invalid}");
        assert_eq!(count(), before, "invalid input must not record a decision");
    }
}

#[tokio::test]
async fn executor_and_required_harness_refuse_mismatched_routes() {
    let rig = HarnessRig::new(json!({"all": {"opencode": "opencode"}}));
    for extra in [
        json!({"executor": "broca", "substrate": "broca"}),
        json!({"executor": "broca", "consumer": "athena", "workload": "sidekick.gather", "availableModels": [CHEAP, PAID]}),
    ] {
        let body = rig.select(extra).await;
        assert_eq!(body["result"]["selected"]["routeKey"], PAID, "{body}");
        assert_eq!(body["result"]["selected"]["harness"], "broca");
        assert_eq!(rejection(&body, CHEAP)["reason"], "not_available");
        assert_eq!(
            rejection(&body, CHEAP)["detail"],
            "harness opencode is not the executor broca"
        );
    }
    let body = rig.select(json!({"requiredHarness": "opencode", "harness": "opencode", "availableModels": [CHEAP, PAID]})).await;
    assert_eq!(body["result"]["selected"]["routeKey"], CHEAP, "{body}");
    assert_eq!(rejection(&body, PAID)["reason"], "not_available");
    assert_eq!(
        rejection(&body, PAID)["detail"],
        "harness_mismatch: task runs on opencode"
    );
    let plain = HarnessRig::new(json!({}));
    let body = plain
        .select(json!({"executor": "broca", "availableModels": [CHEAP, PAID]}))
        .await;
    assert_eq!(body["result"]["selected"]["routeKey"], CHEAP, "{body}");
    let body = plain
        .select(json!({"executor": "broca", "substrate": "broca"}))
        .await;
    assert_eq!(body["result"]["selected"]["routeKey"], PAID, "{body}");
}

#[tokio::test]
async fn panel_seats_resolve_under_athena_and_only_use_broca() {
    let rig = HarnessRig::new(json!({"athena": {"opencode": "opencode"}}));
    let mut config: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&rig.config).unwrap()).unwrap();
    config["model_routing"]["panels"] = json!({"audit": {"topK": 1, "extra": 0, "by": "iq"}});
    std::fs::write(&rig.config, config.to_string()).unwrap();
    let body = rig
        .call(
            ROUTE_SELECT_PANEL_OP,
            json!({
                "substrate": "broca", "panelSpec": {"class": "AUDIT"},
                "availableModels": [CHEAP, PAID]
            }),
            &empty_broca_credentials(),
        )
        .await;
    let members = body["result"]["members"]
        .as_array()
        .unwrap_or_else(|| panic!("{body}"));
    assert!(!members.is_empty(), "{body}");
    assert!(
        members
            .iter()
            .all(|member| member["routeKey"] == PAID && member["harness"] == "broca"),
        "{body}"
    );
    assert!(
        body["result"]["shortfall"].is_null(),
        "the panel is full: {body}"
    );
    let row = rig
        .store
        .get_decision_by_id(body["result"]["decisionID"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert!(
        row.rejected_json
            .contains("harness opencode is not the executor broca"),
        "{}",
        row.rejected_json
    );
}

#[tokio::test]
async fn pins_use_the_mapped_harness_and_executor_pins_reject_off_broca() {
    let rig = HarnessRig::new(json!({"all": {"opencode": "opencode"}}));
    let pin = rig
        .select(json!({"modelPin": CHEAP, "harness": "opencode", "availableModels": [CHEAP]}))
        .await;
    assert_eq!(pin["result"]["selected"]["harness"], "opencode", "{pin}");
    let absent = rig.select(json!({"modelPin": CHEAP})).await;
    assert_eq!(absent["code"], "pinned_route_refused", "{absent}");
    assert!(absent["message"]
        .as_str()
        .unwrap()
        .contains("harness opencode does not serve"));
    let executor = rig.select(json!({"executor": "broca", "pin": {"model": CHEAP}, "pin_fallback": "none", "availableModels": [CHEAP, PAID]})).await;
    assert!(executor["result"]["selected"].is_null(), "{executor}");
    assert!(!executor["result"]["pin_rejected"].is_null(), "{executor}");
}

#[tokio::test]
async fn base_capacity_membership_cannot_admit_an_off_executor_variant() {
    let rig =
        HarnessRig::new(json!({"all": {"opencode/deepseek-v4.1-flash-free/high": "opencode"}}));
    let mut high = fact("opencode", "deepseek-v4.1-flash-free");
    high.variant = Some("high".into());
    rig.store.upsert_fact(&high, FactSource::Override).unwrap();
    let body = rig
        .select(json!({"executor": "broca", "availableModels": [CHEAP, PAID]}))
        .await;
    assert_eq!(body["result"]["selected"]["routeKey"], CHEAP, "{body}");
    let key = format!("{CHEAP}/high");
    assert_eq!(
        rejection(&body, &key)["detail"],
        "harness opencode is not the executor broca"
    );
    let body = rig.select(json!({"executor": "broca", "modelPin": format!("{CHEAP}:high"), "availableModels": [CHEAP, PAID]})).await;
    assert_eq!(body["code"], "pinned_route_refused", "{body}");
}

#[tokio::test]
async fn runner_auth_pin_mapped_off_broca_refuses_even_with_broca_substrate() {
    let rig = HarnessRig::new(json!({"all": {"anthropic-platform": "opencode"}}));
    let mut config: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&rig.config).unwrap()).unwrap();
    config["model_routing"]["score_alias"] = json!({"anthropic-platform": "anthropic"});
    std::fs::write(&rig.config, config.to_string()).unwrap();
    let body = rig.call(ROUTE_SELECT_OP, json!({
        "consumer": "mason", "targetAgent": "mason", "requirements": {"iq": 30, "eq": 0},
        "modelPin": "anthropic-platform/claude-opus-5-5", "substrate": "broca", "harness": "opencode",
        "availableModels": ["anthropic-platform/claude-opus-5-5"]
    }), &platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])])).await;
    assert_eq!(body["code"], "provider_requires_runner_pin", "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("harness opencode"));
    let executor = rig.call(ROUTE_SELECT_OP, json!({
        "consumer": "athena", "targetAgent": "oracle", "executor": "broca", "workload": "athena",
        "requirements": {"iq": 30, "eq": 0}, "substrate": "broca",
        "pin": {"model": "anthropic-platform/claude-opus-5-5"}, "pin_fallback": "none"
    }), &platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])])).await;
    assert_eq!(
        executor["result"]["pin_rejected"], "not_available",
        "{executor}"
    );
    assert!(executor["result"]["selected"].is_null(), "{executor}");
    let mismatch = rig
        .call(
            ROUTE_SELECT_OP,
            json!({
                "targetAgent": "mason", "requirements": {"iq": 30, "eq": 0},
                "modelPin": "anthropic-platform/claude-opus-5-5", "requiredHarness": "broca"
            }),
            &platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]),
        )
        .await;
    assert_eq!(mismatch["code"], "pinned_route_refused", "{mismatch}");
    assert!(mismatch["message"]
        .as_str()
        .unwrap()
        .contains("harness_mismatch: task runs on broca"));
}
