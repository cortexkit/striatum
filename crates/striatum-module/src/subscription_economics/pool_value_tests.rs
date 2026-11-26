use super::*;
use crate::account_quota::StoredReading;
use cortexkit_bus_client::BucketItem;
use striatum_core::subscription::SubscriptionPricing;

const NOW: i64 = 777_600_000;
const CLAUDE_VALUE: f64 = 4.761904761904762;
const SUBSET_VALUE: f64 = 2.380952380952381;
const OPERATOR_VALUE: f64 = 1.5;

macro_rules! sample {
    ($name:literal) => {
        include_bytes!(concat!(
            "../../fixtures/astrocyte-spend-multiplier-report-v2/",
            $name
        ))
    };
}

fn put(key: &str, bytes: &[u8]) -> BucketItem {
    BucketItem::Put {
        key: key.into(),
        revision: 1,
        value: bytes.to_vec().into(),
    }
}

fn pool_put(name: &str, bytes: &[u8]) -> BucketItem {
    let keys: Value = serde_json::from_slice(sample!("bus-keys.json")).unwrap();
    put(keys[name].as_str().unwrap(), bytes)
}

fn claude_all() -> BucketItem {
    pool_put("kv-claude-all.json", sample!("kv-claude-all.json"))
}

fn claude_fable() -> BucketItem {
    pool_put("kv-claude-fable.json", sample!("kv-claude-fable.json"))
}

#[test]
fn pool_value_three_part_keys_replace_only_their_plans_two_part_key() {
    let mut state = multiplier::State::default();
    let legacy_claude = br#"{"quota_provider":"claude","plan":"anthropic/max_20x"}"#;
    let legacy_codex = br#"{"quota_provider":"codex","plan":"openai/pro_20x"}"#;
    const CLAUDE_KEY: &str = "x636c61756465.x616e7468726f7069632f6d61785f323078";
    const CODEX_KEY: &str = "x636f646578.x6f70656e61692f70726f5f323078";
    state.begin();
    state.apply(put(CLAUDE_KEY, legacy_claude));
    state.apply(put(CODEX_KEY, legacy_codex));
    state.apply(BucketItem::CaughtUp);
    assert_eq!(state.rows().len(), 2);

    state.apply(pool_put("kv-claude-fable.json", b"invalid pool row"));
    assert_eq!(state.rows().len(), 1);
    assert_eq!(state.rows()[0]["quota_provider"], "codex");
    state.apply(claude_fable());
    state.apply(claude_all());
    let rows = state.rows();
    assert_eq!(rows.len(), 3, "only the migrated plan loses its old row");
    assert_eq!(
        rows.iter()
            .map(|row| (
                row["quota_provider"].as_str().unwrap(),
                row["pool_id"].as_str()
            ))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("claude", Some("Fable")),
            ("claude", Some("primary")),
            ("codex", None)
        ])
    );
}

fn catalog_pool(provider: &str, pool: &str, models: &[&str]) -> Value {
    json!({"provider_id":provider,"pool_id":pool,"models":models,
        "source_ref":"https://example.invalid/pools","quote":"Synthetic pool coverage",
        "established_at_ms":NOW,"review_by_ms":NOW + 86_400_000})
}

fn quota(provider: &str, catalog: &str, weekly: Value, extra: Vec<Value>) -> AccountQuotaState {
    let id = format!("synthetic-{provider}-account");
    let mut state = AccountQuotaState::default();
    state.readings.readings.insert(
        (provider.into(), id.clone()),
        StoredReading {
            provider: provider.into(),
            account_id: id.clone(),
            received_at_ms: NOW,
            tombstoned_at_ms: None,
            entry: json!({"provider":provider,"apiProvider":catalog,"account":id,
                "fetchedAt":"1970-01-10T00:00:00Z",
                "usage":{"primary":weekly,"extraRateWindows":extra}}),
        },
    );
    state
}

fn weekly(used: f64, reset: &str) -> Value {
    json!({"usedPercent":used,"windowMinutes":10080,"windowKind":"weekly","resetsAt":reset})
}

fn runtime(state: AccountQuotaState, pools: Vec<Value>) -> Runtime {
    Runtime {
        inputs: Some(Arc::new(Inputs {
            state,
            prices: json!({"prices":[],"quota_pools":pools}),
            credentials: BTreeMap::new(),
            unpriced_tiers: BTreeSet::new(),
        })),
        ..Default::default()
    }
}

fn from_kv(mut runtime: Runtime, items: impl IntoIterator<Item = BucketItem>) -> Runtime {
    runtime.multipliers.begin();
    runtime.bucket_batch(
        items.into_iter().chain([BucketItem::CaughtUp]).collect(),
        NOW,
    );
    runtime
}

fn price(runtime: &Runtime, provider: &str, model: &str, now: i64) -> SubscriptionPricing {
    let mut snapshot = ModelRouteStateSnapshot::default();
    overlay_economics(
        &runtime.economics,
        &mut snapshot,
        &[crate::tests::fact(provider, model)],
        now,
        &RoutePricing::default(),
    );
    snapshot.by_capacity[&make_capacity_key(provider, model)]
        .subscription_pool
        .as_ref()
        .unwrap()
        .price(1.0, Some(OPERATOR_VALUE), 0.0, false)
        .unwrap()
}

#[test]
fn pool_value_claude_selects_the_binding_pool_then_falls_back_conservatively() {
    // Higher use alone does not bind when that pool resets much sooner.
    const WEEKLY: &str = "1970-01-16T00:00:00Z";
    for (fable, expected) in [
        (Some(weekly(80.0, "1970-01-10T01:00:00Z")), CLAUDE_VALUE),
        (Some(weekly(80.0, WEEKLY)), SUBSET_VALUE),
        (Some(weekly(60.0, WEEKLY)), SUBSET_VALUE),
        (None, CLAUDE_VALUE),
    ] {
        let mut runtime = from_kv(
            runtime(
                quota(
                    "claude",
                    "anthropic",
                    weekly(60.0, WEEKLY),
                    fable
                        .into_iter()
                        .map(|window| json!({"id":"Fable","window":window}))
                        .collect(),
                ),
                vec![catalog_pool(
                    "anthropic",
                    "Fable",
                    &["anthropic/claude-fable-5"],
                )],
            ),
            [claude_all(), claude_fable()],
        );
        let opus = price(&runtime, "anthropic", "claude-opus-5", NOW);
        assert_eq!(opus.value_multiplier, CLAUDE_VALUE);
        assert_eq!(
            opus.plan_values[0].plan.as_deref(),
            Some("anthropic/max_20x")
        );
        assert_eq!(
            price(&runtime, "anthropic", "claude-fable-5", NOW).value_multiplier,
            expected
        );
        let after_expiry = NOW + 16 * 60_000;
        let fable = |runtime: &Runtime| price(runtime, "anthropic", "claude-fable-5", after_expiry);
        assert_eq!(fable(&runtime).value_multiplier, SUBSET_VALUE);
        runtime.rebuild(after_expiry);
        assert_eq!(fable(&runtime).value_multiplier, SUBSET_VALUE);
        let historical = price(
            &runtime,
            "anthropic",
            "claude-fable-5",
            NOW + 3 * 86_400_000,
        );
        assert_eq!(historical.value_multiplier, SUBSET_VALUE);
        assert!(historical
            .value_source
            .starts_with("astrocyte last eligible "));
    }
}

#[test]
fn pool_value_antigravity_models_come_from_the_catalog_provider_and_pool() {
    let mut pools = vec![
        catalog_pool("google", "Gemini Flash", &["google/gemini-flash"]),
        catalog_pool("google", "Gemini Pro", &["google/gemini-pro"]),
        catalog_pool("google", "Gemini Preview", &["google/gemini-preview"]),
        catalog_pool("antigravity", "Gemini Pro", &["google/uncovered"]),
    ];
    for pro_published in [true, false] {
        if !pro_published {
            pools.remove(1);
        }
        let state = quota(
            "antigravity",
            "google",
            Value::Null,
            ["Gemini Flash", "Gemini Pro"]
                .map(|id| json!({"id":id,"window":weekly(10.0,"1970-01-16T00:00:00Z")}))
                .into(),
        );
        let runtime = from_kv(
            runtime(state, pools.clone()),
            [
                pool_put("kv-gemini-flash.json", sample!("kv-gemini-flash.json")),
                pool_put("kv-gemini-pro.json", sample!("kv-gemini-pro.json")),
                pool_put(
                    "kv-pool-list-missing.json",
                    sample!("kv-pool-list-missing.json"),
                ),
            ],
        );
        let multiplier = |model| price(&runtime, "google", model, NOW).value_multiplier;
        assert_eq!(multiplier("gemini-flash"), SUBSET_VALUE);
        assert_eq!(
            multiplier("gemini-pro"),
            if pro_published {
                SUBSET_VALUE
            } else {
                OPERATOR_VALUE
            }
        );
        assert_eq!(multiplier("gemini-preview"), OPERATOR_VALUE);
        assert_eq!(multiplier("uncovered"), OPERATOR_VALUE);
    }
}

#[test]
fn pool_value_all_only_needs_the_plan_even_without_catalog_provider() {
    let runtime = from_kv(
        runtime(
            quota(
                "codex",
                "openai",
                weekly(10.0, "1970-01-16T00:00:00Z"),
                vec![],
            ),
            vec![],
        ),
        [pool_put(
            "kv-single-pool.json",
            sample!("kv-single-pool.json"),
        )],
    );
    assert_eq!(
        price(&runtime, "openai", "gpt-5", NOW).value_multiplier,
        SUBSET_VALUE
    );
}

#[test]
fn pool_value_report_preserves_latest_and_does_not_price_from_legacy_history() {
    let decode = |bytes| QuotaConsumer::decode_query_reply(bytes).unwrap();
    let window = weekly(10.0, "1970-01-16T00:00:00Z");
    let mut state = quota("claude", "anthropic", window.clone(), vec![]);
    state.readings.readings.extend(
        quota("legacy", "synthetic", window, vec![])
            .readings
            .readings,
    );
    let mut runtime = runtime(state, vec![]);

    let legacy = decode(sample!("legacy-only.json"));
    assert_eq!(legacy["latest"], json!([]));
    assert_eq!(legacy["series"].as_array().unwrap().len(), 1);
    assert_eq!(legacy["series"][0]["pool_id"], "primary");
    assert!(legacy["series"][0]["pool_scope"].is_null());
    runtime.poll_report(legacy, 0, false);
    runtime.rebuild(NOW);
    assert_eq!(
        price(&runtime, "synthetic", "old-model", NOW).value_multiplier,
        OPERATOR_VALUE
    );

    let narrow = decode(sample!("narrow-range.json"));
    let full = decode(sample!("pool-heads.json"));
    assert_eq!(narrow["latest"], full["latest"]);
    assert_eq!(narrow["latest"].as_array().unwrap().len(), 6);
    let ends: Vec<_> = narrow["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["interval_end_ms"].as_i64().unwrap())
        .collect();
    assert_eq!(ends, [691_200_000, 691_200_000]);
    runtime.poll_report(narrow, 0, false);
    runtime.rebuild(NOW);
    assert_eq!(
        price(&runtime, "anthropic", "claude-opus-5", NOW).value_multiplier,
        CLAUDE_VALUE
    );
    assert_eq!(
        price(&runtime, "synthetic", "old-model", NOW).value_multiplier,
        OPERATOR_VALUE
    );
}
