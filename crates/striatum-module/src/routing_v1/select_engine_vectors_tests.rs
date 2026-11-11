use std::{collections::BTreeMap, path::Path, sync::Arc};

use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::{RouteStateStore, RouteStateUpdate};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use crate::broca_credentials::BrocaCredentials;

const NOW: i64 = 1_750_000_000_000;
const PROVIDER: &str = "media-vendor";
const KEY: &str = "opaque-media-key";
const KINDS: [&str; 6] = [
    "transcribe",
    "synthesize",
    "image",
    "video",
    "stream_transcribe",
    "realtime",
];

fn engine(kind: &str) -> Value {
    let format = match kind {
        "image" => "image/png",
        "video" => "video/mp4",
        _ => "audio/wav",
    };
    json!({"kind":kind,"provider":PROVIDER,"model":format!("fixture-{kind}"),
        "local":false,"formats":[format],"languages":["en"],
        "max_duration_ms":60000,"latency_class":"realtime"})
}

fn params(kind: &str) -> Value {
    json!({"consumer":"phone.media","kind":kind,"requirements":{},"privacy":"remote_allowed"})
}

fn key() -> ListedCredential {
    ListedCredential {
        id: KEY.into(),
        categories: vec!["media-provider".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        state: "active".into(),
        provider_ids: vec![PROVIDER.into()],
        ..Default::default()
    }
}

fn credentials(rows: Vec<ListedCredential>, granted: bool, listed_at: i64) -> BrocaCredentials {
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "select-engine-vectors-v1".into(),
            credentials: rows,
            grant_tuples: if granted {
                vec![ListedGrant {
                    selector_kind: "category".into(),
                    selector: "media-provider".into(),
                    operation: "list".into(),
                }]
            } else {
                vec![]
            },
            ..Default::default()
        },
        listed_at as u64,
    );
    credentials.catalog_snapshot = Some(Arc::new(
        crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({"catalog_version":41,"resolved_at_ms":NOW,
                "models":{"media-vendor/fixture-transcribe":{"existence":"present",
                    "rate.input_audio.per_second":{"state":"priced","currency":"USD",
                        "units":4500000,"exponent":9,"per":60,
                        "increment":{"kind":"unknown"},"minimum_quantity":{"kind":"unknown"}}}}}))
            .unwrap(),
        )
        .unwrap(),
    ));
    credentials
}

fn fixture() -> Arc<Harness> {
    let harness = Harness::new("select-engine-vectors");
    harness.edit_config(|config| config["providers"][PROVIDER]["pin_only"] = json!(false));
    super::consumers::replace_catalog(&json!([
        {"module_id":"fixture-media","nonce_verified":true,
            "role_declarations":{"media-provider/v1":{"engines":KINDS.map(engine)}}},
        {"module_id":"callosum","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"phone.media"}]}}}
    ]));
    harness.set_credentials(credentials(vec![key()], true, NOW));
    harness
}

#[derive(Default)]
struct Samples(BTreeMap<String, Vec<u8>>);

impl Samples {
    async fn capture(
        &mut self,
        name: &str,
        harness: &Harness,
        route: &Route,
        params: &Value,
        code: Option<&str>,
        filter: Option<&str>,
    ) -> Value {
        let frame = route.request_frame("route.select_engine", params);
        let request = frame.body.clone();
        let reply = harness.handle(frame).await;
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        if let Some(code) = code {
            assert_eq!(reply.header.ty, FrameType::Error, "{name}: {value}");
            assert_eq!(value["code"], code, "{name}: {value}");
            if let Some(filter) = filter {
                assert_eq!(value["detail"]["filter"], filter, "{name}: {value}");
                assert!(value["detail"].get("retry_at_ms").is_none());
            }
        } else {
            assert_eq!(reply.header.ty, FrameType::Response, "{name}: {value}");
            assert_eq!(value["result"]["expires_at_ms"], NOW + 600_000);
        }
        // Keep both bodies verbatim, including the handler's object-member order.
        let mut pair = b"{\"request\":".to_vec();
        pair.extend_from_slice(&request);
        pair.extend_from_slice(b",\"reply\":");
        pair.extend_from_slice(&reply.body);
        pair.push(b'}');
        assert!(self.0.insert(format!("{name}.json"), pair).is_none());
        value
    }

    fn verify(self) {
        let directory =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-select-engine-v1");
        let update = std::env::var_os("UPDATE_ROUTE_SELECT_ENGINE_VECTORS").is_some();
        let mut sums = String::new();
        for (name, bytes) in &self.0 {
            let path = directory.join(name);
            if update {
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::write(&path, bytes).unwrap();
            }
            assert_eq!(std::fs::read(&path).unwrap(), *bytes, "{name}");
            sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
        }
        let path = directory.join("SHA256SUMS");
        if update {
            std::fs::write(&path, &sums).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), sums);
        let files: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.ends_with(".json"))
            .collect();
        assert_eq!(files.len(), self.0.len(), "unlisted sample files");
    }
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let mut samples = Samples::default();
    let harness = fixture();
    let route = harness.bind(reserved("callosum"), None).await;
    let mut first = Value::Null;
    for kind in KINDS {
        let reply = samples
            .capture(
                &format!("accepted-{kind}"),
                &harness,
                &route,
                &params(kind),
                None,
                None,
            )
            .await;
        assert_eq!(
            reply["result"]["engine"]["model"],
            format!("fixture-{kind}")
        );
        assert_eq!(reply["result"]["engine"]["module"], "fixture-media");
        if kind == "transcribe" {
            first = reply;
        }
    }
    let repeated = samples
        .capture(
            "repeated-transcribe",
            &harness,
            &route,
            &params("transcribe"),
            None,
            None,
        )
        .await;
    assert_eq!(first["result"]["engine"], repeated["result"]["engine"]);
    assert_ne!(
        first["result"]["decision_id"],
        repeated["result"]["decision_id"]
    );
    let mut local = params("transcribe");
    local["privacy"] = json!("local_only");
    let mut phone = engine("transcribe");
    phone["local"] = json!(true);
    phone["provider"] = json!("apple");
    phone["model"] = json!("phone-transcriber");
    local["caller_local"] = json!([phone]);
    let reply = samples
        .capture(
            "accepted-caller-local",
            &harness,
            &route,
            &local,
            None,
            None,
        )
        .await;
    assert_eq!(reply["result"]["engine"]["caller_local"], true);
    assert_eq!(reply["result"]["engine"]["index"], 0);
    assert!(reply["result"]["engine"].get("module").is_none());

    for (name, patch, code, filter) in [
        (
            "unknown-kind",
            json!({"kind":"future_kind"}),
            "no_engine",
            Some("kind"),
        ),
        (
            "requirements-unmet",
            json!({"requirements":{"language":"fr"}}),
            "no_engine",
            Some("requirements"),
        ),
        (
            "privacy-local-only",
            json!({"privacy":"local_only"}),
            "no_engine",
            Some("privacy"),
        ),
        (
            "invalid-request",
            json!({"effect_id":"not-supported"}),
            "invalid_request",
            None,
        ),
        (
            "invalid-requirements",
            json!({"requirements":{"duration_ms":-1}}),
            "invalid_requirements",
            None,
        ),
        (
            "unknown-consumer",
            json!({"consumer":"unknown.media"}),
            "unknown_consumer",
            None,
        ),
    ] {
        let mut request = params("transcribe");
        for (key, value) in patch.as_object().unwrap() {
            request[key] = value.clone();
        }
        samples
            .capture(name, &harness, &route, &request, Some(code), filter)
            .await;
    }
    let foreign = harness.bind(reserved("stranger"), None).await;
    samples
        .capture(
            "caller-not-owner",
            &harness,
            &foreign,
            &params("transcribe"),
            Some("consumer_not_owned"),
            None,
        )
        .await;
    let direct = harness.bind(json!({"kind":"direct"}), None).await;
    samples
        .capture(
            "caller-direct",
            &harness,
            &direct,
            &params("transcribe"),
            Some("consumer_not_owned"),
            None,
        )
        .await;
    let channel = harness.reserve_channel();
    let flow = harness
        .bind_frame(
            json!({
                "op":"route.bind","route_channel":channel,"epoch":1,
                "target":{"kind":"management_surface","module_id":crate::DEFAULT_MODULE_ID},
                "identity":{"project_root":cortexkit_test_support::scratch_root(),
                    "harness":"select-engine-vectors","session":"flow-session"},
                "principal":reserved("callosum"),
                "scope":{"owner":reserved("callosum"),"ref":"phone-media-flow",
                    "scope_epoch":1,"kind":"head","owner_authorized":false,
                    "attributes":{"flow_id":"fixture-flow"}},
            }),
            channel,
            1,
        )
        .await;
    samples
        .capture(
            "flow-scope-not-supported",
            &harness,
            &flow,
            &params("transcribe"),
            Some("flow_scope_not_supported"),
            None,
        )
        .await;

    super::consumers::replace_catalog(&json!([
        {"module_id":"callosum","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"phone.media"}]}}}
    ]));
    samples
        .capture(
            "no-engines",
            &harness,
            &route,
            &params("transcribe"),
            Some("no_engine"),
            Some("kind"),
        )
        .await;

    capture_unavailable(&mut samples).await;
    samples.verify();
}

async fn capture_unavailable(samples: &mut Samples) {
    for name in [
        "route-cooldown",
        "route-quota-exhausted",
        "route-failure-backoff",
        "credential-entitlement-fenced",
        "credential-billing-fenced",
        "credential-refused",
        "pool-billing-wall",
        "pool-rate-limit-wall",
        "provider-reset-account-route-fence",
        "accepted-transient-only-account",
    ] {
        let harness = fixture();
        let store = harness.store();
        let route_key = "media-vendor/fixture-transcribe";
        let until = NOW + 60_000;
        match name {
            "route-cooldown" | "route-quota-exhausted" | "route-failure-backoff" => {
                store
                    .update_route_state(
                        route_key,
                        route_key,
                        &RouteStateUpdate {
                            cooldown_until: (name == "route-cooldown").then_some(until as f64),
                            quota_exhausted_until: (name == "route-quota-exhausted")
                                .then_some(until as f64),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                if name == "route-failure-backoff" {
                    rusqlite::Connection::open(harness.store_path()).unwrap().execute(
                        "UPDATE model_route_state SET failure_backoff_until=?1 WHERE route_key=?2",
                        rusqlite::params![until, route_key],
                    ).unwrap();
                }
            }
            "credential-entitlement-fenced"
            | "credential-billing-fenced"
            | "credential-refused" => {
                let code = match name {
                    "credential-entitlement-fenced" => "ENTITLEMENT_ERROR",
                    "credential-billing-fenced" => "billing_exhausted",
                    _ => "credential_refused",
                };
                store
                    .record_credential_entitlement_fence(
                        &striatum_core::route_state_store::CredentialRefusal {
                            provider_id: PROVIDER.into(),
                            credential_id: KEY.into(),
                            code: code.into(),
                        },
                        None,
                        NOW,
                    )
                    .unwrap();
            }
            "pool-billing-wall" | "pool-rate-limit-wall" => {
                store
                    .pool_wall(
                        PROVIDER,
                        KEY,
                        if name == "pool-billing-wall" {
                            striatum_store::PoolWallKind::Billing
                        } else {
                            striatum_store::PoolWallKind::RateLimited
                        },
                        until,
                        "synthetic provider wall",
                        NOW,
                    )
                    .unwrap();
            }
            _ => {
                let fence = striatum_store::AccountRouteFence {
                    provider: PROVIDER.into(),
                    credential_id: KEY.into(),
                    model: "fixture-transcribe".into(),
                    until_ms: until,
                    decision_id: String::new(),
                    recorded_at: NOW,
                };
                if name == "accepted-transient-only-account" {
                    store.record_transient_account_route_fence(&fence).unwrap();
                } else {
                    store.record_account_route_fence(&fence).unwrap();
                }
            }
        }
        let route = harness.bind(reserved("callosum"), None).await;
        let transient_only = name == "accepted-transient-only-account";
        samples
            .capture(
                name,
                &harness,
                &route,
                &params("transcribe"),
                (!transient_only).then_some("no_engine"),
                (!transient_only).then_some("reachability"),
            )
            .await;
    }

    for name in [
        "listing-missing",
        "listing-grant-missing",
        "listing-stale",
        "listing-failed",
        "no-admissible-media-credential",
    ] {
        let harness = fixture();
        let mut credentials = match name {
            "listing-missing" => BrocaCredentials::default(),
            "listing-grant-missing" => credentials(vec![key()], false, NOW),
            "listing-stale" => credentials(vec![key()], true, 1),
            "no-admissible-media-credential" => {
                let mut llm_only = key();
                llm_only.categories = vec!["llm-provider".into()];
                credentials(vec![llm_only], true, NOW)
            }
            _ => credentials(vec![key()], true, NOW),
        };
        if name == "listing-failed" {
            credentials.note_failure("synthetic listing failure");
        }
        harness.set_credentials(credentials);
        let route = harness.bind(reserved("callosum"), None).await;
        samples
            .capture(
                name,
                &harness,
                &route,
                &params("transcribe"),
                Some("no_engine"),
                Some("reachability"),
            )
            .await;
    }

    let harness = fixture();
    let route = harness.bind(reserved("callosum"), None).await;
    // A missing table induces a stable storage failure, not a fabricated refusal.
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DROP TABLE routing_engine_eval", [])
        .unwrap();
    samples
        .capture(
            "store-error",
            &harness,
            &route,
            &params("transcribe"),
            Some("store_error"),
            None,
        )
        .await;
}
