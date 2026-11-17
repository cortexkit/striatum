use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

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

#[derive(Clone, Copy)]
enum Version {
    V1,
    V2,
}

impl Version {
    fn consumer(self) -> &'static str {
        "colliculus.voice_message"
    }

    fn module(self) -> &'static str {
        match self {
            Self::V1 => "fixture-media",
            Self::V2 => "colliculus",
        }
    }

    fn directory(self) -> &'static str {
        match self {
            Self::V1 => "route-select-engine-v1",
            Self::V2 => "route-select-engine-v2",
        }
    }

    fn catalog(self, with_engines: bool) -> Value {
        let mut modules = match self {
            Self::V1 => json!([
                {"module_id":"colliculus","nonce_verified":true,
                    "role_declarations":{"routing/v1":{"consumers":[{
                        "name":"colliculus.voice_message","selectors":["callosum"]}]}}}
            ]),
            Self::V2 => json!([
                {"module_id":"colliculus","nonce_verified":true,
                    "role_declarations":{"routing/v1":{"consumers":[{
                        "name":"colliculus.voice_message","kind":"transcribe",
                        "selectors":["callosum"]}]}}}
            ]),
        };
        if with_engines {
            match self {
                Self::V1 => modules.as_array_mut().unwrap().push(json!({
                    "module_id":self.module(),"nonce_verified":true,
                    "role_declarations":{"media-provider/v1":{"engines":KINDS.map(engine)}}
                })),
                Self::V2 => {
                    modules[0]["role_declarations"]["media-provider/v1"] =
                        json!({"engines":[engine("transcribe")]})
                }
            }
        }
        modules
    }
}

fn params(version: Version, kind: &str) -> Value {
    json!({"consumer":version.consumer(),"kind":kind,"requirements":{},"privacy":"remote_allowed"})
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
    credentials.catalog_snapshot = Some(
        crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({"catalog_version":41,"resolved_at_ms":NOW,
            "models":{"media-vendor/fixture-transcribe":{"existence":"present",
                "rate.input_audio.per_second":{"state":"priced","currency":"USD",
                    "units":4500000,"exponent":9,"per":60,
                    "increment":{"kind":"unknown"},"minimum_quantity":{"kind":"unknown"}}}}}))
            .unwrap(),
        )
        .unwrap(),
    );
    credentials
}

fn fixture(version: Version) -> Arc<Harness> {
    let harness = Harness::new("select-engine-vectors");
    super::consumers::replace_catalog(&version.catalog(true));
    harness.set_credentials(credentials(vec![key()], true, NOW));
    harness
}

#[derive(Default)]
struct Samples(BTreeMap<String, Vec<u8>>, bool);

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
        self.capture_method(
            name,
            harness,
            route,
            "route.select_engine",
            params,
            code,
            filter,
        )
        .await
    }

    async fn capture_method(
        &mut self,
        name: &str,
        harness: &Harness,
        route: &Route,
        method: &str,
        params: &Value,
        code: Option<&str>,
        filter: Option<&str>,
    ) -> Value {
        let frame = route.request_frame(method, params);
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
            if self.1 && method == "route.select_engine" {
                let mut old = params.clone();
                old["consumer"] = json!("phone.media");
                let refused = route.call(method, &old).await;
                assert_eq!(refused.0, FrameType::Error, "{name}: {refused:?}");
                assert_eq!(refused.1["code"], "unknown_consumer", "{name}: {refused:?}");
                assert_eq!(refused.1["detail"], json!({"consumer":"phone.media"}));
            }
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

    fn verify(self, version: Version) {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-vectors")
            .join(version.directory());
        let update = std::env::var_os(match version {
            Version::V1 => "UPDATE_ROUTE_SELECT_ENGINE_VECTORS",
            Version::V2 => "UPDATE_ROUTE_SELECT_ENGINE_V2_VECTORS",
        })
        .is_some();
        let mut sums = String::new();
        for (name, bytes) in &self.0 {
            let path = directory.join(name);
            if matches!(version, Version::V1) {
                assert!(!update, "archived V1 samples must not be rewritten");
                let archived = std::fs::read(&path).unwrap();
                let mut expected: Value = serde_json::from_slice(&archived).unwrap();
                let actual: Value = serde_json::from_slice(bytes).unwrap();
                if expected["request"]["params"]["consumer"] == "phone.media" {
                    expected["request"]["params"]["consumer"] = json!(version.consumer());
                }
                assert_eq!(actual["request"], expected["request"], "{name}");
                if matches!(
                    name.as_str(),
                    "caller-direct.json" | "caller-not-owner.json"
                ) {
                    assert_eq!(
                        actual["reply"],
                        json!({
                            "code":"consumer_not_owned",
                            "message":"the caller and its scope do not own consumer \"colliculus.voice_message\"",
                            "detail":{"consumer":"colliculus.voice_message"}
                        }),
                        "{name}"
                    );
                } else {
                    // Requests use the current consumer; all other answer bytes
                    // remain the exact archived protocol answer, including order.
                    let answer = |pair: &[u8]| {
                        let marker = b",\"reply\":";
                        let start = pair
                            .windows(marker.len())
                            .position(|w| w == marker)
                            .unwrap()
                            + marker.len();
                        pair[start..pair.len() - 1].to_vec()
                    };
                    assert_eq!(answer(bytes), answer(&archived), "{name}");
                }
                sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&archived)));
                continue;
            }
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
        let files: BTreeSet<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        let expected: BTreeSet<_> = self
            .0
            .keys()
            .cloned()
            .chain(["README.md".into(), "SHA256SUMS".into()])
            .collect();
        assert_eq!(files, expected, "extra or missing vector files");
    }
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    capture_vectors(Version::V1).await;
}

#[tokio::test]
async fn serving_bytes_match_v2_vectors() {
    capture_vectors(Version::V2).await;
}

async fn capture_vectors(version: Version) {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let mut samples = Samples::default();
    samples.1 = matches!(version, Version::V1);
    let harness = fixture(version);
    let route = harness.bind(reserved("callosum"), None).await;
    let mut first = Value::Null;
    for kind in KINDS {
        // A kind-bound voice-message consumer cannot request the other v1 media kinds.
        if matches!(version, Version::V2) && kind != "transcribe" {
            continue;
        }
        let reply = samples
            .capture(
                &format!("accepted-{kind}"),
                &harness,
                &route,
                &params(version, kind),
                None,
                None,
            )
            .await;
        assert_eq!(
            reply["result"]["engine"]["model"],
            format!("fixture-{kind}")
        );
        assert_eq!(reply["result"]["engine"]["module"], version.module());
        if kind == "transcribe" {
            first = reply;
        }
    }
    let repeated = samples
        .capture(
            "repeated-transcribe",
            &harness,
            &route,
            &params(version, "transcribe"),
            None,
            None,
        )
        .await;
    assert_eq!(first["result"]["engine"], repeated["result"]["engine"]);
    assert_ne!(
        first["result"]["decision_id"],
        repeated["result"]["decision_id"]
    );
    let mut local = params(version, "transcribe");
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

    if matches!(version, Version::V2) {
        let executor = harness.bind(reserved("colliculus"), None).await;
        let mut admission = json!({
            "admission_id":"before-expiry","conversation_key":"voice-message",
            "provider":PROVIDER,"model":"fixture-transcribe",
            "decision_id":first["result"]["decision_id"],"hold_wait_max_s":0
        });
        // The provider executor, not the selecting phone, redeems a remote decision.
        // A successful admission before expiry rules out a slot or owner refusal.
        let admitted = executor.call("route.account_for", &admission).await;
        assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
        assert_eq!(admitted.1["result"]["admission"]["state"], "reserved");
        let mut expired = credentials(vec![key()], true, NOW);
        expired.engine_clock_offset_ms = 600_000;
        harness.set_credentials(expired);
        // Existing admissions replay after expiry; only a new admission is refused.
        assert_eq!(
            executor.call("route.account_for", &admission).await,
            admitted
        );
        admission["admission_id"] = json!("at-expiry");
        samples
            .capture_method(
                "decision-expired",
                &harness,
                &executor,
                "route.account_for",
                &admission,
                Some("unknown_decision"),
                None,
            )
            .await;
        harness.set_credentials(credentials(vec![key()], true, NOW));
    }

    let unknown_kind = match version {
        Version::V1 => ("unknown-kind", "no_engine", Some("kind")),
        Version::V2 => ("kind-mismatch", "invalid_request", None),
    };
    for (name, patch, code, filter) in [
        (
            unknown_kind.0,
            json!({"kind":"future_kind"}),
            unknown_kind.1,
            unknown_kind.2,
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
        let mut request = params(version, "transcribe");
        for (key, value) in patch.as_object().unwrap() {
            request[key] = value.clone();
        }
        let reply = samples
            .capture(name, &harness, &route, &request, Some(code), filter)
            .await;
        if name == "kind-mismatch" {
            assert_eq!(reply["detail"]["field"], "kind");
        }
    }
    let foreign = harness.bind(reserved("stranger"), None).await;
    samples
        .capture(
            "caller-not-owner",
            &harness,
            &foreign,
            &params(version, "transcribe"),
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
            &params(version, "transcribe"),
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
            &params(version, "transcribe"),
            Some("flow_scope_not_supported"),
            None,
        )
        .await;

    super::consumers::replace_catalog(&version.catalog(false));
    samples
        .capture(
            "no-engines",
            &harness,
            &route,
            &params(version, "transcribe"),
            Some("no_engine"),
            Some("kind"),
        )
        .await;

    capture_unavailable(&mut samples, version).await;
    samples.verify(version);
}

async fn capture_unavailable(samples: &mut Samples, version: Version) {
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
        let harness = fixture(version);
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
                    crate::routing_v1::native_fixture_sources::failure_backoff(
                        store,
                        route_key,
                        until as f64,
                        None,
                    );
                    // Serving reads the published account snapshot. Load the
                    // new native fixture input before capturing the wire reply.
                    store.reload_account_state_for_test().unwrap();
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
                &params(version, "transcribe"),
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
        let harness = fixture(version);
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
                &params(version, "transcribe"),
                Some("no_engine"),
                Some("reachability"),
            )
            .await;
    }

    let harness = fixture(version);
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
            &params(version, "transcribe"),
            Some("store_error"),
            None,
        )
        .await;
}
