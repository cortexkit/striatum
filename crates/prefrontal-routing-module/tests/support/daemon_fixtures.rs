//! Stand-ins for the two services routing reads before it will select: the
//! Claustrum credential listing and the Fusiform model catalog. Every
//! production daemon runs both, and routing refuses `route.select` until its
//! first billing-plane load.
//!
//! Shared source: routing's real-daemon tests and prefrontal-core's routing
//! e2e both include this file by path, so the two rigs serve routing the same
//! catalog. Each rig registers the services with [`serve_routing_fixtures`]
//! and then waits for routing's health to report both reached, with its own
//! daemon helpers.

#![allow(dead_code)]

use subc_client_rs::{async_trait, HandlerOutcome, ModuleHandle, ModuleHandler, RequestCtx};
use subc_protocol::manifest::{
    Concurrency, ExecutionMode, ManagementOperation, ManagementOperationKind, ModuleManifest,
    ProviderRole, Tool, TrustTier,
};

/// The registered stand-ins; the services stop on drop.
pub struct RoutingFixtures {
    _credentials: ModuleHandle,
    credential_service: tokio::task::JoinHandle<()>,
    _catalog: ModuleHandle,
    catalog_service: tokio::task::JoinHandle<()>,
}

impl Drop for RoutingFixtures {
    fn drop(&mut self) {
        self.credential_service.abort();
        self.catalog_service.abort();
    }
}

/// Register the stand-in Claustrum and Fusiform on the daemon behind
/// `connection_file`. Routing reads them on its next credential and catalog
/// refresh; a rig restarts routing afterwards so the first reads find them.
pub async fn serve_routing_fixtures(connection_file: &std::path::Path) -> RoutingFixtures {
    let (credentials, credential_service) = serve_credential_metadata(connection_file).await;
    let (catalog, catalog_service) = serve_fusiform_catalog(connection_file).await;
    RoutingFixtures {
        _credentials: credentials,
        credential_service,
        _catalog: catalog,
        catalog_service,
    }
}

/// Write the routing config an operator runs with into `config_dir` (the
/// rig's `$XDG_CONFIG_HOME/cortexkit`, where routing reads
/// `prefrontal-routing.jsonc`).
///
/// Routing has no built-in runners or consumers: without this file it knows
/// no runner to start a route on, so every executor selection answers
/// `no_available_model`, and a routing/v1 select for a consumer no module
/// declares answers `unknown_consumer`. The file is produced the way an
/// operator produces theirs, by the module's own `config-convert` command on
/// an empty older document, so it carries the converter's defaults: the
/// `broca` runner through the vault and the standard consumer names.
pub fn write_operator_routing_config(routing_bin: &std::path::Path, config_dir: &std::path::Path) {
    std::fs::create_dir_all(config_dir).unwrap();
    let input = config_dir.join("prefrontal-routing.legacy.json");
    std::fs::write(&input, r#"{"model_routing": {}}"#).unwrap();
    let output = config_dir.join("prefrontal-routing.jsonc");
    // The converter reads and writes only the two files it is given.
    let status = std::process::Command::new(routing_bin)
        .env_clear()
        .arg("config-convert")
        .arg(&input)
        .arg(&output)
        .status()
        .expect("run the routing config converter");
    assert!(status.success(), "config-convert failed: {status}");
    std::fs::remove_file(&input).unwrap();
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&output).unwrap()).unwrap();
    assert_eq!(written["runners"][0]["module"], "broca", "{written}");
    assert!(written["consumers"].get("mason").is_some(), "{written}");
}

/// Whether routing's `supervisor.health_probe` answer says both stand-ins
/// were reached: routing is ready to select.
pub fn routing_health_reached_fixtures(health: &serde_json::Value) -> bool {
    let broca = &health["metrics"]["brocaReachability"];
    health["status"] == "ok"
        && broca["view"] == "routing-health-fixture-v1"
        && broca["lastReachedAtMs"].is_number()
        && health["metrics"]["fusiformCatalog"]["reach"] == "reached"
}

pub struct FusiformCatalog;

pub const FIXTURE_PROVIDERS: &[&str] = &["anthropic", "openai", "xai", "ollama-cloud"];

impl FusiformCatalog {
    fn seed_models() -> serde_json::Value {
        // Both daemon rigs read the same captured continuation page. Routing's
        // integration test compares every served row with its bundled seed facts.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/routing-wire/catalog-seed.json");
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    pub fn reply(request: &serde_json::Value) -> serde_json::Value {
        let golden: serde_json::Value =
            serde_json::from_str(include_str!("../../fixtures/catalog-paged.json")).unwrap();
        let mut page = golden["response"].clone();
        match request["name"].as_str().unwrap() {
            "catalog.get" => {
                if request["arguments"].get("auth_method").is_some() {
                    let provider = request["arguments"]["provider_id"].as_str().unwrap();
                    assert!(FIXTURE_PROVIDERS.contains(&provider));
                    assert_eq!(
                        request["arguments"],
                        serde_json::json!({"include_retired":true,"provider_id":provider,"auth_method":"apikey"})
                    );
                    // No credential-specific rates are served here. no_coverage
                    // lets API-key routes use the catalog's ordinary model prices.
                    return serde_json::json!({"code":"no_coverage","message":"No billing plane declared in the seed fixture"});
                }
                let token = request["arguments"]["page_token"].as_str();
                let mut expected = serde_json::json!({"include_retired":true,"paged":true});
                if let Some(token) = token {
                    assert_eq!(token, page["next_page_token"].as_str().unwrap());
                    expected["page_token"] = serde_json::json!(token);
                    // Serve the seed's prices for selection. The first page's
                    // 000-page/m must not repeat: the client rejects duplicate models.
                    page["models"] = Self::seed_models();
                    page.as_object_mut().unwrap().remove("next_page_token");
                }
                assert_eq!(request["arguments"], expected);
                serde_json::json!({"result":page})
            }
            "catalog.status" => {
                assert_eq!(request["arguments"], serde_json::json!({"polls":1}));
                serde_json::json!({"result":{"catalog_version":page["catalog_version"]}})
            }
            // Routing reads subscription plan prices on every economics refresh.
            // Serve Fusiform's own recorded reply (the pinned served payloads).
            "plan.prices" => {
                assert_eq!(request["arguments"], serde_json::json!({}));
                let served: serde_json::Value =
                    serde_json::from_str(include_str!("../../fixtures/served-payloads.json"))
                        .unwrap();
                let recorded = &served["plan.prices — every curated row"];
                assert_eq!(recorded["request"], *request);
                serde_json::json!({"result":recorded["response"]})
            }
            other => panic!("unexpected Fusiform fixture tool: {other}"),
        }
    }
}

#[async_trait]
impl ModuleHandler for FusiformCatalog {
    async fn handle(&self, _ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request = serde_json::from_slice(&body).unwrap();
        HandlerOutcome::Response(serde_json::to_vec(&Self::reply(&request)).unwrap())
    }
}

async fn serve_fusiform_catalog(
    connection_file: &std::path::Path,
) -> (ModuleHandle, tokio::task::JoinHandle<()>) {
    let tools = ["catalog.get", "catalog.status", "plan.prices"]
        .into_iter()
        .map(|name| Tool {
            name: name.into(),
            description: None,
            execution_mode: ExecutionMode::Pure,
            schema: serde_json::json!({"type":"object"}),
        })
        .collect();
    let manifest = ModuleManifest::builder("fusiform", "0.0.0")
        .trust_tier(Some(TrustTier::FirstParty))
        .protocol_ver(subc_protocol::PROTOCOL_VERSION)
        .provides(vec![ProviderRole::ToolProvider {
            tools,
            identity_scope: Vec::new(),
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    let (handle, serving) =
        subc_client_rs::serve_with_handle(connection_file, manifest, FusiformCatalog)
            .await
            .expect("register fixture Fusiform catalog");
    (
        handle,
        tokio::spawn(async move {
            serving.await.expect("serve fixture Fusiform catalog");
        }),
    )
}

struct CredentialMetadata;

#[async_trait]
impl ModuleHandler for CredentialMetadata {
    async fn handle(&self, _ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Insula, which the both-modules rig also runs, calls Claustrum's
        // auth.watch_scoped. This stand-in models only the listing routing reads and
        // refuses every other operation, so Insula sees an unavailable credential
        // service instead of a panicked handler that never answers.
        if request["method"] != "credential.list_scoped" {
            return HandlerOutcome::Error {
                code: "unknown_method".into(),
                message: format!(
                    "the stand-in Claustrum does not serve {}",
                    request["method"]
                ),
            };
        }
        assert_eq!(request["params"], serde_json::json!({}));
        let credentials: Vec<_> = FIXTURE_PROVIDERS
            .iter()
            .map(|provider| {
                serde_json::json!({
                    "id":format!("apikey:{provider}:routing-health-fixture"),
                    // Claustrum's row shape for an API key (see
                    // fixtures/claustrum-list-scoped.json): type and
                    // auth_method are both "apikey".
                    "categories":["llm-provider"], "type":"apikey", "auth_method":"apikey",
                    "serves":[provider], "provider_ids":[provider], "state":"active",
                    "record_version":1, "operations":["get"],
                })
            })
            .collect();
        HandlerOutcome::Response(
            serde_json::to_vec(&serde_json::json!({
                "result": {
                    "view": "routing-health-fixture-v1",
                    "credentials": credentials
                }
            }))
            .unwrap(),
        )
    }
}

async fn serve_credential_metadata(
    connection_file: &std::path::Path,
) -> (ModuleHandle, tokio::task::JoinHandle<()>) {
    let manifest = ModuleManifest::builder("claustrum", "0.0.0")
        .trust_tier(Some(TrustTier::FirstParty))
        .protocol_ver(subc_protocol::PROTOCOL_VERSION)
        .provides(vec![ProviderRole::ManagementSurface {
            concurrency: Concurrency::ModuleManaged,
            operations: vec![ManagementOperation {
                name: "credential.list_scoped".into(),
                kind: ManagementOperationKind::Query,
                description: None,
            }],
            config_schema: serde_json::json!({"type":"object"}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
        }])
        .build();
    let (handle, serving) =
        subc_client_rs::serve_with_handle(connection_file, manifest, CredentialMetadata)
            .await
            .expect("register fixture credential metadata service");
    let service = tokio::spawn(async move {
        serving.await.expect("serve fixture credential metadata");
    });
    (handle, service)
}
