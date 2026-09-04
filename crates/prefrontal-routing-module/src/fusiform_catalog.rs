//! until this consumer is live, a model swap in opencode.jsonc silently strips phone attachments for that session until someone upserts a model_facts row (script/upsert-model-fact.ts); the next occurrence is NOT a phone bug.
//!
//! Fusiform is authoritative only for existence, rates, limits, and capabilities.
//! Routing scores and policy remain operator-owned. The merge boundary is fenced by
//! [`FusiformFactUpdate`], which cannot represent scores, variants, exclusions,
//! speed multipliers, prompt defaults, AA indices, aliases, or lifecycle dates.
//!
//! | field group | default authority | operator override |
//! | --- | --- | --- |
//! | vision, tool use, context/output limits, output price/tier | Fusiform | an explicit override of that same field wins |
//! | Elo, EQ, speed, variants, exclusions, multipliers, prompt defaults | operator | always retained; Fusiform cannot represent them |
//!
//! Wire decoding is isolated here and pinned to the producer-real fixture copied
//! from `fusiform/crates/fusiform-module/fixtures/served-payloads.json` at Fusiform
//! commit `cab7afa`. `fusiform-protocol` has one manifest migration pending, so one
//! re-pin is expected; no other module should depend on these private wire structs.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate, SqliteRouteStateStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{io::AsyncWriteExt, net::TcpStream};

const FUSIFORM_MODULE_ID: &str = "fusiform";
const CATALOG_GET: &str = "catalog.get";
const CATALOG_STATUS: &str = "catalog.status";
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);
pub const RETRY_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub enum FusiformCatalogError {
    Message(String),
    Json(serde_json::Error),
}

impl fmt::Display for FusiformCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => formatter.write_str(message),
            Self::Json(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for FusiformCatalogError {}

fn error(message: impl Into<String>) -> FusiformCatalogError {
    FusiformCatalogError::Message(message.into())
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct CatalogGetArguments {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogGetRequest {
    name: &'static str,
    arguments: CatalogGetArguments,
}

impl CatalogGetRequest {
    fn all_current() -> Self {
        Self {
            name: CATALOG_GET,
            arguments: CatalogGetArguments::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogStatusArguments {
    polls: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogStatusRequest {
    name: &'static str,
    arguments: CatalogStatusArguments,
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogGetReply {
    catalog_version: i64,
    models: BTreeMap<String, CatalogModelFacts>,
    resolved_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogStatusReply {
    catalog_version: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogModelFacts {
    #[serde(rename = "existence")]
    existence: String,
    #[serde(rename = "capability.attachment")]
    attachment: Option<bool>,
    #[serde(rename = "capability.input_modalities")]
    input_modalities: Option<Vec<String>>,
    #[serde(rename = "capability.tool_call")]
    tool_call: Option<bool>,
    #[serde(rename = "limit.context")]
    context: Option<u64>,
    #[serde(rename = "limit.output")]
    output: Option<u64>,
    #[serde(rename = "rate.output")]
    output_rate: Option<CatalogRate>,
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogRate {
    currency: String,
    exponent: u32,
    state: String,
    units: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct CatalogSnapshot {
    cursor: FusiformCatalogCursor,
    facts: Vec<FusiformFactUpdate>,
}

fn output_cost(rate: Option<&CatalogRate>) -> Option<f64> {
    let rate = rate?;
    if rate.state != "priced" || rate.currency != "USD" || rate.units < 0 || rate.exponent > 18 {
        return None;
    }
    Some(rate.units as f64 / 10_f64.powi(rate.exponent as i32))
}

fn cost_tier(cost_per_mtok_out: Option<f64>) -> Option<f64> {
    let cost = cost_per_mtok_out?;
    Some(if cost <= 0.5 {
        0.0
    } else if cost <= 3.0 {
        1.0
    } else if cost <= 5.0 {
        2.0
    } else if cost <= 15.0 {
        3.0
    } else if cost <= 30.0 {
        4.0
    } else if cost <= 75.0 {
        5.0
    } else {
        6.0
    })
}

fn decode_catalog_get(bytes: &[u8]) -> Result<CatalogSnapshot, FusiformCatalogError> {
    let reply: CatalogGetReply =
        serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)?;
    let mut facts = Vec::with_capacity(reply.models.len());
    for (served_key, model) in reply.models {
        let (provider_id, model_id) = served_key
            .split_once('/')
            .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
            .ok_or_else(|| {
                error(format!(
                    "catalog.get model key is not provider/model: {served_key}"
                ))
            })?;
        let present = match model.existence.as_str() {
            "present" => true,
            "absent" => false,
            other => return Err(error(format!("unknown Fusiform existence value: {other}"))),
        };
        let supports_vision = model
            .input_modalities
            .as_ref()
            .map(|modalities| modalities.iter().any(|value| value == "image"))
            .or(model.attachment);
        let cost_per_mtok_out = output_cost(model.output_rate.as_ref());
        facts.push(FusiformFactUpdate {
            provider_id: provider_id.to_string(),
            model_id: model_id.to_string(),
            present,
            context_window: model.context.map(|value| value as f64),
            max_output_tokens: model.output,
            cost_tier: cost_tier(cost_per_mtok_out),
            cost_per_mtok_out,
            supports_vision,
            supports_tool_use: model.tool_call,
        });
    }
    Ok(CatalogSnapshot {
        cursor: FusiformCatalogCursor {
            catalog_version: reply.catalog_version,
            resolved_at_ms: reply.resolved_at_ms,
        },
        facts,
    })
}

fn decode_catalog_status(bytes: &[u8]) -> Result<CatalogStatusReply, FusiformCatalogError> {
    serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)
}

/// Short-lived Fusiform connection. Callers create one per bootstrap/poll cycle
/// and close it before sleeping, so the consumer never owns an idle socket.
pub struct FusiformCatalogConsumer {
    stream: TcpStream,
    route_channel: u16,
    route_epoch: u32,
    next_corr: u64,
}

impl FusiformCatalogConsumer {
    pub async fn connect(
        connection_file_path: &Path,
        project_root: &Path,
    ) -> Result<Self, FusiformCatalogError> {
        let connection = connection_file::read(connection_file_path)
            .map_err(|reason| error(format!("reading connection file: {reason}")))?;
        let endpoint = connection
            .endpoints
            .first()
            .ok_or_else(|| error("connection file has no endpoints"))?;
        let address = format!("{}:{}", endpoint.host, endpoint.port);
        let stream = tokio::time::timeout(SETUP_TIMEOUT, async {
            let mut stream = TcpStream::connect(&address)
                .await
                .map_err(|reason| error(format!("connect {address}: {reason}")))?;
            authenticate_client(&mut stream, &connection, Duration::from_secs(2))
                .await
                .map_err(|reason| error(format!("authenticate: {reason}")))?;
            Ok::<_, FusiformCatalogError>(stream)
        })
        .await
        .map_err(|_| error("timed out setting up Fusiform connection"))??;
        let mut consumer = Self {
            stream,
            route_channel: 0,
            route_epoch: 0,
            next_corr: 1,
        };
        (consumer.route_channel, consumer.route_epoch) = consumer.route_open(project_root).await?;
        Ok(consumer)
    }

    fn corr(&mut self) -> u64 {
        let corr = self.next_corr;
        self.next_corr = self.next_corr.wrapping_add(1).max(1);
        corr
    }

    async fn route_open(
        &mut self,
        project_root: &Path,
    ) -> Result<(u16, u32), FusiformCatalogError> {
        let corr = self.corr();
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            corr,
            serde_json::to_vec(&json!({
                "op": "route.open",
                "target": { "kind": "management_surface", "module_id": FUSIFORM_MODULE_ID },
                "identity": {
                    "project_root": project_root,
                    "harness": "prefrontal-routing",
                    "session": "fusiform-catalog-consumer",
                },
                "config": [],
            }))
            .map_err(FusiformCatalogError::Json)?,
        )
        .map_err(|reason| error(reason.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|reason| error(reason.to_string()))?;
        let reply = self.read_reply(0, 0, corr).await?;
        if reply.header.ty == FrameType::Error {
            return Err(error(format!(
                "route.open(fusiform) failed: {}",
                String::from_utf8_lossy(&reply.body)
            )));
        }
        let value: Value =
            serde_json::from_slice(&reply.body).map_err(FusiformCatalogError::Json)?;
        let channel = value["route_channel"]
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| error("route.open returned no valid route_channel"))?;
        let epoch = value["route_epoch"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| error("route.open returned no valid route_epoch"))?;
        Ok((channel, epoch))
    }

    async fn call(&mut self, body: Vec<u8>) -> Result<Vec<u8>, FusiformCatalogError> {
        let corr = self.corr();
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            self.route_channel,
            self.route_epoch,
            corr,
            body,
        )
        .map_err(|reason| error(reason.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|reason| error(reason.to_string()))?;
        let reply = self
            .read_reply(self.route_channel, self.route_epoch, corr)
            .await?;
        if reply.header.ty == FrameType::Error {
            return Err(error(String::from_utf8_lossy(&reply.body).to_string()));
        }
        Ok(reply.body)
    }

    async fn close(&mut self) -> Result<(), FusiformCatalogError> {
        let frame = Frame::build(
            FrameType::Goodbye,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            0,
            Vec::new(),
        )
        .map_err(|reason| error(reason.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|reason| error(format!("sending Fusiform goodbye: {reason}")))?;
        self.stream
            .shutdown()
            .await
            .map_err(|reason| error(format!("closing Fusiform connection: {reason}")))
    }

    async fn read_reply(
        &mut self,
        channel: u16,
        epoch: u32,
        corr: u64,
    ) -> Result<Frame, FusiformCatalogError> {
        tokio::time::timeout(REPLY_TIMEOUT, async {
            loop {
                let frame = read_frame(&mut self.stream)
                    .await
                    .map_err(|reason| error(reason.to_string()))?
                    .ok_or_else(|| error("Fusiform connection closed before reply"))?;
                if frame.header.channel == channel
                    && frame.header.epoch == epoch
                    && frame.header.corr == corr
                    && matches!(frame.header.ty, FrameType::Response | FrameType::Error)
                {
                    return Ok(frame);
                }
            }
        })
        .await
        .map_err(|_| error("timed out waiting for Fusiform reply"))?
    }
}

trait CatalogWire {
    async fn catalog_get(&mut self) -> Result<CatalogSnapshot, FusiformCatalogError>;
    async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError>;
    async fn close(&mut self) -> Result<(), FusiformCatalogError>;
}

impl CatalogWire for FusiformCatalogConsumer {
    async fn catalog_get(&mut self) -> Result<CatalogSnapshot, FusiformCatalogError> {
        let body = serde_json::to_vec(&CatalogGetRequest::all_current())
            .map_err(FusiformCatalogError::Json)?;
        let reply = self.call(body).await?;
        decode_catalog_get(&reply)
    }

    async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError> {
        let body = serde_json::to_vec(&CatalogStatusRequest {
            name: CATALOG_STATUS,
            arguments: CatalogStatusArguments { polls: 1 },
        })
        .map_err(FusiformCatalogError::Json)?;
        let reply = self.call(body).await?;
        decode_catalog_status(&reply)
    }

    async fn close(&mut self) -> Result<(), FusiformCatalogError> {
        FusiformCatalogConsumer::close(self).await
    }
}

#[derive(Debug, Clone)]
pub struct CatalogReachHealth {
    poll_ms: u64,
    last_reached_at_ms: Option<u64>,
    last_error: Option<String>,
}

impl CatalogReachHealth {
    pub fn new(poll_ms: u64) -> Self {
        Self {
            poll_ms,
            last_reached_at_ms: None,
            last_error: None,
        }
    }

    pub fn set_poll_ms(&mut self, poll_ms: u64) {
        self.poll_ms = poll_ms;
    }

    fn note_reached(&mut self, now_ms: u64) {
        self.last_reached_at_ms = Some(now_ms);
        self.last_error = None;
    }

    fn note_error(&mut self, reason: impl ToString) {
        self.last_error = Some(reason.to_string());
    }

    pub fn snapshot(&self, now_ms: u64) -> Value {
        let stale_after_ms = self.poll_ms.saturating_mul(2);
        match self.last_reached_at_ms {
            None => json!({
                "reach": "never-reached",
                "lastError": self.last_error,
            }),
            Some(reached_at_ms) => {
                let age_ms = now_ms.saturating_sub(reached_at_ms);
                let reach = if age_ms > stale_after_ms {
                    format!("stale({age_ms}ms)")
                } else {
                    "reached".to_string()
                };
                json!({
                    "reach": reach,
                    "ageMs": age_ms,
                    "lastReachedAtMs": reached_at_ms,
                    "lastError": self.last_error,
                })
            }
        }
    }
}

pub type SharedCatalogHealth = Arc<Mutex<CatalogReachHealth>>;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CycleOutcome {
    get_calls: usize,
}

async fn bootstrap_cycle<C: CatalogWire>(
    client: &mut C,
    store: &SqliteRouteStateStore,
    health: &SharedCatalogHealth,
) -> Result<CycleOutcome, FusiformCatalogError> {
    let snapshot = client.catalog_get().await?;
    store
        .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
        .map_err(|reason| error(format!("apply catalog.get: {reason}")))?;
    if let Ok(mut health) = health.lock() {
        health.note_reached(now_ms());
    }
    client.close().await?;
    Ok(CycleOutcome { get_calls: 1 })
}

async fn poll_cycle<C: CatalogWire>(
    client: &mut C,
    store: &SqliteRouteStateStore,
    health: &SharedCatalogHealth,
) -> Result<CycleOutcome, FusiformCatalogError> {
    let status = client.catalog_status().await?;
    if let Ok(mut health) = health.lock() {
        health.note_reached(now_ms());
    }
    let cursor = store
        .fusiform_catalog_cursor()
        .map_err(|reason| error(format!("read Fusiform cursor: {reason}")))?;
    let should_get = cursor
        .map(|cursor| status.catalog_version > cursor.catalog_version)
        .unwrap_or(true);
    let get_calls = if should_get {
        let snapshot = client.catalog_get().await?;
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .map_err(|reason| error(format!("apply catalog replay: {reason}")))?;
        1
    } else {
        0
    };
    client.close().await?;
    Ok(CycleOutcome { get_calls })
}

pub async fn catalog_refresh_loop(
    connection_file_path: PathBuf,
    store: Arc<SqliteRouteStateStore>,
    health: SharedCatalogHealth,
    poll_ms: u64,
) {
    let project_root = std::env::temp_dir().join("prefrontal-routing-fusiform-consumer");
    let _ = std::fs::create_dir_all(&project_root);
    let mut bootstrapped = false;
    loop {
        let result = match FusiformCatalogConsumer::connect(&connection_file_path, &project_root)
            .await
        {
            Ok(mut consumer) if bootstrapped => poll_cycle(&mut consumer, &store, &health).await,
            Ok(mut consumer) => bootstrap_cycle(&mut consumer, &store, &health).await,
            Err(reason) => Err(reason),
        };
        match result {
            Ok(_) => {
                bootstrapped = true;
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
            Err(reason) => {
                tracing::debug!(error = %reason, "Fusiform catalog refresh cycle failed");
                if let Ok(mut health) = health.lock() {
                    health.note_error(reason);
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    struct Rig {
        dir: PathBuf,
    }

    impl Rig {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "prefrontal-fusiform-{label}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }

        fn store(&self) -> SqliteRouteStateStore {
            SqliteRouteStateStore::open(&prefrontal_routing_store::dev_descriptor(
                "fusiform-catalog-test",
                self.dir.join("route.db").to_string_lossy().into_owned(),
            ))
            .unwrap()
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn fixture() -> Value {
        serde_json::from_str(include_str!("../fixtures/served-payloads.json")).unwrap()
    }

    fn get_reply(name: &str) -> Vec<u8> {
        serde_json::to_vec(&fixture()[name]["response"]).unwrap()
    }

    #[test]
    fn producer_real_served_payload_fixture_decodes_and_is_digest_pinned() {
        let digest = format!(
            "{:x}",
            Sha256::digest(include_bytes!("../fixtures/served-payloads.json"))
        );
        assert_eq!(
            digest,
            "8cd9a59b5dc45a2aebd90b5aee930dac0eb170144a87f8a0bf6289151d633050"
        );
        let snapshot = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        assert_eq!(snapshot.cursor.catalog_version, 0);
        assert_eq!(snapshot.cursor.resolved_at_ms, -1);
        assert_eq!(snapshot.facts.len(), 1);
        let fact = &snapshot.facts[0];
        assert_eq!(
            (fact.provider_id.as_str(), fact.model_id.as_str()),
            ("anthropic", "claude-sonnet-4-5")
        );
        assert_eq!(fact.supports_vision, Some(true));
        assert_eq!(fact.supports_tool_use, Some(true));
        assert_eq!(fact.context_window, Some(200_000.0));
        assert_eq!(fact.max_output_tokens, Some(64_000));
        assert_eq!(fact.cost_per_mtok_out, Some(15.0));
        assert_eq!(fact.cost_tier, Some(3.0));

        let status = decode_catalog_status(&get_reply("catalog.status")).unwrap();
        assert_eq!(status.catalog_version, 0);
    }

    #[test]
    fn bootstrap_creates_unscored_pair_with_payload_vision_without_defaulting_unknown() {
        let rig = Rig::new("bootstrap");
        let store = rig.store();
        let current = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        store
            .apply_fusiform_catalog(&current.facts, current.cursor)
            .unwrap();
        let row = store
            .get_catalog_facts()
            .unwrap()
            .into_iter()
            .find(|row| {
                row.fact.provider_id == "anthropic" && row.fact.model_id == "claude-sonnet-4-5"
            })
            .unwrap();
        assert_eq!(
            row.fact.score_provenance,
            prefrontal_routing_core::ScoreProvenance::Unscored
        );
        assert_eq!(row.fact.supports_vision, Some(true));
        assert_eq!(row.facts_source.as_deref(), Some("fusiform"));

        let unknown = decode_catalog_get(&get_reply(
            "catalog.get — a model whose reasoning capability is unknown",
        ))
        .unwrap();
        store
            .apply_fusiform_catalog(&unknown.facts, unknown.cursor)
            .unwrap();
        let unknown_row = store
            .get_catalog_facts()
            .unwrap()
            .into_iter()
            .find(|row| {
                row.fact.provider_id == "synthetic" && row.fact.model_id == "unknown-reasoning"
            })
            .unwrap();
        assert_eq!(
            unknown_row.fact.supports_vision, None,
            "unknown capability must not default true"
        );
    }

    #[derive(Clone)]
    struct FakeWire {
        status: CatalogStatusReply,
        snapshot: CatalogSnapshot,
        get_calls: usize,
        closed: bool,
    }

    impl CatalogWire for FakeWire {
        async fn catalog_get(&mut self) -> Result<CatalogSnapshot, FusiformCatalogError> {
            self.get_calls += 1;
            Ok(self.snapshot.clone())
        }

        async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError> {
            Ok(self.status.clone())
        }

        async fn close(&mut self) -> Result<(), FusiformCatalogError> {
            self.closed = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn cursor_unchanged_skips_get_and_advance_issues_exactly_one_get() {
        let rig = Rig::new("cursor");
        let store = rig.store();
        let initial = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        store
            .apply_fusiform_catalog(&initial.facts, initial.cursor)
            .unwrap();
        let health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));

        let mut unchanged = FakeWire {
            status: CatalogStatusReply { catalog_version: 0 },
            snapshot: initial.clone(),
            get_calls: 0,
            closed: false,
        };
        let outcome = poll_cycle(&mut unchanged, &store, &health).await.unwrap();
        assert_eq!(outcome.get_calls, 0);
        assert_eq!(unchanged.get_calls, 0);
        assert!(unchanged.closed);

        let mut advanced_snapshot = initial;
        advanced_snapshot.cursor.catalog_version = 1;
        let mut advanced = FakeWire {
            status: CatalogStatusReply { catalog_version: 1 },
            snapshot: advanced_snapshot,
            get_calls: 0,
            closed: false,
        };
        let outcome = poll_cycle(&mut advanced, &store, &health).await.unwrap();
        assert_eq!(outcome.get_calls, 1);
        assert_eq!(advanced.get_calls, 1);
        assert!(advanced.closed);
        assert_eq!(
            store
                .fusiform_catalog_cursor()
                .unwrap()
                .unwrap()
                .catalog_version,
            1
        );
    }

    #[tokio::test]
    async fn health_reach_flips_on_first_successful_catalog_get() {
        let rig = Rig::new("health");
        let store = rig.store();
        let snapshot = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        let health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
        assert_eq!(
            health.lock().unwrap().snapshot(now_ms())["reach"],
            "never-reached"
        );
        let mut wire = FakeWire {
            status: CatalogStatusReply { catalog_version: 0 },
            snapshot,
            get_calls: 0,
            closed: false,
        };
        bootstrap_cycle(&mut wire, &store, &health).await.unwrap();
        assert_eq!(
            health.lock().unwrap().snapshot(now_ms())["reach"],
            "reached"
        );
        assert_eq!(wire.get_calls, 1);
    }
}
