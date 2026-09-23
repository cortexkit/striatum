//! until this consumer is live, a model swap in opencode.jsonc silently strips phone attachments for that session until someone upserts a model_facts row (script/upsert-model-fact.ts); the next occurrence is NOT a phone bug.
//!
//! Fusiform is authoritative only for existence, rates, limits, and capabilities.
//! Routing scores and policy remain operator-owned. The merge boundary is fenced by
//! [`FusiformFactUpdate`], which cannot represent scores, variants, exclusions,
//! speed multipliers, prompt defaults, AA indices, aliases, or lifecycle dates.
//!
//! | field group | default authority | operator override |
//! | --- | --- | --- |
//! | vision, tool use, context/output limits, rates/output tier | Fusiform | an explicit override of that same field wins |
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

use crate::winner_ladder::{self, LadderSources};

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
struct CatalogGetArguments {
    /// Fusiform serves only present models unless asked, and a model it has
    /// retired is then simply missing from the reply. A missing model updates
    /// nothing, so a fact row for it stayed present-and-routable after the
    /// upstream withdrew it: the whole kimi-for-coding provider went absent on
    /// 2026-09-18 and its scored seat kept being rostered while Broca refused
    /// every open. Retired models are requested so their absence is written.
    include_retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogGetRequest {
    name: &'static str,
    arguments: CatalogGetArguments,
}

impl CatalogGetRequest {
    fn all_with_retired() -> Self {
        Self {
            name: CATALOG_GET,
            arguments: CatalogGetArguments {
                include_retired: true,
            },
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
    #[serde(rename = "rate.input")]
    input_rate: Option<CatalogRate>,
    #[serde(rename = "rate.cache_read")]
    cache_read_rate: Option<CatalogRate>,
    #[serde(rename = "rate.cache_write")]
    cache_write_rate: Option<CatalogRate>,
    #[serde(rename = "rate.output")]
    output_rate: Option<CatalogRate>,
    #[serde(rename = "rate.reasoning")]
    reasoning_rate: Option<CatalogRate>,
}

/// Fusiform `RateValue`, tagged by `state` (fusiform-core lib.rs): `priced`
/// carries an amount (currency/units/exponent), `stated_zero` and `unpriced`
/// carry no amount at all. The first live catalog page decoded with a flat
/// struct that required `currency` and failed on the first non-priced rate
/// (`missing field currency`), so the consumer never reached; this shape is
/// the producer's, not a guess.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum CatalogRate {
    Priced {
        currency: String,
        exponent: u32,
        units: i64,
    },
    StatedZero,
    Unpriced {
        #[serde(default)]
        #[allow(dead_code)]
        reason: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct CatalogSnapshot {
    cursor: FusiformCatalogCursor,
    facts: Vec<FusiformFactUpdate>,
}

/// What a served catalog rate yields, or why it yields no price.
///
/// A bare `None` made three different situations identical: the producer served
/// no rate at all, the producer said "unpriced", and this consumer REFUSED to
/// decode a rate it was given. Only the third is a consumer defect, and it is the
/// one that must never hide among the other two.
#[derive(Debug, Clone, PartialEq)]
enum OutputPrice {
    /// Dollars per million tokens for the catalog field being decoded.
    Known(f64),
    /// Nothing to decode: the rate key is absent or has an explicit `unpriced`
    /// state. Checked in Fusiform's own store on 2026-09-06 for ollama-cloud: it
    /// holds existence, limits and capabilities for those models and no price
    /// records because the source catalog publishes none. These rates are
    /// genuinely absent rather than mis-decoded.
    NotPublished,
    /// A rate WAS served and this consumer declined it: a currency it cannot
    /// convert, a negative amount, or an exponent outside the range it can scale.
    /// Silently dropping these would read exactly like "no price published".
    Refused(String),
}

fn rate_cost(rate: Option<&CatalogRate>) -> OutputPrice {
    let Some(rate) = rate else {
        return OutputPrice::NotPublished;
    };
    match rate {
        CatalogRate::Priced {
            currency,
            exponent,
            units,
        } => {
            if currency != "USD" {
                return OutputPrice::Refused(format!(
                    "rate is denominated in {currency}; this consumer converts no currency"
                ));
            }
            if *units < 0 {
                return OutputPrice::Refused(format!("rate has negative units ({units})"));
            }
            if *exponent > 18 {
                return OutputPrice::Refused(format!(
                    "rate exponent {exponent} exceeds the scale this consumer can apply"
                ));
            }
            OutputPrice::Known(*units as f64 / 10_f64.powi(*exponent as i32))
        }
        // A stated zero is a real price of zero; whether zero is "free" is
        // routing policy downstream, and it must not read as absent.
        CatalogRate::StatedZero => OutputPrice::Known(0.0),
        CatalogRate::Unpriced { .. } => OutputPrice::NotPublished,
    }
}

/// Report one refusal per model and rate field per process. A refusal persists
/// across every poll while the upstream keeps publishing that rate, so logging it
/// per cycle would bury it; logging it never is how it stayed invisible.
fn report_refused_rate_once(served_key: &str, catalog_key: &str, detail: &str) {
    static REPORTED: std::sync::OnceLock<Mutex<std::collections::BTreeSet<String>>> =
        std::sync::OnceLock::new();
    let reported = REPORTED.get_or_init(|| Mutex::new(std::collections::BTreeSet::new()));
    let Ok(mut reported) = reported.lock() else {
        return;
    };
    if reported.insert(format!("{served_key}\u{1e}{catalog_key}\u{1e}{detail}")) {
        eprintln!(
            "[prefrontal-routing] fusiform served {catalog_key} for {served_key} but this consumer refused it ({detail}); that rate stays absent"
        );
    }
}

fn decoded_rate(served_key: &str, catalog_key: &str, rate: Option<&CatalogRate>) -> Option<f64> {
    match rate_cost(rate) {
        OutputPrice::Known(cost) => Some(cost),
        OutputPrice::NotPublished => None,
        OutputPrice::Refused(detail) => {
            report_refused_rate_once(served_key, catalog_key, &detail);
            None
        }
    }
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

/// The tool surface wraps every reply as `{"result": <payload>}` while the producer
/// fixture pins the bare payload; accept both so the fixture pin and the live wire
/// decode through one path.
fn unwrap_tool_result(bytes: &[u8]) -> Result<Value, FusiformCatalogError> {
    let value: Value = serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)?;
    Ok(match value {
        Value::Object(ref map) if map.len() == 1 && map.contains_key("result") => {
            value["result"].clone()
        }
        other => other,
    })
}

fn decode_catalog_get(bytes: &[u8]) -> Result<CatalogSnapshot, FusiformCatalogError> {
    let reply: CatalogGetReply =
        serde_json::from_value(unwrap_tool_result(bytes)?).map_err(FusiformCatalogError::Json)?;
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
        let cost_per_mtok_out =
            decoded_rate(&served_key, "rate.output", model.output_rate.as_ref());
        let input_rate = decoded_rate(&served_key, "rate.input", model.input_rate.as_ref());
        let cache_read_rate = decoded_rate(
            &served_key,
            "rate.cache_read",
            model.cache_read_rate.as_ref(),
        );
        let cache_write_rate = decoded_rate(
            &served_key,
            "rate.cache_write",
            model.cache_write_rate.as_ref(),
        );
        let reasoning_rate =
            decoded_rate(&served_key, "rate.reasoning", model.reasoning_rate.as_ref());
        facts.push(FusiformFactUpdate {
            provider_id: provider_id.to_string(),
            model_id: model_id.to_string(),
            present,
            context_window: model.context.map(|value| value as f64),
            max_output_tokens: model.output,
            cost_tier: cost_tier(cost_per_mtok_out),
            cost_per_mtok_out,
            input_rate,
            cache_read_rate,
            cache_write_rate,
            reasoning_rate,
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
    serde_json::from_value(unwrap_tool_result(bytes)?).map_err(FusiformCatalogError::Json)
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
                // Fusiform serves catalog.* on its TOOL surface (fusiform-module lib.rs
                // `provides: ToolProvider`); a management_surface bind answers
                // target_unavailable and the consumer sits at never-reached forever.
                "target": { "kind": "tool_provider", "module_id": FUSIFORM_MODULE_ID },
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
        let body = serde_json::to_vec(&CatalogGetRequest::all_with_retired())
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

/// Apply one fetched catalog snapshot plus the family rules that follow it. A
/// catalog apply can add, withdraw, or re-price a route, so when `ladder` is
/// given the winner ladder is compared around it and a move is announced.
fn apply_catalog_snapshot(
    store: &SqliteRouteStateStore,
    snapshot: &CatalogSnapshot,
    family_config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
    context: &str,
) -> Result<(), FusiformCatalogError> {
    store
        .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
        .map_err(|reason| error(format!("apply {context}: {reason}")))?;
    if let Some(path) = family_config_path {
        crate::config_overrides::apply_family_rules_from_config(store, path)
            .map_err(|reason| error(format!("apply catalog family rules: {reason}")))?;
    }
    if let Some(sources) = ladder {
        winner_ladder::announce(
            store,
            sources,
            &format!("Fusiform catalog apply ({context})"),
        );
    }
    Ok(())
}

async fn bootstrap_cycle<C: CatalogWire>(
    client: &mut C,
    store: &SqliteRouteStateStore,
    health: &SharedCatalogHealth,
    family_config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
) -> Result<CycleOutcome, FusiformCatalogError> {
    let snapshot = client.catalog_get().await?;
    apply_catalog_snapshot(store, &snapshot, family_config_path, ladder, "catalog.get")?;
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
    family_config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
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
        apply_catalog_snapshot(
            store,
            &snapshot,
            family_config_path,
            ladder,
            "catalog replay",
        )?;
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
    ladder_sources: LadderSources,
) {
    let project_root = std::env::temp_dir().join("prefrontal-routing-fusiform-consumer");
    let _ = std::fs::create_dir_all(&project_root);
    let mut bootstrapped = false;
    let mut last_logged_error: Option<String> = None;
    let family_config_path = crate::config_overrides::config_path();
    loop {
        let result =
            match FusiformCatalogConsumer::connect(&connection_file_path, &project_root).await {
                Ok(mut consumer) if bootstrapped => {
                    poll_cycle(
                        &mut consumer,
                        &store,
                        &health,
                        Some(&family_config_path),
                        Some(&ladder_sources),
                    )
                    .await
                }
                Ok(mut consumer) => {
                    bootstrap_cycle(
                        &mut consumer,
                        &store,
                        &health,
                        Some(&family_config_path),
                        Some(&ladder_sources),
                    )
                    .await
                }
                Err(reason) => Err(reason),
            };
        match result {
            Ok(_) => {
                bootstrapped = true;
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
            Err(reason) => {
                // One line per DISTINCT reason: a repeating failure is visible in the
                // health snapshot's lastError, a changing one is worth a log line.
                let rendered = reason.to_string();
                if last_logged_error.as_deref() != Some(rendered.as_str()) {
                    eprintln!("[prefrontal-routing] fusiform catalog cycle failed: {rendered}");
                    last_logged_error = Some(rendered);
                }
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
            let dir = prefrontal_test_support::scratch_root().join(format!(
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
    fn tool_surface_result_envelope_and_bare_payload_decode_identically() {
        // Live wire: the ToolProvider surface answers {"result": <payload>}; the
        // producer fixture pins the bare payload. Both must yield one snapshot.
        let bare = get_reply("catalog.get — one model, current");
        let wrapped = serde_json::to_vec(
            &json!({ "result": fixture()["catalog.get — one model, current"]["response"] }),
        )
        .unwrap();
        let a = decode_catalog_get(&bare).unwrap();
        let b = decode_catalog_get(&wrapped).unwrap();
        assert_eq!(a.facts.len(), b.facts.len());
        assert_eq!(a.facts[0].provider_id, b.facts[0].provider_id);
        assert_eq!(a.facts[0].model_id, b.facts[0].model_id);
        let status_wrapped =
            serde_json::to_vec(&json!({ "result": fixture()["catalog.status"]["response"] }))
                .unwrap();
        assert_eq!(
            decode_catalog_status(&status_wrapped)
                .unwrap()
                .catalog_version,
            decode_catalog_status(&get_reply("catalog.status"))
                .unwrap()
                .catalog_version
        );
    }

    /// The live request asks Fusiform for retired models too; without that flag a
    /// withdrawn model is simply missing from the reply and its fact row never
    /// learns it is gone.
    #[test]
    fn catalog_get_request_asks_for_retired_models() {
        let body: Value = serde_json::to_value(CatalogGetRequest::all_with_retired()).unwrap();
        assert_eq!(body["name"], json!("catalog.get"));
        assert_eq!(body["arguments"]["include_retired"], json!(true));
    }

    #[test]
    fn rate_value_decodes_all_three_producer_states() {
        // Producer enum (fusiform-core RateValue): priced | stated_zero | unpriced.
        // The fixture only exercises `priced`; the live catalog carries the others.
        let priced: CatalogRate = serde_json::from_value(json!({
            "currency": "USD", "exponent": 9, "state": "priced", "units": 15000000000_i64,
            "unit_provenance": {"kind": "assumed_by_policy", "policy": "models-dev-usd-v1"}
        }))
        .unwrap();
        assert_eq!(rate_cost(Some(&priced)), OutputPrice::Known(15.0));
        let zero: CatalogRate = serde_json::from_value(json!({ "state": "stated_zero" })).unwrap();
        assert_eq!(rate_cost(Some(&zero)), OutputPrice::Known(0.0));
        let unpriced: CatalogRate =
            serde_json::from_value(json!({ "state": "unpriced", "reason": "source_omitted" }))
                .unwrap();
        assert_eq!(rate_cost(Some(&unpriced)), OutputPrice::NotPublished);
    }

    #[test]
    fn an_absent_rate_is_not_published_and_a_declined_rate_says_so() {
        // The ollama-cloud shape: Fusiform serves the model with limits and
        // capabilities and NO rate.output key at all, because the upstream catalog
        // it reads publishes no price for those models. All 23 ollama-cloud rows in
        // the live routing store are this case (checked 2026-09-06 in Fusiform's
        // own store, which holds no price record for that provider at all).
        assert_eq!(rate_cost(None), OutputPrice::NotPublished);
        let served: CatalogModelFacts = serde_json::from_value(json!({
            "existence": "present",
            "capability.tool_call": true,
            "limit.context": 262_144,
            "limit.output": 65_536
        }))
        .unwrap();
        assert!(served.input_rate.is_none());
        assert!(served.cache_read_rate.is_none());
        assert!(served.cache_write_rate.is_none());
        assert!(served.output_rate.is_none());
        assert!(served.reasoning_rate.is_none());

        // A rate this consumer declines must be distinguishable from one that was
        // never published: the first is a consumer gap to fix, the second is not.
        let foreign: CatalogRate = serde_json::from_value(json!({
            "currency": "EUR", "exponent": 9, "state": "priced", "units": 15000000000_i64
        }))
        .unwrap();
        let OutputPrice::Refused(detail) = rate_cost(Some(&foreign)) else {
            panic!("a non-USD rate must be reported as refused, not as unpublished");
        };
        assert!(detail.contains("EUR"), "{detail}");

        let oversized: CatalogRate = serde_json::from_value(json!({
            "currency": "USD", "exponent": 30, "state": "priced", "units": 1_i64
        }))
        .unwrap();
        assert!(matches!(
            rate_cost(Some(&oversized)),
            OutputPrice::Refused(_)
        ));
    }

    #[test]
    fn unpublished_unpriced_and_refused_rates_remain_absent_and_zero_is_served() {
        let mut response = fixture()["catalog.get — one model, current"]["response"].clone();
        let model = response["models"]["anthropic/claude-sonnet-4-5"]
            .as_object_mut()
            .unwrap();
        model.insert("rate.input".to_string(), json!({ "state": "stated_zero" }));
        model.insert(
            "rate.cache_read".to_string(),
            json!({ "state": "unpriced", "reason": "source_omitted" }),
        );
        model.insert(
            "rate.cache_write".to_string(),
            json!({ "currency": "EUR", "exponent": 9, "state": "priced", "units": 1_i64 }),
        );
        model.remove("rate.reasoning");

        let snapshot = decode_catalog_get(&serde_json::to_vec(&response).unwrap()).unwrap();
        let fact = &snapshot.facts[0];
        assert_eq!(fact.input_rate, Some(0.0));
        assert_eq!(fact.cache_read_rate, None);
        assert_eq!(fact.cache_write_rate, None);
        assert_eq!(fact.reasoning_rate, None);
        assert_eq!(fact.cost_per_mtok_out, Some(15.0));

        let rig = Rig::new("zero-and-absent-rates");
        let store = rig.store();
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .unwrap();
        use prefrontal_routing_core::RouteStateStore as _;
        let stored = store
            .get_fact_by_route_key("anthropic/claude-sonnet-4-5")
            .unwrap()
            .expect("fixture fact survives catalog sync");
        assert_eq!(stored.input_rate, Some(0.0));
        assert_eq!(stored.cache_read_rate, None);
        assert_eq!(stored.cache_write_rate, None);
        assert_eq!(stored.reasoning_rate, None);

        response["models"]["anthropic/claude-sonnet-4-5"]["rate.reasoning"] = json!({ "currency": "USD", "exponent": 9, "state": "priced", "units": 2_000_000_000_i64 });
        let priced_reasoning = decode_catalog_get(&serde_json::to_vec(&response).unwrap())
            .unwrap()
            .facts
            .pop()
            .unwrap();
        assert_eq!(priced_reasoning.reasoning_rate, Some(2.0));

        response["models"]["anthropic/claude-sonnet-4-5"]["rate.reasoning"] =
            json!({ "currency": "USD", "exponent": 19, "state": "priced", "units": 1_i64 });
        let refused_reasoning = decode_catalog_get(&serde_json::to_vec(&response).unwrap())
            .unwrap()
            .facts
            .pop()
            .unwrap();
        assert_eq!(refused_reasoning.reasoning_rate, None);
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
        assert_eq!(fact.input_rate, Some(3.0));
        assert_eq!(fact.cache_read_rate, Some(0.3));
        assert_eq!(fact.cache_write_rate, Some(3.75));
        assert_eq!(fact.reasoning_rate, None);
        assert_eq!(fact.cost_tier, Some(3.0));

        let status = decode_catalog_status(&get_reply("catalog.status")).unwrap();
        assert_eq!(status.catalog_version, 0);
    }

    #[test]
    fn fixture_catalog_rates_reach_the_model_fact_used_by_selection_cost() {
        use prefrontal_routing_core::RouteStateStore as _;

        let rig = Rig::new("rate-card-selection-fact");
        let store = rig.store();
        let snapshot = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .unwrap();

        let fact = store
            .get_fact_by_route_key("anthropic/claude-sonnet-4-5")
            .unwrap()
            .expect("fixture fact survives catalog sync");
        assert_eq!(fact.input_rate, Some(3.0));
        assert_eq!(fact.cache_read_rate, Some(0.3));
        assert_eq!(fact.cache_write_rate, Some(3.75));
        assert_eq!(fact.reasoning_rate, None);
        assert_eq!(fact.cost_per_m_tok_out, Some(15.0));
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
        // First-sight rows are minted only under operator-known providers; make the
        // fixture's synthetic provider known with one placeholder fact so the
        // capability arm (not the provider scope) is what this test exercises.
        use prefrontal_routing_core::route_state_store::RouteStateStore as _;
        let mut placeholder = row.fact.clone();
        placeholder.provider_id = "synthetic".to_string();
        placeholder.model_id = "placeholder".to_string();
        store
            .upsert_fact(&placeholder, prefrontal_routing_core::FactSource::Seed)
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
        let outcome = poll_cycle(&mut unchanged, &store, &health, None, None)
            .await
            .unwrap();
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
        let outcome = poll_cycle(&mut advanced, &store, &health, None, None)
            .await
            .unwrap();
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
        bootstrap_cycle(&mut wire, &store, &health, None, None)
            .await
            .unwrap();
        assert_eq!(
            health.lock().unwrap().snapshot(now_ms())["reach"],
            "reached"
        );
        assert_eq!(wire.get_calls, 1);
    }
}
