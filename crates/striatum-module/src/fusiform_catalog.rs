//! until this consumer is live, a model swap in opencode.jsonc silently strips phone attachments for that session until someone upserts a model_facts row (script/upsert-model-fact.ts); the next occurrence is NOT a phone bug.
//!
//! Fusiform supplies existence, rates, limits, capabilities, and operator-graded
//! Elo/EQ. Routing policy and stored placements remain operator-owned. The fact merge boundary is fenced by
//! [`FusiformFactUpdate`], which cannot represent scores, variants, exclusions,
//! speed multipliers, prompt defaults, AA indices, aliases, or lifecycle dates.
//!
//! | field group | default authority | operator override |
//! | --- | --- | --- |
//! | vision, tool use, context/output limits, rates/output tier | Fusiform | an explicit override of that same field wins |
//! | published reasoning efforts (`capability.reasoning_options`) | Fusiform | none; the operator's `model_routing.variants` must name one of them |
//! | Elo, EQ | operator | live Elo overrides win, then Fusiform operator grades, then stored placements |
//! | speed, variants, exclusions, multipliers, prompt defaults | operator | always retained; Fusiform cannot represent them |
//!
//! Wire decoding is isolated here and pinned to the producer-real fixture copied
//! from `fusiform/crates/fusiform-module/fixtures/served-payloads.json` at Fusiform
//! commit `9591c73`; no other module should depend on these private wire structs.
//! `catalog-paged.json` pins the paged request and response from the same producer
//! fixture at Fusiform `245826d`.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use striatum_core::{BillingPlane, BillingRate, RouteStateStore as _};
use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate, SqliteRouteStateStore};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::winner_ladder::{self, LadderSources};

const FUSIFORM_MODULE_ID: &str = "fusiform";
const CATALOG_GET: &str = "catalog.get";
const CATALOG_STATUS: &str = "catalog.status";
const MAX_CATALOG_RESTARTS: usize = 3;
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);
pub const RETRY_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Default)]
pub(crate) struct PlanConcurrencyCatalog {
    caps: BTreeMap<(String, String), crate::concurrency_caps::EffectiveCap>,
    per_model: BTreeMap<(String, String), BTreeMap<String, u64>>,
    pub(crate) warnings: Vec<String>,
}

impl PlanConcurrencyCatalog {
    pub(crate) fn load(prices: &Value, now: i64) -> Self {
        use crate::concurrency_caps::{CapScope, EffectiveCap};
        let mut out = Self::default();
        let mut plans: BTreeMap<(&str, &str), Vec<&Value>> = BTreeMap::new();
        for row in prices["prices"].as_array().into_iter().flatten() {
            if let (Some(provider), Some(tier)) =
                (row["provider_id"].as_str(), row["tier"].as_str())
            {
                plans.entry((provider, tier)).or_default().push(row);
            }
        }
        for ((provider, tier), rows) in plans {
            let warning = if rows.len() != 1 {
                Some("multiple plan rows match; concurrency cap ignored")
            } else if rows[0].get("concurrency").is_none_or(Value::is_null) {
                None
            } else {
                let value = &rows[0]["concurrency"];
                let limit = value["limit"].as_u64().filter(|n| *n > 0);
                let scope = match value["scope"].as_str() {
                    Some("account") => Some(CapScope::Account),
                    Some("account_model") => Some(CapScope::AccountModel),
                    _ => None,
                };
                if let (Some(limit), Some(scope)) = (limit, scope) {
                    out.caps.insert(
                        (provider.to_owned(), tier.to_owned()),
                        EffectiveCap {
                            limit: Some(limit),
                            scope,
                        },
                    );
                    if scope == CapScope::AccountModel {
                        let models = value["per_model"]
                            .as_object()
                            .into_iter()
                            .flatten()
                            .filter_map(|(key, value)| {
                                value.as_u64().filter(|n| *n > 0).map(|n| (key.clone(), n))
                            })
                            .collect();
                        out.per_model
                            .insert((provider.to_owned(), tier.to_owned()), models);
                    }
                    value["review_by_ms"]
                        .as_i64()
                        .filter(|review| *review < now)
                        .map(|_| "concurrency cap due for review; still applied")
                } else {
                    Some("invalid concurrency limit or scope; cap ignored")
                }
            };
            if let Some(reason) = warning {
                let message = format!("{provider}/{tier}: {reason}");
                out.warnings.push(message);
            }
        }
        out
    }

    pub(crate) fn cap(
        &self,
        provider: &str,
        tier: &str,
    ) -> Option<crate::concurrency_caps::EffectiveCap> {
        self.caps
            .get(&(provider.to_owned(), tier.to_owned()))
            .copied()
    }

    pub(crate) fn model_caps(&self, provider: &str, tier: &str) -> Option<&BTreeMap<String, u64>> {
        self.per_model.get(&(provider.to_owned(), tier.to_owned()))
    }
}

#[derive(Debug)]
pub enum FusiformCatalogError {
    Message(String),
    Json(serde_json::Error),
    Refused { code: String, message: String },
}

impl fmt::Display for FusiformCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => formatter.write_str(message),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Refused { code, message } => write!(formatter, "{code}: {message}"),
        }
    }
}

impl std::error::Error for FusiformCatalogError {}

fn error(message: impl Into<String>) -> FusiformCatalogError {
    FusiformCatalogError::Message(message.into())
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct CatalogGetArguments {
    // No fact filter is sent: Fusiform then serves every fact family, which
    // routing needs, including the `model.` facts (`model.canonical_id`,
    // `model.status`) beside `rate.`, `limit.` and `capability.`. A filter that
    // listed only those three prefixes would silently drop identity and status.
    /// Fusiform serves only present models unless asked, and a model it has
    /// retired is then simply missing from the reply. A missing model updates
    /// nothing, so a fact row for it stayed present-and-routable after the
    /// upstream withdrew it: the whole kimi-for-coding provider went absent on
    /// 2026-09-18 and its scored seat kept being rostered while Broca refused
    /// every open. Retired models are requested so their absence is written.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    include_retired: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    paged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_method: Option<String>,
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
                paged: true,
                ..Default::default()
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
    /// Kept undecoded here: each entry is decoded on its own in
    /// `decode_catalog_get`, so one unreadable entry cannot reject the others.
    models: BTreeMap<String, Value>,
    resolved_at_ms: i64,
    #[serde(default)]
    curated: Vec<CatalogCuratedModel>,
    #[serde(default)]
    login_methods: Vec<CatalogCuratedModel>,
    #[serde(default)]
    quality_scores: Option<QualityScores>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
struct QualityScores {
    graded_by: String,
    graded_at_ms: i64,
}

/// Keep disclosures with their model pages until the entire read is assembled.
/// Only then can decoding and catalog application publish a coherent snapshot.
#[derive(Debug, Deserialize, Serialize)]
struct CatalogPage {
    source: String,
    catalog_version: i64,
    resolved_at_ms: i64,
    models: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    withheld: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    uncertain: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    curated: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    login_methods: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    aliased: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quality_scores: Option<QualityScores>,
}

impl CatalogPage {
    fn append(&mut self, mut page: Self) -> Result<(), FusiformCatalogError> {
        if self.source != page.source
            || self.catalog_version != page.catalog_version
            || self.resolved_at_ms != page.resolved_at_ms
            || matches!((&self.quality_scores, &page.quality_scores),
                (Some(first), Some(next)) if first.graded_by != next.graded_by)
        {
            return Err(error("Fusiform catalog page identity changed during read"));
        }
        if page.models.keys().any(|key| self.models.contains_key(key)) {
            return Err(error("Fusiform catalog pages repeated a model"));
        }
        match (&mut self.quality_scores, page.quality_scores.take()) {
            (Some(first), Some(next)) => {
                first.graded_at_ms = first.graded_at_ms.max(next.graded_at_ms);
            }
            (scores @ None, Some(next)) => *scores = Some(next),
            _ => {}
        }
        self.models.append(&mut page.models);
        self.withheld.append(&mut page.withheld);
        self.uncertain.append(&mut page.uncertain);
        self.curated.append(&mut page.curated);
        self.login_methods.append(&mut page.login_methods);
        self.aliased.append(&mut page.aliased);
        self.next_page_token = page.next_page_token;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogCuratedModel {
    model: String,
    auth_methods: Vec<String>,
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
    /// Outer `None`: the key was not served. Inner `None`: served as `null`.
    #[serde(
        rename = "capability.reasoning_options",
        default,
        deserialize_with = "served_value"
    )]
    reasoning_options: Option<Option<Vec<Value>>>,
    /// models.dev's `canonical_model_id`, passed through by Fusiform: routes
    /// with the same value serve the same weights. `null` and an unserved key
    /// both mean the upstream states no identity.
    #[serde(rename = "model.canonical_id", default)]
    canonical_id: Option<String>,
    /// models.dev's model status (`deprecated`, `beta`, `alpha`), passed
    /// through by Fusiform. `null` and an unserved key both mean no status.
    #[serde(rename = "model.status", default)]
    status: Option<String>,
    #[serde(rename = "model.lineage", default)]
    lineage: Option<String>,
    #[serde(rename = "model.type", default)]
    model_type: Option<String>,
    #[serde(rename = "model.kind", default)]
    model_kind: Option<String>,
}

fn served_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// The effort values Broca will admit for a model: the `values` of every
/// `{"type": "effort"}` reasoning option, in first-seen order. Toggle and
/// token-budget options carry no named variant. `None` when the catalog did
/// not serve `capability.reasoning_options` at all.
fn published_efforts(options: &Option<Option<Vec<Value>>>) -> Option<Vec<String>> {
    let options = options.as_ref()?;
    let mut efforts: Vec<String> = Vec::new();
    for option in options.iter().flatten() {
        if option.get("type").and_then(Value::as_str) != Some("effort") {
            continue;
        }
        for value in option
            .get("values")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            if !efforts.iter().any(|known| known == value) {
                efforts.push(value.to_string());
            }
        }
    }
    Some(efforts)
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
    NotBilled,
    BilledAs {
        key: String,
    },
    Unpriced,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CatalogSnapshot {
    cursor: FusiformCatalogCursor,
    facts: Vec<FusiformFactUpdate>,
    rates: BTreeMap<String, ModelRates>,
    pub(crate) wire_apis: BTreeMap<String, String>,
    /// Rows serving an endpoint in `wire.api`, distinct from `wire_apis`' wire families.
    pub(crate) wire_api_models: BTreeSet<String>,
    pub(crate) model_kinds: BTreeMap<String, String>,
    audio_input_prices: BTreeMap<String, AudioInputPrice>,
    media_grades: BTreeMap<String, MediaGrade>,
    media_rates: BTreeMap<String, BTreeMap<String, MediaRate>>,
    video_facts: BTreeMap<String, VideoFacts>,
    quality_scores: Option<QualityScores>,
    grades: BTreeMap<String, striatum_store::OperatorQuality>,
    decommissioned: BTreeMap<String, String>,
    /// `(provider_id, model_id)` of entries served under a well-formed
    /// `provider/model` key whose facts could not be decoded. Applying the
    /// snapshot leaves their stored rows and cached base rates as they were:
    /// an entry this consumer cannot read is not evidence the model is gone.
    retained: BTreeSet<(String, String)>,
}

/// Video output limits are independent of an engine's input-duration limit.
/// Keep missing or malformed fields unknown rather than inventing support.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct VideoFacts {
    pub max_output_duration_ms: Option<u64>,
    pub aspect_ratios: Option<Vec<String>>,
    pub audio: Option<bool>,
}

impl VideoFacts {
    fn decode(route: &str, entry: &Value) -> Self {
        fn field<T: serde::de::DeserializeOwned>(
            route: &str,
            entry: &Value,
            key: &str,
        ) -> Option<T> {
            let value = entry.get(key)?;
            match serde_json::from_value(value.clone()) {
                Ok(value) => Some(value),
                Err(_) => {
                    // Catalog refreshes can repeat the same malformed field.
                    // Name each route and field once, not on every selection.
                    static REPORTED: std::sync::OnceLock<Mutex<BTreeSet<String>>> =
                        std::sync::OnceLock::new();
                    if let Ok(mut reported) = REPORTED.get_or_init(Default::default).lock() {
                        if reported.insert(format!("{route}:{key}")) {
                            eprintln!("[striatum] warning: video catalog {route} has invalid {key}; the fact remains unknown");
                        }
                    }
                    None
                }
            }
        }
        Self {
            max_output_duration_ms: field(route, entry, "limit.output_duration_ms"),
            aspect_ratios: field(route, entry, "capability.aspect_ratios"),
            audio: field(route, entry, "capability.output_audio"),
        }
    }
}

impl CatalogSnapshot {
    pub(crate) fn video_facts(&self, route: &str) -> Option<&VideoFacts> {
        self.video_facts.get(route)
    }
    /// A grade compares models of one media type, never unrelated catalog rows.
    #[allow(
        dead_code,
        reason = "Media facts must not alter legacy engine selection."
    )]
    pub(crate) fn media_grade(&self, kind: &str, route: &str) -> Option<f64> {
        let expected_type = match kind {
            "transcribe" | "stream_transcribe" => "transcription",
            "synthesize" => "speech",
            "image" | "video" | "realtime" => kind,
            _ => return None,
        };
        let grade = self.media_grades.get(route)?;
        (grade.model_type == expected_type).then_some(grade.value)
    }

    /// Keys retain the catalog's `rate.` prefix so no rate can disappear when
    /// deciding whether every incurred charge can be measured.
    #[allow(
        dead_code,
        reason = "Media facts must not alter legacy engine selection."
    )]
    pub(crate) fn media_rates(&self, route: &str) -> Option<&BTreeMap<String, MediaRate>> {
        self.media_rates.get(route)
    }

    pub(crate) fn is_decommissioned(&self, route: &str) -> bool {
        self.decommissioned.contains_key(route)
    }
    pub(crate) fn engine_cost(
        &self,
        request: &cortexkit_role_routing::select_engine::SelectEngineRequest,
        provider: &str,
        model: &str,
    ) -> Option<f64> {
        use cortexkit_role_routing::select_engine::Kind;
        if !matches!(request.kind, Kind::Transcribe | Kind::StreamTranscribe) {
            return None;
        }
        let seconds = request.requirements.duration_ms? as f64 / 1000.0;
        let price = self
            .audio_input_prices
            .get(&format!("{provider}/{model}"))?;
        // Transcript length belongs to the speech, not the model. Output-token
        // prices therefore do not enter the transcription ranking estimate.
        let cost = if let Some((amount, per)) = price.per_second {
            amount * seconds / per as f64
        } else {
            seconds * price.tokens_per_second? * price.per_million_tokens? / 1_000_000.0
        };
        cost.is_finite().then_some(cost)
    }
    pub(crate) fn input_price(&self, model: &str) -> Option<f64> {
        self.model(model).and_then(|fact| fact.input_rate)
    }

    pub(crate) fn model(&self, model: &str) -> Option<&FusiformFactUpdate> {
        self.facts
            .iter()
            .find(|fact| model == format!("{}/{}", fact.provider_id, fact.model_id))
    }

    /// Keep the provider index tied to the version of the catalog that supplied it.
    pub(crate) fn provider_index(&self) -> (i64, BTreeSet<String>) {
        let providers = self
            .facts
            .iter()
            .map(|row| row.provider_id.clone())
            .chain(self.retained.iter().map(|(provider, _)| provider.clone()))
            .collect();
        (self.cursor.catalog_version, providers)
    }

    /// The decoded rows and cursor, for tests outside this module that apply a
    /// snapshot to a store directly.
    #[cfg(test)]
    pub(crate) fn parts(&self) -> (&[FusiformFactUpdate], FusiformCatalogCursor) {
        (&self.facts, self.cursor)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct MediaGrade {
    model_type: String,
    value: f64,
}

/// Only absent or explicitly unknown quantity rules permit billing the measured
/// quantity unchanged. Preserve other kinds so callers can leave the route
/// unpriced rather than silently ignore a possible quantum or floor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RateQuantityKind {
    Unspecified,
    Unrecognised(Value),
}

impl RateQuantityKind {
    fn decode(value: Option<&Value>) -> Self {
        match value {
            None => Self::Unspecified,
            Some(value) if value.get("kind").and_then(Value::as_str) == Some("unknown") => {
                Self::Unspecified
            }
            Some(value) => Self::Unrecognised(value.get("kind").unwrap_or(value).clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MediaRate {
    pub(crate) state: Option<String>,
    /// USD for one unit, not the served amount for `per` units. Token rates have
    /// no request-unit conversion and deliberately carry no unit price here.
    pub(crate) unit_price: Option<f64>,
    pub(crate) per: Option<u64>,
    pub(crate) increment: RateQuantityKind,
    pub(crate) minimum_quantity: RateQuantityKind,
    pub(crate) minimum: Option<u64>,
}

impl MediaRate {
    fn decode(model: &str, key: &str, value: &Value) -> Self {
        let per = value
            .get("per")
            .and_then(Value::as_u64)
            .filter(|per| *per > 0);
        let unit_price = key
            .strip_prefix("rate.")
            .and_then(|key| key.split_once('.'))
            .filter(|(role, unit)| !role.is_empty() && unit.starts_with("per_"))
            .and_then(|_| per)
            .and_then(|per| {
                let rate = serde_json::from_value::<CatalogRate>(value.clone()).ok()?;
                decoded_rate(model, key, Some(&rate)).map(|amount| amount / per as f64)
            });
        Self {
            state: value
                .get("state")
                .and_then(Value::as_str)
                .map(str::to_owned),
            unit_price,
            per,
            increment: RateQuantityKind::decode(value.get("increment")),
            minimum_quantity: RateQuantityKind::decode(value.get("minimum_quantity")),
            minimum: match value["minimum_quantity"]["kind"].as_str() {
                Some("none") => Some(0),
                Some("minimum") => value["minimum_quantity"]["quantity"].as_u64(),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct AudioInputPrice {
    per_second: Option<(f64, u64)>,
    per_million_tokens: Option<f64>,
    tokens_per_second: Option<f64>,
}

impl AudioInputPrice {
    fn decode(model: &str, entry: &Value) -> Self {
        let decode = |key: &str| {
            let value = entry.get(key)?;
            let rate: CatalogRate = serde_json::from_value(value.clone()).ok()?;
            decoded_rate(model, key, Some(&rate))
        };
        Self {
            per_second: entry["rate.input_audio.per_second"]["per"]
                .as_u64()
                .filter(|per| *per > 0)
                .and_then(|per| decode("rate.input_audio.per_second").map(|rate| (rate, per))),
            per_million_tokens: entry
                .get("rate.input_audio")
                .filter(|rate| rate.get("per").is_none())
                .and_then(|_| decode("rate.input_audio")),
            tokens_per_second: entry["limit.audio_tokens_per_second"]
                .as_f64()
                .filter(|value| value.is_finite() && *value > 0.0),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ModelRates {
    rates: BTreeMap<String, BillingRate>,
    inherited: BTreeSet<String>,
}

/// The basket rates of every model on a billing-plane page. Unlike the
/// whole-catalog read, a plane page is still all-or-nothing: any unreadable
/// entry rejects the page.
fn model_rates(models: &Value) -> Result<BTreeMap<String, ModelRates>, FusiformCatalogError> {
    models
        .as_object()
        .ok_or_else(|| error("catalog models must be an object"))?
        .iter()
        .map(|(model, facts)| {
            entry_rates(model, facts)
                .map(|rates| (model.clone(), rates))
                .map_err(|reason| error(format!("model {model}: {reason}")))
        })
        .collect()
}

/// The basket rates of one served model entry. A rate that cannot be decoded
/// makes the whole entry unreadable instead of quietly dropping that one rate.
fn entry_rates(model: &str, facts: &Value) -> Result<ModelRates, String> {
    let mut rates = ModelRates::default();
    for (key, value) in facts
        .as_object()
        .ok_or_else(|| "model facts are not an object".to_string())?
    {
        // Mode rates establish service-tier prices. Lifetime rates must also
        // survive in memory so settlement never substitutes a different TTL.
        if !striatum_core::BasketDimension::ALL
            .into_iter()
            .any(|dimension| {
                key == dimension.fact_key()
                    || key
                        .strip_prefix(dimension.fact_key())
                        .is_some_and(|suffix| {
                            suffix.starts_with(".above_context.")
                                || suffix.starts_with(".mode.")
                                || (dimension == striatum_core::BasketDimension::CacheWrite
                                    && suffix.starts_with(".ttl."))
                        })
            })
        {
            continue;
        }
        let served =
            CatalogRate::deserialize(value).map_err(|reason| format!("{key}: {reason}"))?;
        let rate = match &served {
            CatalogRate::BilledAs { key } => BillingRate::BilledAs { key: key.clone() },
            CatalogRate::NotBilled => BillingRate::NotBilled,
            _ => match decoded_rate(model, key, Some(&served)) {
                Some(rate) => BillingRate::Priced { rate },
                None => BillingRate::Unpriced,
            },
        };
        rates.rates.insert(key.clone(), rate);
        if value.get("inherited_from").is_some_and(Value::is_object) {
            rates.inherited.insert(key.clone());
        }
    }
    Ok(rates)
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct PlaneDescriptor {
    provider_id: String,
    auth_method: String,
    kind: String,
    source_ref: Option<String>,
    review_by_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct BillingRule {
    model: String,
    fact_key: String,
    state: String,
    as_class: Option<String>,
    source_ref: String,
    established_by: String,
    established_at_ms: i64,
    review_by_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct PlanePage {
    plane: PlaneDescriptor,
    rates: BTreeMap<String, ModelRates>,
    wire_apis: BTreeMap<String, String>,
    wire_api_models: BTreeSet<String>,
    billing_rules: Vec<BillingRule>,
}

#[derive(Debug, Clone, PartialEq)]
enum PlaneReply {
    Served(PlanePage),
    Refused { code: String, message: String },
}

fn wire_families(models: &Value) -> BTreeMap<String, String> {
    models
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(model, entry)| {
            Some((
                model.clone(),
                entry.get("wire.family")?.as_str()?.to_owned(),
            ))
        })
        .collect()
}

fn row_serves_wire_api(entry: &Value) -> bool {
    entry.get("wire.api").is_some_and(|fact| {
        fact.get("value")
            .unwrap_or(fact)
            .as_str()
            .is_some_and(|endpoint| !endpoint.trim().is_empty())
    })
}

fn decode_plane_get(bytes: &[u8]) -> Result<PlaneReply, FusiformCatalogError> {
    let value = unwrap_tool_result(bytes)?;
    let refusal = value.get("error").unwrap_or(&value);
    if let Some(code) = refusal.get("code").and_then(Value::as_str) {
        return Ok(PlaneReply::Refused {
            code: code.to_string(),
            message: refusal
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(PlaneReply::Served(PlanePage {
        plane: serde_json::from_value(value["plane"].clone())
            .map_err(FusiformCatalogError::Json)?,
        rates: model_rates(&value["models"])?,
        wire_apis: wire_families(&value["models"]),
        wire_api_models: value["models"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, entry)| row_serves_wire_api(entry))
            .map(|(model, _)| model.clone())
            .collect(),
        billing_rules: serde_json::from_value(
            value
                .get("billing_rules")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(FusiformCatalogError::Json)?,
    }))
}

/// The plane catalog's base rates once `snapshot` is applied: the snapshot's
/// own rates, plus the previous rates of every entry it could not read, so an
/// unreadable entry keeps its prices instead of losing them.
fn base_rates_after(
    mut prior: BTreeMap<String, ModelRates>,
    snapshot: &CatalogSnapshot,
) -> BTreeMap<String, ModelRates> {
    let mut base = snapshot.rates.clone();
    for (provider_id, model_id) in &snapshot.retained {
        let served_key = format!("{provider_id}/{model_id}");
        if let Some(rates) = prior.remove(&served_key) {
            base.insert(served_key, rates);
        }
    }
    base
}

/// Published atomically by the catalog poller, read without I/O by selection.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PlaneCatalog {
    version: Option<i64>,
    base: BTreeMap<String, ModelRates>,
    pages: BTreeMap<(String, String), PlaneReply>,
}

impl PlaneCatalog {
    pub(crate) fn offers_tier(&self, model: &str, tier: &str) -> bool {
        self.base.get(model).is_some_and(|rates| {
            rates
                .rates
                .keys()
                .any(|key| key.ends_with(&format!(".mode.{tier}")))
        })
    }

    pub(crate) fn mode_rates(&self, model: &str, tier: &str) -> BTreeMap<String, BillingRate> {
        self.base
            .get(model)
            .map(|rates| {
                rates
                    .rates
                    .iter()
                    .filter_map(|(key, rate)| {
                        key.strip_suffix(&format!(".mode.{tier}"))
                            .map(|base| (base.to_owned(), rate.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A served plane is authoritative, including when it has no wire fact.
    /// Only routes without a served plane may use the ordinary catalog value.
    pub(crate) fn wire_api(&self, model: &str, auth_method: &str) -> Option<Option<&str>> {
        let provider = model.split_once('/')?.0;
        match self.pages.get(&(provider.into(), auth_method.into())) {
            Some(PlaneReply::Served(page)) => Some(page.wire_apis.get(model).map(String::as_str)),
            _ => None,
        }
    }

    /// A served plane's endpoint facts override the ordinary catalog, even when absent.
    pub(crate) fn row_serves_wire_api(&self, model: &str, auth_method: &str) -> Option<bool> {
        let provider = model.split_once('/')?.0;
        match self.pages.get(&(provider.into(), auth_method.into())) {
            Some(PlaneReply::Served(page)) => Some(page.wire_api_models.contains(model)),
            _ => None,
        }
    }

    pub(crate) fn subscription_plan_providers(&self) -> BTreeSet<String> {
        self.pages
            .values()
            .filter_map(|reply| match reply {
                PlaneReply::Served(page) if page.plane.kind == "quota_proxy" => {
                    Some(page.plane.provider_id.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn is_loaded(&self) -> bool {
        self.version.is_some()
    }

    #[cfg(test)]
    pub(crate) fn with_test_catalog(mut self, bytes: &[u8]) -> Self {
        self.base = decode_catalog_get(bytes).unwrap().rates;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_wire_apis(mut self, wire_apis: &BTreeMap<String, String>) -> Self {
        for reply in self.pages.values_mut() {
            if let PlaneReply::Served(page) = reply {
                page.wire_apis = wire_apis.clone();
            }
        }
        self
    }

    #[cfg(test)]
    pub(crate) fn with_wire_api_models(mut self, models: &BTreeSet<String>) -> Self {
        for reply in self.pages.values_mut() {
            if let PlaneReply::Served(page) = reply {
                page.wire_api_models = models.clone();
            }
        }
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_plane(mut self, bytes: &[u8]) -> Self {
        let PlaneReply::Served(page) = decode_plane_get(bytes).unwrap() else {
            panic!("expected a served billing plane");
        };
        self.pages.insert(
            (
                page.plane.provider_id.clone(),
                page.plane.auth_method.clone(),
            ),
            PlaneReply::Served(page),
        );
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_refusal(mut self, provider: &str, method: &str, bytes: &[u8]) -> Self {
        let reply = decode_plane_get(bytes).unwrap();
        assert!(matches!(reply, PlaneReply::Refused { .. }));
        self.pages.insert((provider.into(), method.into()), reply);
        self
    }

    #[cfg(test)]
    pub(crate) fn ordinary_test_catalog(facts: &[striatum_core::ModelFact]) -> Self {
        let mut catalog = Self {
            version: Some(0),
            ..Self::default()
        };
        for fact in facts {
            if let Some(plane) = &fact.billing_plane {
                catalog.pages.insert(
                    (fact.provider_id.clone(), plane.auth_method.clone()),
                    PlaneReply::Served(PlanePage {
                        plane: PlaneDescriptor {
                            provider_id: plane.provider_id.clone(),
                            auth_method: plane.auth_method.clone(),
                            kind: plane.kind.clone(),
                            source_ref: None,
                            review_by_ms: None,
                        },
                        rates: BTreeMap::from([(
                            format!("{}/{}", fact.provider_id, fact.model_id),
                            ModelRates {
                                rates: plane.rates.clone(),
                                inherited: BTreeSet::new(),
                            },
                        )]),
                        billing_rules: vec![],
                        wire_apis: BTreeMap::new(),
                        wire_api_models: BTreeSet::new(),
                    }),
                );
                continue;
            }
            catalog.base.insert(
                format!("{}/{}", fact.provider_id, fact.model_id),
                ModelRates {
                    rates: striatum_core::BasketDimension::ALL
                        .into_iter()
                        .filter_map(|dimension| {
                            fact.basket_rate(dimension).map(|rate| {
                                (dimension.fact_key().into(), BillingRate::Priced { rate })
                            })
                        })
                        .collect(),
                    inherited: BTreeSet::new(),
                },
            );
            catalog.pages.insert(
                (fact.provider_id.clone(), "apikey".into()),
                PlaneReply::Refused {
                    code: "no_coverage".into(),
                    message: "test catalog".into(),
                },
            );
        }
        catalog
    }

    /// The billing plane for one route served by one credential method.
    ///
    /// `multiplier_configured` says the operator configured a
    /// `subscription_multipliers` entry covering this route (see
    /// [`crate::broca_credentials::BrocaCredentials::price_facts`]). A plan whose
    /// rates Fusiform does not serve (a missing or refused subscription plane)
    /// then bills at the catalog's base list rates, which the pricing
    /// path divides by that multiplier. Without such an entry, only rates with
    /// catalog inheritance are used; unmarked rates stay [`BillingRate::Unpriced`].
    /// A missing plane uses these same inherited or operator-subsidized list rates.
    pub(crate) fn for_route(
        &self,
        model: &str,
        method: &crate::broca_credentials::ServingMethod,
        multiplier_configured: bool,
    ) -> BillingPlane {
        let provider = model
            .split_once('/')
            .map_or(model, |(provider, _)| provider);
        let auth_method = method.auth_method.as_deref().unwrap_or("ambiguous");
        let subscription_method = method
            .auth_method
            .as_deref()
            .is_some_and(|method| !striatum_core::billing::is_api_metered_auth_method(method));
        let mut plane = BillingPlane {
            provider_id: provider.into(),
            auth_method: auth_method.into(),
            kind: "unpriced".into(),
            selection_reason: method.reason.clone(),
            rates: BTreeMap::new(),
        };
        let fallback = match self.pages.get(&(provider.into(), auth_method.into())) {
            Some(PlaneReply::Served(page)) => {
                plane.kind = page.plane.kind.clone();
                plane.rates = page
                    .rates
                    .get(model)
                    .map(|rates| rates.rates.clone())
                    .unwrap_or_default();
                false
            }
            Some(PlaneReply::Refused { code, .. }) => {
                plane.kind = if code == "no_coverage" {
                    "no_coverage fallback".into()
                } else {
                    format!("refused: {code}")
                };
                code == "no_coverage" || subscription_method
            }
            None => {
                if subscription_method {
                    plane.kind = "missing plane fallback".into();
                }
                subscription_method
            }
        };
        if fallback {
            if let Some(base) = self.base.get(model) {
                plane.rates = base
                    .rates
                    .iter()
                    .map(|(key, rate)| {
                        let stated = auth_method == "apikey"
                            || base.inherited.contains(key)
                            || multiplier_configured;
                        (
                            key.clone(),
                            if stated {
                                rate.clone()
                            } else {
                                BillingRate::Unpriced
                            },
                        )
                    })
                    .collect();
                if auth_method == "apikey" && !plane.rates.contains_key("rate.reasoning") {
                    plane.rates.insert(
                        "rate.reasoning".into(),
                        BillingRate::BilledAs {
                            key: "rate.output".into(),
                        },
                    );
                }
            }
        }
        plane
    }
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
        CatalogRate::StatedZero | CatalogRate::NotBilled => OutputPrice::Known(0.0),
        CatalogRate::Unpriced | CatalogRate::BilledAs { .. } | CatalogRate::Unknown => {
            OutputPrice::NotPublished
        }
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
            "[striatum] fusiform served {catalog_key} for {served_key} but this consumer refused it ({detail}); that rate stays absent"
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

pub(crate) fn decode_catalog_get(bytes: &[u8]) -> Result<CatalogSnapshot, FusiformCatalogError> {
    let value = unwrap_tool_result(bytes)?;
    let reply: CatalogGetReply =
        serde_json::from_value(value).map_err(FusiformCatalogError::Json)?;
    let mut auth_methods = BTreeMap::new();
    let documented: BTreeSet<_> = reply
        .login_methods
        .iter()
        .map(|row| row.model.clone())
        .collect();
    for row in reply.curated.into_iter().chain(reply.login_methods) {
        let mut methods = row.auth_methods;
        methods.sort();
        methods.dedup();
        if auth_methods.insert(row.model.clone(), methods).is_some() {
            return Err(error(format!(
                "duplicate Fusiform auth-method model: {}",
                row.model
            )));
        }
    }
    let mut facts = Vec::with_capacity(reply.models.len());
    let mut rates = BTreeMap::new();
    let mut retained = BTreeSet::new();
    let mut wire_apis = BTreeMap::new();
    let mut wire_api_models = BTreeSet::new();
    let mut model_kinds = BTreeMap::new();
    let mut audio_input_prices = BTreeMap::new();
    let mut media_grades = BTreeMap::new();
    let mut media_rates = BTreeMap::new();
    let mut video_facts = BTreeMap::new();
    let mut grades = BTreeMap::new();
    let mut decommissioned = BTreeMap::new();
    // (served key, reason) for every entry left out of this snapshot.
    let mut unreadable: Vec<(String, String)> = Vec::new();
    for (served_key, entry) in reply.models {
        // The operator's withdrawal is independent of whether this model has
        // been graded. The canonical model ID makes the withdrawal apply to
        // reseller routes for the same model too.
        if entry.get("quality.decommissioned").and_then(Value::as_bool) == Some(true) {
            let model = entry
                .get("model.canonical_id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .unwrap_or(&served_key);
            decommissioned.insert(served_key.clone(), model.to_owned());
        }
        if reply
            .quality_scores
            .as_ref()
            .is_some_and(|scores| scores.graded_by == "operator")
        {
            if let (Some(model_type), Some(value)) = (
                entry.get("model.type").and_then(Value::as_str),
                entry
                    .get("quality.grade")
                    .and_then(Value::as_f64)
                    .filter(|grade| grade.is_finite() && *grade > 0.0),
            ) {
                media_grades.insert(
                    served_key.clone(),
                    MediaGrade {
                        model_type: model_type.to_owned(),
                        value,
                    },
                );
            }
            if let (Some(elo), Some(eq)) = (
                entry.get("quality.elo").and_then(Value::as_f64),
                entry.get("quality.eq").and_then(Value::as_f64),
            ) {
                let model = entry
                    .get("model.canonical_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .unwrap_or(&served_key)
                    .to_owned();
                grades.insert(
                    served_key.clone(),
                    striatum_store::OperatorQuality { model, elo, eq },
                );
            }
        }
        let entry_auth_methods = auth_methods.remove(&served_key);
        // A key that does not split into provider/model names no stored row,
        // so skipping it leaves nothing behind to protect.
        let Some((provider_id, model_id)) = served_key
            .split_once('/')
            .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        else {
            unreadable.push((served_key, "key is not provider/model".to_string()));
            continue;
        };
        match decode_model_entry(
            &served_key,
            provider_id,
            model_id,
            &entry,
            entry_auth_methods,
        ) {
            Ok((mut fact, model_rates)) => {
                fact.login_methods_documented = documented.contains(&served_key);
                if entry.get("model.type").and_then(Value::as_str) == Some("video") {
                    video_facts.insert(served_key.clone(), VideoFacts::decode(&served_key, &entry));
                }
                media_rates.insert(
                    served_key.clone(),
                    entry
                        .as_object()
                        .into_iter()
                        .flatten()
                        .filter(|(key, _)| key.starts_with("rate."))
                        .map(|(key, value)| {
                            (key.clone(), MediaRate::decode(&served_key, key, value))
                        })
                        .collect(),
                );
                audio_input_prices.insert(
                    served_key.clone(),
                    AudioInputPrice::decode(&served_key, &entry),
                );
                if let Some(kind) = entry.get("model.kind").and_then(Value::as_str) {
                    model_kinds.insert(served_key.clone(), kind.to_owned());
                }
                facts.push(fact);
                if row_serves_wire_api(&entry) {
                    wire_api_models.insert(served_key.clone());
                }
                if let Some(api) = entry.get("wire.family").and_then(Value::as_str) {
                    wire_apis.insert(served_key.clone(), api.to_owned());
                }
                rates.insert(served_key, model_rates);
            }
            Err(reason) => {
                retained.insert((provider_id.to_string(), model_id.to_string()));
                unreadable.push((served_key, reason));
            }
        }
    }
    report_unreadable_entries_once(reply.catalog_version, &unreadable);
    Ok(CatalogSnapshot {
        cursor: FusiformCatalogCursor {
            catalog_version: reply.catalog_version,
            resolved_at_ms: reply.resolved_at_ms,
        },
        facts,
        rates,
        retained,
        wire_apis,
        wire_api_models,
        model_kinds,
        audio_input_prices,
        media_grades,
        media_rates,
        video_facts,
        quality_scores: reply.quality_scores,
        grades,
        decommissioned,
    })
}

/// Decode one served model entry into its stored fact row and basket rates.
/// Any error is local to this entry; the caller keeps decoding the others.
fn decode_model_entry(
    served_key: &str,
    provider_id: &str,
    model_id: &str,
    entry: &Value,
    auth_methods: Option<Vec<String>>,
) -> Result<(FusiformFactUpdate, ModelRates), String> {
    let model = CatalogModelFacts::deserialize(entry).map_err(|reason| reason.to_string())?;
    let present = match model.existence.as_str() {
        "present" => true,
        "absent" => false,
        other => return Err(format!("unknown existence value {other:?}")),
    };
    let model_rates = entry_rates(served_key, entry)?;
    let supports_vision = model
        .input_modalities
        .as_ref()
        .map(|modalities| modalities.iter().any(|value| value == "image"))
        .or(model.attachment);
    let cost_per_mtok_out = decoded_rate(served_key, "rate.output", model.output_rate.as_ref());
    let input_rate = decoded_rate(served_key, "rate.input", model.input_rate.as_ref());
    let cache_read_rate = decoded_rate(
        served_key,
        "rate.cache_read",
        model.cache_read_rate.as_ref(),
    );
    let cache_write_rate = decoded_rate(
        served_key,
        "rate.cache_write",
        model.cache_write_rate.as_ref(),
    );
    let reasoning_rate = decoded_rate(served_key, "rate.reasoning", model.reasoning_rate.as_ref());
    let fact = FusiformFactUpdate {
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        auth_methods,
        login_methods_documented: false,
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
        reasoning_efforts: published_efforts(&model.reasoning_options),
        // A blank id names no model; keep it out so it cannot group
        // unrelated routes together.
        canonical_id: model
            .canonical_id
            .filter(|canonical_id| !canonical_id.trim().is_empty()),
        upstream_status: model.status.filter(|status| !status.trim().is_empty()),
        lineage: model.lineage,
        model_type: model.model_type,
        model_kind: model.model_kind,
    };
    Ok((fact, model_rates))
}

/// Log unreadable catalog entries once per catalog version. The same version
/// can be decoded again after a wake or by an on-demand upsert, so a line per
/// decode would repeat without news; a new version is worth one line even if
/// the list is unchanged.
fn report_unreadable_entries_once(catalog_version: i64, unreadable: &[(String, String)]) {
    const NAMED: usize = 3;
    static LAST_REPORTED_VERSION: Mutex<Option<i64>> = Mutex::new(None);
    if unreadable.is_empty() {
        return;
    }
    let Ok(mut last) = LAST_REPORTED_VERSION.lock() else {
        return;
    };
    if *last == Some(catalog_version) {
        return;
    }
    *last = Some(catalog_version);
    let named = unreadable
        .iter()
        .take(NAMED)
        .map(|(key, reason)| format!("{key} ({reason})"))
        .collect::<Vec<_>>()
        .join(", ");
    let more = if unreadable.len() > NAMED {
        ", ..."
    } else {
        ""
    };
    eprintln!(
        "[striatum] fusiform catalog version {catalog_version}: skipped {} unreadable entries, their stored routes left unchanged: {named}{more}",
        unreadable.len()
    );
}

fn decode_catalog_status(bytes: &[u8]) -> Result<CatalogStatusReply, FusiformCatalogError> {
    serde_json::from_value(unwrap_tool_result(bytes)?).map_err(FusiformCatalogError::Json)
}

/// Short-lived Fusiform connection. Callers create one per bootstrap/poll cycle
/// and close it before sleeping, so the consumer never owns an idle socket.
pub struct FusiformCatalogConsumer {
    credentials: Option<crate::broca_credentials::SharedBrocaCredentials>,
    stream: TcpStream,
    route_channel: u16,
    route_epoch: u32,
    next_corr: u64,
}

impl FusiformCatalogConsumer {
    #[cfg(test)]
    pub(crate) async fn dry_run_refresh(
        &mut self,
        store: &SqliteRouteStateStore,
        credentials: crate::broca_credentials::SharedBrocaCredentials,
    ) -> Result<(), FusiformCatalogError> {
        self.credentials = Some(credentials);
        let snapshot = self.catalog_get().await?;
        self.refresh_planes(Some(&snapshot)).await?;
        apply_catalog_snapshot(store, &snapshot, None, None, None, "dry run copy")?;
        self.close().await
    }

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
            credentials: None,
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
                    "harness": "striatum",
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

    async fn call_frame(&mut self, body: Vec<u8>) -> Result<Frame, FusiformCatalogError> {
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
        Ok(reply)
    }

    async fn call(&mut self, body: Vec<u8>) -> Result<Vec<u8>, FusiformCatalogError> {
        let reply = self.call_frame(body).await?;
        if let Ok(value) = unwrap_tool_result(&reply.body) {
            let refusal = value
                .get("error")
                .or_else(|| (reply.header.ty == FrameType::Error).then_some(&value));
            if let Some(refusal) = refusal {
                if let Some(code) = refusal["code"].as_str() {
                    return Err(FusiformCatalogError::Refused {
                        code: code.to_string(),
                        message: refusal["message"].as_str().unwrap_or_default().to_string(),
                    });
                }
            }
        }
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
    fn credentials(&self) -> Option<crate::broca_credentials::SharedBrocaCredentials> {
        None
    }
    async fn catalog_get(&mut self) -> Result<CatalogSnapshot, FusiformCatalogError>;
    async fn catalog_status(&mut self) -> Result<CatalogStatusReply, FusiformCatalogError>;
    async fn close(&mut self) -> Result<(), FusiformCatalogError>;
    async fn refresh_planes(
        &mut self,
        _snapshot: Option<&CatalogSnapshot>,
    ) -> Result<(), FusiformCatalogError> {
        Ok(())
    }
}

impl CatalogWire for FusiformCatalogConsumer {
    fn credentials(&self) -> Option<crate::broca_credentials::SharedBrocaCredentials> {
        self.credentials.clone()
    }
    async fn refresh_planes(
        &mut self,
        snapshot: Option<&CatalogSnapshot>,
    ) -> Result<(), FusiformCatalogError> {
        let Some(credentials) = self.credentials.clone() else {
            return Ok(());
        };
        let (keys, mut cache) = {
            let guard = credentials
                .lock()
                .map_err(|_| error("credential cache lock poisoned"))?;
            (guard.plane_keys(), guard.billing_catalog.clone())
        };
        if let Some(snapshot) = snapshot {
            cache.version = Some(snapshot.cursor.catalog_version);
            cache.base = base_rates_after(std::mem::take(&mut cache.base), snapshot);
            cache.pages.clear();
        }
        for (provider_id, auth_method) in keys {
            if cache
                .pages
                .contains_key(&(provider_id.clone(), auth_method.clone()))
            {
                continue;
            }
            let body = serde_json::to_vec(&CatalogGetRequest {
                name: CATALOG_GET,
                arguments: CatalogGetArguments {
                    include_retired: true,
                    provider_id: Some(provider_id.clone()),
                    auth_method: Some(auth_method.clone()),
                    ..Default::default()
                },
            })
            .map_err(FusiformCatalogError::Json)?;
            // no_coverage is a valid answer for an undeclared billing plane,
            // not a failed ordinary catalog poll. Error frames use this decoder
            // too, so neither transport envelope borrows API rows accidentally.
            let frame = self.call_frame(body).await?;
            let page = decode_plane_get(&frame.body)?;
            if let PlaneReply::Served(page) = &page {
                if page.plane.provider_id != provider_id || page.plane.auth_method != auth_method {
                    return Err(error("Fusiform replied on a different billing plane"));
                }
            }
            cache.pages.insert((provider_id, auth_method), page);
        }
        credentials
            .lock()
            .map_err(|_| error("credential cache lock poisoned"))?
            .publish_billing_catalog(cache);
        Ok(())
    }

    async fn catalog_get(&mut self) -> Result<CatalogSnapshot, FusiformCatalogError> {
        for restart in 0..=MAX_CATALOG_RESTARTS {
            let mut request = CatalogGetRequest::all_with_retired();
            let mut whole: Option<CatalogPage> = None;
            let mut tokens = BTreeSet::new();
            loop {
                let body = serde_json::to_vec(&request).map_err(FusiformCatalogError::Json)?;
                let reply = match self.call(body).await {
                    Err(FusiformCatalogError::Refused { code, .. })
                        if code == "catalog_changed" && request.arguments.page_token.is_some() =>
                    {
                        if restart == MAX_CATALOG_RESTARTS {
                            return Err(FusiformCatalogError::Refused {
                                code,
                                message: format!(
                                    "whole-catalog read exhausted {MAX_CATALOG_RESTARTS} restarts"
                                ),
                            });
                        }
                        // catalog_changed invalidates this continuation token.
                        // Discard the partial read and start from page one rather
                        // than retrying the obsolete token or publishing its pages.
                        break;
                    }
                    other => other?,
                };
                let page: CatalogPage = serde_json::from_value(unwrap_tool_result(&reply)?)
                    .map_err(FusiformCatalogError::Json)?;
                let next = page.next_page_token.clone();
                if let Some(whole) = whole.as_mut() {
                    whole.append(page)?;
                } else {
                    whole = Some(page);
                }
                let Some(token) = next else {
                    let bytes = serde_json::to_vec(&whole).map_err(FusiformCatalogError::Json)?;
                    return decode_catalog_get(&bytes);
                };
                if !tokens.insert(token.clone()) {
                    return Err(error("Fusiform catalog repeated a page token"));
                }
                request.arguments.page_token = Some(token);
            }
        }
        unreachable!("each exhausted catalog restart returns an error")
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
    unserved_routes: Vec<String>,
    reported_unserved_routes: BTreeSet<String>,
}

impl CatalogReachHealth {
    pub fn new(poll_ms: u64) -> Self {
        Self {
            poll_ms,
            last_reached_at_ms: None,
            last_error: None,
            unserved_routes: Vec::new(),
            reported_unserved_routes: BTreeSet::new(),
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

    fn note_unserved_routes(&mut self, routes: Vec<String>) -> Vec<String> {
        self.unserved_routes = routes;
        let mut newly_reported = Vec::new();
        for route in &self.unserved_routes {
            if self.reported_unserved_routes.insert(route.clone()) {
                newly_reported.push(route.clone());
            }
        }
        newly_reported
    }

    pub fn snapshot(&self, now_ms: u64) -> Value {
        let stale_after_ms = self.poll_ms.saturating_mul(2);
        match self.last_reached_at_ms {
            None => json!({
                "reach": "never-reached",
                "lastError": self.last_error,
                "unservedRoutes": self.unserved_routes,
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
                    "unservedRoutes": self.unserved_routes,
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

/// Apply one fetched catalog snapshot. A
/// catalog apply can add, withdraw, or re-price a route, so when `ladder` is
/// given the winner ladder is compared around it and a move is announced.
fn apply_catalog_snapshot(
    store: &SqliteRouteStateStore,
    snapshot: &CatalogSnapshot,
    config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
    credentials: Option<&crate::broca_credentials::SharedBrocaCredentials>,
    context: &str,
) -> Result<(), FusiformCatalogError> {
    apply_mapped_catalog(store, snapshot, credentials, None)
        .map_err(|reason| error(format!("apply {context}: {reason}")))?;
    if let Some(path) = config_path {
        let shadowed = crate::config_overrides::live_config(path).shadowed_operator_grades(
            &store
                .operator_quality()
                .map_err(|reason| error(format!("read operator quality: {reason}")))?,
        );
        if !shadowed.is_empty() {
            eprintln!(
                "[striatum] these models entries set elo or eq over the grade Fusiform serves, and the settings win: {shadowed:?}"
            );
        }
        report_dormant_config_models(store, path, snapshot.cursor.catalog_version, credentials)?;
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

fn report_dormant_config_models(
    store: &SqliteRouteStateStore,
    path: &Path,
    version: i64,
    credentials: Option<&crate::broca_credentials::SharedBrocaCredentials>,
) -> Result<(), FusiformCatalogError> {
    let config = crate::config_overrides::live_config(path);
    let presence = config
        .model_config_presence(store, credentials)
        .map_err(|reason| {
            error(format!(
                "read the catalog to check models entries: {reason}"
            ))
        })?;
    config.report_dormant_models(path, version, presence);
    Ok(())
}

pub(crate) fn report_unserved_routes(
    store: &SqliteRouteStateStore,
    health: &SharedCatalogHealth,
) -> Result<(), FusiformCatalogError> {
    let routes = crate::account_writer::prune_unserved_unpicked_facts(store)
        .map_err(|reason| error(format!("list and prune absent routes: {reason}")))?;
    if let Ok(mut health) = health.lock() {
        let newly_reported = health.note_unserved_routes(routes);
        if !newly_reported.is_empty() {
            eprintln!(
                "[striatum] Fusiform does not serve these routes: {}",
                newly_reported.join(", ")
            );
        }
    }
    Ok(())
}

pub(crate) fn apply_mapped_catalog(
    store: &SqliteRouteStateStore,
    snapshot: &CatalogSnapshot,
    credentials: Option<&crate::broca_credentials::SharedBrocaCredentials>,
    explicit_provider: Option<&str>,
) -> Result<(), FusiformCatalogError> {
    let mut providers = credentials
        .and_then(|cache| cache.lock().ok().map(|guard| guard.mapped_providers()))
        .unwrap_or_default();
    if let Some(provider) = explicit_provider {
        providers.insert(provider.to_string());
    }
    let retained: HashSet<(String, String)> = snapshot.retained.iter().cloned().collect();
    crate::account_writer::apply_fusiform_catalog_snapshot_for_providers(
        store,
        &snapshot.facts,
        snapshot.cursor,
        &providers,
        &retained,
    )
    .map_err(|reason| error(format!("apply mapped catalog: {reason}")))?;
    let mut grades = snapshot.grades.clone();
    let mut decommissioned = snapshot.decommissioned.clone();
    // Cache grades for provider aliases resolved from credential metadata,
    // using the same catalog targets as pricing for coverage and shadow notices.
    if let Some(credentials) = credentials {
        let mut facts = store
            .get_facts()
            .map_err(|reason| error(format!("read catalog targets: {reason}")))?;
        let targets = credentials
            .lock()
            .map_err(|_| error("credential cache poisoned"))?
            .catalog_targets(&facts);
        striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
        for fact in facts {
            let route = format!("{}/{}", fact.provider_id, fact.model_id);
            let catalog_key = striatum_core::score_alias::catalog_key(&targets, &route);
            if let Some(grade) = snapshot.grades.get(&catalog_key) {
                grades.entry(route).or_insert_with(|| grade.clone());
            }
            if let Some(model) = snapshot.decommissioned.get(&catalog_key) {
                decommissioned
                    .entry(format!("{}/{}", fact.provider_id, fact.model_id))
                    .or_insert_with(|| model.clone());
            }
        }
    }
    store
        .replace_operator_quality_snapshot(
            snapshot
                .quality_scores
                .as_ref()
                .is_some_and(|scores| scores.graded_by == "operator"),
            &grades,
            &decommissioned,
        )
        .map_err(|reason| error(format!("apply operator quality: {reason}")))?;
    if let Some(credentials) = credentials {
        let mut credentials = credentials
            .lock()
            .map_err(|_| error("credential cache poisoned"))?;
        credentials.wire_apis = snapshot.wire_apis.clone();
        credentials.wire_api_models = snapshot.wire_api_models.clone();
        credentials.catalog_snapshot = Some(Arc::new(snapshot.clone()));
    }
    Ok(())
}

/// `route.model_upsert` places operator-supplied quality scores on a model. First
/// apply the facts cached from Fusiform, fetching them if the model is not cached.
/// An operator can then onboard a provider before the background import runs
/// without creating a score-only row missing prices or allowed login methods.
pub(crate) async fn ingest_before_upsert(
    store: &SqliteRouteStateStore,
    credentials: &crate::broca_credentials::SharedBrocaCredentials,
    provider: &str,
    model: &str,
) -> Result<(), FusiformCatalogError> {
    let (snapshot, connection) = {
        let guard = credentials
            .lock()
            .map_err(|_| error("credential cache poisoned"))?;
        (
            guard.catalog_snapshot.clone(),
            guard.connection_file_path.clone(),
        )
    };
    let cached = snapshot.as_ref().is_some_and(|snapshot| {
        snapshot
            .facts
            .iter()
            .any(|fact| fact.provider_id == provider && fact.model_id == model)
    });
    let snapshot = if !cached {
        if let Some(connection) = connection {
            let root = std::env::temp_dir().join("routing-upsert-catalog");
            let mut client = FusiformCatalogConsumer::connect(&connection, &root).await?;
            let snapshot = client.catalog_get().await?;
            client.close().await?;
            Some(Arc::new(snapshot))
        } else {
            snapshot
        }
    } else {
        snapshot
    };
    if let Some(snapshot) = snapshot {
        apply_mapped_catalog(store, &snapshot, Some(credentials), Some(provider))?;
        credentials
            .lock()
            .map_err(|_| error("credential cache poisoned"))?
            .catalog_wake
            .notify_one();
    }
    Ok(())
}

/// Run one cycle's store work (catalog apply, family rules, ladder comparison,
/// pruning) on a blocking thread, so the SQLite writes and the ladder
/// computation never hold an async worker. A panic is resumed on the caller,
/// as it was when the work ran on the loop's own task.
async fn on_blocking_thread<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, FusiformCatalogError> + Send + 'static,
) -> Result<T, FusiformCatalogError> {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        // The blocking pool cancels only while the runtime shuts down.
        Err(failure) => Err(error(format!(
            "catalog store work did not finish: {failure}"
        ))),
    }
}

/// The store half of a cycle that fetched a snapshot: apply it, then prune and
/// report the routes Fusiform no longer serves.
async fn apply_and_report(
    store: &Arc<SqliteRouteStateStore>,
    health: &SharedCatalogHealth,
    snapshot: CatalogSnapshot,
    config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
    credentials: Option<crate::broca_credentials::SharedBrocaCredentials>,
    context: &'static str,
) -> Result<(), FusiformCatalogError> {
    let (store, health) = (Arc::clone(store), Arc::clone(health));
    let config_path = config_path.map(Path::to_path_buf);
    let ladder = ladder.cloned();
    on_blocking_thread(move || {
        apply_catalog_snapshot(
            &store,
            &snapshot,
            config_path.as_deref(),
            ladder.as_ref(),
            credentials.as_ref(),
            context,
        )?;
        report_unserved_routes(&store, &health)
    })
    .await
}

async fn bootstrap_cycle<C: CatalogWire>(
    client: &mut C,
    store: &Arc<SqliteRouteStateStore>,
    health: &SharedCatalogHealth,
    config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
) -> Result<CycleOutcome, FusiformCatalogError> {
    let snapshot = client.catalog_get().await?;
    client.refresh_planes(Some(&snapshot)).await?;
    apply_and_report(
        store,
        health,
        snapshot,
        config_path,
        ladder,
        client.credentials(),
        "catalog.get",
    )
    .await?;
    if let Ok(mut health) = health.lock() {
        health.note_reached(now_ms());
    }
    client.close().await?;
    Ok(CycleOutcome { get_calls: 1 })
}

async fn poll_cycle<C: CatalogWire>(
    client: &mut C,
    store: &Arc<SqliteRouteStateStore>,
    health: &SharedCatalogHealth,
    config_path: Option<&Path>,
    ladder: Option<&LadderSources>,
) -> Result<CycleOutcome, FusiformCatalogError> {
    let status = client.catalog_status().await?;
    if let Ok(mut health) = health.lock() {
        health.note_reached(now_ms());
    }
    let cursor_store = Arc::clone(store);
    let cursor = on_blocking_thread(move || {
        cursor_store
            .fusiform_catalog_cursor()
            .map_err(|reason| error(format!("read Fusiform cursor: {reason}")))
    })
    .await?;
    let should_get = cursor
        .map(|cursor| status.catalog_version > cursor.catalog_version)
        .unwrap_or(true);
    let get_calls = if should_get {
        let snapshot = client.catalog_get().await?;
        client.refresh_planes(Some(&snapshot)).await?;
        apply_and_report(
            store,
            health,
            snapshot,
            config_path,
            ladder,
            client.credentials(),
            "catalog replay",
        )
        .await?;
        1
    } else {
        client.refresh_planes(None).await?;
        if let (Some(path), Some(cursor)) = (config_path, cursor) {
            let path = path.to_path_buf();
            let store = Arc::clone(store);
            let credentials = client.credentials();
            on_blocking_thread(move || {
                report_dormant_config_models(
                    &store,
                    &path,
                    cursor.catalog_version,
                    credentials.as_ref(),
                )
            })
            .await?;
        }
        0
    };
    client.close().await?;
    Ok(CycleOutcome { get_calls })
}

pub async fn catalog_refresh_loop(
    connection_file_path: PathBuf,
    store: Arc<SqliteRouteStateStore>,
    health: SharedCatalogHealth,
    _poll_ms: u64,
    ladder_sources: LadderSources,
    credentials: crate::broca_credentials::SharedBrocaCredentials,
) {
    let project_root =
        std::env::temp_dir().join(format!("{}-fusiform-consumer", crate::DEFAULT_MODULE_ID));
    let _ = std::fs::create_dir_all(&project_root);
    let mut bootstrapped = false;
    let mut last_logged_error: Option<String> = None;
    let config_path = crate::config_overrides::config_path();
    let wake = {
        let mut guard = credentials.lock().unwrap();
        guard.connection_file_path = Some(connection_file_path.clone());
        Arc::clone(&guard.catalog_wake)
    };
    loop {
        let result =
            match FusiformCatalogConsumer::connect(&connection_file_path, &project_root).await {
                Ok(mut consumer) if bootstrapped => {
                    consumer.credentials = Some(Arc::clone(&credentials));
                    poll_cycle(
                        &mut consumer,
                        &store,
                        &health,
                        Some(&config_path),
                        Some(&ladder_sources),
                    )
                    .await
                }
                Ok(mut consumer) => {
                    consumer.credentials = Some(Arc::clone(&credentials));
                    bootstrap_cycle(
                        &mut consumer,
                        &store,
                        &health,
                        Some(&config_path),
                        Some(&ladder_sources),
                    )
                    .await
                }
                Err(reason) => Err(reason),
            };
        match result {
            Ok(_) => {
                bootstrapped = true;
                let cadence = crate::config_overrides::live_config(&config_path).catalog_poll_ms();
                if let Ok(mut health) = health.lock() {
                    health.set_poll_ms(cadence);
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(cadence)) => {},
                    _ = wake.notified() => { bootstrapped = false; },
                }
            }
            Err(reason) => {
                // One line per DISTINCT reason: a repeating failure is visible in the
                // health snapshot's lastError, a changing one is worth a log line.
                let rendered = reason.to_string();
                if last_logged_error.as_deref() != Some(rendered.as_str()) {
                    eprintln!("[striatum] fusiform catalog cycle failed: {rendered}");
                    last_logged_error = Some(rendered);
                }
                if let Ok(mut health) = health.lock() {
                    health.note_error(reason);
                }
                tokio::select! {
                    _ = tokio::time::sleep(RETRY_INTERVAL) => {},
                    _ = wake.notified() => { bootstrapped = false; },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    include!("fusiform_catalog_login_methods_tests.rs");
    include!("operator_quality_tests.rs");
    include!("fusiform_catalog_paging_tests.rs");
    include!("billing_readiness_tests.rs");
    include!("lineage_tests.rs");
    use super::*;
    use sha2::{Digest, Sha256};

    struct Rig {
        dir: PathBuf,
    }

    impl Rig {
        fn new(label: &str) -> Self {
            let dir = cortexkit_test_support::scratch_root().join(format!(
                "prefrontal-fusiform-{label}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self { dir }
        }

        fn store(&self) -> SqliteRouteStateStore {
            SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
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

    fn media_snapshot(models: Value, graded_by: Option<&str>) -> CatalogSnapshot {
        let mut reply = json!({"catalog_version": 1, "resolved_at_ms": 1, "models": models});
        if let Some(graded_by) = graded_by {
            reply["quality_scores"] = json!({"graded_by": graded_by, "graded_at_ms": 1});
        }
        decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap()
    }

    fn speech_reply() -> Value {
        fixture()["catalog.get — Eleven Flash TTS"]["response"].clone()
    }

    #[test]
    fn media_grades_require_operator_positive_numbers_and_matching_types() {
        let kinds = [
            ("transcription", "transcribe"),
            ("transcription", "stream_transcribe"),
            ("speech", "synthesize"),
            ("image", "image"),
            ("video", "video"),
            ("realtime", "realtime"),
        ];
        for (model_type, kind) in kinds {
            let row =
                json!({"existence": "present", "model.type": model_type, "quality.grade": 80});
            let snapshot = media_snapshot(json!({"p/m": row}), Some("operator"));
            assert_eq!(snapshot.media_grade(kind, "p/m"), Some(80.0), "{kind}");
            assert_eq!(snapshot.media_grade(kind, "p/missing"), None);
            for (_, other_kind) in kinds {
                if !matches!(
                    (kind, other_kind),
                    ("transcribe", "stream_transcribe") | ("stream_transcribe", "transcribe")
                ) && kind != other_kind
                {
                    assert_eq!(snapshot.media_grade(other_kind, "p/m"), None);
                }
            }
            for marker in [None, Some("creator"), Some("Operator")] {
                let snapshot = media_snapshot(json!({"p/m": row}), marker);
                assert_eq!(snapshot.media_grade(kind, "p/m"), None);
            }
            for grade in [
                Value::Null,
                json!(0),
                json!(-1),
                json!("NaN"),
                json!("Infinity"),
            ] {
                let mut invalid = row.clone();
                invalid["quality.grade"] = grade;
                let snapshot = media_snapshot(json!({"p/m": invalid}), Some("operator"));
                assert_eq!(snapshot.media_grade(kind, "p/m"), None);
            }
            for model_type in [Value::Null, json!("chat"), json!("stream_transcribe")] {
                let mut wrong_type = row.clone();
                wrong_type["model.type"] = model_type;
                let snapshot = media_snapshot(json!({"p/m": wrong_type}), Some("operator"));
                assert_eq!(snapshot.media_grade(kind, "p/m"), None);
            }
            let mut missing = row.clone();
            missing.as_object_mut().unwrap().remove("quality.grade");
            assert_eq!(
                media_snapshot(json!({"p/m": missing}), Some("operator")).media_grade(kind, "p/m"),
                None
            );
            let mut missing_type = row;
            missing_type.as_object_mut().unwrap().remove("model.type");
            assert_eq!(
                media_snapshot(json!({"p/m": missing_type}), Some("operator"))
                    .media_grade(kind, "p/m"),
                None
            );
        }
    }

    #[test]
    fn media_facts_decode_absent_and_deprecated_rows_without_filtering() {
        for existence in ["present", "absent"] {
            for status in ["deprecated", "beta", "alpha"] {
                let mut reply = speech_reply();
                let row = &mut reply["models"]["elevenlabs/eleven_flash_v2_5"];
                row["existence"] = json!(existence);
                row["model.status"] = json!(status);
                row["quality.grade"] = json!(75);
                reply["quality_scores"] = json!({"graded_by": "operator", "graded_at_ms": 1});
                let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
                let route = "elevenlabs/eleven_flash_v2_5";
                assert_eq!(snapshot.media_grade("synthesize", route), Some(75.0));
                assert_eq!(
                    snapshot.media_rates(route).unwrap()["rate.input.per_character"].unit_price,
                    Some(0.00004)
                );
                let fact = snapshot.model(route).unwrap();
                assert_eq!(fact.present, existence == "present");
                assert_eq!(fact.upstream_status.as_deref(), Some(status));
            }
        }
    }

    #[test]
    fn media_rates_normalize_pinned_units_and_retain_every_rate_key_and_state() {
        let payloads = fixture();
        let mut media_rows = 0;
        for payload in payloads.as_object().unwrap().values() {
            let reply = &payload["response"];
            let Some(models) = reply["models"].as_object() else {
                continue;
            };
            let snapshot = decode_catalog_get(&serde_json::to_vec(reply).unwrap()).unwrap();
            for (route, row) in models {
                if !matches!(
                    row["model.type"].as_str(),
                    Some("transcription" | "speech" | "image" | "video" | "realtime")
                ) {
                    continue;
                }
                media_rows += 1;
                let expected: BTreeMap<_, _> = row
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| key.starts_with("rate."))
                    .map(|(key, value)| (key.clone(), value["state"].as_str().map(str::to_owned)))
                    .collect();
                if row.get("existence").is_none() {
                    // The grade-only reply has no complete fact row or rates;
                    // it must not invent a readable row in the rate table.
                    assert!(expected.is_empty());
                    assert!(snapshot.media_rates(route).is_none());
                    continue;
                }
                let rates = snapshot.media_rates(route).expect("readable media row");
                assert_eq!(
                    rates
                        .iter()
                        .map(|(key, rate)| (key.clone(), rate.state.clone()))
                        .collect::<BTreeMap<_, _>>(),
                    expected
                );
                for (key, rate) in rates {
                    let expected_price = match (route.as_str(), key.as_str()) {
                        ("elevenlabs/eleven_flash_v2_5", "rate.input.per_character") => {
                            Some(0.00004)
                        }
                        ("openai/gpt-transcribe", "rate.input_audio.per_second") => Some(0.000075),
                        ("openai/gpt-live-transcribe", "rate.input_audio.per_second") => {
                            Some(0.017 / 60.0)
                        }
                        _ => None,
                    };
                    match (rate.unit_price, expected_price) {
                        (Some(actual), Some(expected)) => {
                            assert!((actual - expected).abs() < 1e-12, "{route} {key}: {actual}")
                        }
                        (actual, expected) => assert_eq!(actual, expected, "{route} {key}"),
                    }
                    assert_eq!(rate.per, row[key]["per"].as_u64());
                    assert_eq!(rate.increment, RateQuantityKind::Unspecified);
                    assert_eq!(rate.minimum_quantity, RateQuantityKind::Unspecified);
                }
            }
        }
        assert_eq!(media_rows, 9);
    }

    #[test]
    fn media_rates_keep_known_zero_incurred_and_refused_states_distinct() {
        for state in [
            "priced",
            "stated_zero",
            "not_billed",
            "unpriced",
            "billed_as",
            "future_state",
        ] {
            let mut reply = speech_reply();
            let rate =
                &mut reply["models"]["elevenlabs/eleven_flash_v2_5"]["rate.input.per_character"];
            rate["state"] = json!(state);
            rate["key"] = json!("rate.output");
            let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
            let decoded = &snapshot
                .media_rates("elevenlabs/eleven_flash_v2_5")
                .unwrap()["rate.input.per_character"];
            assert_eq!(decoded.state.as_deref(), Some(state));
            assert_eq!(
                decoded.unit_price,
                match state {
                    "priced" => Some(0.00004),
                    "stated_zero" | "not_billed" => Some(0.0),
                    _ => None,
                }
            );
        }
        for (field, invalid) in [
            ("per", json!(0)),
            ("per", Value::Null),
            ("per", json!(-1)),
            ("currency", json!("EUR")),
            ("units", json!(-1)),
            ("exponent", json!(19)),
        ] {
            let mut reply = speech_reply();
            reply["models"]["elevenlabs/eleven_flash_v2_5"]["rate.input.per_character"][field] =
                invalid;
            let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
            let decoded = &snapshot
                .media_rates("elevenlabs/eleven_flash_v2_5")
                .unwrap()["rate.input.per_character"];
            assert_eq!(decoded.state.as_deref(), Some("priced"));
            assert_eq!(decoded.unit_price, None, "{field}");
        }
    }

    #[test]
    fn media_rate_quantity_kinds_keep_unknown_absent_and_unrecognised_distinct() {
        for field in ["increment", "minimum_quantity"] {
            for shape in [
                None,
                Some(json!({"kind": "unknown"})),
                Some(json!({"kind": "stated", "quantity": 10})),
                Some(json!({"kind": "none"})),
                Some(json!({"kind": 7})),
            ] {
                let mut reply = speech_reply();
                let rate = &mut reply["models"]["elevenlabs/eleven_flash_v2_5"]
                    ["rate.input.per_character"];
                match &shape {
                    Some(shape) => {
                        rate[field] = shape.clone();
                    }
                    None => {
                        rate.as_object_mut().unwrap().remove(field);
                    }
                }
                let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
                let rate = &snapshot
                    .media_rates("elevenlabs/eleven_flash_v2_5")
                    .unwrap()["rate.input.per_character"];
                let decoded = if field == "increment" {
                    &rate.increment
                } else {
                    &rate.minimum_quantity
                };
                let expected = match shape {
                    None => RateQuantityKind::Unspecified,
                    Some(shape) if shape["kind"] == "unknown" => RateQuantityKind::Unspecified,
                    Some(shape) => RateQuantityKind::Unrecognised(shape["kind"].clone()),
                };
                assert_eq!(*decoded, expected);
            }
        }
    }

    #[test]
    fn media_decoding_preserves_legacy_engine_cost() {
        use cortexkit_role_routing::select_engine::SelectEngineRequest;
        let mut reply = speech_reply();
        let mut audio = fixture()["catalog.get — curated unit-billed transcription rate"]
            ["response"]["models"]["openai/gpt-transcribe"]
            .clone();
        assert!(audio.is_object());
        audio["model.type"] = json!("chat");
        audio["rate.output"] =
            json!({"state": "priced", "currency": "USD", "exponent": 0, "units": 999});
        audio["rate.input_audio.per_second"]["increment"] = json!({"kind": "stated"});
        audio["rate.input_audio.per_second"]["minimum_quantity"] = json!({"kind": "stated"});
        reply["models"]["openai/gpt-transcribe"] = audio;
        reply["models"]["google/gemini-3.5-transcribe-live"] = fixture()
            ["catalog.get — Gemini Transcribe Live token rates"]["response"]["models"]
            ["google/gemini-3.5-transcribe-live"]
            .clone();
        let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
        for kind in ["transcribe", "stream_transcribe", "synthesize"] {
            let request: SelectEngineRequest = serde_json::from_value(json!({"consumer": "c", "kind": kind, "privacy": "remote_allowed", "requirements": {"duration_ms": 60_000, "input_bytes": 1000}})).unwrap();
            assert_eq!(
                snapshot.engine_cost(&request, "openai", "gpt-transcribe"),
                if kind == "synthesize" {
                    None
                } else {
                    Some(0.0045)
                }
            );
            assert_eq!(
                snapshot.engine_cost(&request, "elevenlabs", "eleven_flash_v2_5"),
                None
            );
            assert_eq!(
                snapshot.engine_cost(&request, "google", "gemini-3.5-transcribe-live"),
                if kind == "synthesize" {
                    None
                } else {
                    Some(0.00525)
                }
            );
        }
    }

    #[test]
    fn media_decoding_preserves_chat_quality_without_media_grade_type_or_value_gates() {
        let row = json!({"existence": "absent", "model.type": "speech", "model.status": "deprecated", "quality.elo": 1200, "quality.eq": 90, "quality.grade": 0, "quality.decommissioned": true});
        for marker in [None, Some("creator"), Some("operator")] {
            let snapshot = media_snapshot(json!({"p/m": row}), marker);
            if marker == Some("operator") {
                let quality = &snapshot.grades["p/m"];
                assert_eq!((quality.elo, quality.eq), (1200.0, 90.0));
            } else {
                assert!(snapshot.grades.is_empty());
            }
        }
    }

    #[test]
    fn media_decoding_preserves_decommissioning_without_operator_or_grade_gates() {
        let row = json!({"existence": "absent", "model.type": "speech", "model.status": "deprecated", "quality.grade": 0, "quality.decommissioned": true});
        for marker in [None, Some("creator"), Some("operator")] {
            let snapshot = media_snapshot(json!({"p/m": row}), marker);
            assert!(snapshot.is_decommissioned("p/m"));
        }
    }

    #[test]
    fn price_rate_lifetimes_survive_catalog_and_plane_ingestion_verbatim() {
        for (model, lifetimes) in [
            ("anthropic/claude", [("5m", 3.75), ("1h", 6.0)]),
            ("openai/gpt-6.1", [("30m", 3.125), ("12h", 5.0)]),
        ] {
            let mut entry = readable_entry(200_000);
            for (lifetime, rate) in lifetimes {
                entry[format!("rate.cache_write.ttl.{lifetime}")] = json!({"state": "priced", "currency": "USD", "exponent": 6, "units": (rate * 1_000_000.0) as i64});
                entry[format!("rate.cache_write.ttl.{lifetime}.above_context.200000")] = json!({"state": "priced", "currency": "USD", "exponent": 6, "units": (rate * 2_000_000.0) as i64});
                entry[format!("rate.cache_write.ttl.{lifetime}.mode.fast")] = json!({"state": "priced", "currency": "USD", "exponent": 6, "units": (rate * 3_000_000.0) as i64});
            }
            let snapshot = isolation_catalog(1, json!({model: entry.clone()}));
            let plane_page = model_rates(&json!({model: entry})).unwrap();
            let catalog = PlaneCatalog {
                base: snapshot.rates,
                ..Default::default()
            };
            assert_eq!(catalog.base, plane_page);
            let mode = catalog.mode_rates(model, "fast");
            for (lifetime, rate) in lifetimes {
                let key = format!("rate.cache_write.ttl.{lifetime}");
                assert_eq!(
                    catalog.base[model].rates[&key],
                    BillingRate::Priced { rate }
                );
                assert_eq!(
                    catalog.base[model].rates[&format!("{key}.above_context.200000")],
                    BillingRate::Priced { rate: rate * 2.0 }
                );
                assert_eq!(mode[&key], BillingRate::Priced { rate: rate * 3.0 });
            }
        }
    }

    /// Only `effort` options name variants; toggles and token budgets do not,
    /// and a served `null` or empty list publishes none. The served shapes are
    /// the ones in the producer fixture, set on its current one-model reply.
    #[test]
    fn reasoning_options_decode_to_the_published_effort_values() {
        let served = |options: Option<Value>| {
            let mut reply: Value =
                serde_json::from_slice(&get_reply("catalog.get — one model, current")).unwrap();
            for facts in reply["models"].as_object_mut().unwrap().values_mut() {
                let facts = facts.as_object_mut().unwrap();
                match &options {
                    Some(options) => {
                        facts.insert("capability.reasoning_options".into(), options.clone());
                    }
                    None => {
                        facts.remove("capability.reasoning_options");
                    }
                }
            }
            decode_catalog_get(&serde_json::to_vec(&reply).unwrap())
                .unwrap()
                .facts[0]
                .reasoning_efforts
                .clone()
        };
        let efforts = |values: &[&str]| Some(values.iter().map(|v| (*v).to_string()).collect());
        assert_eq!(
            served(Some(json!([
                {"type": "toggle"},
                {"type": "effort", "values": ["low", "medium", "high"]},
                {"max": 127999, "min": 1024, "type": "budget_tokens"}
            ]))),
            efforts(&["low", "medium", "high"])
        );
        assert_eq!(
            served(Some(json!([{"min": 1024, "type": "budget_tokens"}]))),
            efforts(&[])
        );
        assert_eq!(served(Some(json!([]))), efforts(&[]));
        assert_eq!(served(Some(Value::Null)), efforts(&[]));
        assert_eq!(
            served(None),
            None,
            "a catalog that does not serve the key leaves the row unread"
        );
    }

    /// Routing's catalog reads carry no fact filter, so Fusiform serves every
    /// fact family, the `model.` facts (identity and status) included. A filter
    /// listing only `rate.`, `limit.` or `capability.` prefixes would drop them.
    #[test]
    fn catalog_reads_request_every_fact_family_including_model_facts() {
        let arguments = serde_json::to_value(CatalogGetRequest::all_with_retired()).unwrap()
            ["arguments"]
            .clone();
        let keys = arguments
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(keys, ["include_retired", "paged"], "no fact filter is sent");

        let mut reply: Value =
            serde_json::from_slice(&get_reply("catalog.get — one model, current")).unwrap();
        for facts in reply["models"].as_object_mut().unwrap().values_mut() {
            facts["model.status"] = json!("deprecated");
            facts["model.canonical_id"] = json!("acme/one-1");
        }
        let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
        assert_eq!(
            snapshot.facts[0].upstream_status.as_deref(),
            Some("deprecated")
        );
        assert_eq!(
            snapshot.facts[0].canonical_id.as_deref(),
            Some("acme/one-1")
        );
    }

    fn muse_reply() -> Value {
        let mut reply: Value =
            serde_json::from_slice(include_bytes!("../fixtures/muse-curated.json")).unwrap();
        // The fixture omits catalog_version and resolved_at_ms, required for
        // polling. Add those fields without changing its model and curated rows.
        reply["catalog_version"] = json!(1);
        reply["resolved_at_ms"] = json!(-1);
        reply
    }

    fn stored_muse_facts(rig: &Rig) -> Vec<striatum_core::ModelFact> {
        use striatum_core::{FactSource, RouteStateStore as _};
        let store = rig.store();
        // Hard admission rejects unscored facts before checking credentials.
        // Score only these test fixtures so the credential policy is exercised;
        // do not add a Muse placement to the production catalog.
        for model in ["muse-wonder-full-1.0", "unrestricted"] {
            store
                .upsert_fact(&crate::tests::fact("meta", model), FactSource::Seed)
                .unwrap();
        }
        let snapshot = decode_catalog_get(&serde_json::to_vec(&muse_reply()).unwrap()).unwrap();
        assert_eq!(snapshot.facts[0].auth_methods, Some(vec!["oauth".into()]));
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .unwrap();
        drop(store);
        rig.store()
            .get_facts()
            .unwrap()
            .into_iter()
            .filter(|fact| fact.provider_id == "meta")
            .collect()
    }

    fn muse_credential(method: &str, mapped: bool) -> cortexkit_claustrum_client::ListedCredential {
        cortexkit_claustrum_client::ListedCredential {
            id: format!("{method}:meta"),
            categories: vec!["llm-provider".into()],
            credential_type: method.into(),
            auth_method: Some(method.into()),
            serves: vec!["meta".into()],
            provider_ids: if mapped {
                vec!["meta".into()]
            } else {
                Vec::new()
            },
            state: "active".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn new_vault_mapping_wakes_catalog_and_ingests_prices_and_auth_at_unchanged_version() {
        use striatum_core::RouteStateStore as _;
        let rig = Rig::new("mapped-muse-ingest");
        let store = rig.store();
        let mut reply = muse_reply();
        reply["models"]["meta/muse-wonder-1.0"] = json!({
            "existence": "present", "rate.input": {"state":"priced","currency":"USD","exponent":0,"units":4},
            "rate.output": {"state":"priced","currency":"USD","exponent":0,"units":20}
        });
        reply["models"]["meta/muse-spark-1.3-contributor"] = json!({
            "existence": "present", "rate.input": {"state":"priced","currency":"USD","exponent":3,"units":100},
            "rate.output": {"state":"priced","currency":"USD","exponent":3,"units":200},
            "rate.cache_read": {"state":"priced","currency":"USD","exponent":3,"units":2}
        });
        reply["curated"]
            .as_array_mut()
            .unwrap()
            .push(json!({"model":"meta/muse-wonder-1.0","auth_methods":["oauth"]}));
        reply["curated"]
            .as_array_mut()
            .unwrap()
            .push(json!({"model":"meta/muse-spark-1.3-contributor","auth_methods":["oauth"]}));
        let snapshot = decode_catalog_get(&serde_json::to_vec(&reply).unwrap()).unwrap();
        let credentials = Arc::new(Mutex::new(
            crate::broca_credentials::BrocaCredentials::default(),
        ));
        apply_mapped_catalog(&store, &snapshot, Some(&credentials), None).unwrap();
        assert!(store
            .get_fact_by_route_key("meta/muse-wonder-1.0")
            .unwrap()
            .is_none());
        let wake = Arc::clone(&credentials.lock().unwrap().catalog_wake);
        credentials.lock().unwrap().apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "mapped".into(),
                credentials: vec![muse_credential("oauth", true)],
                ..Default::default()
            },
            1,
        );
        tokio::time::timeout(Duration::from_secs(1), wake.notified())
            .await
            .expect("a mapping must wake the catalog loop, not wait ten minutes");
        assert!(credentials
            .lock()
            .unwrap()
            .plane_keys()
            .contains(&("meta".into(), "oauth".into())));
        apply_mapped_catalog(&store, &snapshot, Some(&credentials), None).unwrap();
        let fact = store
            .get_fact_by_route_key("meta/muse-wonder-1.0")
            .unwrap()
            .unwrap();
        assert_eq!(fact.cost_per_m_tok_out, Some(20.0));
        assert_eq!(fact.input_rate, Some(4.0));
        assert_eq!(fact.auth_methods, Some(vec!["oauth".into()]));
        let contributor = store
            .get_fact_by_route_key("meta/muse-spark-1.3-contributor")
            .unwrap()
            .unwrap();
        assert_eq!(contributor.input_rate, Some(0.10));
        assert_eq!(contributor.cost_per_m_tok_out, Some(0.20));
        assert_eq!(contributor.cache_read_rate, Some(0.002));
        assert_eq!(
            fact.score_provenance,
            striatum_core::ScoreProvenance::Unscored
        );
        assert_eq!(
            store.fusiform_catalog_cursor().unwrap(),
            Some(snapshot.cursor)
        );
    }

    #[test]
    fn muse_golden_curated_auth_methods_gate_credential_admission() {
        use striatum_core::{
            admit_routes, ExclusionPolicy, ModelRequirements, DEFAULT_ROUTING_CONFIG,
        };
        let rig = Rig::new("muse-auth-admission");
        let facts = stored_muse_facts(&rig);
        let restricted = "meta/muse-wonder-full-1.0";
        for (methods, admitted) in [
            (vec!["apikey"], false),
            (vec!["oauth"], true),
            (vec!["apikey", "oauth"], true),
        ] {
            let mut cache = crate::broca_credentials::BrocaCredentials::default();
            cache.apply_listing(
                &cortexkit_claustrum_client::ScopedCredentialListing {
                    credentials: methods
                        .iter()
                        .map(|method| muse_credential(method, true))
                        .collect(),
                    view: "muse-test".into(),
                    ..Default::default()
                },
                1,
            );
            let (routes, fallback) = cache.served_routes(&facts, 1);
            assert!(fallback.is_none());
            assert!(routes.iter().any(|key| key == "meta/unrestricted"));
            let available = routes.into_iter().collect();
            let admission = admit_routes(
                &facts,
                None,
                ExclusionPolicy::default(),
                None,
                &available,
                &ModelRequirements { iq: 0.0, eq: 0.0 },
                &DEFAULT_ROUTING_CONFIG,
            );
            assert_eq!(
                admission
                    .admitted
                    .iter()
                    .any(|row| row.route.route_key == restricted),
                admitted,
                "credential methods {methods:?}"
            );
            if !admitted {
                assert!(admission
                    .rejected
                    .iter()
                    .any(|row| row.route_key == restricted && row.reason == "not_available"));
            }
            cache.refresh_serving_methods(
                &crate::account_quota::AccountQuotaState::default(),
                &facts,
                1,
                &[],
            );
            if methods.contains(&"oauth") {
                assert!(cache
                    .plane_keys()
                    .contains(&("meta".into(), "oauth".into())));
            }
            if methods.len() > 1 {
                assert!(!cache.plane_keys().contains(&("meta".into(), "apikey".into())),
                    "an ineligible key must not make the restricted model's billing method ambiguous");
            }
        }
    }

    #[test]
    fn muse_requires_fresh_provider_membership_even_with_catalog_fallback() {
        let rig = Rig::new("muse-no-auto-route");
        let facts = stored_muse_facts(&rig);
        let restricted = "meta/muse-wonder-full-1.0";
        let mut cache = crate::broca_credentials::BrocaCredentials::default();
        assert!(!cache
            .served_routes(&facts, 1)
            .0
            .contains(&restricted.into()));
        cache.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: vec![muse_credential("oauth", false)],
                view: "unmapped".into(),
                ..Default::default()
            },
            1,
        );
        assert!(
            cache
                .served_routes(&facts, 1)
                .0
                .contains(&restricted.into()),
            "one catalog vendor in serves supplies account-set membership"
        );
        assert!(cache
            .served_routes(&facts, 1)
            .0
            .contains(&"meta/unrestricted".into()));
        cache.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: vec![muse_credential("oauth", true)],
                view: "mapped".into(),
                ..Default::default()
            },
            1,
        );
        assert!(cache
            .served_routes(&facts, 1)
            .0
            .contains(&restricted.into()));
        assert!(
            !cache
                .served_routes(&facts, 1 + 16 * 60 * 1000)
                .0
                .contains(&restricted.into()),
            "a stale listing cannot prove the allowed credential is still mapped"
        );
    }

    #[test]
    fn curated_auth_methods_refresh_all_variants_and_fingerprint() {
        use striatum_core::{FactSource, RouteStateStore as _};
        let rig = Rig::new("muse-auth-refresh");
        let facts = stored_muse_facts(&rig);
        let store = rig.store();
        let mut variant = facts
            .iter()
            .find(|fact| fact.model_id == "muse-wonder-full-1.0")
            .unwrap()
            .clone();
        variant.variant = Some("high".into());
        variant.auth_methods = None;
        store.upsert_fact(&variant, FactSource::Override).unwrap();
        assert_eq!(
            store.get_fact_by_route_key("meta/muse-wonder-full-1.0/high").unwrap().unwrap().auth_methods,
            Some(vec!["oauth".into()]),
            "a new score variant must inherit the served model's policy before the next catalog poll"
        );
        let mut reply = muse_reply();
        let apply = |reply: &Value| {
            let snapshot = decode_catalog_get(&serde_json::to_vec(reply).unwrap()).unwrap();
            store
                .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
                .unwrap();
        };
        apply(&reply);
        let restricted_fingerprint = store.catalog_fingerprint().unwrap();
        for key in [
            "meta/muse-wonder-full-1.0",
            "meta/muse-wonder-full-1.0/high",
        ] {
            assert_eq!(
                store
                    .get_fact_by_route_key(key)
                    .unwrap()
                    .unwrap()
                    .auth_methods,
                Some(vec!["oauth".into()])
            );
        }
        reply["curated"][0]["auth_methods"] = json!([]);
        reply["catalog_version"] = json!(2);
        apply(&reply);
        assert_ne!(restricted_fingerprint, store.catalog_fingerprint().unwrap());
        assert_eq!(
            store
                .get_fact_by_route_key("meta/muse-wonder-full-1.0/high")
                .unwrap()
                .unwrap()
                .auth_methods,
            Some(Vec::new())
        );
        let empty_policy = store.get_facts().unwrap();
        let mut cache = crate::broca_credentials::BrocaCredentials::default();
        cache.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                credentials: vec![muse_credential("oauth", true)],
                view: "empty-policy".into(),
                ..Default::default()
            },
            1,
        );
        assert!(
            !cache
                .served_routes(&empty_policy, 1)
                .0
                .iter()
                .any(|key| key.starts_with("meta/muse-wonder-full-1.0")),
            "an empty allowlist denies every credential, including OAuth"
        );
        reply.as_object_mut().unwrap().remove("curated");
        reply["catalog_version"] = json!(3);
        apply(&reply);
        let fact = store
            .get_fact_by_route_key("meta/muse-wonder-full-1.0/high")
            .unwrap()
            .unwrap();
        assert_eq!(
            fact.auth_methods, None,
            "absence removes a prior catalog restriction"
        );
        assert_eq!(
            fact.scores, variant.scores,
            "catalog policy must not rewrite score overrides"
        );
    }

    #[test]
    fn all_pinned_plane_cases_and_refusals_decode() {
        let fixture = fixture();
        let mut served = 0;
        let mut refused = 0;
        for (name, case) in fixture.as_object().unwrap() {
            if case["request"]["arguments"].get("auth_method").is_none() {
                continue;
            }
            let payload = case.get("response").unwrap_or(&case["error"]);
            let decoded = decode_plane_get(&serde_json::to_vec(payload).unwrap())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let wrapped = json!({"result": payload});
            assert_eq!(
                decoded,
                decode_plane_get(&serde_json::to_vec(&wrapped).unwrap()).unwrap()
            );
            match decoded {
                PlaneReply::Served(page) => {
                    served += 1;
                    assert_eq!(
                        page.plane.auth_method,
                        case["request"]["arguments"]["auth_method"]
                    );
                    assert_eq!(
                        page.plane.provider_id,
                        case["request"]["arguments"]["provider_id"]
                    );
                }
                PlaneReply::Refused { code, .. } => {
                    refused += 1;
                    assert_eq!(code, case["error"]["code"]);
                }
            }
        }
        assert_eq!((served, refused), (25, 5));
        let pin: Value =
            serde_json::from_str(include_str!("../fixtures/served-payloads.pin.json")).unwrap();
        assert_eq!(pin["commit"], "9591c735106d7d31f6ba917be96b00810be81af0");
        assert_eq!(
            pin["sha256"],
            format!(
                "{:x}",
                Sha256::digest(include_bytes!("../fixtures/served-payloads.json"))
            )
        );
    }

    fn fixture_plane(name: &str, model: &str) -> BillingPlane {
        let PlaneReply::Served(page) = decode_plane_get(&get_reply(name)).unwrap() else {
            panic!("served fixture")
        };
        BillingPlane {
            provider_id: page.plane.provider_id,
            auth_method: page.plane.auth_method,
            kind: page.plane.kind,
            selection_reason: "roster".into(),
            rates: page.rates[model].rates.clone(),
        }
    }

    fn vector() -> striatum_core::ClassVector {
        striatum_core::ClassVector {
            workload_class: "ephemeral".into(),
            sample_count: 50,
            trailing_window_days: 7.0,
            statistic: "mean".into(),
            is_overlay: false,
            fresh_input: 0.0,
            output_excluding_reasoning: 0.0,
            reasoning: 0.0,
            cached_read: 0.0,
            cache_write: 1000.0,
        }
    }

    #[test]
    fn chatgpt_cache_writes_are_priced_at_input_via_billed_as() {
        let mut fact = crate::tests::fact("openai", "gpt-5.6-luna");
        fact.cache_write_rate = Some(99.0);
        fact.billing_plane = Some(fixture_plane(
            "chatgpt luna — named, unfiltered",
            "openai/gpt-5.6-luna",
        ));
        let price = fact.basket_price(&vector());
        assert!(price.unpriced.is_empty());
        let expected = 0.0002 / striatum_core::BASKET_REFERENCE_COST_PER_MESSAGE;
        assert!((price.cost.unwrap() - expected).abs() < 1e-12);
    }

    #[test]
    fn anthropic_oauth_uses_quota_proxy_prices_not_the_api_fact() {
        let mut fact = crate::tests::fact("anthropic", "claude-sonnet-4-5");
        fact.cache_write_rate = Some(99.0);
        fact.billing_plane = Some(fixture_plane(
            "oauth anthropic — quota weights",
            "anthropic/claude-sonnet-4-5",
        ));
        assert_eq!(fact.billing_plane.as_ref().unwrap().kind, "quota_proxy");
        assert_eq!(
            fact.basket_rate(striatum_core::BasketDimension::CacheWrite),
            Some(3.75)
        );
        let expected = 0.00375 / striatum_core::BASKET_REFERENCE_COST_PER_MESSAGE;
        assert!((fact.basket_price(&vector()).cost.unwrap() - expected).abs() < 1e-12);
    }

    fn fallback_catalog() -> PlaneCatalog {
        PlaneCatalog {
            version: Some(0),
            base: model_rates(&json!({"openai/example": {
                "rate.input": {"state":"priced", "currency":"USD", "units":2000000000_i64, "exponent":9},
                "rate.output": {"state":"priced", "currency":"USD", "units":4000000000_i64, "exponent":9, "inherited_from":{"basis":"family"}}
            }})).unwrap(),
            pages: ["oauth", "apikey"].into_iter().map(|method| (("openai".into(), method.into()), PlaneReply::Refused { code:"no_coverage".into(), message:"undeclared".into() })).collect(),
        }
    }

    #[test]
    fn subscription_no_coverage_refuses_unmarked_rates_and_ranks_as_max() {
        let mut fact = crate::tests::fact("openai", "example");
        fact.billing_plane = Some(fallback_catalog().for_route(
            "openai/example",
            &crate::broca_credentials::ServingMethod {
                auth_method: Some("oauth".into()),
                reason: "single listed method".into(),
            },
            false,
        ));
        let mut vector = vector();
        vector.cache_write = 0.0;
        vector.fresh_input = 1000.0;
        assert_eq!(
            fact.basket_rate(striatum_core::BasketDimension::FreshInput),
            None
        );
        assert_eq!(
            fact.basket_rate(striatum_core::BasketDimension::OutputExcludingReasoning),
            Some(4.0)
        );
        assert_eq!(
            fact.basket_price(&vector).unpriced,
            [striatum_core::BasketDimension::FreshInput]
        );
        fact.class_vector = Some(vector);
        assert_eq!(striatum_core::selection_cost(&fact, true, true), 1.0);
        fact.billing_plane.as_mut().unwrap().rates.clear();
        fact.cost_per_m_tok_out = None;
        fact.cost_tier = None;
        assert_eq!(striatum_core::selection_cost(&fact, true, false), 1.0);
    }

    #[test]
    fn apikey_no_coverage_keeps_ordinary_catalog_rates() {
        let plane = fallback_catalog().for_route(
            "openai/example",
            &crate::broca_credentials::ServingMethod {
                auth_method: Some("apikey".into()),
                reason: "single listed method".into(),
            },
            false,
        );
        assert_eq!(plane.rate("rate.input"), Some(2.0));
        assert_eq!(plane.rate("rate.output"), Some(4.0));
        assert_eq!(plane.rate("rate.reasoning"), Some(4.0));
    }

    fn antigravity_catalog(marked: bool) -> PlaneCatalog {
        let mut entry = json!({"existence":"present"});
        for (key, units) in [
            ("rate.input", 2_000_000_000_i64),
            ("rate.output", 10_000_000_000),
            ("rate.reasoning", 10_000_000_000),
            ("rate.cache_read", 100_000_000),
            ("rate.cache_write", 2_000_000_000),
        ] {
            entry[key] = json!({"state":"priced", "currency":"USD", "units":units, "exponent":9});
            if marked {
                entry[key]["inherited_from"] = json!({
                    "basis":"alias", "provider_id":"google", "model_id":"gemini-4-carbon"
                });
            }
        }
        PlaneCatalog::default().with_test_catalog(
            &serde_json::to_vec(&json!({"catalog_version":1,"resolved_at_ms":1000,
                "models":{"google/antigravity-gemini-4-carbon":entry}}))
            .unwrap(),
        )
    }

    fn antigravity_task_objective(
        catalog: &PlaneCatalog,
        configured: bool,
    ) -> striatum_core::model_router::ObjectiveBreakdown {
        use striatum_core::model_router::{
            ModelRouteStateSnapshot, RouteRuntimeState, SelectModelRouteInput,
        };
        use striatum_core::subscription::{QuotaAccount, SubscriptionPlan, SubscriptionPool};
        use striatum_core::task_profile::{ProfileLevel, TaskProfile, TokenVector};
        let key = "google/antigravity-gemini-4-carbon";
        let mut fact = crate::tests::fact("google", "antigravity-gemini-4-carbon");
        fact.subscription_multiplier = Some(15.0);
        fact.billing_plane = Some(catalog.for_route(
            key,
            &crate::broca_credentials::ServingMethod {
                auth_method: Some("antigravity".into()),
                reason: "selected account class".into(),
            },
            configured,
        ));
        let runtime = RouteRuntimeState {
            subscription_pool: Some(Arc::new(SubscriptionPool {
                provider: "google".into(),
                now_ms: 1000,
                plans: vec![SubscriptionPlan {
                    tier: None,
                    plan: None,
                    measured: None,
                    fee_usd: 200.0,
                    accounts: vec![QuotaAccount {
                        weekly_used: 50.0,
                        weekly_remaining_fraction: 0.5,
                        hours_to_reset: 84.0,
                        short_walled: false,
                        short_windows: vec![],
                        banked_resets: 0,
                        banked_expires_at_ms: None,
                        banked_credit_expiries_ms: Vec::new(),
                        prepaid_credits: false,
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let profile = TaskProfile {
            tokens: TokenVector {
                fresh_input: 1000.0,
                output_excluding_reasoning: 2000.0,
                reasoning: 300.0,
                cached_read: 4000.0,
                cache_write: 500.0,
            },
            level: ProfileLevel::OwnCell,
            task_count: 20,
            own_task_count: 20,
            own_weight: 1.0,
            keeps_tokenizer: true,
        };
        striatum_core::model_router::select_model_route_with_profiles(
            SelectModelRouteInput {
                target_agent: "athena".into(),
                target_is_head: false,
                requirements: striatum_core::ModelRequirements { iq: 20.0, eq: 0.0 },
                available_models: [key.into()].into(),
                facts: vec![fact],
                runtime_snapshot: Some(ModelRouteStateSnapshot {
                    by_capacity: [(key.into(), runtime)].into(),
                    ..Default::default()
                }),
                exclude_route_keys: vec![],
                operator_exclude_keys: vec![],
                required_capabilities: None,
                is_subscription_route: Some(Box::new(|_| true)),
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            },
            &Default::default(),
            None,
            &[],
            &Default::default(),
            &[(key.into(), profile)].into(),
        )
        .unwrap()
        .score
        .objective
    }

    #[test]
    fn antigravity_missing_or_refused_plane_prices_alias_with_multiplier_and_pressure() {
        for refusal in [None, Some("no_coverage"), Some("unavailable")] {
            let mut catalog = antigravity_catalog(true);
            if let Some(code) = refusal {
                catalog.pages.insert(
                    ("google".into(), "antigravity".into()),
                    PlaneReply::Refused {
                        code: code.into(),
                        message: "no subscription token plane".into(),
                    },
                );
            }
            let objective = antigravity_task_objective(&catalog, false);
            let task = objective.task_pricing.unwrap();
            let list = 0.0264;
            assert_eq!(
                task.unpriced_dimensions,
                Vec::<String>::new(),
                "{refusal:?}"
            );
            assert!((task.list_cost_usd.unwrap() - list).abs() < 1e-12);
            assert_eq!(task.value_multiplier, 15.0);
            assert_eq!(task.pressure, 2.0);
            assert!((task.final_cost_usd.unwrap() - list / 15.0 * 2.0).abs() < 1e-12);
            assert!(!objective.price_unknown);
        }
    }

    #[test]
    fn missing_or_refused_metered_planes_do_not_gain_subscription_fallback() {
        for method in ["apikey", "gateway"] {
            for refusal in [None, Some("unavailable")] {
                let mut catalog = antigravity_catalog(true);
                if let Some(code) = refusal {
                    catalog.pages.insert(
                        ("google".into(), method.into()),
                        PlaneReply::Refused {
                            code: code.into(),
                            message: "unavailable".into(),
                        },
                    );
                }
                let plane = catalog.for_route(
                    "google/antigravity-gemini-4-carbon",
                    &crate::broca_credentials::ServingMethod {
                        auth_method: Some(method.into()),
                        reason: "selected account class".into(),
                    },
                    true,
                );
                assert!(plane.rates.is_empty(), "{method} {refusal:?}");
            }
        }
    }

    #[test]
    fn antigravity_unmarked_rates_need_the_existing_configured_multiplier_override() {
        let catalog = antigravity_catalog(false);
        let objective = antigravity_task_objective(&catalog, false);
        let task = objective.task_pricing.unwrap();
        assert_eq!(task.unpriced_dimensions.len(), 5);
        assert_eq!(task.final_cost_usd, None);
        assert_eq!(objective.cost, 1.0);
        assert!(objective.price_unknown);
        let mut partial = antigravity_catalog(true);
        partial
            .base
            .get_mut("google/antigravity-gemini-4-carbon")
            .unwrap()
            .rates
            .insert("rate.cache_read".into(), BillingRate::Unpriced);
        let partial_objective = antigravity_task_objective(&partial, false);
        assert!(partial_objective.price_unknown);
        assert_eq!(partial_objective.cost, 1.0);
        assert_eq!(
            partial_objective.task_pricing.unwrap().unpriced_dimensions,
            ["cached_read"]
        );
        let configured = antigravity_task_objective(&catalog, true);
        assert!(!configured.price_unknown);
        assert_eq!(configured.task_pricing.unwrap().value_multiplier, 15.0);
    }

    /// OpenCode's free models on the production path: Fusiform states input and
    /// output as `stated_zero` and serves no cache-write rate, and price_facts
    /// attaches an apikey plane built from those catalog rates before selection.
    /// The free route prices at zero; its priced sibling bills cache writes at
    /// its stated input rate even though the plane has no separate write rate.
    #[test]
    fn free_route_prices_at_zero_through_the_plane_price_facts_attaches() {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        use striatum_core::BasketDimension::CacheWrite;
        let zero = json!({"state": "stated_zero"});
        let half_dollar =
            json!({"state": "priced", "currency": "USD", "units": 500_000_000_i64, "exponent": 9});
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "opencode-apikey".into(),
                credentials: vec![ListedCredential {
                    id: "opencode-key".into(),
                    auth_method: Some("apikey".into()),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["opencode".into()],
                    credential_type: "apikey".into(),
                    state: "active".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            1,
        );
        credentials.billing_catalog = PlaneCatalog {
            version: Some(1),
            base: model_rates(&json!({
                "opencode/free": {"rate.input": zero, "rate.output": zero, "rate.cache_read": zero},
                "opencode/paid": {"rate.input": zero, "rate.output": half_dollar, "rate.cache_read": zero},
            }))
            .unwrap(),
            pages: [(
                ("opencode".into(), "apikey".into()),
                PlaneReply::Refused {
                    code: "no_coverage".into(),
                    message: "undeclared".into(),
                },
            )]
            .into_iter()
            .collect(),
        };
        // The fact's own rate fields mirror the live store row (input, output and
        // cache-read set, cache-write and reasoning absent). Once price_facts
        // attaches a billing plane, basket_rate reads the plane's rates instead.
        let mut facts = vec![
            crate::tests::fact("opencode", "free"),
            crate::tests::fact("opencode", "paid"),
        ];
        for fact in &mut facts {
            fact.input_rate = Some(0.0);
            fact.cache_read_rate = Some(0.0);
            fact.cache_write_rate = None;
            fact.reasoning_rate = None;
            fact.class_vector = Some(vector());
        }
        facts[0].cost_per_m_tok_out = Some(0.0);
        facts[1].cost_per_m_tok_out = Some(0.5);
        credentials.price_facts(&mut facts, &striatum_core::RoutePricing::default());

        let free = &facts[0];
        let plane = free.billing_plane.as_ref().unwrap();
        assert_eq!(plane.auth_method, "apikey");
        assert_eq!(plane.kind, "no_coverage fallback");
        assert!(!plane.rates.contains_key("rate.cache_write"));
        assert_eq!(free.basket_rate(CacheWrite), Some(0.0));
        assert_eq!(free.basket_price(&vector()).cost, Some(0.0));
        assert_eq!(striatum_core::selection_cost(free, false, true), 0.0);
        assert_eq!(striatum_core::selection_cost(free, false, false), 0.0);

        let paid = &facts[1];
        assert_eq!(paid.basket_rate(CacheWrite), Some(0.0));
        let paid_price = paid.basket_price(&vector());
        assert!(paid_price.cost.is_some());
        assert!(paid_price.unpriced.is_empty());
        assert_eq!(
            striatum_core::selection_cost(paid, false, true),
            paid_price.cost.unwrap()
        );

        // A cache-write rate the plane explicitly marks unpriced is a statement,
        // not an absence: it stays unknown even on a free route.
        let mut refused = free.clone();
        refused
            .billing_plane
            .as_mut()
            .unwrap()
            .rates
            .insert("rate.cache_write".into(), BillingRate::Unpriced);
        assert_eq!(refused.basket_rate(CacheWrite), None);
        assert_eq!(refused.basket_price(&vector()).cost, None);
    }

    #[test]
    fn chatgpt_roster_with_an_apikey_also_listed_prices_on_chatgpt() {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let listing = ScopedCredentialListing {
            view: "mixed-openai".into(),
            credentials: vec![
                ListedCredential {
                    id: "arbitrary-login-id".into(),
                    auth_method: Some("chatgpt".into()),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["openai".into()],
                    refresh_adapter: Some("openai".into()),
                    credential_type: "oauth".into(),
                    state: "active".into(),
                    account_id: Some("account-a".into()),
                    ..Default::default()
                },
                ListedCredential {
                    id: "arbitrary-api-id".into(),
                    auth_method: Some("apikey".into()),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["openai".into()],
                    credential_type: "apikey".into(),
                    state: "active".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut state = crate::account_quota::AccountQuotaState::default();
        crate::account_quota::apply_roster_listing(&mut state, &listing, 1);
        crate::account_quota::apply_usage_reply(
            &mut state,
            &serde_json::to_vec(&json!({"result":[{
                "provider":"codex", "apiProvider":"openai", "account":"account-a", "source":"vault",
                "fetchedAt":"2026-10-07T00:00:00Z", "usage":{}
            }]}))
            .unwrap(),
            1,
        )
        .unwrap();
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(&listing, 1);
        let mut facts = vec![crate::tests::fact("openai", "gpt-5.6-luna")];
        credentials.refresh_serving_methods(&state, &facts, 1, &[]);
        credentials.billing_catalog.pages.insert(
            ("openai".into(), "chatgpt".into()),
            decode_plane_get(&get_reply("chatgpt luna — named, unfiltered")).unwrap(),
        );
        credentials.price_facts(&mut facts, &striatum_core::RoutePricing::default());
        let plane = facts[0].billing_plane.as_ref().unwrap();
        assert_eq!(plane.auth_method, "chatgpt");
        assert_eq!(plane.selection_reason, "roster");
        assert_eq!(plane.rate("rate.cache_write"), Some(0.2));
    }

    #[tokio::test]
    async fn refusals_decode_without_failing_poll_and_planes_refresh_only_on_catalog_change() {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let rig = Rig::new("plane-poll");
        let store = Arc::new(rig.store());
        let facts = vec![
            crate::tests::fact("google", "example"),
            crate::tests::fact("openai", "example"),
        ];
        let listing = ScopedCredentialListing {
            view: "planes".into(),
            credentials: vec![
                ListedCredential {
                    provider_ids: vec!["google".into()],
                    refresh_adapter: Some("antigravity".into()),
                    auth_method: Some("antigravity".into()),
                    credential_type: "oauth".into(),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    ..Default::default()
                },
                ListedCredential {
                    provider_ids: vec!["openai".into()],
                    credential_type: "bogus".into(),
                    auth_method: Some("bogus".into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let credentials = Arc::new(Mutex::new(
            crate::broca_credentials::BrocaCredentials::default(),
        ));
        {
            let mut guard = credentials.lock().unwrap();
            guard.apply_listing(&listing, 1);
            guard.refresh_serving_methods(
                &crate::account_quota::AccountQuotaState::default(),
                &facts,
                1,
                &[],
            );
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let peer = tokio::spawn(async move {
            let mut planes = 0;
            let mut gets = 0;
            loop {
                let frame = read_frame(&mut server).await.unwrap().unwrap();
                if frame.header.ty == FrameType::Goodbye {
                    break;
                }
                let request: Value = serde_json::from_slice(&frame.body).unwrap();
                let plane = request["arguments"].get("auth_method").is_some();
                let payload = if plane {
                    planes += 1;
                    assert!(request["arguments"].get("at_ms").is_none());
                    let code = if request["arguments"]["auth_method"] == "bogus" {
                        "bad_request"
                    } else {
                        "no_coverage"
                    };
                    json!({"code":code,"message":"refused"})
                } else if request["name"] == CATALOG_STATUS {
                    json!({"catalog_version":1})
                } else {
                    gets += 1;
                    let mut payload =
                        fixture()["catalog.get — one model, current"]["response"].clone();
                    payload["catalog_version"] = json!(1);
                    payload
                };
                let response = Frame::build(
                    if plane {
                        FrameType::Error
                    } else {
                        FrameType::Response
                    },
                    Flags::new(false, Priority::Interactive, false),
                    frame.header.channel,
                    frame.header.epoch,
                    frame.header.corr,
                    serde_json::to_vec(&payload).unwrap(),
                )
                .unwrap();
                write_frame(&mut server, &response).await.unwrap();
            }
            (gets, planes)
        });
        let mut client = FusiformCatalogConsumer {
            stream,
            credentials: Some(Arc::clone(&credentials)),
            route_channel: 1,
            route_epoch: 1,
            next_corr: 1,
        };
        let mut snapshot =
            decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        client.refresh_planes(Some(&snapshot)).await.unwrap();
        client.refresh_planes(None).await.unwrap(); // no network request
        snapshot.cursor.catalog_version = 1;
        client.refresh_planes(Some(&snapshot)).await.unwrap(); // both refusals retried
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .unwrap();
        let health = Arc::new(Mutex::new(CatalogReachHealth::new(100)));
        assert_eq!(
            poll_cycle(&mut client, &store, &health, None, None)
                .await
                .unwrap()
                .get_calls,
            0
        );
        assert_eq!(peer.await.unwrap(), (0, 4));
        assert_eq!(
            store
                .fusiform_catalog_cursor()
                .unwrap()
                .unwrap()
                .catalog_version,
            1
        );
        let mut priced = facts;
        credentials
            .lock()
            .unwrap()
            .price_facts(&mut priced, &striatum_core::RoutePricing::default());
        assert_eq!(
            priced[0].billing_plane.as_ref().unwrap().kind,
            "no_coverage fallback"
        );
        assert_eq!(
            priced[1].billing_plane.as_ref().unwrap().kind,
            "refused: bad_request"
        );
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
        assert_eq!(body["arguments"]["paged"], json!(true));
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
        use striatum_core::RouteStateStore as _;
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
            "afeac184c2481f67afe3c7dee03b5a96c069cfa86ffd7fb037f9674c5de4aa46"
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
        assert_eq!(fact.context_window, Some(1_000_000.0));
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
    #[ignore = "measurement requires FUSIFORM_SERVED_PAYLOADS pointing at the producer golden payloads"]
    fn measure_golden_chat_exclusions() {
        let path = std::env::var("FUSIFORM_SERVED_PAYLOADS").expect("producer golden payload path");
        let bytes = std::fs::read(path).unwrap();
        let payloads: Value = serde_json::from_slice(&bytes).unwrap();
        let rig = Rig::new("golden-chat-exclusions");
        let mut model = rig.store().get_facts().unwrap().remove(0);
        let mut seen = BTreeSet::new();
        let mut excluded = BTreeSet::new();
        let mut catalogs = 0;
        for (name, payload) in payloads.as_object().unwrap() {
            let response = &payload["response"];
            if response.get("models").is_none() {
                continue;
            }
            let snapshot = decode_catalog_get(&serde_json::to_vec(response).unwrap())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            catalogs += 1;
            for fact in snapshot.parts().0 {
                let route = format!("{}/{}", fact.provider_id, fact.model_id);
                seen.insert(route.clone());
                model.model_type = fact.model_type.clone();
                model.model_kind = fact.model_kind.clone();
                if let Some(reason) =
                    striatum_core::model_facts::llm_classification_rejection(&model, false)
                {
                    excluded.insert((route, reason));
                }
            }
        }
        assert!(catalogs > 0);
        println!("producer_sha256={:x}", Sha256::digest(&bytes));
        println!(
            "catalogs={catalogs} distinct_rows={} newly_excluded={}",
            seen.len(),
            excluded.len()
        );
        for (route, reason) in excluded {
            println!("{route}\t{reason}");
        }
    }

    #[test]
    fn fixture_catalog_rates_reach_the_model_fact_used_by_selection_cost() {
        use striatum_core::RouteStateStore as _;

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
            striatum_core::ScoreProvenance::Unscored
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
        use striatum_core::route_state_store::RouteStateStore as _;
        let mut placeholder = row.fact.clone();
        placeholder.provider_id = "synthetic".to_string();
        placeholder.model_id = "placeholder".to_string();
        store
            .upsert_fact(&placeholder, striatum_core::FactSource::Seed)
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
        let store = Arc::new(rig.store());
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
        let store = Arc::new(rig.store());
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

    #[tokio::test]
    async fn catalog_sync_lists_unserved_overrides_and_reports_each_route_once() {
        use striatum_core::{FactSource, RouteStateStore as _};

        let rig = Rig::new("health-unserved-override");
        let store = Arc::new(rig.store());
        store
            .upsert_fact(
                &crate::tests::fact("test", "unserved-override"),
                FactSource::Override,
            )
            .unwrap();
        let initial = decode_catalog_get(&get_reply("catalog.get — one model, current")).unwrap();
        let health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
        let mut bootstrap = FakeWire {
            status: CatalogStatusReply { catalog_version: 0 },
            snapshot: initial.clone(),
            get_calls: 0,
            closed: false,
        };
        bootstrap_cycle(&mut bootstrap, &store, &health, None, None)
            .await
            .unwrap();

        let snapshot = health.lock().unwrap().snapshot(now_ms());
        assert!(snapshot["unservedRoutes"]
            .as_array()
            .unwrap()
            .contains(&json!("test/unserved-override")));
        let reported_before = health.lock().unwrap().reported_unserved_routes.len();
        assert!(reported_before > 0);
        assert!(health
            .lock()
            .unwrap()
            .reported_unserved_routes
            .contains("test/unserved-override"));

        let mut advanced = initial;
        let next_version = advanced.cursor.catalog_version + 1;
        advanced.cursor.catalog_version = next_version;
        let mut poll = FakeWire {
            status: CatalogStatusReply {
                catalog_version: next_version,
            },
            snapshot: advanced,
            get_calls: 0,
            closed: false,
        };
        poll_cycle(&mut poll, &store, &health, None, None)
            .await
            .unwrap();
        assert_eq!(poll.get_calls, 1);
        assert_eq!(
            health.lock().unwrap().reported_unserved_routes.len(),
            reported_before,
            "a continuing stale route is logged only once"
        );
        assert!(health.lock().unwrap().snapshot(now_ms())["unservedRoutes"]
            .as_array()
            .unwrap()
            .contains(&json!("test/unserved-override")));
    }

    fn isolation_catalog(version: i64, models: Value) -> CatalogSnapshot {
        decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": version,
                "resolved_at_ms": version * 1_000,
                "models": models,
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn readable_entry(context: u64) -> Value {
        json!({
            "existence": "present",
            "limit.context": context,
            "rate.output": {"state": "priced", "currency": "USD", "exponent": 0, "units": 2},
        })
    }

    fn catalog_page(model: &str, quality_scores: Option<Value>) -> CatalogPage {
        let mut entry = readable_entry(32_000);
        entry["quality.elo"] = json!(1400.0);
        entry["quality.eq"] = json!(120.0);
        serde_json::from_value(json!({
            "source": "fusiform",
            "catalog_version": 42,
            "resolved_at_ms": 42000,
            "models": {format!("test/{model}"): entry},
            "quality_scores": quality_scores,
        }))
        .unwrap()
    }

    #[test]
    fn catalog_pages_merge_quality_disclosures_and_apply() {
        let scores = |time| Some(json!({"graded_by": "operator", "graded_at_ms": time}));
        let mut whole = catalog_page("first", scores(100));
        whole.append(catalog_page("second", None)).unwrap();
        whole.append(catalog_page("third", scores(200))).unwrap();
        let snapshot = decode_catalog_get(&serde_json::to_vec(&whole).unwrap()).unwrap();
        assert_eq!(snapshot.quality_scores.as_ref().unwrap().graded_at_ms, 200);
        assert_eq!(snapshot.facts.len(), 3);
        let rig = Rig::new("page-quality");
        let store = rig.store();
        apply_mapped_catalog(&store, &snapshot, None, Some("test")).unwrap();
        assert_eq!(
            store
                .get_catalog_facts()
                .unwrap()
                .iter()
                .filter(|row| row.fact.provider_id == "test")
                .count(),
            3
        );
        let quality = store.operator_quality().unwrap();
        assert_eq!(quality.len(), 3);
        assert!(quality.contains_key("test/third"));

        let mut no_disclosure = catalog_page("first", None);
        no_disclosure
            .append(catalog_page("second", scores(200)))
            .unwrap();
        no_disclosure
            .append(catalog_page("third", scores(100)))
            .unwrap();
        assert_eq!(no_disclosure.quality_scores.unwrap().graded_at_ms, 200);
    }

    #[test]
    fn catalog_pages_reject_identity_and_grader_changes() {
        for field in ["source", "catalog_version", "resolved_at_ms", "graded_by"] {
            let scores = Some(json!({"graded_by": "operator", "graded_at_ms": 100}));
            let mut whole = catalog_page("first", scores.clone());
            let mut page = catalog_page("second", scores);
            match field {
                "source" => page.source = "other".into(),
                "catalog_version" => page.catalog_version += 1,
                "resolved_at_ms" => page.resolved_at_ms += 1,
                "graded_by" => page.quality_scores.as_mut().unwrap().graded_by = "other".into(),
                _ => unreachable!(),
            }
            assert_eq!(
                whole.append(page).unwrap_err().to_string(),
                "Fusiform catalog page identity changed during read",
                "{field}"
            );
        }
    }

    #[test]
    fn each_unreadable_catalog_entry_is_skipped_without_rejecting_the_others() {
        let snapshot = isolation_catalog(
            2,
            json!({
                "test/fine": readable_entry(32_000),
                "test/unknown-existence": {"existence": "deprecated"},
                "test/bad-rate": {
                    "existence": "present",
                    "rate.input": {"state": "priced", "currency": "USD"},
                },
                "test/bad-type": {"existence": "present", "limit.context": "lots"},
                "no-provider-split": readable_entry(1),
                "/empty-provider": readable_entry(1),
                "empty-model/": readable_entry(1),
            }),
        );
        let served: Vec<(&str, &str, Option<f64>)> = snapshot
            .facts
            .iter()
            .map(|fact| {
                (
                    fact.provider_id.as_str(),
                    fact.model_id.as_str(),
                    fact.context_window,
                )
            })
            .collect();
        assert_eq!(served, vec![("test", "fine", Some(32_000.0))]);
        assert_eq!(snapshot.rates.keys().collect::<Vec<_>>(), vec!["test/fine"]);
        // Only well-formed keys are retained; a key naming no provider/model
        // pair matches no stored row and protects nothing.
        assert_eq!(
            snapshot.retained,
            BTreeSet::from([
                ("test".to_string(), "bad-rate".to_string()),
                ("test".to_string(), "bad-type".to_string()),
                ("test".to_string(), "unknown-existence".to_string()),
            ])
        );
    }

    #[tokio::test]
    async fn unreadable_entry_keeps_its_stored_row_while_an_omitted_route_is_withdrawn() {
        use striatum_core::{FactSource, RouteStateStore as _};

        let rig = Rig::new("entry-isolation");
        let store = Arc::new(rig.store());
        for model in ["kept", "gone", "fine"] {
            store
                .upsert_fact(&crate::tests::fact("test", model), FactSource::Seed)
                .unwrap();
        }
        let row = |model: &str| {
            store
                .get_catalog_facts()
                .unwrap()
                .into_iter()
                .find(|row| row.fact.provider_id == "test" && row.fact.model_id == model)
                .map(|row| serde_json::to_value(row).unwrap())
        };
        let health = Arc::new(Mutex::new(CatalogReachHealth::new(600_000)));
        let mut bootstrap = FakeWire {
            status: CatalogStatusReply { catalog_version: 1 },
            snapshot: isolation_catalog(
                1,
                json!({
                    "test/kept": readable_entry(100_000),
                    "test/gone": readable_entry(100_000),
                    "test/fine": readable_entry(100_000),
                }),
            ),
            get_calls: 0,
            closed: false,
        };
        bootstrap_cycle(&mut bootstrap, &store, &health, None, None)
            .await
            .unwrap();
        let kept_before = row("kept").unwrap();
        assert_eq!(kept_before["fusiformPresent"], json!(true));
        assert!(row("gone").is_some());

        // Version 2 serves `kept` with an existence value this consumer cannot
        // read and no longer serves `gone` at all.
        let mut poll = FakeWire {
            status: CatalogStatusReply { catalog_version: 2 },
            snapshot: isolation_catalog(
                2,
                json!({
                    "test/kept": {"existence": "deprecated"},
                    "test/fine": readable_entry(200_000),
                }),
            ),
            get_calls: 0,
            closed: false,
        };
        poll_cycle(&mut poll, &store, &health, None, None)
            .await
            .unwrap();
        assert_eq!(poll.get_calls, 1);

        assert_eq!(
            row("kept"),
            Some(kept_before),
            "an unreadable entry's stored row stays exactly as it was"
        );
        assert_eq!(row("fine").unwrap()["contextWindow"], json!(200_000.0));
        let unserved = health.lock().unwrap().snapshot(now_ms())["unservedRoutes"].clone();
        assert_eq!(
            unserved,
            json!(["test/gone"]),
            "the omitted route is still withdrawn and the unreadable one is not"
        );
        assert!(
            row("gone").is_none(),
            "the withdrawn, never-picked route is pruned"
        );
    }

    #[test]
    fn unreadable_entry_keeps_its_previous_base_rates() {
        let previous = isolation_catalog(
            1,
            json!({"test/kept": readable_entry(1), "test/gone": readable_entry(1)}),
        );
        let next = isolation_catalog(
            2,
            json!({"test/kept": {"existence": "deprecated"}, "test/fine": readable_entry(1)}),
        );
        let base = base_rates_after(previous.rates.clone(), &next);
        assert_eq!(
            base.keys().collect::<Vec<_>>(),
            vec!["test/fine", "test/kept"]
        );
        assert_eq!(base["test/kept"], previous.rates["test/kept"]);
    }
}

#[cfg(test)]
mod multiplier_fallback_tests {
    //! An oauth route Fusiform does not cover prices at the catalog's list rates
    //! when the operator configured a subscription multiplier for its provider,
    //! and stays unknown otherwise. Giraffe-, not xai-specific: any plan whose
    //! rates the catalog does not serve behaves the same way.

    use crate::broca_credentials::ServingMethod;
    use serde_json::json;
    use striatum_core::billing::BillingRate;
    use striatum_core::{selection_cost, RoutePricing};

    use super::{model_rates, BillingPlane, ModelRates, PlaneCatalog, PlaneReply};

    fn oauth_serving() -> ServingMethod {
        ServingMethod {
            auth_method: Some("oauth".into()),
            reason: "single listed method".into(),
        }
    }

    fn apikey_serving() -> ServingMethod {
        ServingMethod {
            auth_method: Some("apikey".into()),
            reason: "single listed method".into(),
        }
    }

    fn rates(provider: &str, model: &str) -> ModelRates {
        let served = json!({
            "rate.input": {"state": "priced", "currency": "USD", "units": 2_000_000_000_i64, "exponent": 9},
            "rate.output": {"state": "priced", "currency": "USD", "units": 6_000_000_000_i64, "exponent": 9},
            "rate.cache_read": {"state": "priced", "currency": "USD", "units": 500_000_000_i64, "exponent": 9},
        });
        let mut keyed: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        keyed.insert(format!("{provider}/{model}"), served);
        model_rates(&serde_json::Value::Object(keyed))
            .expect("served base rates")
            .remove(&format!("{provider}/{model}"))
            .expect("the model's rates")
    }

    fn unserviceable_catalog(provider: &str, model: &str) -> PlaneCatalog {
        PlaneCatalog {
            version: Some(0),
            base: std::collections::BTreeMap::from([(
                format!("{provider}/{model}"),
                rates(provider, model),
            )]),
            pages: ["oauth", "apikey"]
                .into_iter()
                .map(|method| {
                    (
                        (provider.to_string(), method.to_string()),
                        PlaneReply::Refused {
                            code: "no_coverage".into(),
                            message: "no plan rates served".into(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// The decision path's inputs: list rates from the catalog base, the
    /// plane's coverage verdict, and a token basket the workload actually uses.
    fn fact(
        provider: &str,
        model: &str,
        elo: f64,
        multiplier: Option<f64>,
    ) -> striatum_core::ModelFact {
        let mut fact = crate::tests::fact(provider, model);
        fact.scores.elo = elo;
        fact.scores.eq = 1_500.0;
        fact.cost_per_m_tok_out = Some(6.0);
        fact.input_rate = Some(2.0);
        fact.cache_read_rate = Some(0.5);
        fact.subscription_multiplier = multiplier;
        fact.class_vector = Some(striatum_core::ClassVector {
            workload_class: "ephemeral".into(),
            sample_count: 50,
            trailing_window_days: 7.0,
            statistic: "mean".into(),
            is_overlay: true,
            fresh_input: 10_000.0,
            output_excluding_reasoning: 2_000.0,
            reasoning: 0.0,
            cached_read: 5_000.0,
            cache_write: 0.0,
        });
        fact
    }

    fn sign_in(credentials: &mut crate::broca_credentials::BrocaCredentials) {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let listing = ScopedCredentialListing {
            view: "oauth-only".into(),
            credentials: vec![ListedCredential {
                id: "plan-login".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["giraffe".into()],
                refresh_adapter: Some("giraffe".into()),
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                state: "active".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        credentials.apply_listing(&listing, 1);
        credentials.billing_catalog = unserviceable_catalog("giraffe", "stripes-1");
    }

    fn priced_by_oauth_route(pricing: &RoutePricing) -> striatum_core::ModelFact {
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        sign_in(&mut credentials);
        let mut facts = vec![fact("giraffe", "stripes-1", 1_700.0, None)];
        credentials.price_facts(&mut facts, pricing);
        // A live selection reads the multiplier onto the fact in the store's
        // overlay (get_facts_with_observed_speed); this sets it the same way.
        facts[0].subscription_multiplier = pricing.subscription_multiplier("giraffe/stripes-1");
        facts.remove(0)
    }

    fn priced_credentials(pricing: &RoutePricing) -> Vec<striatum_core::ModelFact> {
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        sign_in(&mut credentials);
        let mut facts = vec![fact("giraffe", "stripes-1", 1_700.0, None)];
        credentials.price_facts(&mut facts, pricing);
        facts
    }

    #[test]
    fn oauth_route_with_a_configured_multiplier_keeps_list_rates_divided_by_it() {
        let multiplier = 15.0;
        let pricing = RoutePricing {
            subscription_multipliers: std::collections::HashMap::from([(
                "giraffe".to_string(),
                multiplier,
            )]),
            ..Default::default()
        };
        let priced = priced_by_oauth_route(&pricing);
        let plane = priced.billing_plane.as_ref().expect("plane attached");
        assert_eq!(plane.auth_method, "oauth");
        assert_eq!(plane.kind, "no_coverage fallback");
        assert_eq!(plane.rate("rate.input"), Some(2.0));
        assert_eq!(plane.rate("rate.output"), Some(6.0));
        assert_eq!(plane.rate("rate.cache_read"), Some(0.5));
        // The undisclosed rate is neither inherited nor configured here, so no
        // one states it.
        assert_eq!(plane.rates.get("rate.cache_write"), None);

        // The fact's multiplier is what divides the list price.
        let priced = priced_by_oauth_route(&pricing);
        assert_eq!(priced.subscription_multiplier, Some(multiplier));
        let list_dollars = (10_000.0 * 2.0 + 2_000.0 * 6.0 + 5_000.0 * 0.5) / 1_000_000.0;
        let expected = list_dollars / striatum_core::BASKET_REFERENCE_COST_PER_MESSAGE / multiplier;
        let basket = priced.basket_price(priced.class_vector.as_ref().unwrap());
        assert!(basket.unpriced.is_empty(), "{:?}", basket.unpriced);
        assert!((basket.cost.unwrap() - expected).abs() < 1e-12);
        assert!(
            (selection_cost(&priced, false, true) - expected).abs() < 1e-12,
            "basket prices at list divided by the multiplier"
        );
        // The output-only ladder divides by the same multiplier.
        assert_eq!(
            selection_cost(&priced, false, false),
            (6.0 / multiplier) / striatum_core::routing_config::COST_REFERENCE_PER_MTOK
        );
    }

    #[test]
    fn the_priced_oauth_route_beats_a_pricier_metered_route() {
        let pricing = RoutePricing {
            subscription_multipliers: std::collections::HashMap::from([(
                "giraffe".to_string(),
                15.0,
            )]),
            ..Default::default()
        };
        let plan = priced_by_oauth_route(&pricing);
        assert_eq!(plan.subscription_multiplier, Some(15.0));
        // gpt-style API route at $15/Mtok output, $4/Mtok input, $1/Mtok cache read.
        let mut api = fact("interchange", "thoroughfare-1", 1_700.0, None);
        api.cost_per_m_tok_out = Some(15.0);
        api.input_rate = Some(4.0);
        api.cache_read_rate = Some(1.0);
        api.billing_plane = Some(BillingPlane {
            provider_id: "interchange".into(),
            auth_method: "apikey".into(),
            kind: "upstream".into(),
            selection_reason: "single listed method".into(),
            rates: std::collections::BTreeMap::from([
                ("rate.input".into(), BillingRate::Priced { rate: 4.0 }),
                ("rate.output".into(), BillingRate::Priced { rate: 15.0 }),
                ("rate.cache_read".into(), BillingRate::Priced { rate: 1.0 }),
            ]),
        });

        let selection = striatum_core::model_router::select_model_route(
            striatum_core::model_router::SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
                requirements: striatum_core::ModelRequirements { iq: 20.0, eq: 0.0 },
                available_models: ["giraffe/stripes-1", "interchange/thoroughfare-1"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                facts: vec![plan.clone(), api.clone()],
                runtime_snapshot: None,
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            },
        )
        .expect("a route is selected");
        let explanation = selection.explanation.join("\n");
        assert_eq!(selection.route_key, "giraffe/stripes-1", "{explanation}");
        assert!(!explanation.contains("price_unknown"), "{explanation}");
        assert!(
            !selection.score.objective.price_unknown,
            "a configured multiplier makes the uncovered plan priceable"
        );
    }

    #[test]
    fn oauth_route_without_a_multiplier_stays_unknown_and_says_why() {
        let priced = priced_by_oauth_route(&RoutePricing::default());
        let plane = priced.billing_plane.as_ref().expect("plane attached");
        assert_eq!(plane.rate("rate.input"), None);
        assert_eq!(plane.rate("rate.output"), None);
        assert_eq!(plane.rate("rate.cache_read"), None);
        assert_eq!(plane.rates.get("rate.input"), Some(&BillingRate::Unpriced));
        assert_eq!(selection_cost(&priced, false, true), 1.0);
        assert_eq!(selection_cost(&priced, false, false), 1.0);

        let selection = striatum_core::model_router::select_model_route(
            striatum_core::model_router::SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
                requirements: striatum_core::ModelRequirements { iq: 20.0, eq: 0.0 },
                available_models: ["giraffe/stripes-1"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                facts: vec![priced],
                runtime_snapshot: None,
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            },
        )
        .expect("the sole adequate route stays selectable");
        assert!(selection.score.objective.price_unknown);
        let explanation = selection.explanation.join("\n");
        assert!(
            explanation.contains(
                "price_unknown: giraffe has no plan pricing and no subscription multiplier"
            ),
            "{explanation}"
        );
    }

    #[test]
    fn a_rate_the_served_plane_states_overrides_the_configured_fallback() {
        let served: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/served-payloads.json"))
                .expect("served payload fixture");
        let page = super::decode_plane_get(
            &serde_json::to_vec(&served["oauth anthropic — quota weights"]["response"])
                .expect("payload bytes"),
        )
        .expect("served oauth page");
        let super::PlaneReply::Served(page) = page else {
            panic!("fixture is a served plane");
        };
        let served_output = page.rates["anthropic/claude-sonnet-4-5"].rates["rate.output"].clone();
        let priced_catalog = PlaneCatalog {
            version: Some(0),
            base: std::collections::BTreeMap::from([(
                "anthropic/claude-sonnet-4-5".to_string(),
                rates("anthropic", "claude-sonnet-4-5"),
            )]),
            pages: std::collections::BTreeMap::from([(
                ("anthropic".to_string(), "oauth".to_string()),
                super::PlaneReply::Served(page),
            )]),
        };
        let plane = priced_catalog.for_route("anthropic/claude-sonnet-4-5", &oauth_serving(), true);
        assert_eq!(plane.kind, "quota_proxy");
        assert_eq!(plane.rates["rate.output"], served_output);
        assert_ne!(
            plane.rate("rate.output"),
            Some(6.0),
            "the catalog base rate must not win over a served page rate"
        );
    }

    #[test]
    fn a_provider_without_a_configured_multiplier_keeps_todays_unknown_rates() {
        // Control for the multiplier gate: the same uncovered oauth route, and
        // an apikey route beside it, on a provider the configuration does not
        // name.
        let plan = unserviceable_catalog("giraffe", "stripes-1").for_route(
            "giraffe/stripes-1",
            &oauth_serving(),
            false,
        );
        assert_eq!(plan.kind, "no_coverage fallback");
        assert_eq!(plan.rate("rate.input"), None);
        let api = unserviceable_catalog("giraffe", "stripes-1").for_route(
            "giraffe/stripes-1",
            &apikey_serving(),
            false,
        );
        assert_eq!(api.rate("rate.input"), Some(2.0));
    }

    #[test]
    fn price_facts_reads_the_selections_multipliers_for_both_panel_and_select_paths() {
        // The multiplier comes from the configuration the selection was built
        // with, not from a process-wide map.
        let with = RoutePricing {
            subscription_multipliers: std::collections::HashMap::from([(
                "giraffe".to_string(),
                15.0,
            )]),
            ..Default::default()
        };
        assert_eq!(
            priced_credentials(&with)[0]
                .billing_plane
                .as_ref()
                .unwrap()
                .rate("rate.output"),
            Some(6.0)
        );
        assert_eq!(
            priced_credentials(&RoutePricing::default())[0]
                .billing_plane
                .as_ref()
                .unwrap()
                .rate("rate.output"),
            None
        );
    }
}
