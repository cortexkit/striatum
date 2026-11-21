//! Models available to a classifier caller come from that daemon-attested
//! module's own validated `classifier/v1` description, never another module's.

use std::collections::{BTreeMap, BTreeSet};

pub(crate) use cortexkit_role_classifier::describe::ModelEntry as ServedModel;
use cortexkit_role_routing::select::{Modality, Requirements};
use serde_json::Value;
use striatum_core::{fact_to_route, ModelFact};

use crate::account_refusal_log::LogLimiter;
use crate::broca_credentials::BrocaCredentials;

// Mirrors classifier 0.6.1's SERVING_KINDS until routing pins that release.
const SERVING_KINDS: &[&str] = &["decision", "classifier"];
const LAST_GOOD_MS: i64 = 5 * 60_000;
pub(crate) const DESCRIBE_REQUEST: &[u8] = br#"{"method":"role.describe","params":{}}"#;

pub(crate) fn describe_options() -> subc_client_rs::CallOptions {
    subc_client_rs::CallOptions {
        role_versions: Some(BTreeMap::from([("classifier".into(), "v1".into())])),
        timeout: std::time::Duration::from_secs(1),
        ..Default::default()
    }
}

pub(crate) fn models_from_describe(value: &Value) -> Option<Vec<ServedModel>> {
    let value = value.get("result").unwrap_or(value);
    let describe =
        serde_json::from_value::<cortexkit_role_classifier::describe::RoleDescribe>(value.clone())
            .ok()?;
    cortexkit_role_classifier::describe::check_describe(&describe).ok()?;
    Some(describe.models)
}

pub(crate) async fn fetch_describe(
    consumer: &subc_client_rs::SubcConsumer,
    _connection_file: &std::path::Path,
    module: &str,
    settings: &[crate::config_policy::RunnerPolicy],
    credentials: &crate::SharedBrocaCredentials,
) -> Result<Option<Value>, DescribeError> {
    let server = consumer
        .server_describe()
        .await
        .map_err(|_| DescribeError::DaemonExchange)?;
    if !server
        .capabilities
        .iter()
        .any(|capability| capability == subc_protocol::scope::CAP_ROUTE_ROLE_VERSIONS_V1)
    {
        dropped(
            module,
            "*",
            "daemon_role_versions_unavailable",
            crate::now_ms() as i64,
        );
        return Ok(None);
    }
    let bytes = super::runners::enrollment::describe(
        consumer,
        module,
        subc_protocol::BindIdentity::new(crate::DEFAULT_MODULE_ID, "classifier-models"),
        DESCRIBE_REQUEST.to_vec(),
        describe_options(),
        settings,
        credentials,
    )
    .await
    .map_err(DescribeError::Runner)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| DescribeError::Malformed)?;
    if value.get("result").unwrap_or(&value)["role"] != "classifier/v1" {
        dropped(module, "*", "wrong_describe_role", crate::now_ms() as i64);
        return Ok(None);
    }
    Ok(Some(value))
}

pub(crate) enum DescribeError {
    DaemonExchange,
    Runner(super::runners::enrollment::DescribeError),
    Malformed,
}

impl DescribeError {
    pub(crate) fn needs_catalog_reconnect(&self) -> bool {
        matches!(
            self,
            Self::DaemonExchange
                | Self::Runner(super::runners::enrollment::DescribeError::TargetChangedNeverSent)
        )
    }
}

#[derive(Default)]
pub(crate) struct DescribeCache {
    modules: BTreeSet<String>,
    last_good: BTreeMap<String, i64>,
    logs: LogLimiter,
}

impl DescribeCache {
    pub(crate) fn revoke(&mut self, module: &str, credentials: &mut BrocaCredentials) {
        self.last_good.remove(module);
        credentials.classifier_models.remove(module);
    }
    pub(crate) fn catalog(
        &mut self,
        modules: &Value,
        credentials: &mut BrocaCredentials,
        now: i64,
    ) -> BTreeSet<String> {
        self.modules = modules
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                (entry["nonce_verified"] == true
                    && entry["capabilities"]["provides"]
                        .as_array()
                        .is_some_and(|roles| roles.iter().any(|role| role == "classifier/v1")))
                .then(|| entry["module_id"].as_str().map(str::to_owned))
                .flatten()
            })
            .collect();
        self.last_good
            .retain(|module, _| self.modules.contains(module));
        self.expire(credentials, now);
        self.modules.clone()
    }

    pub(crate) fn reply(
        &mut self,
        module: &str,
        value: Option<&Value>,
        credentials: &mut BrocaCredentials,
        now: i64,
    ) {
        if !self.modules.contains(module) {
            return;
        }
        if let Some(value) = value {
            if value.get("result").unwrap_or(value)["role"] != "classifier/v1" {
                dropped(module, "*", "wrong_describe_role", now);
                self.revoke(module, credentials);
                return;
            }
            for model in value.get("result").unwrap_or(value)["models"]
                .as_array()
                .into_iter()
                .flatten()
            {
                if model
                    .get("max_questions")
                    .and_then(Value::as_u64)
                    .is_none_or(|limit| limit == 0)
                {
                    if let Some(model) = model["model"].as_str() {
                        dropped(module, model, "missing_question_limit", now);
                    }
                }
            }
        }
        if let Some(models) = value.and_then(models_from_describe) {
            credentials
                .classifier_models
                .insert(module.to_owned(), models);
            self.last_good.insert(module.to_owned(), now);
        } else if let Some(suppressed) = self.logs.admit(module, "classifier_describe_failed", now)
        {
            crate::warn_operator(format!("couldn't read the classifier model list; keeping the last good list for up to five minutes, then offering none module={module:?} suppressed={suppressed}"));
        }
        self.expire(credentials, now);
    }

    pub(crate) fn expire(&self, credentials: &mut BrocaCredentials, now: i64) {
        credentials.classifier_models.retain(|module, _| {
            self.modules.contains(module)
                && self
                    .last_good
                    .get(module)
                    .is_some_and(|at| now.saturating_sub(*at) < LAST_GOOD_MS)
        });
    }
}

fn dropped(module: &str, model: &str, reason: &'static str, now: i64) {
    use std::sync::{LazyLock, Mutex};
    static LOGS: LazyLock<Mutex<LogLimiter>> = LazyLock::new(|| Mutex::new(LogLimiter::default()));
    let Ok(mut logs) = LOGS.lock() else {
        return;
    };
    if let Some(suppressed) = logs.admit(&format!("{module}/{model}"), reason, now) {
        crate::warn_operator(format!("left a classifier model out of selection (daemon_role_versions_unavailable: the daemon can't open a classifier route; wrong_describe_role: the reply wasn't classifier/v1; missing_catalog_row: Fusiform has no row for it; missing_question_limit: its listing states no max_questions) module={module:?} model={model:?} reason={reason} suppressed={suppressed}"));
    }
}

pub(crate) fn report_missing_rows(credentials: &BrocaCredentials, module: &str, now: i64) {
    for model in credentials
        .classifier_models
        .get(module)
        .into_iter()
        .flatten()
    {
        if credentials
            .catalog_snapshot
            .as_ref()
            .and_then(|catalog| catalog.model(&model.model))
            .is_none()
        {
            dropped(module, &model.model, "missing_catalog_row", now);
        }
    }
}

pub(crate) fn eligible(
    credentials: &BrocaCredentials,
    module: &str,
    fact: &ModelFact,
    requirements: &Requirements,
    now: i64,
) -> bool {
    let key = format!("{}/{}", fact.provider_id, fact.model_id);
    let Some(model) = credentials
        .classifier_models
        .get(module)
        .and_then(|models| models.iter().find(|model| model.model == key))
    else {
        return false;
    };
    let Some(catalog) = credentials.catalog_snapshot.as_ref() else {
        dropped(module, &key, "missing_catalog_row", now);
        return false;
    };
    let Some(kind) = catalog.model_kinds.get(&key) else {
        dropped(module, &key, "missing_catalog_row", now);
        return false;
    };
    if !SERVING_KINDS.contains(&kind.as_str()) {
        return false;
    }
    let Some(catalog_model) = catalog.model(&key) else {
        return false;
    };
    let limit = model.max_questions;
    if limit == 0 {
        dropped(module, &key, "missing_question_limit", now);
        return false;
    }
    if requirements
        .questions
        .is_some_and(|questions| questions > limit)
    {
        return false;
    }
    if requirements
        .modalities
        .as_ref()
        .is_some_and(|modalities| modalities.contains(&Modality::Image))
        && (!model.images.as_ref().is_some_and(|images| images.max > 0)
            || catalog_model.supports_vision == Some(false))
    {
        return false;
    }
    if requirements.context_window.is_some_and(|floor| {
        catalog_model
            .context_window
            .is_some_and(|limit| limit < floor as f64)
    }) {
        return false;
    }
    requirements.max_cost_usd.as_ref().is_none_or(|cap| {
        let cap = cap.as_f64();
        catalog
            .input_price(&key)
            .zip(cap)
            .is_some_and(|(price, cap)| {
                price * requirements.input_tokens.unwrap_or(0) as f64 / 1_000_000.0 <= cap
            })
    })
}

pub(crate) fn input_price(credentials: &BrocaCredentials, fact: &ModelFact) -> Option<f64> {
    credentials
        .catalog_snapshot
        .as_ref()?
        .input_price(&format!("{}/{}", fact.provider_id, fact.model_id))
}

pub(crate) fn ordered_routes(credentials: &BrocaCredentials, facts: &[ModelFact]) -> Vec<String> {
    let mut facts: Vec<_> = facts.iter().collect();
    facts.sort_by(|a, b| {
        let price = |fact| input_price(credentials, fact).unwrap_or(f64::INFINITY);
        price(a)
            .total_cmp(&price(b))
            .then_with(|| fact_to_route(a).route_key.cmp(&fact_to_route(b).route_key))
    });
    facts
        .into_iter()
        .map(|fact| fact_to_route(fact).route_key)
        .collect()
}
