use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use subc_client_rs::{CallOptions, ConsumerOptions, SubcConsumer};

use crate::broca_credentials::BrocaCredentials;

use super::super::{accounts, Caller, Deps};
use super::DescribeCache;

pub(crate) fn tier_view_unknown(credentials: &BrocaCredentials, module: &str, now: i64) -> bool {
    match credentials.runner_describe_observed.get(module) {
        Some(at) => now.saturating_sub(*at) >= super::describe_cache::LAST_GOOD_MS,
        None => !credentials.runner_service_tiers.contains_key(module),
    }
}

pub(crate) async fn fetch_describe(
    consumer: &SubcConsumer,
    connection_file: &Path,
    module: &str,
) -> Option<Value> {
    consumer
        .call(
            subc_protocol::RouteTarget::ManagementSurface {
                module_id: module.to_owned(),
            },
            subc_protocol::BindIdentity::new(
                connection_file.parent().unwrap_or(Path::new(".")),
                "prefrontal-routing",
                "runner-service-tiers",
            ),
            br#"{"method":"role.describe","params":{}}"#.to_vec(),
            CallOptions {
                timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

/// A bound decision cannot step down its tier. After a restart, poll once on
/// admission rather than treating the empty cache as evidence that the bound
/// non-standard service tier is unsupported.
pub(crate) async fn refresh_admission_tiers(body: &[u8], caller: &Caller, deps: &Deps<'_>) {
    let Ok(request) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    if request["method"] != "route.account_for" || request["params"]["mode"] == "continue" {
        return;
    }
    let params = &request["params"];
    let bound = params["decision_id"]
        .as_str()
        .or_else(|| params["model_decision_id"].as_str())
        .and_then(|id| deps.store.decision_service_tier(id).ok().flatten());
    let tier = bound.as_ref().map(|(tier, _)| tier.as_str());
    if tier.is_none_or(|tier| tier == "standard") {
        return;
    }
    let Some(module) = accounts::admission_runner(caller, deps) else {
        return;
    };
    let path = deps.broca_credentials.lock().ok().and_then(|credentials| {
        tier_view_unknown(&credentials, &module, crate::now_ms() as i64)
            .then(|| credentials.connection_file_path.clone())
            .flatten()
    });
    let Some(path) = path else {
        return;
    };
    // Bound the connection, route open and describe together. No credentials
    // lock is held while the runner or daemon is unavailable.
    let describe = tokio::time::timeout(Duration::from_secs(2), async {
        let consumer = SubcConsumer::connect(&path, ConsumerOptions::default())
            .await
            .ok()?;
        fetch_describe(&consumer, &path, &module).await
    })
    .await
    .ok()
    .flatten();
    if let Ok(mut credentials) = deps.broca_credentials.lock() {
        DescribeCache::default().reply(
            &module,
            describe.as_ref(),
            &mut credentials,
            crate::now_ms() as i64,
        );
    }
}
