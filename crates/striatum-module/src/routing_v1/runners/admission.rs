use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use subc_client_rs::{CallOptions, ConsumerOptions, SubcConsumer};

use crate::broca_credentials::BrocaCredentials;

use super::super::{accounts, Caller, Deps};
use super::DescribeCache;

type DescribeFlight = tokio::sync::Mutex<Option<tokio::time::Instant>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct AdmissionDescribes(Arc<Mutex<BTreeMap<String, Arc<DescribeFlight>>>>);

impl AdmissionDescribes {
    fn flight(&self, module: &str) -> Option<Arc<DescribeFlight>> {
        let mut flights = self.0.lock().ok()?;
        let now = tokio::time::Instant::now();
        flights.retain(|_, flight| {
            Arc::strong_count(flight) > 1
                || flight
                    .try_lock()
                    .is_ok_and(|retry| retry.is_some_and(|at| at > now))
        });
        Some(flights.entry(module.to_owned()).or_default().clone())
    }
}

const DESCRIBE_RETRY_DELAY: Duration = Duration::from_secs(5);

pub(crate) fn tier_view_unknown(credentials: &BrocaCredentials, module: &str, now: i64) -> bool {
    match credentials.runner_describe_observed.get(module) {
        Some(at) => now.saturating_sub(*at) >= super::describe_cache::LAST_GOOD_MS,
        None => !credentials.runner_service_tiers.contains_key(module),
    }
}

pub(crate) async fn fetch_describe(
    consumer: &SubcConsumer,
    _connection_file: &Path,
    module: &str,
    settings: &[crate::config_policy::RunnerPolicy],
    credentials: &crate::SharedBrocaCredentials,
) -> Option<Value> {
    fetch_describe_result(consumer, module, settings, credentials)
        .await
        .ok()
        .flatten()
}

pub(super) async fn fetch_describe_result(
    consumer: &SubcConsumer,
    module: &str,
    settings: &[crate::config_policy::RunnerPolicy],
    credentials: &crate::SharedBrocaCredentials,
) -> Result<Option<Value>, super::enrollment::DescribeError> {
    super::enrollment::describe(
        consumer,
        module,
        subc_protocol::BindIdentity::new(crate::DEFAULT_MODULE_ID, "runner-service-tiers"),
        br#"{"method":"role.describe","params":{}}"#.to_vec(),
        CallOptions {
            timeout: Duration::from_secs(1),
            ..Default::default()
        },
        settings,
        credentials,
    )
    .await
    .map(|bytes| serde_json::from_slice(&bytes).ok())
}

/// Poll unknown tier views before admission. A cold cache cannot establish
/// either that a bound tier is unavailable or that a fresh request must step down.
pub(crate) async fn refresh_admission_tiers(request: &Value, caller: &Caller, deps: &Deps<'_>) {
    if request["method"] != "route.account_for" || request["params"]["mode"] == "continue" {
        return;
    }
    let params = &request["params"];
    let bound = params["decision_id"]
        .as_str()
        .or_else(|| params["model_decision_id"].as_str())
        .and_then(|id| deps.store.decision_service_tier(id).ok().flatten());
    let tier = bound
        .as_ref()
        .map(|(tier, _)| tier.as_str())
        .or_else(|| params["service_tier"].as_str());
    if !tier.is_some_and(|tier| tier != "standard") {
        return;
    }
    let Some(module) = accounts::admission_runner(caller, deps) else {
        return;
    };
    refresh_account_inputs(deps.broca_credentials).await;
    let module = module.as_str();
    refresh_admission_describe(module, deps.broca_credentials, |path| async move {
        let consumer = SubcConsumer::connect(&path, ConsumerOptions::default())
            .await
            .ok()?;
        fetch_describe(
            &consumer,
            &path,
            module,
            &super::super::operator_config(deps).v1_runners(),
            deps.broca_credentials,
        )
        .await
    })
    .await;
}

async fn refresh_admission_describe<F>(
    module: &str,
    credentials: &crate::SharedBrocaCredentials,
    describe: impl FnOnce(std::path::PathBuf) -> F,
) where
    F: std::future::Future<Output = Option<Value>>,
{
    let Some(flights) = credentials
        .lock()
        .ok()
        .map(|credentials| credentials.admission_describes.clone())
    else {
        return;
    };
    let Some(flight) = flights.flight(module) else {
        return;
    };
    // Join the in-flight refresh rather than admitting against an unknown view
    // while another request may be about to publish a successful describe.
    let mut retry_at = flight.lock().await;
    if retry_at.is_some_and(|at| at > tokio::time::Instant::now()) {
        return;
    }
    let path = credentials.lock().ok().and_then(|credentials| {
        tier_view_unknown(&credentials, module, crate::now_ms() as i64)
            .then(|| credentials.connection_file_path.clone())
            .flatten()
    });
    let Some(path) = path else {
        return;
    };
    // Bound the connection, route open and describe together. No credentials
    // lock is held while the runner or daemon is unavailable.
    let describe = tokio::time::timeout(Duration::from_secs(2), describe(path))
        .await
        .ok()
        .flatten();
    if let Ok(mut credentials) = credentials.lock() {
        let accepted = DescribeCache::default().reply(
            module,
            describe.as_ref(),
            &mut credentials,
            crate::now_ms() as i64,
        );
        *retry_at = (!accepted).then(|| tokio::time::Instant::now() + DESCRIBE_RETRY_DELAY);
    }
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn a_failed_request_describe_is_shared_and_retried_after_backoff() {
    use std::cell::Cell;

    let credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
    credentials.lock().unwrap().connection_file_path = Some("unused-with-injected-describe".into());
    let calls = Cell::new(0);
    let fail = |_| async {
        calls.set(calls.get() + 1);
        tokio::time::sleep(Duration::from_secs(1)).await;
        None
    };
    tokio::join!(
        refresh_admission_describe("runner", &credentials, fail),
        refresh_admission_describe("runner", &credentials, fail),
    );
    refresh_admission_describe("runner", &credentials, fail).await;
    assert_eq!(
        calls.get(),
        1,
        "one dial for the two concurrent requests and immediate retry"
    );
    refresh_admission_describe("other-runner", &credentials, fail).await;
    assert_eq!(
        calls.get(),
        2,
        "one runner's failure must not delay another"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    refresh_admission_describe("runner", &credentials, fail).await;
    assert_eq!(calls.get(), 3, "retry after the negative result expires");
}

async fn refresh_account_inputs(credentials: &crate::SharedBrocaCredentials) {
    let Some((gate, path)) = credentials.lock().ok().and_then(|credentials| {
        crate::service_tiers::unobserved_account_input(&credentials)?;
        Some((
            credentials.admission_inputs.clone(),
            credentials.connection_file_path.clone()?,
        ))
    }) else {
        return;
    };
    if let Ok(guard) = gate.clone().try_lock_owned() {
        let credentials = credentials.clone();
        // The task owns the fetch so a waiting admission's deadline cannot
        // cancel the load shared with another admission.
        tokio::spawn(async move {
            let _guard = guard;
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                let listing = async {
                    use cortexkit_claustrum_client::CredentialResolver;
                    let needed = credentials
                        .lock()
                        .is_ok_and(|credentials| credentials.listing_version().is_none());
                    if needed {
                        let resolver = crate::vault::VaultResolver::new(path.clone(), "striatum");
                        if let Ok(listing) = resolver.list_scoped().await {
                            if let Ok(mut credentials) = credentials.lock() {
                                credentials.apply_listing(&listing, crate::now_ms());
                            }
                        }
                    }
                };
                let plans = async {
                    let needed = credentials.lock().is_ok_and(|credentials| {
                        credentials.plan_prices["prices"].as_array().is_none()
                    });
                    if needed {
                        let prices = crate::subscription_economics::fetch_plan_prices(
                            &path,
                            path.parent().unwrap_or(Path::new(".")),
                        )
                        .await;
                        if prices["prices"].is_array() {
                            if let Ok(mut credentials) = credentials.lock() {
                                credentials.publish_plan_prices(&prices);
                            }
                        }
                    }
                };
                tokio::join!(
                    listing,
                    plans,
                    crate::broca_credentials::wait_for_billing_catalog(
                        &credentials,
                        Duration::from_secs(2),
                    )
                );
            })
            .await;
        });
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), gate.lock()).await;
}
