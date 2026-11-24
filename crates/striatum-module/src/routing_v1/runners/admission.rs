use std::path::Path;
use std::time::Duration;

use cortexkit_claustrum_client::{CredentialResolver, ScopedCredentialListing};
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
        fetch_describe(
            &consumer,
            &path,
            &module,
            &super::super::operator_config(deps).v1_runners(),
            deps.broca_credentials,
        )
        .await
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
                let listing = refresh_cold_listing(&credentials, async {
                    crate::vault::VaultResolver::new(path.clone(), "striatum")
                        .list_scoped()
                        .await
                        .ok()
                });
                let plans = async {
                    let needed = credentials.lock().is_ok_and(|credentials| {
                        credentials.plan_prices["prices"].as_array().is_none()
                    });
                    if needed {
                        if let Some(prices) = crate::subscription_economics::fetch_plan_prices(
                            &path,
                            path.parent().unwrap_or(Path::new(".")),
                        )
                        .await
                        {
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

async fn refresh_cold_listing(
    credentials: &crate::SharedBrocaCredentials,
    list: impl std::future::Future<Output = Option<ScopedCredentialListing>>,
) {
    let needed = credentials
        .lock()
        .is_ok_and(|cache| cache.listing_version().is_none());
    if needed {
        if let Some(listing) = list.await {
            if let Ok(mut cache) = credentials.lock() {
                // The regular vault watch may have published while the cold read was in flight.
                if cache.listing_version().is_none() {
                    cache.apply_listing(&listing, crate::now_ms());
                }
            }
        }
    }
}

#[tokio::test]
async fn delayed_cold_admission_listing_cannot_overwrite_watched_view() {
    let credentials = crate::SharedBrocaCredentials::default();
    let (started, in_flight) = tokio::sync::oneshot::channel();
    let (release, delayed) = tokio::sync::oneshot::channel();
    let listing = |view: &str| ScopedCredentialListing {
        view: view.into(),
        ..Default::default()
    };
    tokio::join!(
        refresh_cold_listing(&credentials, async {
            started.send(()).unwrap();
            delayed.await.unwrap();
            Some(listing("active-key-view"))
        }),
        async {
            in_flight.await.unwrap();
            credentials
                .lock()
                .unwrap()
                .apply_listing(&listing("needs-reauth-view"), crate::now_ms());
            release.send(()).unwrap();
        }
    );
    assert_eq!(
        credentials
            .lock()
            .unwrap()
            .listing_version()
            .map(|(view, _)| view),
        Some("needs-reauth-view")
    );
}
