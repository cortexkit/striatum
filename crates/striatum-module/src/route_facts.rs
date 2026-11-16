//! Durable runner facts share the direct-call handlers and their authorization.
use std::{path::PathBuf, time::Duration};

use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{Message, StreamCursor};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{broca_credentials::SharedBrocaCredentials, routing_v1, SharedQuotaCache, SharedStore};

pub(crate) fn fact_subjects(names: &AccountNames) -> Vec<String> {
    cortexkit_bus_naming::EVENT_CONSUMERS
        .iter()
        .find(|consumer| consumer.durable == "m_striatum")
        .into_iter()
        .flat_map(|consumer| consumer.filter_subjects)
        .map(|relative| format!("ck.{}.{relative}", names.account()))
        .filter(|subject| {
            names
                .parse_event_subject(subject)
                .is_ok_and(|parsed| parsed.event == "route_fact" && parsed.version == 1)
        })
        .collect()
}

#[derive(Clone, Default)]
pub(crate) struct Health {
    pub filters_ready: bool,
    pub applied: u64,
    pub refused: u64,
    pub oldest_unapplied_at_ms: Option<u64>,
    pub retries: u64,
}

impl Serialize for Health {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        json!({
            "filtersReady":self.filters_ready, "applied":self.applied,
            "refused":self.refused, "retries":self.retries,
            "oldestUnappliedAtMs":self.oldest_unapplied_at_ms,
            "oldestUnappliedAgeMs":self.oldest_unapplied_at_ms.map(|at| crate::now_ms().saturating_sub(at)),
        }).serialize(serializer)
    }
}

/// The payload seam between a durable transport and the shared handler.
pub(crate) struct Delivery {
    pub message: Message,
    pub body: Vec<u8>,
    pub published_at_ms: u64,
}

/// Only a confirmed durable with every publisher filter can enable the stream.
pub(crate) fn filters_ready(
    names: &AccountNames,
    durable: Option<&str>,
    filters: &[String],
) -> bool {
    let expected = fact_subjects(names);
    durable == Some("m_striatum")
        && !expected.is_empty()
        && expected.iter().all(|subject| filters.contains(subject))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fact {
    op: String,
    params: Value,
}

#[derive(Clone)]
pub(crate) struct Handler {
    pub quota_cache: SharedQuotaCache,
    pub store: SharedStore,
    pub credentials: SharedBrocaCredentials,
    pub config_path: PathBuf,
}

impl Handler {
    pub(crate) async fn dispatch(&self, module: &str, body: &[u8]) -> Result<Value, String> {
        let fact: Fact = serde_json::from_slice(body).map_err(|_| "invalid_request".to_owned())?;
        if !matches!(fact.op.as_str(), "route.account_observe" | "route.report") {
            return Err("unknown_method".into());
        }
        let handler = self.clone();
        let caller = routing_v1::Caller::for_event(module);
        crate::run_route_handler_off_async_workers(async move {
            let deps = routing_v1::Deps {
                quota_cache: &handler.quota_cache,
                store: &handler.store,
                routing_config_path: &handler.config_path,
                operator_config_path: &handler.config_path,
                broca_credentials: &handler.credentials,
            };
            let request = json!({"method":fact.op,"params":fact.params});
            let bytes = serde_json::to_vec(&request).map_err(crate::ModuleError::Json)?;
            let reply = if routing_v1::claims(&bytes) {
                routing_v1::answer(&fact.op, &fact.params, &caller, &deps)
                    .await
                    .map_err(|refusal| refusal.code.as_str().to_owned())
            } else {
                match crate::select::answer_legacy_account(
                    &fact.op,
                    &bytes,
                    &fact.params,
                    &caller.principal,
                    &handler.store,
                    &handler.credentials,
                    &handler.config_path,
                    &crate::config_overrides::live_config(&handler.config_path),
                    None,
                ) {
                    crate::account_selection::OpReply::Result(value) => Ok(value),
                    crate::account_selection::OpReply::Error { code, .. } => Err(code),
                }
            };
            Ok(reply)
        })
        .await
        .map_err(|_| "internal_error".to_owned())?
    }

    /// Return false for events outside the fact and catalog event contracts.
    pub(crate) async fn consume<C: StreamCursor>(
        &self,
        cursor: &mut C,
        names: &AccountNames,
        delivery: &Delivery,
    ) -> Result<bool, String> {
        let Delivery {
            message,
            body,
            published_at_ms,
        } = delivery;
        let published_at_ms = *published_at_ms;
        let Ok(subject) = names.parse_event_subject(&message.subject) else {
            return Ok(false);
        };
        if subject.version != 1 {
            return Ok(false);
        }
        if subject.module == "fusiform" && subject.event == "catalog_changed" {
            // The catalog loop already coalesces wakeups and refreshes off-thread.
            let wake = self
                .credentials
                .lock()
                .map_err(|_| "catalog credentials poisoned")?
                .catalog_wake
                .clone();
            wake.notify_one();
            cursor.ack().await.map_err(|e| e.to_string())?;
            return Ok(true);
        }
        if subject.event != "route_fact" {
            return Ok(false);
        }
        if let Ok(mut cache) = self.quota_cache.lock() {
            cache.consumer.route_facts.oldest_unapplied_at_ms = Some(published_at_ms);
        }
        let mut backoff = Duration::from_millis(100);
        loop {
            let started = std::time::Instant::now();
            let apply = async {
                let publisher_subject = names
                    .event_subject(&subject.module, "route_fact", 1)
                    .map_err(|e| e.to_string())?;
                if fact_subjects(names).contains(&publisher_subject) {
                    striatum_store::job_timing::run(
                        "route_fact",
                        self.dispatch(&subject.module, body),
                    )
                    .await
                } else {
                    Err("account_not_authorized".into())
                }
            };
            tokio::pin!(apply);
            let result = loop {
                tokio::select! {
                    result = &mut apply => break result,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        cursor.in_progress().await.map_err(|e| e.to_string())?;
                        self.log_slow(&subject.module, &message.id);
                    }
                }
            };
            if started.elapsed() > Duration::from_millis(100) {
                self.log_slow(&subject.module, &message.id);
            }
            match result {
                Err(code) if transient(&code) => {
                    if let Ok(mut cache) = self.quota_cache.lock() {
                        cache.consumer.route_facts.retries += 1;
                    }
                    eprintln!(
                        "[striatum] route fact retry module={} id={} code={} age_ms={}",
                        subject.module,
                        message.id,
                        code,
                        crate::now_ms().saturating_sub(published_at_ms)
                    );
                    cursor.in_progress().await.map_err(|e| e.to_string())?;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                result => {
                    let refused = result.is_err();
                    if let Ok(mut cache) = self.quota_cache.lock() {
                        if let Err(code) = result {
                            cache.consumer.route_facts.refused += 1;
                            eprintln!(
                                "[striatum] route fact refused module={} id={} code={code}",
                                subject.module, message.id
                            );
                        } else {
                            cache.consumer.route_facts.applied += 1;
                        }
                    }
                    // Handler completion includes the transaction and publication.
                    if refused {
                        cursor
                            .term(Some("route_fact_refused"))
                            .await
                            .map_err(|e| e.to_string())?;
                    } else {
                        cursor.ack().await.map_err(|e| e.to_string())?;
                    }
                    if let Ok(mut cache) = self.quota_cache.lock() {
                        cache.consumer.route_facts.oldest_unapplied_at_ms = None;
                    }
                    return Ok(true);
                }
            }
        }
    }

    fn log_slow(&self, module: &str, id: &str) {
        if let Ok(cache) = self.quota_cache.lock() {
            let health = &cache.consumer;
            eprintln!("[striatum] slow route fact module={module} id={id} attached={} pending={:?} oldest_age_ms={:?} applied={} refused={}",
                health.attached, health.lag,
                health.route_facts.oldest_unapplied_at_ms.map(|at| crate::now_ms().saturating_sub(at)),
                health.route_facts.applied, health.route_facts.refused);
        }
    }
}

fn transient(code: &str) -> bool {
    matches!(
        code,
        "internal_error"
            | "store_error"
            | "store_unavailable"
            | "account_selection_unavailable"
            | "routing_away"
            | "shutting_down"
    )
}
