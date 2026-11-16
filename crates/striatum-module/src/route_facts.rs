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
    pub ack_wait_ms: Option<u64>,
    pub connection: Option<async_nats::Client>,
    pub revisions: Option<tokio::sync::watch::Receiver<u64>>,
}

impl Health {
    pub(crate) fn ready(&self, bound: bool) -> bool {
        bound
            && self.filters_ready
            && self.connection.as_ref().is_none_or(|connection| {
                connection.connection_state() == async_nats::connection::State::Connected
            })
            && self
                .revisions
                .as_ref()
                .is_none_or(|changes| matches!(changes.has_changed(), Ok(false)))
    }
}

impl Serialize for Health {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        json!({
            "filtersReady":self.filters_ready, "applied":self.applied,
            "ackWaitMs": self.ack_wait_ms,
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
    pub delivery_count: u64,
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
            delivery_count,
        } = delivery;
        let published_at_ms = *published_at_ms;
        let Ok(subject) = names.parse_event_subject(&message.subject) else {
            return Ok(false);
        };
        if subject.version != 1 {
            return Ok(false);
        }
        if body.len() > 4096 {
            tracing::warn!(
                module = subject.module,
                id = message.id,
                "oversized event terminated"
            );
            cursor
                .term(Some("body_over_cap"))
                .await
                .map_err(|e| e.to_string())?;
            return Ok(true);
        }
        if subject.module == "fusiform" && subject.event == "catalog_changed" {
            let version = serde_json::from_slice::<Value>(body)
                .ok()
                .and_then(|body| body["catalog_version"].as_i64())
                .filter(|version| *version >= 0);
            let Some(version) = version else {
                tracing::warn!(id = message.id, "malformed catalog event terminated");
                cursor
                    .term(Some("invalid_catalog_version"))
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(true);
            };
            let store = self.store.clone();
            let cursor_version =
                crate::stage_timing::spawn_job_blocking(move || store.fusiform_catalog_cursor())
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
            if cursor_version.is_none_or(|cursor| version > cursor.catalog_version) {
                self.wake_catalog()?;
            }
            cursor.ack().await.map_err(|e| e.to_string())?;
            return Ok(true);
        }
        if subject.event != "route_fact" {
            return Ok(false);
        }
        if let Ok(mut cache) = self.quota_cache.lock() {
            cache.consumer.route_facts.oldest_unapplied_at_ms = Some(published_at_ms);
        }
        let started = std::time::Instant::now();
        let result = async {
            let publisher_subject = names
                .event_subject(&subject.module, "route_fact", 1)
                .map_err(|e| e.to_string())?;
            if fact_subjects(names).contains(&publisher_subject) {
                striatum_store::job_timing::run("route_fact", self.dispatch(&subject.module, body))
                    .await
            } else {
                Err("account_not_authorized".into())
            }
        }
        .await;
        if started.elapsed() > Duration::from_millis(100) {
            self.log_slow(&subject.module, &message.id);
        }
        match result {
            Err(code) if transient(&code) => {
                if let Ok(mut cache) = self.quota_cache.lock() {
                    cache.consumer.route_facts.retries += 1;
                }
                tracing::warn!(
                    module = subject.module,
                    id = message.id,
                    code,
                    "route fact deferred for redelivery"
                );
                cursor
                    .nak(retry_delay(*delivery_count))
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(true);
            }
            result => {
                let refused = result.is_err();
                if let Ok(mut cache) = self.quota_cache.lock() {
                    if let Err(code) = result {
                        cache.consumer.route_facts.refused += 1;
                        tracing::warn!(
                            module = subject.module,
                            id = message.id,
                            code,
                            "route fact terminated"
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

    pub(crate) fn wake_catalog(&self) -> Result<(), String> {
        self.credentials
            .lock()
            .map_err(|_| "catalog credentials poisoned")?
            .catalog_wake
            .notify_one();
        Ok(())
    }

    fn log_slow(&self, module: &str, id: &str) {
        if let Ok(cache) = self.quota_cache.lock() {
            let health = &cache.consumer;
            tracing::warn!(module, id, attached = health.attached, pending = ?health.lag,
                oldest_age_ms = ?health.route_facts.oldest_unapplied_at_ms.map(|at| crate::now_ms().saturating_sub(at)),
                "slow route fact");
        }
    }
}

pub(crate) fn retry_delay(delivery_count: u64) -> Duration {
    Duration::from_secs((1u64 << delivery_count.saturating_sub(1).min(5)).min(30))
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
