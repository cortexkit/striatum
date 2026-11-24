//! Durable runner facts share the direct-call handlers and their authorization.
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{ContentDigest, Message, StreamCursor};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::quota_bus::{content_digest_matches, CONTENT_DIGEST_HEADER};
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
    pub duplicate: u64,
    pub refused: u64,
    pub publishers: BTreeMap<String, PublisherHealth>,
    pub retries: u64,
    pub ack_wait_ms: Option<u64>,
    pub last_delivery_at_ms: Option<u64>,
    pub last_ack_at_ms: Option<u64>,
    pub delivered_sequence: Option<u64>,
    pub bound_since: Option<std::time::Instant>,
    pub connection: Option<async_nats::Client>,
    pub revisions: Option<tokio::sync::watch::Receiver<u64>>,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublisherHealth {
    pub applied: u64,
    pub duplicate: u64,
    pub refused: u64,
    pub retried: u64,
    pub last_applied_at_ms: Option<u64>,
}

impl Health {
    pub(crate) fn reader_bound(&self, attached: bool) -> bool {
        attached
            && self.connection.as_ref().is_none_or(|connection| {
                connection.connection_state() == async_nats::connection::State::Connected
            })
            && self
                .revisions
                .as_ref()
                .is_none_or(|changes| matches!(changes.has_changed(), Ok(false)))
    }
    pub(crate) fn ready(&self, attached: bool) -> bool {
        self.filters_ready && self.reader_bound(attached)
    }
}

impl Serialize for Health {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        json!({
            "filtersReady":self.filters_ready, "applied":self.applied,
            "duplicate":self.duplicate, "publishers":self.publishers,
            "ackWaitMs": self.ack_wait_ms,
            "lastDeliveryAtMs": self.last_delivery_at_ms,
            "lastAckAtMs": self.last_ack_at_ms,
            "deliveredSequence": self.delivered_sequence,
            "refused":self.refused, "retries":self.retries,
        })
        .serialize(serializer)
    }
}

#[cfg(test)]
#[test]
fn health_does_not_advertise_unavailable_backlog_age() {
    let health = serde_json::to_value(Health::default()).unwrap();
    assert!(health.get("oldestUnappliedAgeMs").is_none(), "{health}");
    assert!(health.get("oldestUnappliedAtMs").is_none(), "{health}");
}

/// The payload seam between a durable transport and the shared handler.
pub(crate) struct Delivery {
    pub message: Message,
    pub body: Vec<u8>,
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
        && expected.iter().all(|subject| {
            filters
                .iter()
                .any(|filter| equivalent_subject(names, subject, filter))
        })
}

fn equivalent_subject(names: &AccountNames, left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    match (
        names.parse_event_subject(left),
        names.parse_event_subject(right),
    ) {
        (Ok(left_parts), Ok(right_parts)) => {
            let left_prefix = format!("ck.{}.event.{}.", names.account(), left_parts.module);
            let right_prefix = format!("ck.{}.event.{}.", names.account(), right_parts.module);
            // Only the module token is aliased; every remaining token must match
            // exactly. A filter ending in `.v1.striatum` cannot replace `.v1`.
            striatum_core::module_identity::equal(&left_parts.module, &right_parts.module)
                && left
                    .strip_prefix(&left_prefix)
                    .zip(right.strip_prefix(&right_prefix))
                    .is_some_and(|(left, right)| left == right)
        }
        _ => false,
    }
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
    pub(crate) async fn dispatch(&self, module: &str, body: &[u8]) -> Result<bool, String> {
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
            if fact.op == "route.report" {
                return Ok(routing_v1::report::report(&fact.params, &caller, &deps)
                    .map(|(_, duplicate)| duplicate)
                    .map_err(|refusal| refusal.code.as_str().to_owned()));
            }
            let reply = if routing_v1::claims_request(&request) {
                routing_v1::answer(&fact.op, &fact.params, &caller, &deps)
                    .await
                    .map_err(|refusal| refusal.code.as_str().to_owned())
            } else {
                let bytes = serde_json::to_vec(&request).map_err(crate::ModuleError::Json)?;
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
            Ok(reply.map(|answer| answer["observation"]["duplicate"] == true))
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
            delivery_count,
        } = delivery;
        let Ok(subject) = names.parse_event_subject(&message.subject) else {
            return Ok(false);
        };
        if subject.version != 1 {
            return Ok(false);
        }
        if subject.event == "route_fact" {
            // The adapter's Message.digest is computed locally, not supplied by the publisher.
            let reason = match message.headers.get(CONTENT_DIGEST_HEADER) {
                None => Some("content_digest_missing"),
                Some(header) => match header.parse::<ContentDigest>() {
                    Ok(digest) if content_digest_matches(body, digest) => None,
                    _ => Some("content_digest_mismatch"),
                },
            };
            if let Some(reason) = reason {
                tracing::warn!(
                    publisher = subject.module,
                    subject = message.subject,
                    message_id = message.id,
                    reason,
                    "route fact terminated"
                );
                cursor.term(Some(reason)).await.map_err(|e| e.to_string())?;
                self.refused_publisher(&subject.module, &subject.event, subject.version);
                return Ok(true);
            }
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
            self.refused_publisher(&subject.module, &subject.event, subject.version);
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
            let applied_version = self
                .credentials
                .lock()
                .map_err(|_| "catalog credentials poisoned")?
                .catalog_snapshot
                .as_ref()
                .map(|snapshot| snapshot.version());
            let applied_version = if applied_version.is_some() {
                applied_version
            } else {
                // Startup fetches the full catalog independently of event wakes.
                let store = self.store.clone();
                crate::stage_timing::spawn_job_blocking(move || store.fusiform_catalog_cursor())
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?
                    .map(|cursor| cursor.catalog_version)
            };
            if applied_version.is_none_or(|applied| version > applied) {
                self.wake_catalog()?;
            }
            cursor.ack().await.map_err(|e| e.to_string())?;
            return Ok(true);
        }
        if subject.event != "route_fact" {
            return Ok(false);
        }
        let started = std::time::Instant::now();
        let result = async {
            let publisher_subject = names
                .event_subject(&subject.module, "route_fact", 1)
                .map_err(|e| e.to_string())?;
            if fact_subjects(names)
                .iter()
                .any(|allowed| equivalent_subject(names, allowed, &publisher_subject))
            {
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
                if let Ok(mut cache) = self.quota_cache.lock() {
                    let health = &mut cache.consumer.route_facts;
                    health.retries += 1;
                    health
                        .publishers
                        .entry(
                            striatum_core::module_identity::canonical(&subject.module).into_owned(),
                        )
                        .or_default()
                        .retried += 1;
                }
                return Ok(true);
            }
            result => {
                let refused = result.is_err();
                if let Ok(mut cache) = self.quota_cache.lock() {
                    let health = &mut cache.consumer.route_facts;
                    let publisher = health
                        .publishers
                        .entry(
                            striatum_core::module_identity::canonical(&subject.module).into_owned(),
                        )
                        .or_default();
                    if let Err(code) = result {
                        health.refused += 1;
                        publisher.refused += 1;
                        tracing::warn!(
                            module = subject.module,
                            id = message.id,
                            code,
                            "route fact terminated"
                        );
                    } else if result == Ok(true) {
                        health.duplicate += 1;
                        publisher.duplicate += 1;
                    } else {
                        health.applied += 1;
                        publisher.applied += 1;
                        publisher.last_applied_at_ms = Some(crate::now_ms());
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

    pub(crate) fn wake_catalog_rebind(&self) -> Result<(), String> {
        self.credentials
            .lock()
            .map_err(|_| "catalog credentials poisoned")?
            .catalog_rebind_wake
            .notify_one();
        Ok(())
    }

    pub(crate) fn refused_subject(&self, names: &AccountNames, raw_subject: &str) {
        if let Ok(subject) = names.parse_event_subject(raw_subject) {
            self.refused_publisher(&subject.module, &subject.event, subject.version);
        }
    }

    pub(crate) fn refused_publisher(&self, module: &str, event: &str, version: u32) {
        if event == "route_fact" && version == 1 {
            if let Ok(mut cache) = self.quota_cache.lock() {
                let health = &mut cache.consumer.route_facts;
                health.refused += 1;
                health
                    .publishers
                    .entry(striatum_core::module_identity::canonical(module).into_owned())
                    .or_default()
                    .refused += 1;
            }
        }
    }

    fn log_slow(&self, module: &str, id: &str) {
        if let Ok(cache) = self.quota_cache.lock() {
            let health = &cache.consumer;
            tracing::warn!(module, id, attached = health.attached, pending = ?health.lag,
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
