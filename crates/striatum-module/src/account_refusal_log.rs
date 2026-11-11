//! Bounded, metadata-only diagnostics for refused account operations.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

const INTERVAL_MS: i64 = 60_000;
const MAX_KEYS: usize = 4096;

#[derive(Debug, Clone, Default)]
pub(crate) struct LogLimiter {
    entries: BTreeMap<(String, String), (i64, u64)>,
}

impl LogLimiter {
    pub(crate) fn admit(&mut self, actor: &str, reason: &str, now: i64) -> Option<u64> {
        let key = (actor.to_owned(), reason.to_owned());
        if let Some((last, suppressed)) = self.entries.get_mut(&key) {
            if now.saturating_sub(*last) < INTERVAL_MS {
                *suppressed = suppressed.saturating_add(1);
                return None;
            }
            let count = std::mem::take(suppressed);
            *last = now;
            return Some(count);
        }
        if self.entries.len() >= MAX_KEYS {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (at, _))| at)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, (now, 0));
        Some(0)
    }
}

#[derive(Default)]
struct Trace {
    reason: &'static str,
    exclusions: Option<String>,
}

thread_local! {
    // Account handlers are synchronous, including when dispatched by the
    // blocking-worker bridge. The trace never spans an async suspension.
    static TRACE: RefCell<Option<Trace>> = const { RefCell::new(None) };
}

struct TraceGuard(Option<Trace>);

impl Drop for TraceGuard {
    fn drop(&mut self) {
        TRACE.with(|trace| *trace.borrow_mut() = self.0.take());
    }
}

pub(crate) fn checking(reason: &'static str) {
    TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            trace.reason = reason;
        }
    });
}

pub(crate) fn because<T>(reason: &'static str, reply: T) -> T {
    checking(reason);
    reply
}

fn exclusions(detail: &Value) -> String {
    detail["exclusions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some(format!(
                "{}:{}",
                entry["account_id"].as_str()?,
                entry["reason"].as_str()?
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn allocator_refusal(detail: &Value) {
    let summary = exclusions(detail);
    let candidates = detail["exclusions"].as_array().is_some_and(|entries| {
        entries
            .iter()
            .any(|entry| entry["reason"] == "not_in_runner_candidates")
    });
    if candidates {
        checking("candidates_filter");
    }
    TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            if !summary.is_empty() {
                trace.exclusions = Some(summary);
            }
        }
    });
}

pub(crate) trait RefusalReply {
    fn refusal(&self) -> Option<(&str, Value)>;
}

impl RefusalReply for crate::account_selection::OpReply {
    fn refusal(&self) -> Option<(&str, Value)> {
        match self {
            Self::Error { code, detail, .. } => Some((code, detail.clone().unwrap_or(Value::Null))),
            Self::Result(_) => None,
        }
    }
}

impl RefusalReply for Result<Value, cortexkit_role_routing::errors::Refusal> {
    fn refusal(&self) -> Option<(&str, Value)> {
        self.as_ref()
            .err()
            .map(|refusal| (refusal.code.as_str(), Value::Object(refusal.detail.clone())))
    }
}

pub(crate) fn record<R: RefusalReply>(
    op: &str,
    principal: &str,
    params: &Value,
    call: impl FnOnce() -> R,
) -> R {
    let previous = TRACE.with(|trace| {
        trace.replace(Some(Trace {
            reason: "account_context",
            ..Default::default()
        }))
    });
    let _guard = TraceGuard(previous);
    let reply = call();
    if matches!(op, "route.account_for" | "route.account_observe") {
        if let Some((code, detail)) = reply.refusal() {
            allocator_refusal(&detail);
            TRACE.with(|trace| {
                let trace = trace.borrow();
                let trace = trace.as_ref().unwrap();
                let reason = detail["field"]
                    .as_str()
                    .map(|field| format!("{}:{field}", trace.reason));
                emit(
                    op,
                    principal,
                    params,
                    code,
                    reason.as_deref().unwrap_or(trace.reason),
                    trace.exclusions.as_deref().unwrap_or(&exclusions(&detail)),
                );
            });
        }
    }
    reply
}

pub(crate) fn emit(
    op: &str,
    principal: &str,
    params: &Value,
    code: &str,
    reason: &str,
    exclusions: &str,
) {
    static LIMITER: OnceLock<Mutex<LogLimiter>> = OnceLock::new();
    let suppressed = LIMITER
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .admit(principal, reason, crate::now_ms() as i64);
    let Some(suppressed) = suppressed else { return };
    // Select individual scalar metadata and presence flags, never serialize
    // request objects, error messages, headers or credential material.
    let fields = json!({
        "op":op, "principal":principal, "code":code, "reason":reason,
        "provider":params["provider"].as_str(), "model":params["model"].as_str(),
        "admission_id":params["admission_id"].as_str(), "mode":params["mode"].as_str(),
        "decision_id_present":params.get("decision_id").is_some(),
        "candidates_present":params.get("candidates").is_some(),
        "requested_account_present":params.get("requested_account").is_some(),
        "conversation_key_present":params.get("conversation_key").is_some(),
        "hold_wait_max_s":params["hold_wait_max_s"].as_u64(),
        "exclusions":exclusions, "suppressed":suppressed,
    });
    crate::warn_operator(format!("routing refused an account call: {fields}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_is_per_actor_and_reason_and_reports_suppression() {
        let mut limiter = LogLimiter::default();
        assert_eq!(limiter.admit("runner-a", "candidates_filter", 0), Some(0));
        assert_eq!(limiter.admit("runner-a", "candidates_filter", 1), None);
        assert_eq!(limiter.admit("runner-a", "candidates_filter", 2), None);
        assert_eq!(limiter.admit("runner-b", "candidates_filter", 2), Some(0));
        assert_eq!(limiter.admit("runner-a", "pin_mismatch", 2), Some(0));
        assert_eq!(
            limiter.admit("runner-a", "candidates_filter", 60_000),
            Some(2)
        );
    }
}
