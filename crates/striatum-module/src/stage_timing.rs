//! Request-stage measurements shared by the live slow-call log and release bench.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use striatum_store::{SqliteRouteStateStore, StoreStageTiming};

/// Match the existing slow route.select log: strictly more than one second.
pub(crate) const SLOW_ACCOUNT_CALL: Duration = Duration::from_millis(1_000);
static SERIALIZATION_NS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn serialization_ns() -> u64 {
    SERIALIZATION_NS.load(Ordering::Relaxed)
}

pub(crate) fn record_serialization(start: Instant) -> Duration {
    let elapsed = start.elapsed();
    SERIALIZATION_NS.fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    elapsed
}

pub(crate) struct AccountCallTiming {
    started: Instant,
    before: StoreStageTiming,
}

impl AccountCallTiming {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            before: SqliteRouteStateStore::thread_stage_timing(),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub(crate) fn emit(
        &self,
        total: Duration,
        operation: &str,
        mode: &str,
        provider: &str,
        principal: &str,
        serialization: Duration,
        after: StoreStageTiming,
    ) {
        if total <= SLOW_ACCOUNT_CALL {
            return;
        }
        let ms = |after: u64, before: u64| after.saturating_sub(before) as f64 / 1_000_000.0;
        // JSON-quoted labels keep even invalid request values on one log line.
        let mode = if operation == "route.account_for" {
            format!(" mode={}", serde_json::to_string(mode).unwrap())
        } else {
            String::new()
        };
        let line = format!("[routing] slow {operation} total_ms={} writer_queue_wait_ms={:.3} sql_ms={:.3} commit_ms={:.3} serialization_ms={:.3} operation={operation}{mode} provider={} principal={}",
            total.as_millis(), ms(after.writer_queue_ns, self.before.writer_queue_ns),
            ms(after.sql_ns, self.before.sql_ns), ms(after.commit_ns, self.before.commit_ns),
            serialization.as_secs_f64() * 1_000.0,
            serde_json::to_string(provider).unwrap(), serde_json::to_string(principal).unwrap());
        #[cfg(test)]
        LOG_LINES.with(|lines| {
            if let Some(lines) = lines.borrow_mut().as_mut() {
                lines.push(line.clone());
            }
        });
        eprintln!("{line}");
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static ACCOUNT_DELAY: std::cell::Cell<Duration> = const { std::cell::Cell::new(Duration::ZERO) };
    pub(crate) static LOG_LINES: std::cell::RefCell<Option<Vec<String>>> = const { std::cell::RefCell::new(None) };
}
