//! Request-stage measurements shared by the live slow-call log and release bench.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use striatum_store::StoreStageTiming;

/// Expose calls approaching account clients' deadlines without logging fast calls.
pub(crate) const SLOW_ACCOUNT_CALL: Duration = Duration::from_millis(250);
static SERIALIZATION_NS: AtomicU64 = AtomicU64::new(0);

/// Carry the current background pass into the blocking pool and merge only its
/// completed work. The scope is restored before the pool reuses the thread.
pub(crate) async fn spawn_job_blocking<F, T>(f: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (result, work) =
        tokio::task::spawn_blocking(striatum_store::job_timing::blocking(f)).await?;
    striatum_store::job_timing::record(work);
    Ok(result)
}

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
}

impl AccountCallTiming {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
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
        stages: StoreStageTiming,
    ) {
        if total <= SLOW_ACCOUNT_CALL {
            return;
        }
        let ms = |ns: u64| ns as f64 / 1_000_000.0;
        // JSON-quoted labels keep even invalid request values on one log line.
        let mode = if operation == "route.account_for" {
            format!(" mode={}", serde_json::to_string(mode).unwrap())
        } else {
            String::new()
        };
        let line = format!("[routing] slow {operation} total_ms={} writer_queue_wait_ms={:.3} sql_ms={:.3} commit_ms={:.3} serialization_ms={:.3} operation={operation}{mode} provider={} principal={}",
            total.as_millis(), ms(stages.writer_queue_ns),
            ms(stages.sql_ns), ms(stages.commit_ns),
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

    pub(crate) fn emit_panel(
        &self,
        total: Duration,
        params: &serde_json::Value,
        counts: (Option<u64>, Option<u64>),
        serialization: Duration,
        stages: StoreStageTiming,
    ) {
        if total < SLOW_ACCOUNT_CALL {
            return;
        }
        let ms = |ns: u64| ns as f64 / 1_000_000.0;
        let label = |key: &str| serde_json::to_string(&params[key].as_str()).unwrap();
        let number =
            |value: Option<u64>| value.map_or_else(|| "null".to_owned(), |n| n.to_string());
        let line = format!("[routing] slow route.select_panel total_ms={} writer_queue_wait_ms={:.3} sql_ms={:.3} commit_ms={:.3} serialization_ms={:.3} operation=route.select_panel consumer={} class={} estimate={} package_tokens={} seated={} requested={}",
            total.as_millis(), ms(stages.writer_queue_ns), ms(stages.sql_ns), ms(stages.commit_ns),
            serialization.as_secs_f64() * 1_000.0, label("consumer"), label("class"),
            params["estimate"].as_bool().unwrap_or(false), number(params["package_tokens"].as_u64()),
            number(counts.0), number(counts.1));
        #[cfg(test)]
        LOG_LINES.with(|lines| {
            if let Some(lines) = lines.borrow_mut().as_mut() {
                lines.push(line.clone());
            }
        });
        eprintln!("{line}");
    }

    pub(crate) fn emit_head_choices(
        &self,
        total: Duration,
        serialization: Duration,
        stages: StoreStageTiming,
    ) {
        if total < SLOW_ACCOUNT_CALL {
            return;
        }
        let ms = |ns: u64| ns as f64 / 1_000_000.0;
        let line = format!("[routing] slow route.head_choices total_ms={} writer_queue_wait_ms={:.3} sql_ms={:.3} commit_ms={:.3} serialization_ms={:.3} operation=route.head_choices",
            total.as_millis(), ms(stages.writer_queue_ns), ms(stages.sql_ns), ms(stages.commit_ns),
            serialization.as_secs_f64() * 1_000.0);
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
    pub(crate) static ACCOUNT_PROBE: std::cell::RefCell<Option<AccountProbe>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct AccountProbe {
    pub injected: StoreStageTiming,
    pub reached: tokio::sync::oneshot::Sender<()>,
    pub resume: tokio::sync::oneshot::Receiver<()>,
    pub expected: tokio::sync::oneshot::Sender<StoreStageTiming>,
}

#[cfg(test)]
impl AccountProbe {
    pub(crate) async fn pause(
        self,
    ) -> (
        StoreStageTiming,
        tokio::sync::oneshot::Sender<StoreStageTiming>,
    ) {
        striatum_store::record_stage_timing_for_test(self.injected);
        self.reached.send(()).unwrap();
        self.resume.await.unwrap();
        (self.injected, self.expected)
    }
}
