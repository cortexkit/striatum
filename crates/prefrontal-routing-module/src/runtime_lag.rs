use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::AtomicU64;

const TICK: Duration = Duration::from_millis(250);
const LAG_THRESHOLD_MS: u64 = 500;
const LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub(crate) struct RuntimeLagProbe {
    inner: Arc<ProbeState>,
}

#[derive(Default)]
struct ProbeState {
    started: AtomicBool,
    #[cfg(test)]
    latest_lag_ms: AtomicU64,
}

impl RuntimeLagProbe {
    pub(crate) fn start(&self, runtime_name: &'static str) {
        self.start_with_reporter(runtime_name, |line| eprintln!("{line}"));
    }

    fn start_with_reporter(
        &self,
        runtime_name: &'static str,
        report: impl Fn(String) + Send + Sync + 'static,
    ) {
        if self.inner.started.swap(true, Ordering::Relaxed) {
            return;
        }
        let probe = self.clone();
        let report = Arc::new(report);
        let first_tick = Instant::now() + TICK;
        tokio::spawn(async move {
            let mut ticks =
                tokio::time::interval_at(tokio::time::Instant::from_std(first_tick), TICK);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut expected = first_tick;
            let mut limiter = LagLogLimiter::default();
            loop {
                ticks.tick().await;
                let now = Instant::now();
                let lag_ms = now
                    .saturating_duration_since(expected)
                    .as_millis()
                    .min(u64::MAX as u128) as u64;
                // Start the next deadline from this wake-up so one stall does not persist.
                expected = now + TICK;
                probe.record(now, lag_ms);
                if let Some(suppressed) = limiter.report(now, lag_ms) {
                    report(lag_line(runtime_name, lag_ms, suppressed));
                }
            }
        });
    }

    #[cfg(test)]
    fn latest_lag_ms(&self) -> u64 {
        self.inner.latest_lag_ms.load(Ordering::Relaxed)
    }

    fn record(&self, _now: Instant, _lag_ms: u64) {
        #[cfg(test)]
        self.inner.latest_lag_ms.store(_lag_ms, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct LagLogLimiter {
    last_report: Option<Instant>,
    suppressed: u64,
}

impl LagLogLimiter {
    fn report(&mut self, now: Instant, lag_ms: u64) -> Option<u64> {
        if lag_ms <= LAG_THRESHOLD_MS {
            self.last_report = None;
            self.suppressed = 0;
            return None;
        }
        if self
            .last_report
            .is_some_and(|last| now.saturating_duration_since(last) < LOG_INTERVAL)
        {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        self.last_report = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

fn lag_line(runtime_name: &str, lag_ms: u64, suppressed: u64) -> String {
    if suppressed == 0 {
        format!("[runtime] scheduling lag {lag_ms} ms on {runtime_name}")
    } else {
        format!(
            "[runtime] scheduling lag {lag_ms} ms on {runtime_name} (suppressed {suppressed} lines)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread};

    #[test]
    fn routing_stall_logs_then_recovers_after_the_worker_becomes_idle() {
        let (tx, rx) = mpsc::channel();
        let probe = RuntimeLagProbe::default();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            probe.start_with_reporter("routing runtime", move |line| {
                let _ = tx.send(line);
            });
            tokio::time::sleep(Duration::from_millis(40)).await;
            // Blocking the only worker demonstrates the timer's scheduling delay.
            let blocker = tokio::spawn(async {
                thread::sleep(Duration::from_millis(850));
            });
            tokio::time::timeout(Duration::from_secs(2), async {
                while probe.latest_lag_ms() <= LAG_THRESHOLD_MS {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the probe should observe the blocked routing worker");
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while probe.latest_lag_ms() > LAG_THRESHOLD_MS {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("lag should return under threshold after the routing worker becomes idle");
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(probe.latest_lag_ms() <= LAG_THRESHOLD_MS);
        });
        let line = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(line.contains("scheduling lag"), "{line}");
        assert!(line.contains("on routing runtime"), "{line}");
        let lag_ms = line
            .strip_prefix("[runtime] scheduling lag ")
            .and_then(|rest| rest.split_once(" ms "))
            .and_then(|(lag, _)| lag.parse::<u64>().ok())
            .unwrap();
        assert!(lag_ms > LAG_THRESHOLD_MS, "{line}");
        assert!(
            rx.try_recv().is_err(),
            "a second lag line followed the idle period"
        );
    }

    #[test]
    fn persistent_lag_rate_limit_counts_suppressed_ticks() {
        let start = Instant::now();
        let mut limiter = LagLogLimiter::default();
        assert_eq!(limiter.report(start, 600), Some(0));
        assert_eq!(limiter.report(start + Duration::from_secs(3), 620), None);
        assert_eq!(
            limiter.report(start + Duration::from_secs(10), 630),
            Some(1)
        );
        assert_eq!(
            lag_line("routing runtime", 630, 1),
            "[runtime] scheduling lag 630 ms on routing runtime (suppressed 1 lines)"
        );
    }
}
