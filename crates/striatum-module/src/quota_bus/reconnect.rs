use std::{
    future::Future,
    time::{Duration, Instant},
};

pub(crate) struct Backoff {
    next: Instant,
    seed: u64,
    base_seconds: u64,
}

impl Backoff {
    pub(crate) fn new(now: Instant, seed: u64) -> Self {
        Self {
            next: now,
            seed: seed.max(1),
            base_seconds: 1,
        }
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.next
    }
    pub(crate) fn failed(&mut self, now: Instant) {
        // Per-process jitter prevents modules restarted together from retrying
        // in lockstep. It is scheduling entropy, not a security credential.
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        let permille = 800 + self.seed % 401;
        let milliseconds = (self.base_seconds * permille).min(60_000);
        self.next = now + Duration::from_millis(milliseconds);
        self.base_seconds = (self.base_seconds * 2).min(60);
    }
    /// Reset only for a handler's confirmed ACK or a continuously bound minute.
    pub(crate) fn healthy(&mut self) {
        self.base_seconds = 1;
    }
    pub(crate) fn observe_bound(&mut self, now: Instant, since: Option<Instant>) {
        if since
            .is_some_and(|since| now.saturating_duration_since(since) >= Duration::from_secs(60))
        {
            self.healthy();
        }
    }
    pub(crate) async fn attempt<T>(
        &mut self,
        now: Instant,
        connect: impl Future<Output = Result<T, String>>,
    ) -> Option<Result<T, String>> {
        if now < self.next {
            return None;
        }
        let started = Instant::now();
        let result = connect.await;
        if result.is_err() {
            self.failed(now + started.elapsed());
        }
        Some(result)
    }
}
