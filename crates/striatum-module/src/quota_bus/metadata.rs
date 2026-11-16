//! Optional info failures cannot stop delivery; a missing durable needs a new bind.
use std::{future::Future, time::Duration};

use crate::route_facts::Handler;

#[async_trait::async_trait]
pub(crate) trait Source: Send {
    async fn read(&mut self) -> Result<(), String>;
}

pub(crate) struct SdkSource<'a> {
    pub client: &'a cortexkit_bus_client::BusClient,
    pub names: &'a cortexkit_bus_naming::AccountNames,
    pub handler: &'a Handler,
    pub bound_revision: u64,
}

#[async_trait::async_trait]
impl Source for SdkSource<'_> {
    async fn read(&mut self) -> Result<(), String> {
        super::confirm(self.client, self.names, self.handler, self.bound_revision).await
    }
}

pub(crate) async fn with_metadata<T>(
    source: &mut impl Source,
    handler: &Handler,
    work: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let monitor = async {
        let mut retry = Duration::from_secs(30);
        loop {
            let result = tokio::time::timeout(Duration::from_secs(5), source.read())
                .await
                .unwrap_or_else(|_| Err("consumer-info timeout".to_owned()));
            let delay = match result {
                Ok(()) => {
                    retry = Duration::from_secs(30);
                    Duration::from_secs(10)
                }
                Err(error) => {
                    if let Ok(mut cache) = handler.quota_cache.lock() {
                        cache.consumer.route_facts.filters_ready = false;
                        cache.consumer.lag = None;
                        cache.consumer.ack_pending = None;
                    }
                    // A missing durable cannot serve more messages. Return its
                    // error so the caller retries attachment on the connection
                    // backoff; timeouts and permission refusals only retry info.
                    if super::no_durable(&error) {
                        return Err(error);
                    }
                    tracing::warn!(
                        error,
                        retry_in_s = retry.as_secs(),
                        "event metadata unavailable; route_fact_stream withdrawn"
                    );
                    let delay = retry;
                    retry = (retry * 2).min(Duration::from_secs(600));
                    delay
                }
            };
            tokio::time::sleep(delay).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    };
    tokio::select! {
        biased;
        result = monitor => { result?; Err("metadata monitor stopped".into()) }
        result = work => result,
    }
}
