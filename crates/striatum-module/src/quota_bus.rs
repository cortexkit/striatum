//! One SDK reader for routing facts, catalog changes and Insula notices.
use std::{path::PathBuf, sync::Arc, time::Duration};

use cortexkit_bus_client::{BusClient, BusClientOptions, EventDelivery, EventItem};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{
    BusError, BusResult, ContentDigest, Message, StreamCursor, StreamDelivery,
};
use subc_client_rs::{ConsumerOptions, SubcConsumer};

use crate::route_facts::{Delivery, Handler};

const DURABLE: &str = "m_striatum";

pub(super) struct AttachedBus {
    pub names: AccountNames,
    pub subject: String,
    pub deliveries: tokio::sync::mpsc::Receiver<Result<(EventDelivery, u64), String>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for AttachedBus {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn attach(path: PathBuf, handler: Handler) -> Result<AttachedBus, String> {
    tokio::time::timeout(Duration::from_secs(15), attach_inner(path, handler))
        .await
        .map_err(|_| "bus attach timeout".to_owned())?
}

async fn attach_inner(path: PathBuf, handler: Handler) -> Result<AttachedBus, String> {
    let consumer = SubcConsumer::connect(&path, ConsumerOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    let client = BusClient::connect(
        Arc::new(consumer),
        BusClientOptions::new(crate::DEFAULT_MODULE_ID).map_err(|e| e.to_string())?,
    )
    .await
    .map_err(|e| e.to_string())?;
    let names = AccountNames::derive(client.acct()).map_err(|e| e.to_string())?;
    let mut changes = client.connection_changes();
    let bound_revision = *changes.borrow_and_update();
    let reader = client
        .event_reader(DURABLE)
        .await
        .map_err(|e| e.to_string())?;
    confirm(&client, &names, &handler, bound_revision).await?;
    handler.wake_catalog()?;
    let subject = names
        .event_subject("insula", "quota_sampled", 1)
        .map_err(|e| e.to_string())?;
    let (tx, deliveries) = tokio::sync::mpsc::channel(1);
    let task_names = names.clone();
    let task = tokio::spawn(async move {
        let result = read_events(
            client,
            task_names,
            &handler,
            reader,
            changes,
            bound_revision,
            &tx,
        )
        .await;
        unbound(&handler);
        let _ = tx
            .send(result.and(Err("event reader stopped".into())))
            .await;
    });
    Ok(AttachedBus {
        names,
        subject,
        deliveries,
        task,
    })
}

async fn read_events(
    client: BusClient,
    names: AccountNames,
    handler: &Handler,
    mut reader: cortexkit_bus_client::EventReader,
    mut changes: tokio::sync::watch::Receiver<u64>,
    mut bound_revision: u64,
    tx: &tokio::sync::mpsc::Sender<Result<(EventDelivery, u64), String>>,
) -> Result<(), String> {
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    loop {
        let item = {
            let next = reader.next();
            tokio::pin!(next);
            loop {
                tokio::select! {
                    biased;
                    changed = changes.changed() => {
                        unbound(handler);
                        changed.map_err(|_| "bus credential maintenance stopped".to_owned())?;
                        // End the old pull before checking a replacement binding. The SDK
                        // also watches this revision and rebinds its own next pull.
                        break None;
                    }
                    _ = tick.tick() => { confirm(&client, &names, handler, bound_revision).await?; }
                    result = &mut next => { break Some(result.map_err(|e| e.to_string())?); }
                }
            }
        };
        match item {
            None => {
                bound_revision = *changes.borrow();
                reader = client
                    .event_reader(DURABLE)
                    .await
                    .map_err(|e| e.to_string())?;
                confirm(&client, &names, handler, bound_revision).await?;
                handler.wake_catalog()?;
            }
            Some(EventItem::Delivery(delivery)) => {
                let context = async_nats::jetstream::new(client.connection().client());
                let stream = context
                    .get_stream(&names.streams().event)
                    .await
                    .map_err(|e| e.to_string())?;
                let metadata = stream
                    .get_raw_message(delivery.stream_sequence)
                    .await
                    .map_err(|e| e.to_string())?;
                let published_at_ms =
                    (metadata.time.unix_timestamp_nanos() / 1_000_000).max(0) as u64;
                tx.send(Ok((delivery, published_at_ms)))
                    .await
                    .map_err(|_| "bus receiver closed".to_owned())?;
            }
            Some(EventItem::Malformed(delivery)) => {
                tracing::warn!(subject = delivery.raw_subject, reason = ?delivery.reason, "malformed event terminated");
                delivery.term().await.map_err(|e| e.to_string())?;
            }
            Some(_) => return Err("unsupported SDK event item".to_owned()),
        }
    }
}

fn unbound(handler: &Handler) {
    if let Ok(mut cache) = handler.quota_cache.lock() {
        cache.consumer.attached = false;
        cache.consumer.route_facts.filters_ready = false;
    }
}

async fn confirm(
    client: &BusClient,
    names: &AccountNames,
    handler: &Handler,
    bound_revision: u64,
) -> Result<(), String> {
    let mut revisions = client.connection_changes();
    if *revisions.borrow_and_update() != bound_revision {
        unbound(handler);
        return Err("event reader awaiting rebind".into());
    }
    let connection = client.connection().client();
    if connection.connection_state() != async_nats::connection::State::Connected {
        unbound(handler);
        return Err("event reader disconnected".into());
    }
    let context = async_nats::jetstream::new(connection.clone());
    let consumer = context
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            DURABLE,
            &names.streams().event,
        )
        .await
        .map_err(|e| e.to_string())?;
    let info = consumer.cached_info();
    let mut filters = info.config.filter_subjects.clone();
    if !info.config.filter_subject.is_empty() {
        filters.push(info.config.filter_subject.clone());
    }
    let oldest = if info.num_ack_pending > 0 || info.num_pending > 0 {
        let stream = context
            .get_stream(&names.streams().event)
            .await
            .map_err(|e| e.to_string())?;
        let mut oldest = None;
        for filter in &filters {
            if let Ok(message) = stream
                .get_first_raw_message_by_subject(filter, info.ack_floor.stream_sequence + 1)
                .await
            {
                let at = (message.time.unix_timestamp_nanos() / 1_000_000).max(0) as u64;
                oldest = Some(oldest.map_or(at, |previous: u64| previous.min(at)));
            }
        }
        oldest
    } else {
        None
    };
    if oldest.is_some_and(|at| crate::now_ms().saturating_sub(at) > 30_000) {
        tracing::warn!(pending = info.num_pending, unacked = info.num_ack_pending, oldest_age_ms = ?oldest.map(|at| crate::now_ms().saturating_sub(at)), "event reader lag exceeds 30 seconds");
    }
    if !matches!(revisions.has_changed(), Ok(false)) {
        unbound(handler);
        return Err("event connection changed during filter confirmation".into());
    }
    if let Ok(mut cache) = handler.quota_cache.lock() {
        cache.consumer.attached = true;
        cache.consumer.lag = Some(info.num_pending);
        cache.consumer.ack_pending = Some(info.num_ack_pending);
        let health = &mut cache.consumer.route_facts;
        health.ack_wait_ms = Some(info.config.ack_wait.as_millis().min(u64::MAX as u128) as u64);
        health.filters_ready = crate::route_facts::filters_ready(names, Some(&info.name), &filters);
        health.connection = Some(connection);
        health.revisions = Some(revisions);
        health.oldest_unapplied_at_ms = oldest;
    }
    tracing::debug!(
        ack_wait_ms = info.config.ack_wait.as_millis(),
        "event durable confirmed"
    );
    Ok(())
}

pub(super) fn payload(
    names: &AccountNames,
    event: &EventDelivery,
    published_at_ms: u64,
) -> Result<Delivery, String> {
    let subject = match &event.subject.reader {
        Some(reader) => names.event_subject_with_reader(
            &event.subject.module,
            &event.subject.event,
            event.subject.version,
            reader,
        ),
        None => names.event_subject(
            &event.subject.module,
            &event.subject.event,
            event.subject.version,
        ),
    };
    let subject = subject.map_err(|e| e.to_string())?;
    message(
        subject,
        event.msg_id.clone(),
        &event.body,
        event.headers().clone(),
        event.header("Ck-Content-Digest"),
        published_at_ms,
        event.delivered_count.max(1) as u64,
    )
}

pub(super) fn message(
    subject: String,
    id: String,
    body: &[u8],
    headers: cortexkit_bus_trait::Headers,
    digest: Option<&str>,
    published_at_ms: u64,
    delivery_count: u64,
) -> Result<Delivery, String> {
    let insula = subject.contains(".event.insula.quota_sampled.v1");
    let digest = if insula {
        digest
            .ok_or("quota notice lacks Ck-Content-Digest")?
            .parse::<ContentDigest>()
            .map_err(str::to_owned)?
    } else {
        ContentDigest::of_bytes(body)
    };
    Ok(Delivery {
        message: Message {
            subject,
            id,
            digest,
            headers,
        },
        body: body.to_vec(),
        published_at_ms,
        delivery_count,
    })
}

/// A fetched SDK delivery is settled through the same cursor contract as tests.
pub(super) struct Cursor<'a> {
    pub event: &'a EventDelivery,
    pub notice: Option<StreamDelivery>,
}
fn bus_error(error: cortexkit_bus_client::Error) -> BusError {
    BusError::unavailable(error.to_string())
}
#[async_trait::async_trait]
impl StreamCursor for Cursor<'_> {
    async fn next(&mut self) -> BusResult<Option<StreamDelivery>> {
        Ok(self.notice.take())
    }
    async fn ack(&mut self) -> BusResult<()> {
        self.event.ack().await.map_err(bus_error)
    }
    async fn nak(&mut self, delay: Duration) -> BusResult<()> {
        self.event.nak(delay).await.map_err(bus_error)
    }
    async fn term(&mut self, _: Option<&str>) -> BusResult<()> {
        self.event.term().await.map_err(bus_error)
    }
    // Snapshot reads do not settle the notice; SDK 0.1.1 has no progress ACK.
    async fn in_progress(&mut self) -> BusResult<()> {
        Ok(())
    }
}
