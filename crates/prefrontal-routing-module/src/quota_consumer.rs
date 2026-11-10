//! Module-to-module quota consumption: a SECOND, outbound connection from this
//! module to the daemon, acting as an ordinary consumer of `insula`'s (the quota
//! module, formerly `ai-provider-quota`) `usage.get`. This is independent of the module's inbound provider connection
//! (the one that serves `route.select`): the daemon keys connections by the
//! socket/handshake, so one process holds two authenticated connections without
//! conflation.
//!
//! Single-task-owned: the quota event task owns its reusable route, while one-shot
//! callers keep their connections local. There is no correlation mux: each request
//! reads frames until its own `corr` reply. The refresh task awaits quota replies
//! WITHOUT holding any lock that `route.select` needs, so outbound calls cannot
//! deadlock the inbound serve loop.

use std::path::Path;
use std::time::Duration;

use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{io::AsyncWriteExt, net::TcpStream};

const QUOTA_MODULE_ID: &str = "insula";
const USAGE_GET_OP: &str = "usage.get";
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum QuotaConsumerError {
    Message(String),
    Transport(String),
    Route(String),
    Json(serde_json::Error),
}

impl std::fmt::Display for QuotaConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(m) => write!(f, "{m}"),
            Self::Transport(m) => write!(f, "{m}"),
            Self::Route(m) => write!(f, "{m}"),
            Self::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl std::error::Error for QuotaConsumerError {}

fn msg(s: impl Into<String>) -> QuotaConsumerError {
    QuotaConsumerError::Message(s.into())
}

fn transport(s: impl Into<String>) -> QuotaConsumerError {
    QuotaConsumerError::Transport(s.into())
}

fn route_error(s: impl Into<String>) -> QuotaConsumerError {
    QuotaConsumerError::Route(s.into())
}

/// Decode the `usage.get` result array ENTRY BY ENTRY. Insula extends its
/// entries additively, and a whole-array decode turned one entry routing could
/// not read into a failed refresh: the cache then served the last accepted
/// snapshot indefinitely. An entry that does not decode becomes a degraded
/// entry of class `decode_failed` for its provider and account instead, so the
/// rest of the poll still lands and the cache's degraded-entry handling applies
/// to that one entry. Only a result that is not an array fails the refresh.
pub(crate) fn decode_usage_result(
    result: &serde_json::Value,
) -> Result<Vec<prefrontal_routing_core::quota::ProviderUsage>, QuotaConsumerError> {
    let entries = result
        .as_array()
        .ok_or_else(|| msg("usage.get 'result' is not an array"))?;
    Ok(entries
        .iter()
        .map(|entry| {
            serde_json::from_value(entry.clone()).unwrap_or_else(|error| {
                let field = |name: &str| entry.get(name).and_then(serde_json::Value::as_str);
                let provider = field("provider").unwrap_or("<unnamed>").to_string();
                let account = field("account").map(str::to_string);
                let reason = format!("routing could not decode this usage.get entry: {error}");
                eprintln!(
                    "[striatum] quota refresh: {provider} account {}: {reason}",
                    account.as_deref().unwrap_or("-")
                );
                prefrontal_routing_core::quota::ProviderUsage {
                    provider,
                    api_provider: field("apiProvider").map(str::to_string),
                    account,
                    source: None,
                    usage: None,
                    spend: Vec::new(),
                    saved_resets: None,
                    error: Some(reason),
                    error_class: Some("decode_failed".into()),
                    stale: None,
                    fetched_at: None,
                }
            })
        })
        .collect())
}

/// An outbound daemon connection for quota queries. Insula's event consumer keeps
/// the route open; one-shot raw reads send `goodbye` after success.
pub struct QuotaConsumer {
    stream: TcpStream,
    route_channel: u16,
    route_epoch: u32,
    next_corr: u64,
}

impl QuotaConsumer {
    pub(crate) fn should_reconnect(error: &QuotaConsumerError) -> bool {
        matches!(
            error,
            QuotaConsumerError::Transport(_) | QuotaConsumerError::Route(_)
        )
    }

    /// Connect outbound, authenticate, and `route.open` the quota module's
    /// ManagementSurface. `project_root` is the bind identity's project root; the
    /// quota module is machine-global so it does not key on it, but the bind needs
    /// a valid identity.
    pub async fn connect(
        connection_file_path: &Path,
        project_root: &Path,
    ) -> Result<Self, QuotaConsumerError> {
        Self::connect_module(
            connection_file_path,
            project_root,
            QUOTA_MODULE_ID,
            "management_surface",
        )
        .await
    }

    pub(crate) async fn connect_module(
        connection_file_path: &Path,
        project_root: &Path,
        module: &str,
        kind: &str,
    ) -> Result<Self, QuotaConsumerError> {
        let conn = connection_file::read(connection_file_path)
            .map_err(|e| msg(format!("reading connection file: {e}")))?;
        let endpoint = conn
            .endpoints
            .first()
            .ok_or_else(|| msg("connection file has no endpoints"))?;
        let addr = format!("{}:{}", endpoint.host, endpoint.port);
        let stream = tokio::time::timeout(SETUP_TIMEOUT, async {
            let mut stream = TcpStream::connect(&addr)
                .await
                .map_err(|e| msg(format!("connect {addr}: {e}")))?;
            authenticate_client(&mut stream, &conn, Duration::from_secs(2))
                .await
                .map_err(|e| msg(format!("authenticate: {e}")))?;
            Ok::<_, QuotaConsumerError>(stream)
        })
        .await
        .map_err(|_| msg("timed out setting up quota connection"))??;

        let mut consumer = Self {
            stream,
            route_channel: 0,
            route_epoch: 0,
            next_corr: 1,
        };
        (consumer.route_channel, consumer.route_epoch) =
            consumer.route_open(project_root, module, kind).await?;
        Ok(consumer)
    }

    fn corr(&mut self) -> u64 {
        let c = self.next_corr;
        self.next_corr += 1;
        c
    }

    async fn route_open(
        &mut self,
        project_root: &Path,
        module: &str,
        kind: &str,
    ) -> Result<(u16, u32), QuotaConsumerError> {
        let corr = self.corr();
        let body = serde_json::json!({
            "op": "route.open",
            "target": { "kind": kind, "module_id": module },
            "identity": {
                "project_root": project_root,
                "harness": "striatum",
                "session": "quota-consumer",
            },
            "config": [],
        });
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            corr,
            serde_json::to_vec(&body).map_err(QuotaConsumerError::Json)?,
        )
        .map_err(|e| msg(e.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|e| transport(e.to_string()))?;
        let reply = self.read_reply(0, 0, corr).await?;
        if reply.header.ty == FrameType::Error {
            return Err(route_error(format!(
                "route.open(insula) failed: {}",
                String::from_utf8_lossy(&reply.body)
            )));
        }
        let value: serde_json::Value =
            serde_json::from_slice(&reply.body).map_err(QuotaConsumerError::Json)?;
        let route_channel = value["route_channel"]
            .as_u64()
            .map(|c| c as u16)
            .ok_or_else(|| msg("route.open returned no route_channel"))?;
        let route_epoch = value["route_epoch"]
            .as_u64()
            .and_then(|epoch| u32::try_from(epoch).ok())
            .ok_or_else(|| msg("route.open returned no valid route_epoch"))?;
        Ok((route_channel, route_epoch))
    }

    /// Call `usage.get` on the open quota route; returns the decoded `ProviderUsage[]`.
    #[cfg(test)]
    pub async fn usage_get(
        &mut self,
    ) -> Result<Vec<prefrontal_routing_core::quota::ProviderUsage>, QuotaConsumerError> {
        let body = self.usage_get_body(serde_json::json!({})).await?;
        // The quota module wraps the array as `{ "result": ProviderUsage[] }`.
        let value: serde_json::Value =
            serde_json::from_slice(&body).map_err(QuotaConsumerError::Json)?;
        let result = value
            .get("result")
            .ok_or_else(|| msg("usage.get response missing 'result'"))?;
        let windows = decode_usage_result(result)?;
        self.goodbye().await?;
        Ok(windows)
    }

    /// Insula's unfiltered feed returns every served provider. Keep the exact
    /// envelope bytes so account consumers also see `completeProviders`.
    pub async fn usage_get_raw(&mut self) -> Result<Vec<u8>, QuotaConsumerError> {
        let body = self.usage_get_raw_on_open_route().await?;
        self.goodbye().await?;
        Ok(body)
    }

    /// Read usage without closing the route so a long-lived consumer can reuse it.
    pub(crate) async fn usage_get_raw_on_open_route(
        &mut self,
    ) -> Result<Vec<u8>, QuotaConsumerError> {
        self.usage_get_body(serde_json::json!({})).await
    }

    /// Preserve the raw body substring: the notice digest covers the producer's
    /// bytes, not a consumer's reserialized JSON tree.
    pub async fn events_get_raw(&mut self, event_key: &str) -> Result<Vec<u8>, QuotaConsumerError> {
        let body = self.events_get_raw_on_open_route(event_key).await?;
        self.goodbye().await?;
        Ok(body)
    }

    /// Read an event without closing the route so a long-lived consumer can reuse it.
    pub(crate) async fn events_get_raw_on_open_route(
        &mut self,
        event_key: &str,
    ) -> Result<Vec<u8>, QuotaConsumerError> {
        let body = self
            .operation_body(serde_json::json!({
                "method": "events_get",
                "params": {"event_key": event_key, "event_name": "quota_sampled"},
            }))
            .await?;
        Ok(body)
    }

    /// Send one `usage.get` with `params` and return the successful reply body.
    async fn usage_get_body(
        &mut self,
        params: serde_json::Value,
    ) -> Result<Vec<u8>, QuotaConsumerError> {
        let body = serde_json::json!({ "method": USAGE_GET_OP, "params": params });
        self.operation_body(body).await
    }

    /// Optional economics queries are read-only. Missing modules or operations
    /// are normal refusals and are handled as unavailable by the adapter.
    pub(crate) async fn query(
        &mut self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, QuotaConsumerError> {
        let bytes = self.operation_body(body).await?;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(QuotaConsumerError::Json)?;
        Ok(value.get("result").cloned().unwrap_or(value))
    }

    pub(crate) async fn operation_body(
        &mut self,
        body: serde_json::Value,
    ) -> Result<Vec<u8>, QuotaConsumerError> {
        self.operation_at(self.route_channel, self.route_epoch, body)
            .await
    }

    async fn operation_at(
        &mut self,
        channel: u16,
        epoch: u32,
        body: serde_json::Value,
    ) -> Result<Vec<u8>, QuotaConsumerError> {
        let corr = self.corr();
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            epoch,
            corr,
            serde_json::to_vec(&body).map_err(QuotaConsumerError::Json)?,
        )
        .map_err(|e| msg(e.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|e| transport(e.to_string()))?;
        let reply = self.read_reply(channel, epoch, corr).await?;
        if reply.header.ty == FrameType::Error {
            let operation = body
                .get("method")
                .or_else(|| body.get("op"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("operation");
            return Err(route_error(format!(
                "{operation} failed: {}",
                String::from_utf8_lossy(&reply.body)
            )));
        }
        Ok(reply.body)
    }

    /// Close the whole daemon connection cleanly after a completed refresh.
    async fn goodbye(&mut self) -> Result<(), QuotaConsumerError> {
        let frame = Frame::build(
            FrameType::Goodbye,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            0,
            Vec::new(),
        )
        .map_err(|e| msg(e.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|e| transport(format!("sending quota connection goodbye: {e}")))?;
        self.stream
            .shutdown()
            .await
            .map_err(|e| transport(format!("closing quota connection: {e}")))
    }

    /// Read frames until the terminal reply (Response or Error) for `(channel, corr)`.
    async fn read_reply(
        &mut self,
        channel: u16,
        epoch: u32,
        corr: u64,
    ) -> Result<Frame, QuotaConsumerError> {
        loop {
            let frame = tokio::time::timeout(READ_TIMEOUT, read_frame(&mut self.stream))
                .await
                .map_err(|_| transport("timed out awaiting quota reply"))?
                .map_err(|e| transport(e.to_string()))?
                .ok_or_else(|| transport("quota connection closed mid-request"))?;
            if frame.header.corr == corr
                && frame.header.channel == channel
                && frame.header.epoch == epoch
                && matches!(frame.header.ty, FrameType::Response | FrameType::Error)
            {
                return Ok(frame);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::Instant;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn connection_file_path(port: u16) -> std::path::PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        // nanos: cross-run uniqueness under pid reuse
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-quota-consumer-{pid}-{nanos}-{n}.json",
            pid = std::process::id()
        ));
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "schema": 1,
                "endpoints": [{ "host": "127.0.0.1", "port": port }],
                "key": vec![7u8; 32],
                "daemon_id": vec![0u8; 16],
                "pid": std::process::id(),
                "daemon_ver": "test"
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn consecutive_cycles_use_fresh_connections_and_send_connection_goodbye() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_frame(&mut stream)
                    .await
                    .unwrap()
                    .expect("usage request");
                assert_eq!(request.header.ty, FrameType::Request);
                assert_eq!(request.header.channel, 7);
                assert_eq!(request.header.epoch, 3);
                assert_eq!(
                    request.header.corr, 1,
                    "each cycle starts its own correlation space"
                );
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(body["method"], USAGE_GET_OP);

                let response = Frame::build_with_version(
                    request.header.ver,
                    FrameType::Response,
                    request.header.flags,
                    request.header.channel,
                    request.header.epoch,
                    request.header.corr,
                    serde_json::to_vec(&serde_json::json!({ "result": [] })).unwrap(),
                )
                .unwrap();
                write_frame(&mut stream, &response).await.unwrap();

                let goodbye = read_frame(&mut stream)
                    .await
                    .unwrap()
                    .expect("connection goodbye");
                assert_eq!(goodbye.header.ty, FrameType::Goodbye);
                assert_eq!(goodbye.header.channel, 0);
                assert_eq!(goodbye.header.epoch, 0);
            }
        });

        for _ in 0..2 {
            let consumer_stream = TcpStream::connect(address).await.unwrap();
            let mut consumer = QuotaConsumer {
                stream: consumer_stream,
                route_channel: 7,
                route_epoch: 3,
                next_corr: 1,
            };
            assert!(consumer.usage_get().await.unwrap().is_empty());
        }
        server.await.unwrap();
    }

    /// Serve one `usage.get` reply carrying `result` and return what the
    /// consumer decoded from it.
    async fn usage_get_against(
        result: serde_json::Value,
    ) -> Result<Vec<prefrontal_routing_core::quota::ProviderUsage>, QuotaConsumerError> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut stream).await.unwrap().expect("request");
            let response = Frame::build_with_version(
                request.header.ver,
                FrameType::Response,
                request.header.flags,
                request.header.channel,
                request.header.epoch,
                request.header.corr,
                serde_json::to_vec(&serde_json::json!({ "result": result })).unwrap(),
            )
            .unwrap();
            write_frame(&mut stream, &response).await.unwrap();
            // Drain the goodbye so the client's shutdown completes.
            let _ = read_frame(&mut stream).await;
        });
        let mut consumer = QuotaConsumer {
            stream: TcpStream::connect(address).await.unwrap(),
            route_channel: 7,
            route_epoch: 3,
            next_corr: 1,
        };
        let decoded = consumer.usage_get().await;
        server.await.unwrap();
        decoded
    }

    #[tokio::test]
    async fn events_get_requests_the_key_and_preserves_body_bytes() {
        const REPLY: &[u8] = br#"{"result":{"body":{"provider":"codex","seq":9007199254740993,"epoch":"opaque"},"digest":"notice-digest"}}"#;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let key = "a".repeat(64);
        let expected_key = key.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut stream).await.unwrap().unwrap();
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(
                body,
                serde_json::json!({
                    "method": "events_get", "params": {"event_key": expected_key, "event_name": "quota_sampled"}
                })
            );
            let response = Frame::build_with_version(
                request.header.ver,
                FrameType::Response,
                request.header.flags,
                request.header.channel,
                request.header.epoch,
                request.header.corr,
                REPLY.to_vec(),
            )
            .unwrap();
            write_frame(&mut stream, &response).await.unwrap();
            assert_eq!(
                read_frame(&mut stream).await.unwrap().unwrap().header.ty,
                FrameType::Goodbye
            );
        });
        let mut consumer = QuotaConsumer {
            stream: TcpStream::connect(address).await.unwrap(),
            route_channel: 7,
            route_epoch: 3,
            next_corr: 1,
        };
        assert_eq!(consumer.events_get_raw(&key).await.unwrap(), REPLY);
        server.await.unwrap();
    }

    /// The reply that stalled routing's quota refresh from 2026-10-02 16:56Z:
    /// one Codex account's prepaid-credits pool arrived with `basis: "unstated"`
    /// and no `remaining`. The refresh loop then failed `usage.get` on every
    /// 3-second retry (the daemon log shows routing's insula `route.open` moving
    /// from a 60 s to a 3 s cadence at 16:56:04Z) while selection kept the
    /// 16:55:04Z snapshot. Both accounts must decode as healthy readings.
    #[tokio::test]
    async fn usage_get_accepts_a_codex_credits_pool_with_no_stated_balance() {
        let windows = usage_get_against(serde_json::json!([
            {
                "provider": "codex", "apiProvider": "openai",
                "account": "00000000-0000-4000-8000-0000000000a2",
                "usage": { "primary": { "usedPercent": 2.0, "windowMinutes": 10080 } },
                "spend": [{
                    "basis": "reported", "funding": "unknown", "id": "credits",
                    "label": "Prepaid credits",
                    "remaining": { "exponent": 0, "minor": 62500, "unit": "credit" }
                }]
            },
            {
                "provider": "codex", "apiProvider": "openai",
                "account": "00000000-0000-4000-8000-0000000000a3",
                "usage": { "primary": { "usedPercent": 2.0, "windowMinutes": 10080 } },
                "spend": [{
                    "basis": "unstated", "funding": "unknown", "id": "credits",
                    "label": "Prepaid credits"
                }]
            }
        ]))
        .await
        .expect("the poll must land");
        assert_eq!(windows.len(), 2);
        for window in &windows {
            assert!(
                window.is_healthy(),
                "{:?} must be a healthy reading, not a decode failure: {:?}",
                window.account,
                window.error
            );
        }
    }

    /// One entry routing cannot read must cost only that entry: the rest of the
    /// poll lands, and the unreadable one becomes a degraded `decode_failed`
    /// entry for its provider and account.
    #[tokio::test]
    async fn an_undecodable_entry_degrades_alone_instead_of_failing_the_poll() {
        let windows = usage_get_against(serde_json::json!([
            {
                "provider": "claude", "account": "good",
                "usage": { "primary": { "usedPercent": 10.0 } }
            },
            {
                "provider": "codex", "apiProvider": "openai", "account": "odd",
                "usage": { "primary": { "usedPercent": "ten" } }
            }
        ]))
        .await
        .expect("one undecodable entry must not fail the whole poll");
        assert_eq!(windows.len(), 2);
        assert!(windows[0].is_healthy());
        assert_eq!(windows[1].provider, "codex");
        assert_eq!(windows[1].api_provider.as_deref(), Some("openai"));
        assert_eq!(windows[1].account.as_deref(), Some("odd"));
        assert_eq!(windows[1].error_class.as_deref(), Some("decode_failed"));
        assert!(!windows[1].is_healthy());

        assert!(
            decode_usage_result(&serde_json::json!({ "not": "an array" })).is_err(),
            "a result that is not an array is still a failed refresh"
        );
    }

    #[tokio::test]
    async fn dead_port_returns_an_error_within_setup_bound() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let connection_file = connection_file_path(port);
        let started = Instant::now();

        let result = QuotaConsumer::connect(&connection_file, Path::new("/tmp/quota-test")).await;
        let error = match result {
            Ok(_) => panic!("a dead daemon port must fail the cycle setup"),
            Err(error) => error,
        };

        assert!(
            started.elapsed() < SETUP_TIMEOUT,
            "dead-port setup took longer than {SETUP_TIMEOUT:?}: {error}"
        );
        let _ = std::fs::remove_file(connection_file);
    }

    #[test]
    fn quota_dial_targets_insula() {
        // The rename class this fences: a caller whose compiled dial lags a module-id
        // rename compiles fine, registers nothing, and fails only at runtime with
        // unknown_module (the daemon refuses; the module never sees the call and reads
        // healthy throughout). SUBC's CLI walked exactly that window today. The wire id
        // is pinned here so a regression to the old name reddens a test by name instead
        // of surfacing as a silent quota-signal outage.
        assert_eq!(QUOTA_MODULE_ID, "insula");
    }
}
