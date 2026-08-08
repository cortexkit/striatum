//! Module-to-module quota consumption: a SECOND, outbound connection from this
//! module to the daemon, acting as an ordinary consumer of `insula`'s (the quota
//! module, formerly `ai-provider-quota`) `usage.get`. This is independent of the module's inbound provider connection
//! (the one that serves `route.select`): the daemon keys connections by the
//! socket/handshake, so one process holds two authenticated connections without
//! conflation.
//!
//! Single-task-owned: only the background refresh task drives these short-lived
//! connections, so there is no correlation mux: each request reads frames until
//! its own `corr` reply. The refresh task awaits `usage_get` WITHOUT holding any lock that
//! `route.select` needs (the cache lock is dropped before the await), so the
//! outbound call can never deadlock the inbound serve loop.

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
    Json(serde_json::Error),
}

impl std::fmt::Display for QuotaConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(m) => write!(f, "{m}"),
            Self::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl std::error::Error for QuotaConsumerError {}

fn msg(s: impl Into<String>) -> QuotaConsumerError {
    QuotaConsumerError::Message(s.into())
}

/// An outbound consumer connection to the daemon, used for one quota refresh cycle.
/// The connection is closed after a successful `usage_get`; callers must create a
/// new consumer for the next cycle.
pub struct QuotaConsumer {
    stream: TcpStream,
    route_channel: u16,
    route_epoch: u32,
    next_corr: u64,
}

impl QuotaConsumer {
    /// Connect outbound, authenticate, and `route.open` the quota module's
    /// ManagementSurface. `project_root` is the bind identity's project root; the
    /// quota module is machine-global so it does not key on it, but the bind needs
    /// a valid identity.
    pub async fn connect(
        connection_file_path: &Path,
        project_root: &Path,
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
        (consumer.route_channel, consumer.route_epoch) = consumer.route_open(project_root).await?;
        Ok(consumer)
    }

    fn corr(&mut self) -> u64 {
        let c = self.next_corr;
        self.next_corr += 1;
        c
    }

    async fn route_open(&mut self, project_root: &Path) -> Result<(u16, u32), QuotaConsumerError> {
        let corr = self.corr();
        let body = serde_json::json!({
            "op": "route.open",
            "target": { "kind": "management_surface", "module_id": QUOTA_MODULE_ID },
            "identity": {
                "project_root": project_root,
                "harness": "prefrontal-routing",
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
            .map_err(|e| msg(e.to_string()))?;
        let reply = self.read_reply(0, 0, corr).await?;
        if reply.header.ty == FrameType::Error {
            return Err(msg(format!(
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
    pub async fn usage_get(
        &mut self,
    ) -> Result<Vec<prefrontal_routing_core::quota::ProviderUsage>, QuotaConsumerError> {
        let corr = self.corr();
        let body = serde_json::json!({ "method": USAGE_GET_OP, "params": {} });
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            self.route_channel,
            self.route_epoch,
            corr,
            serde_json::to_vec(&body).map_err(QuotaConsumerError::Json)?,
        )
        .map_err(|e| msg(e.to_string()))?;
        write_frame(&mut self.stream, &frame)
            .await
            .map_err(|e| msg(e.to_string()))?;
        let reply = self
            .read_reply(self.route_channel, self.route_epoch, corr)
            .await?;
        if reply.header.ty == FrameType::Error {
            return Err(msg(format!(
                "usage.get failed: {}",
                String::from_utf8_lossy(&reply.body)
            )));
        }
        // The quota module wraps the array as `{ "result": ProviderUsage[] }`.
        let value: serde_json::Value =
            serde_json::from_slice(&reply.body).map_err(QuotaConsumerError::Json)?;
        let result = value
            .get("result")
            .ok_or_else(|| msg("usage.get response missing 'result'"))?;
        let windows = serde_json::from_value(result.clone()).map_err(QuotaConsumerError::Json)?;
        self.goodbye().await?;
        Ok(windows)
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
            .map_err(|e| msg(format!("sending quota connection goodbye: {e}")))?;
        self.stream
            .shutdown()
            .await
            .map_err(|e| msg(format!("closing quota connection: {e}")))
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
                .map_err(|_| msg("timed out awaiting quota reply"))?
                .map_err(|e| msg(e.to_string()))?
                .ok_or_else(|| msg("quota connection closed mid-request"))?;
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
        let path = std::env::temp_dir().join(format!(
            "prefrontal-routing-quota-consumer-{pid}-{n}.json",
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
