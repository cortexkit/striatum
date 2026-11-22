//! Adopt the daemon's private stream and correlate claims on the module loop.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use subc_client_rs::{
    registration::{self, ClaimError, ClaimMinter},
    ModuleStream,
};
use subc_os::registration_channel as os;
use subc_protocol::{
    claim::{
        Claim, ClaimMintReply, ClaimMintRequest, ClaimPresentReply, ClaimPresentRequest,
        ClaimPurpose, ClaimRateLimitedDetail,
    },
    error_codes, ErrorBody, Frame, FrameType,
};
use subc_transport::{read_frame, write_frame};
use tokio::{
    sync::{mpsc, oneshot},
    time::{timeout, timeout_at, Instant},
};

use crate::{control_flags, ModuleError};

const MINT_TIMEOUT: Duration = Duration::from_secs(10);
const PRESENT_TIMEOUT: Duration = Duration::from_secs(2);
const PRESENT_CORR: u64 = 1;
// Settings allocates from the bottom of the channel-zero correlation range.
const FIRST_CLAIM_CORR: u64 = 1 << 63;

pub(super) fn adopt() -> Result<Option<Box<dyn ModuleStream>>, ModuleError> {
    #[cfg(unix)]
    let handoff_named = std::env::var_os(os::REGISTRATION_FD_ENV).is_some();
    #[cfg(windows)]
    let handoff_named = std::env::var_os(os::REGISTRATION_PIPE_ENV).is_some();
    #[cfg(not(any(unix, windows)))]
    let handoff_named = std::env::var_os(os::REGISTRATION_FD_ENV).is_some()
        || std::env::var_os(os::REGISTRATION_PIPE_ENV).is_some();
    if !handoff_named {
        return Ok(None);
    }
    match registration::adopt_channel() {
        Ok(Some(stream)) => Ok(Some(stream)),
        Ok(None) => {
            let reason = os::registration_refusal()
                .map(|reason| reason.to_string())
                .unwrap_or_else(|| "no validated stream".to_owned());
            Err(ModuleError::Message(format!(
                "adopt registration channel: {reason}"
            )))
        }
        Err(error) => Err(ModuleError::Message(format!(
            "adopt registration channel: {error}"
        ))),
    }
}

// The SDK retains the same minter for SubcConsumer. Raw consumers need a local
// reference because the SDK only exposes installation, not the installed owner.
static MINTER: OnceLock<Arc<Minter>> = OnceLock::new();

pub(crate) fn claim_bound() -> bool {
    MINTER.get().is_some()
}

/// Kept only while the module loop owns its registration reader.
pub(super) struct ClaimSession(Arc<Minter>);

impl ClaimSession {
    pub(super) fn install(writer: &mpsc::Sender<Frame>, ver: u8) -> Result<Self, ModuleError> {
        let minter = Arc::new(Minter {
            writer: writer.downgrade(),
            ver,
            pending: Mutex::new(Some(Pending {
                next_corr: FIRST_CLAIM_CORR,
                replies: HashMap::new(),
            })),
        });
        registration::install_claim_minter(minter.clone()).map_err(|error| {
            ModuleError::Message(format!("install registration claim minter: {error}"))
        })?;
        MINTER.set(minter.clone()).map_err(|_| {
            ModuleError::Message("registration claim minter already installed".into())
        })?;
        Ok(Self(minter))
    }

    pub(super) fn dispatch(&self, frame: Frame) -> Option<Frame> {
        let header = frame.header;
        if header.channel != 0
            || header.epoch != 0
            || header.corr < FIRST_CLAIM_CORR
            || !matches!(header.ty, FrameType::Response | FrameType::Error)
        {
            return Some(frame);
        }
        if let Some(reply) = self.0.remove(header.corr) {
            let _ = reply.send(frame);
        }
        // A late reply to a timed-out claim belongs to neither settings nor a route.
        None
    }
}

impl Drop for ClaimSession {
    fn drop(&mut self) {
        // Dropping the reply senders wakes in-flight calls as unavailable. The
        // process-wide SDK owner must not keep the module writer or reader alive.
        self.0.pending.lock().unwrap().take();
    }
}

struct Pending {
    next_corr: u64,
    replies: HashMap<u64, oneshot::Sender<Frame>>,
}

struct Minter {
    writer: mpsc::WeakSender<Frame>,
    ver: u8,
    pending: Mutex<Option<Pending>>,
}

impl Minter {
    fn remove(&self, corr: u64) -> Option<oneshot::Sender<Frame>> {
        self.pending.lock().unwrap().as_mut()?.replies.remove(&corr)
    }

    async fn call(&self, body: Vec<u8>) -> Result<Frame, ClaimError> {
        let writer = self.writer.upgrade().ok_or(ClaimError::Unavailable)?;
        let (corr, response) = {
            let mut state = self.pending.lock().unwrap();
            let pending = state.as_mut().ok_or(ClaimError::Unavailable)?;
            let corr = pending.next_corr;
            pending.next_corr = corr.checked_add(1).ok_or(ClaimError::Unavailable)?;
            let (reply, response) = oneshot::channel();
            pending.replies.insert(corr, reply);
            (corr, response)
        };
        let _pending = PendingCall { minter: self, corr };
        let frame = Frame::build_with_version(
            self.ver,
            FrameType::Request,
            control_flags(),
            0,
            0,
            corr,
            body,
        )
        .map_err(|_| ClaimError::Protocol)?;
        writer
            .send(frame)
            .await
            .map_err(|_| ClaimError::Unavailable)?;
        response.await.map_err(|_| ClaimError::Unavailable)
    }
}

struct PendingCall<'a> {
    minter: &'a Minter,
    corr: u64,
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        self.minter.remove(self.corr);
    }
}

#[async_trait::async_trait]
impl ClaimMinter for Minter {
    async fn mint(&self, request: ClaimMintRequest) -> Result<ClaimMintReply, ClaimError> {
        let body = serde_json::to_vec(&request).map_err(|_| ClaimError::Protocol)?;
        let deadline = Instant::now() + MINT_TIMEOUT;
        loop {
            let reply = timeout_at(deadline, self.call(body.clone()))
                .await
                .map_err(|_| ClaimError::Timeout)??;
            match reply.header.ty {
                FrameType::Response => {
                    return serde_json::from_slice(&reply.body).map_err(|_| ClaimError::Protocol);
                }
                FrameType::Error => {
                    let error = decode_refusal(&reply)?;
                    if error.code != error_codes::CLAIM_RATE_LIMITED {
                        return Err(ClaimError::Refused { code: error.code });
                    }
                    let detail: ClaimRateLimitedDetail =
                        serde_json::from_value(error.detail.ok_or(ClaimError::Protocol)?)
                            .map_err(|_| ClaimError::Protocol)?;
                    let delay = Duration::from_millis(detail.retry_after_ms);
                    if delay >= deadline.saturating_duration_since(Instant::now()) {
                        return Err(ClaimError::Timeout);
                    }
                    tokio::time::sleep(delay).await;
                }
                _ => return Err(ClaimError::Protocol),
            }
        }
    }
}

fn decode_refusal(reply: &Frame) -> Result<ErrorBody, ClaimError> {
    serde_json::from_slice(&reply.body).map_err(|_| ClaimError::Protocol)
}

pub(crate) async fn mint_for_connection() -> Result<Option<Claim>, ClaimError> {
    let Some(minter) = MINTER.get() else {
        return Ok(None);
    };
    minter
        .mint(ClaimMintRequest::new(ClaimPurpose::SelfConnection))
        .await
        .map(|reply| Some(reply.claim))
}

/// Present before any route traffic. Returns the first unused correlation; in
/// legacy mode no frame is written and the first route request still uses 1.
pub(crate) async fn present_for_connection(
    stream: &mut impl ModuleStream,
    claim: Option<Claim>,
) -> Result<u64, ClaimError> {
    let Some(claim) = claim else {
        return Ok(PRESENT_CORR);
    };
    let body =
        serde_json::to_vec(&ClaimPresentRequest::new(claim)).map_err(|_| ClaimError::Protocol)?;
    let present = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        PRESENT_CORR,
        body,
    )
    .map_err(|_| ClaimError::Protocol)?;
    let reply = timeout(PRESENT_TIMEOUT, async {
        write_frame(stream, &present)
            .await
            .map_err(|_| ClaimError::Unavailable)?;
        read_frame(stream)
            .await
            .map_err(|_| ClaimError::Unavailable)?
            .ok_or(ClaimError::Unavailable)
    })
    .await
    .map_err(|_| ClaimError::Timeout)??;
    let header = reply.header;
    if (header.channel, header.epoch, header.corr) != (0, 0, PRESENT_CORR) {
        return Err(ClaimError::Protocol);
    }
    match header.ty {
        FrameType::Response => {
            serde_json::from_slice::<ClaimPresentReply>(&reply.body)
                .map_err(|_| ClaimError::Protocol)?;
            Ok(PRESENT_CORR + 1)
        }
        FrameType::Error => Err(ClaimError::Refused {
            code: decode_refusal(&reply)?.code,
        }),
        _ => Err(ClaimError::Protocol),
    }
}
