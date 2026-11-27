//! Exercise the real module's private registration channel with a fake daemon.

use std::{
    collections::HashSet,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
};

use cortexkit_test_support::{fenced_command, stage_test_binary, ScratchDir};
use serde::Serialize;
use serde_json::{json, Value};
use subc_os::registration_channel::DAEMON_PID_ENV;
#[cfg(unix)]
use subc_os::registration_channel::{
    registration_socketpair, ChildDescriptorSlots, REGISTRATION_FD_ENV as HANDOFF_ENV,
};
#[cfg(windows)]
use subc_os::registration_channel::{RegistrationPipe, REGISTRATION_PIPE_ENV as HANDOFF_ENV};
use subc_protocol::{
    claim::{
        Claim, ClaimMintReply, ClaimMintRequest, ClaimPresentReply, ClaimPresentRequest,
        ClaimPurpose,
    },
    Flags, Frame, FrameType, ModuleHelloAckBody, Priority,
};
use subc_transport::{authenticate_server, read_frame, write_frame, AuthError};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::JoinSet,
    time::{timeout, Duration},
};

const KEY: [u8; 32] = [7; 32];
const DAEMON: [u8; 16] = [0; 16];
const DAEMON_VERSION: &str = "0.50.17";
const NONCE: &str = "legacy-test-nonce";
const DEADLINE: Duration = Duration::from_secs(12);

struct Rig {
    scratch: ScratchDir,
    tcp: TcpListener,
}

impl Rig {
    async fn new() -> Self {
        let scratch = ScratchDir::new("routing-registration-channel");
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connection = scratch.path().join("connection.json");
        super::connection_file::write_private(
            &connection,
            json!({
                "schema": 1,
                "endpoints": [{"host": "127.0.0.1", "port": tcp.local_addr().unwrap().port()}],
                "key": KEY,
                "daemon_id": DAEMON,
                "pid": std::process::id(),
                "daemon_ver": DAEMON_VERSION,
            })
            .to_string()
            .as_bytes(),
        );
        Self { scratch, tcp }
    }

    fn command(&self) -> std::process::Command {
        let root = self.scratch.path();
        let binary = stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
        let mut command = fenced_command(&binary, root);
        command
            .current_dir(root)
            .arg("--subc")
            .arg(root.join("connection.json"))
            .env("SUBC_LAUNCH_NONCE", NONCE)
            .env(DAEMON_PID_ENV, std::process::id().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    }

    #[cfg(unix)]
    async fn launch(&self) -> (Child, impl Stream) {
        let (peer, inherited, descriptor) = registration_socketpair().unwrap();
        let mut slots = ChildDescriptorSlots::default();
        slots.channel(inherited).unwrap();
        let mut command = self.command();
        command.env(HANDOFF_ENV, descriptor);
        slots.install(&mut command);
        peer.set_nonblocking(true).unwrap();
        (
            spawn(command),
            tokio::net::UnixStream::from_std(peer).unwrap(),
        )
    }

    #[cfg(windows)]
    async fn launch(&self) -> (Child, impl Stream) {
        let pipe = RegistrationPipe::new().unwrap();
        let mut command = self.command();
        command.env(HANDOFF_ENV, pipe.name());
        let child = spawn(command);
        let pid = child.id().unwrap();
        let peer = tokio::task::spawn_blocking(move || {
            pipe.accept_tokio_pipe_server(pid, std::time::Instant::now() + DEADLINE)
        })
        .await
        .unwrap()
        .unwrap();
        (child, peer)
    }

    async fn ack(&self, peer: &mut (impl AsyncWrite + Unpin), hello: &Frame) {
        let ack = ModuleHelloAckBody {
            negotiated_ver: hello.header.ver,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: Some(
                serde_json::to_value(striatum_store::dev_descriptor(
                    "striatum",
                    self.scratch.path().join("store.db").to_str().unwrap(),
                ))
                .unwrap(),
            ),
            machine_id: None,
        };
        reply(peer, hello, FrameType::HelloAck, ack).await;
    }
}

fn spawn(command: std::process::Command) -> Child {
    Command::from(command).kill_on_drop(true).spawn().unwrap()
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

async fn auth(peer: &mut (impl Stream + ?Sized)) {
    // The transport takes a sized stream type; the mutable reference is sized.
    let mut peer = peer;
    authenticate(&mut peer).await.unwrap();
}

async fn authenticate(peer: &mut impl Stream) -> Result<(), AuthError> {
    authenticate_server(peer, &KEY, &DAEMON, DAEMON_VERSION, Duration::from_secs(2))
        .await
        .map(|_| ())
}

async fn recv(peer: &mut (impl AsyncRead + Unpin)) -> Frame {
    read_frame(peer).await.unwrap().expect("module frame")
}

async fn reply(
    peer: &mut (impl AsyncWrite + Unpin),
    req: &Frame,
    kind: FrameType,
    body: impl Serialize,
) {
    let h = req.header;
    let frame = Frame::build_with_version(
        h.ver,
        kind,
        h.flags,
        h.channel,
        h.epoch,
        h.corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(peer, &frame).await.unwrap();
}

async fn stop(child: Child, peer: &mut (impl AsyncWrite + Unpin), ver: u8) {
    let goodbye = Frame::build_with_version(
        ver,
        FrameType::Goodbye,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        0,
        vec![],
    )
    .unwrap();
    write_frame(peer, &goodbye).await.unwrap();
    let output = timeout(DEADLINE, child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn hello_uses_socketpair_without_launch_nonce_or_tcp_dial() {
    assert_hello_uses_private_channel().await;
}

#[cfg(windows)]
#[tokio::test]
async fn hello_uses_named_pipe_without_launch_nonce_or_tcp_dial() {
    assert_hello_uses_private_channel().await;
}

async fn assert_hello_uses_private_channel() {
    let rig = Rig::new().await;
    let (child, mut socket) = rig.launch().await;
    let hello = timeout(DEADLINE, async {
        tokio::select! {
            _ = rig.tcp.accept() => panic!("module dialed the connection-file endpoint for registration"),
            hello = async { auth(&mut socket).await; recv(&mut socket).await } => hello,
        }
    }).await.unwrap();
    assert_eq!(hello.header.ty, FrameType::Hello);
    let body = serde_json::from_slice::<Value>(&hello.body).unwrap();
    assert_eq!(body["manifest"]["module_id"], "striatum");
    assert!(body.get("launch_nonce").is_none());
    rig.ack(&mut socket, &hello).await;
    stop(child, &mut socket, hello.header.ver).await;
}

#[tokio::test]
async fn broken_channel_fails_instead_of_dialing_tcp() {
    let rig = Rig::new().await;
    let mut command = rig.command();
    command.env(HANDOFF_ENV, "broken");
    let output = timeout(DEADLINE, async {
        tokio::select! {
            _ = rig.tcp.accept() => panic!("module fell back to the TCP endpoint after a broken handoff"),
            output = spawn(command).wait_with_output() => output.unwrap(),
        }
    }).await.unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    #[cfg(unix)]
    let reason = "invalid registration metadata";
    #[cfg(windows)]
    let reason = "registration channel is not open";
    assert!(
        stderr.contains(&format!("adopt registration channel: {reason}")),
        "{stderr}"
    );
}

#[tokio::test]
async fn legacy_hello_still_dials_tcp_and_presents_launch_nonce() {
    let rig = Rig::new().await;
    let mut command = rig.command();
    command.env_remove(HANDOFF_ENV);
    let child = spawn(command);
    let (mut peer, hello) = timeout(DEADLINE, async {
        let (mut peer, _) = rig.tcp.accept().await.unwrap();
        auth(&mut peer).await;
        let hello = recv(&mut peer).await;
        (peer, hello)
    })
    .await
    .unwrap();
    assert_eq!(hello.header.ty, FrameType::Hello);
    assert_eq!(
        serde_json::from_slice::<Value>(&hello.body).unwrap()["launch_nonce"],
        NONCE
    );
    rig.ack(&mut peer, &hello).await;
    stop(child, &mut peer, hello.header.ver).await;
}

type Issued = Arc<Mutex<HashSet<String>>>;

async fn mint_claims(peer: &mut Box<dyn Stream>, issued: Issued) {
    let mut next_claim = 0_u128;
    loop {
        let req = recv(peer).await;
        assert_eq!(req.header.ty, FrameType::Request);
        assert_eq!((req.header.channel, req.header.epoch), (0, 0));
        let mint: ClaimMintRequest = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(mint.purpose, ClaimPurpose::SelfConnection);
        next_claim += 1;
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let token = URL_SAFE_NO_PAD.encode(next_claim.to_be_bytes());
        assert!(issued.lock().unwrap().insert(token.clone()));
        let response = ClaimMintReply::new(Claim::new(token), 0);
        reply(peer, &req, FrameType::Response, response).await;
    }
}

async fn accept_claimed(mut peer: TcpStream, issued: Issued) -> Option<Value> {
    match authenticate(&mut peer).await {
        // An SDK attempt may be cancelled immediately after dial; no framed
        // request used that socket's authority.
        Err(AuthError::UnexpectedEof { actual: 0, .. }) => return None,
        Err(AuthError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return None
        }
        result => result.unwrap(),
    }
    let first = read_frame(&mut peer).await.unwrap()?;
    assert_eq!(first.header.ty, FrameType::Request);
    assert_eq!((first.header.channel, first.header.epoch), (0, 0));
    let body: Value = serde_json::from_slice(&first.body).unwrap();
    assert_eq!(
        body["op"], "claim.present",
        "first consumer frame was not claim.present"
    );
    let present: ClaimPresentRequest = serde_json::from_value(body).unwrap();
    assert!(
        issued.lock().unwrap().remove(present.claim.expose_secret()),
        "claim was not minted for this connection or was reused"
    );
    reply(
        &mut peer,
        &first,
        FrameType::Response,
        ClaimPresentReply::new(),
    )
    .await;
    let (req, body) = loop {
        let req = read_frame(&mut peer).await.unwrap()?;
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        match body["op"].as_str() {
            Some("server.describe") => {
                reply(
                    &mut peer,
                    &req,
                    FrameType::Response,
                    json!({"op":"server.describe", "protocol_ver":req.header.ver,
                    "subc_ops":[],"capabilities":[],"connected_clients":1}),
                )
                .await
            }
            Some("catalog.list") => {
                reply(
                    &mut peer,
                    &req,
                    FrameType::Response,
                    json!({"op":"catalog.list", "generation":1,"modules":[],"subc_ops":[]}),
                )
                .await
            }
            _ => break (req, body),
        }
    };
    if body["op"] == "route.open" && body["target"]["module_id"] == "ckbus" {
        reply(
            &mut peer,
            &req,
            FrameType::Response,
            json!({"op":"route.open", "route_channel":7, "route_epoch":3}),
        )
        .await;
        let call = recv(&mut peer).await;
        let call_body: Value = serde_json::from_slice(&call.body).unwrap();
        assert_eq!(call_body["method"], "ckbus.credential");
        // A terminal refusal moves quota to its Insula fallback without a NATS server.
        reply(
            &mut peer,
            &call,
            FrameType::Error,
            json!({"code":"forbidden", "message":"fixture"}),
        )
        .await;
    } else {
        reply(
            &mut peer,
            &req,
            FrameType::Error,
            json!({"code":"forbidden", "message":"fixture"}),
        )
        .await;
    }
    Some(body)
}

#[tokio::test]
async fn quota_and_sdk_present_minted_claims_before_route_traffic() {
    let rig = Rig::new().await;
    let (_child, mut socket) = rig.launch().await;
    let (mut primary, hello): (Box<dyn Stream>, _) = timeout(DEADLINE, async {
        tokio::select! {
            // Let a TCP-registering implementation start so this test fails on
            // its missing consumer claim, not on which transport carried HELLO.
            accepted = rig.tcp.accept() => {
                let (mut peer, _) = accepted.unwrap();
                auth(&mut peer).await;
                let hello = recv(&mut peer).await;
                (Box::new(peer) as Box<dyn Stream>, hello)
            },
            hello = async { auth(&mut socket).await; recv(&mut socket).await } => {
                (Box::new(socket) as Box<dyn Stream>, hello)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(hello.header.ty, FrameType::Hello);
    rig.ack(&mut primary, &hello).await;
    let issued: Issued = Arc::default();
    let mut consumers = JoinSet::new();
    let mint = mint_claims(&mut primary, Arc::clone(&issued));
    tokio::pin!(mint);
    let (mut quota, mut sdk) = (false, false);
    let result = timeout(DEADLINE, async {
        while !quota || !sdk {
            tokio::select! {
                () = &mut mint => unreachable!(),
                accepted = rig.tcp.accept() => {
                    consumers.spawn(accept_claimed(accepted.unwrap().0, Arc::clone(&issued)));
                }
                Some(body) = consumers.join_next() => {
                    let Some(body) = body.unwrap() else { continue };
                    quota |= body["op"] == "route.open" && body["target"]["module_id"] == "insula";
                    sdk |= body["op"] == "route.open" && body["target"]["module_id"] == "ckbus";
                }
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "observed claims and route traffic: Insula={quota}, SDK bus={sdk}"
    );
}
