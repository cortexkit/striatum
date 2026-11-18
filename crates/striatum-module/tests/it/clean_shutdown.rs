//! Drive the real module over its authenticated supervisor connection.

use std::{
    io,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cortexkit_test_support::{fenced_command, stage_test_binary, ScratchDir};
use serde_json::json;
use subc_protocol::{Flags, Frame, FrameType, ModuleHelloAckBody, Priority};
use subc_transport::{authenticate_server, read_frame, write_frame};
use tokio::{io::AsyncWriteExt, net::TcpListener, process::Command, time::timeout};

#[tokio::test]
async fn clean_shutdown_on_goodbye_leaves_no_sqlite_sidecars() {
    assert_store_closed(Stop::Goodbye).await;
}

#[tokio::test]
async fn clean_shutdown_on_eof_leaves_no_sqlite_sidecars() {
    assert_store_closed(Stop::Eof).await;
}

#[tokio::test]
async fn clean_shutdown_on_read_error_leaves_no_sqlite_sidecars() {
    assert_store_closed(Stop::ReadError).await;
}

#[derive(Debug)]
enum Stop {
    Goodbye,
    Eof,
    ReadError,
}

async fn assert_store_closed(stop: Stop) {
    let scratch = ScratchDir::new("routing-clean-shutdown");
    let root = scratch.path();
    let state = root.join("store.db");
    let archive = root.join("history.db");
    let connection_file = root.join("connection.json");
    let key = [7_u8; 32];
    let daemon = [0_u8; 16];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    std::fs::write(
        &connection_file,
        serde_json::to_vec(&json!({
            "schema": 1,
            "endpoints": [{"host": "127.0.0.1", "port": listener.local_addr().unwrap().port()}],
            "key": key,
            "daemon_id": daemon,
            "pid": std::process::id(),
            "daemon_ver": "0.40.0",
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&connection_file, std::fs::Permissions::from_mode(0o600)).unwrap();

    let binary = stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
    let mut module = Command::from(fenced_command(&binary, root))
        .current_dir(root)
        .arg("--subc")
        .arg(&connection_file)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start ck-striatum");

    let ready = timeout(Duration::from_secs(10), async {
        let (mut peer, _) = listener.accept().await?;
        authenticate_server(&mut peer, &key, &daemon, "0.40.0", Duration::from_secs(2))
            .await
            .unwrap();
        let hello = read_frame(&mut peer).await.unwrap().expect("module HELLO");
        assert_eq!(hello.header.ty, FrameType::Hello);
        let ver = hello.header.ver;
        write_frame(
            &mut peer,
            &control_frame(
                ver,
                FrameType::HelloAck,
                serde_json::to_vec(&ModuleHelloAckBody {
                    negotiated_ver: ver,
                    subc_ops: vec![],
                    subc_capabilities: vec![],
                    storage: Some(
                        serde_json::to_value(striatum_store::dev_descriptor(
                            "striatum",
                            state.to_str().unwrap(),
                        ))
                        .unwrap(),
                    ),
                    machine_id: None,
                })
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        // The module answers PING only once the store and all serving loops are up.
        write_frame(&mut peer, &control_frame(ver, FrameType::Ping, vec![]))
            .await
            .unwrap();
        let pong = read_frame(&mut peer).await.unwrap().expect("module PONG");
        assert_eq!(pong.header.ty, FrameType::Pong);
        Ok::<_, io::Error>((peer, ver))
    });
    let (mut peer, ver) = tokio::select! {
        status = module.wait() => panic!("ck-striatum exited before serving: {status:?}"),
        ready = ready => ready.expect("ck-striatum startup timed out").unwrap(),
    };

    let sidecars = [
        "store.db-wal",
        "store.db-shm",
        "history.db-wal",
        "history.db-shm",
    ];
    assert!(
        state.is_file() && archive.is_file(),
        "expected a split store"
    );
    for name in sidecars {
        assert!(
            root.join(name).metadata().is_ok_and(|file| file.len() > 0),
            "expected non-empty {name} before stop"
        );
    }

    if matches!(stop, Stop::Goodbye) {
        let deadline_ms = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
            + Duration::from_secs(30))
        .as_millis();
        let notice = json!({"op": "module.draining", "reason": "stop", "deadline_ms": deadline_ms});
        let frame = control_frame(ver, FrameType::Push, serde_json::to_vec(&notice).unwrap());
        write_frame(&mut peer, &frame).await.unwrap();
    }
    let stop_started = Instant::now();
    match stop {
        Stop::Goodbye => write_frame(&mut peer, &control_frame(ver, FrameType::Goodbye, vec![]))
            .await
            .unwrap(),
        Stop::ReadError => peer.write_all(&[0]).await.unwrap(),
        Stop::Eof => {}
    }
    peer.shutdown().await.unwrap();
    let output = timeout(Duration::from_secs(8), module.wait_with_output())
        .await
        .expect("ck-striatum did not stop")
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    match stop {
        Stop::ReadError => {
            assert!(!output.status.success());
            assert!(stderr.contains("unexpected EOF"), "{stderr}");
        }
        Stop::Goodbye | Stop::Eof => {
            assert!(output.status.success(), "{}: {stderr}", output.status);
        }
    }
    eprintln!("{stop:?} to process exit: {:?}", stop_started.elapsed());

    // Do not open either database after reaping: SQLite could remove sidecars
    // during recovery and conceal a shutdown that never closed the store.
    let remaining: Vec<_> = sidecars
        .into_iter()
        .filter(|name| root.join(name).exists())
        .collect();
    assert!(remaining.is_empty(), "clean shutdown left {remaining:?}");
}

fn control_frame(ver: u8, ty: FrameType, body: Vec<u8>) -> Frame {
    Frame::build_with_version(
        ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        0,
        0,
        0,
        body,
    )
    .unwrap()
}
