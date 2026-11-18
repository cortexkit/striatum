//! Supervision tests check how serving responds to background tasks stopping.

use super::*;

/// Long enough for any spawned loop in these tests to finish; a wait that
/// outlives it means a stopped loop went unnoticed.
const STOP_NOTICE_LIMIT: Duration = Duration::from_secs(5);

async fn wait_for_frame_or_stop(
    loops: &mut BackgroundLoops,
    read_half: &mut tokio::io::DuplexStream,
) -> Result<Option<Frame>, ModuleError> {
    tokio::time::timeout(STOP_NOTICE_LIMIT, next_frame_or_loop_stop(read_half, loops))
        .await
        .expect("the serving loop must notice a stopped background loop")
}

#[tokio::test]
async fn a_panicking_background_loop_ends_serving_with_its_name() {
    // The other half stays open, so subc never sends EOF: only the
    // stopped loop can end the wait.
    let (_subc, mut module) = tokio::io::duplex(1024);
    let mut loops = BackgroundLoops::default();
    loops.spawn("catalog refresh", std::future::pending());
    loops.spawn("quota events", async { panic!("quota bus gone") });

    let error = wait_for_frame_or_stop(&mut loops, &mut module)
        .await
        .expect_err("a panicked loop must end serving with an error");
    let message = error.to_string();
    assert!(message.contains("`quota events`"), "{message}");
    assert!(message.contains("panicked: quota bus gone"), "{message}");
}

#[tokio::test]
async fn a_returning_background_loop_ends_serving_with_its_name() {
    let (_subc, mut module) = tokio::io::duplex(1024);
    let mut loops = BackgroundLoops::default();
    loops.spawn("retention", async {});

    let error = wait_for_frame_or_stop(&mut loops, &mut module)
        .await
        .expect_err("a loop that returns must end serving with an error");
    let message = error.to_string();
    assert!(message.contains("`retention`"), "{message}");
    assert!(message.contains("returned"), "{message}");
}

#[tokio::test]
async fn frames_and_eof_are_served_while_background_loops_run() {
    let (mut subc, mut module) = tokio::io::duplex(1024);
    let mut loops = BackgroundLoops::default();
    loops.spawn("winner ladder watch", std::future::pending());
    let ping = Frame::build(FrameType::Ping, control_flags(), 0, 0, 7, Vec::new()).unwrap();
    write_frame(&mut subc, &ping).await.unwrap();

    let frame = wait_for_frame_or_stop(&mut loops, &mut module)
        .await
        .unwrap()
        .expect("the written frame");
    assert_eq!(frame.header.corr, 7);

    drop(subc);
    assert!(wait_for_frame_or_stop(&mut loops, &mut module)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn an_empty_loop_set_never_reports_a_stop() {
    let (subc, mut module) = tokio::io::duplex(1024);
    let mut loops = BackgroundLoops::default();
    drop(subc);
    assert!(wait_for_frame_or_stop(&mut loops, &mut module)
        .await
        .unwrap()
        .is_none());
}
