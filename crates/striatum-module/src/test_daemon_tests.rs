//! Shared control-probe handling for mock daemon connections.

use serde_json::json;
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame, FrameIoError};

pub(crate) async fn read_application_frame(
    stream: &mut tokio::net::TcpStream,
) -> Result<Option<Frame>, FrameIoError> {
    loop {
        let Some(request) = read_frame(stream).await? else {
            return Ok(None);
        };
        if request.header.channel != 0
            || request.header.ty != FrameType::Request
            || serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()
                .is_none_or(|body| body["op"] != "server.describe")
        {
            return Ok(Some(request));
        }
        // Advertising this capability makes the SDK request structured reasons
        // when routes close, just as it does against a current daemon.
        let reply = Frame::build(
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            0,
            request.header.epoch,
            request.header.corr,
            serde_json::to_vec(&json!({"op":"server.describe", "protocol_ver":1,
                "subc_ops":[], "capabilities":["route-close-reasons/v1"],
                "connected_clients":1}))
            .unwrap(),
        )
        .unwrap();
        write_frame(stream, &reply).await?;
    }
}
