use super::*;

fn not_ready() -> ErrorBody {
    ErrorBody {
        code: "routing_not_ready".into(),
        message:
            "routing catalog targets await a catalog snapshot and credential listing; retry shortly"
                .into(),
        detail: Some(json!({"retryable": true, "retry_after_ms": 1000})),
    }
}

pub(super) async fn serve(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let principal = route_principal(channel, epoch);
    if principal != "reserved:occipital" && !is_fence_lift_operator(&principal) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "not_permitted",
            &format!("route.catalog_targets: principal '{principal}' is not permitted"),
        )
        .await;
    }
    let params = serde_json::from_slice::<MethodParamsBody<BTreeMap<String, serde_json::Value>>>(
        &frame.body,
    );
    if !params.is_ok_and(|body| body.params.is_empty()) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.catalog_targets expects params {}",
        )
        .await;
    }

    let result = (|| {
        let guard = credentials.lock().map_err(|_| not_ready())?;
        if guard.listing_version().is_none() {
            return Err(not_ready());
        }
        let snapshot = guard.catalog_snapshot.as_ref().ok_or_else(not_ready)?;
        // The map and version must agree even while SQLite rows and cached
        // billing rates refresh independently.
        let (catalog_version, providers) = snapshot.provider_index();
        let targets = guard.catalog_resolution_for_providers(&providers).targets;
        Ok(json!({"targets": targets, "catalog_version": catalog_version}))
    })();
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return send_route_error_body(writer, ver, channel, epoch, corr, error).await;
        }
    };
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&json!({"result": result})).map_err(ModuleError::Json)?,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}
