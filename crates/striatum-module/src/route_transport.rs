//! Frame dispatch, daemon-attested route bindings, and response framing.
//! Keep the wire and runtime handoff separate from the operations it dispatches.

use super::*;

pub(super) type RouteEpochs = HashMap<u16, u32>;

/// The caller identity the daemon attested when it bound each route (for
/// example `reserved:broca` for the supervised Broca module), by channel, with
/// the epoch it was bound under. The account-selection operations namespace
/// admissions and affinity by it and accept a report only for an admission
/// they issued to the same identity; consumer admission also reads it. It is a
/// process global so the shared request handler keeps its signature.
static ROUTE_PRINCIPALS: OnceLock<Mutex<HashMap<u16, (u32, String)>>> = OnceLock::new();

pub(super) fn route_principals() -> &'static Mutex<HashMap<u16, (u32, String)>> {
    ROUTE_PRINCIPALS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The principal label for a request on `channel` under `epoch`; a route whose
/// bind carried no attestation is `unattested`.
pub(super) fn route_principal(channel: u16, epoch: u32) -> String {
    route_principals()
        .lock()
        .ok()
        .and_then(|map| {
            map.get(&channel)
                .filter(|(bound_epoch, _)| *bound_epoch == epoch)
                .map(|(_, label)| label.clone())
        })
        .unwrap_or_else(|| account_selection::principal_label(None))
}

/// The scope stamp of each bind whose route this module records decisions for,
/// by channel, reduced to the fields the decision ledger keeps: the Entorhinal
/// project id and the session folder. `None` for a route opened without a
/// scope. The daemon sends both only to a target that provides
/// `scope-workdir/v1`, so a scope it stamped without them records nothing.
static ROUTE_SCOPES: OnceLock<Mutex<HashMap<u16, (u32, Option<DecisionScope>)>>> = OnceLock::new();

/// Authorization is kept separately from decision-ledger metadata so a scope
/// without a project id still carries its daemon-attested owner and permission.
static ROUTE_SCOPE_AUTHORITY: OnceLock<
    Mutex<HashMap<u16, (u32, Option<routing_v1::ScopeAuthority>)>>,
> = OnceLock::new();

pub(super) fn route_scope_authority(
    channel: u16,
    epoch: u32,
) -> Option<routing_v1::ScopeAuthority> {
    ROUTE_SCOPE_AUTHORITY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .ok()
        .and_then(|map| {
            map.get(&channel)
                .filter(|(bound, _)| *bound == epoch)
                .and_then(|(_, scope)| scope.clone())
        })
}

fn route_scopes() -> &'static Mutex<HashMap<u16, (u32, Option<DecisionScope>)>> {
    ROUTE_SCOPES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The project and folder a route's scope was bound to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DecisionScope {
    pub(super) project_id: Option<String>,
    pub(super) workdir: Option<String>,
}

impl DecisionScope {
    /// The fields of a bind's scope stamp; `None` when the bind carried no
    /// scope at all. The stamp is the daemon's, read from the control frame,
    /// never from a request.
    fn from_stamp(scope: Option<&subc_protocol::scope::ScopeStamp>) -> Option<Self> {
        scope.map(|stamp| Self {
            project_id: stamp.project_id.clone(),
            workdir: stamp.workdir.clone(),
        })
    }
}

/// Record the scope of the route bound on `channel` under `epoch`.
fn remember_route_scope(channel: u16, epoch: u32, scope: Option<DecisionScope>) {
    if let Ok(mut map) = route_scopes().lock() {
        map.insert(channel, (epoch, scope));
    }
}

pub(super) fn clear_route_scope(channel: u16, epoch: u32) {
    head_choices::forget_bind(channel, epoch);
    if let Ok(mut map) = route_scopes().lock() {
        if map.get(&channel).is_some_and(|(bound, _)| *bound == epoch) {
            map.remove(&channel);
        }
    }
    if let Ok(mut map) = ROUTE_SCOPE_AUTHORITY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    {
        if map.get(&channel).is_some_and(|(bound, _)| *bound == epoch) {
            map.remove(&channel);
        }
    }
}

/// The scope the request on `channel` was bound under, when it still holds
/// for `epoch`.
pub(super) fn route_scope(channel: u16, epoch: u32) -> Option<DecisionScope> {
    route_scopes().lock().ok().and_then(|map| {
        map.get(&channel)
            .filter(|(bound, _)| *bound == epoch)
            .and_then(|(_, scope)| scope.clone())
    })
}

pub(super) fn clear_route_epoch(route_epochs: &mut RouteEpochs, channel: u16, epoch: u32) {
    if route_epochs.get(&channel) == Some(&epoch) {
        route_epochs.remove(&channel);
        clear_route_scope(channel, epoch);
        if let Ok(mut principals) = route_principals().lock() {
            if principals
                .get(&channel)
                .is_some_and(|(bound, _)| *bound == epoch)
            {
                principals.remove(&channel);
            }
        }
    }
}

/// Returns `Ok(false)` to stop the loop (graceful goodbye / EOF).
pub(super) async fn handle_frame(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    quota_cache: &SharedQuotaCache,
    store: &SharedStore,
    route_epochs: &mut RouteEpochs,
    min_band: &SharedMinBand,
    routing_config_path: &Path,
    operator_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
    catalog_health: &SharedCatalogHealth,
) -> Result<bool, ModuleError> {
    match frame.header.ty {
        FrameType::Ping if frame.header.channel == 0 => {
            let pong = Frame::build_with_version(
                frame.header.ver,
                FrameType::Pong,
                frame.header.flags,
                0,
                0,
                frame.header.corr,
                Vec::new(),
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, pong).await?;
            Ok(true)
        }
        FrameType::Goodbye if frame.header.channel == 0 => Ok(false),
        FrameType::Goodbye => {
            clear_route_epoch(route_epochs, frame.header.channel, frame.header.epoch);
            Ok(true)
        }
        FrameType::Request if frame.header.channel == 0 => {
            handle_control_request(
                frame,
                writer,
                route_epochs,
                catalog_health,
                broca_credentials,
                quota_cache,
            )
            .await?;
            Ok(true)
        }
        FrameType::Request => {
            let Some(&route_epoch) = route_epochs.get(&frame.header.channel) else {
                return Ok(true);
            };
            if frame.header.epoch != route_epoch {
                return Ok(true);
            }
            // Data-plane request on a route channel. Spawn so a slow selection
            // never head-of-line-blocks another route, and run the handler on
            // the blocking pool because it calls the SQLite store synchronously
            // (see run_route_handler_off_async_workers). Reading frames and
            // forwarding replies stay on this async task.
            let writer = writer.clone();
            let quota_cache = Arc::clone(quota_cache);
            let store = Arc::clone(store);
            let min_band = Arc::clone(min_band);
            let routing_config_path = routing_config_path.to_path_buf();
            let operator_config_path = operator_config_path.to_path_buf();
            let broca_credentials = Arc::clone(broca_credentials);
            let (ver, channel, corr) = (frame.header.ver, frame.header.channel, frame.header.corr);
            let handler = async move {
                answer_route_request(&writer, ver, channel, route_epoch, corr, |reply| {
                    run_route_handler_off_async_workers(async move {
                        routing_v1::record_call(&frame.body, channel, route_epoch, &store)?;
                        // A flow-scoped route reaches only the ops in
                        // diagnostics_findings::FLOW_SCOPE_OPS.
                        if let Some(refusal) = diagnostics_findings::flow_route_refusal(
                            &frame.body,
                            channel,
                            route_epoch,
                        ) {
                            if let Ok(request) =
                                serde_json::from_slice::<serde_json::Value>(&frame.body)
                            {
                                let op = request["method"].as_str().unwrap_or_default();
                                if matches!(op, "route.account_for" | "route.account_observe") {
                                    account_refusal_log::emit(
                                        op,
                                        &route_principal(channel, route_epoch),
                                        &request["params"],
                                        &refusal.code,
                                        "flow_scope",
                                        "",
                                    );
                                }
                            }
                            return send_route_error_body(
                                &reply,
                                ver,
                                channel,
                                route_epoch,
                                corr,
                                refusal,
                            )
                            .await;
                        }
                        routing_v1::runners::refresh_admission_tiers(
                            &frame.body,
                            &routing_v1::Caller::for_route(channel, route_epoch),
                            &routing_v1::Deps {
                                quota_cache: &quota_cache,
                                store: &store,
                                routing_config_path: &routing_config_path,
                                operator_config_path: &operator_config_path,
                                broca_credentials: &broca_credentials,
                            },
                        )
                        .await;
                        // `routing_v1::claims` sends every `route.select` request
                        // to the v1 parser, which rejects unknown parameters.
                        if routing_v1::claims(&frame.body) {
                            return routing_v1::serve(
                                frame,
                                route_epoch,
                                &reply,
                                &routing_v1::Deps {
                                    quota_cache: &quota_cache,
                                    store: &store,
                                    routing_config_path: &routing_config_path,
                                    operator_config_path: &operator_config_path,
                                    broca_credentials: &broca_credentials,
                                },
                            )
                            .await;
                        }
                        handle_select_request(
                            frame,
                            route_epoch,
                            &reply,
                            &quota_cache,
                            &store,
                            &min_band,
                            &routing_config_path,
                            &operator_config_path,
                            &broca_credentials,
                        )
                        .await
                    })
                })
                .await;
            };
            #[cfg(test)]
            let handler = routing_v1::inherit_capabilities(handler);
            tokio::spawn(handler);
            Ok(true)
        }
        _ => Ok(true),
    }
}

/// Serve one route-channel request and make sure its caller hears back.
///
/// `handle` replies through the sender it is given, and every frame it sends is
/// forwarded to the connection. When it returns an error, the error is logged.
/// If it had not sent a reply and the connection is still open, the caller is
/// answered with an `internal_error` frame instead of waiting out its own
/// timeout. A handler that fails after replying gets no second frame, so a
/// caller never receives two answers to one request.
pub(super) async fn answer_route_request<F, Fut>(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    handle: F,
) where
    F: FnOnce(mpsc::Sender<Frame>) -> Fut,
    Fut: std::future::Future<Output = Result<(), ModuleError>>,
{
    let (reply_tx, mut reply_rx) = mpsc::channel(1);
    let handled = handle(reply_tx);
    tokio::pin!(handled);
    let mut replied = false;
    let mut egress_open = true;
    let result = loop {
        let frame = tokio::select! {
            result = &mut handled => break result,
            Some(frame) = reply_rx.recv() => frame,
        };
        replied = true;
        egress_open = egress_open && send(writer, frame).await.is_ok();
    };
    // A send completes once the frame is queued, so everything the handler sent
    // before returning is already buffered here.
    while let Ok(frame) = reply_rx.try_recv() {
        replied = true;
        egress_open = egress_open && send(writer, frame).await.is_ok();
    }
    let Err(error) = result else {
        return;
    };
    let when = if replied { " after replying" } else { "" };
    warn_operator(format!(
        "[striatum] WARN: route request on channel {channel} (corr {corr}) failed{when}: {error}"
    ));
    if replied || !egress_open {
        return;
    }
    if let Err(send_error) = send_route_error(
        writer,
        ver,
        channel,
        epoch,
        corr,
        "internal_error",
        &format!("routing failed before replying: {error}"),
    )
    .await
    {
        warn_operator(format!(
            "[striatum] WARN: route request on channel {channel} (corr {corr}) got no failure reply: {send_error}"
        ));
    }
}

/// Run one route-channel request handler on Tokio's blocking pool and wait for
/// its result.
///
/// Route handlers call the SQLite route-state store synchronously, and a store
/// call can wait on disk I/O or on another connection's write lock. Run on an
/// async worker, that wait would also stall every other task scheduled on the
/// worker: other routes, the frame loop and the background loops. A blocking
/// thread drives the handler instead, with `Handle::block_on`, so the handler
/// code is unchanged: it awaits its reply sends exactly as before and writes in
/// the same order. `block_in_place` is not an option because it panics on a
/// current-thread runtime.
///
/// A panic in the handler is resumed here, so it ends this task just as it did
/// when the handler ran on the task itself.
pub(super) async fn run_route_handler_off_async_workers<F, T>(handler: F) -> Result<T, ModuleError>
where
    F: std::future::Future<Output = Result<T, ModuleError>> + Send + 'static,
    T: Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    #[cfg(test)]
    let handler = routing_v1::inherit_capabilities(handler);
    #[cfg(test)]
    let test_thread = TestThreadState::capture();
    let outcome = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        let _test_thread = test_thread.install();
        runtime.block_on(handler)
    })
    .await;
    match outcome {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The blocking pool cancels only while the runtime shuts down.
        Err(error) => Err(ModuleError::Message(format!(
            "route handler did not run to completion: {error}"
        ))),
    }
}

pub(super) async fn handle_control_request(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    route_epochs: &mut RouteEpochs,
    catalog_health: &SharedCatalogHealth,
    broca_credentials: &SharedBrocaCredentials,
    quota_cache: &SharedQuotaCache,
) -> Result<(), ModuleError> {
    let request = match serde_json::from_slice::<ModuleControlRequest>(&frame.body) {
        Ok(request) => request,
        Err(error) => {
            // A newer daemon may send a control shape this module cannot read.
            // Refuse only that request, without losing existing routes or health.
            tracing::warn!(corr = frame.header.corr, error = %error, "refusing undecodable control request");
            return send_route_error(
                writer,
                frame.header.ver,
                0,
                0,
                frame.header.corr,
                "invalid_request",
                &format!("invalid control request: {error}"),
            )
            .await;
        }
    };
    match request {
        ModuleControlRequest::RouteBind {
            route_channel,
            epoch,
            principal,
            scope,
            identity,
            ..
        } => {
            head_choices::remember_bind(
                route_channel,
                epoch,
                principal.as_ref(),
                scope.is_some(),
                &identity.harness,
                &identity.session,
            );
            if let Ok(mut map) = route_principals().lock() {
                map.insert(
                    route_channel,
                    (
                        epoch,
                        account_selection::principal_label(principal.as_ref()),
                    ),
                );
            }
            remember_route_scope(
                route_channel,
                epoch,
                DecisionScope::from_stamp(scope.as_ref()),
            );
            if let Ok(mut map) = ROUTE_SCOPE_AUTHORITY
                .get_or_init(|| Mutex::new(HashMap::new()))
                .lock()
            {
                map.insert(
                    route_channel,
                    (
                        epoch,
                        scope.as_ref().map(|stamp| routing_v1::ScopeAuthority {
                            owner: account_selection::principal_label(Some(&stamp.owner)),
                            reference: stamp.scope_ref.clone(),
                            kind: stamp.kind.clone(),
                            scope_epoch: stamp.scope_epoch,
                            project_id: stamp.project_id.clone(),
                            owner_authorized: stamp.owner_authorized,
                            flow_id: stamp.attributes.flow_id.clone(),
                            run_id: stamp.attributes.run_id.clone(),
                        }),
                    ),
                );
            }
            // The catalog is machine-global, but the wire identity is binding-specific.
            // Install it only after the accepted bind ACK is queued.
            let body = serde_json::to_vec(&ModuleControlResponse::RouteBindAck {})
                .map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                frame.header.ver,
                FrameType::Response,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, response).await?;
            route_epochs.insert(route_channel, epoch);
            Ok(())
        }
        ModuleControlRequest::HealthCheck {} => {
            // Unknown Broca reachability and stale quota snapshots can coexist.
            // Unmapped credential IDs remain in metrics without affecting status.
            let (mut report, pricing_ready) = broca_credentials
                .lock()
                .map(|credentials| {
                    (
                        credentials.health_report(now_ms()),
                        credentials.billing_catalog.is_loaded(),
                    )
                })
                .unwrap_or_else(|_| (BrocaCredentials::default().health_report(now_ms()), false));
            let broca_metrics = report.metrics.take();
            let (quota_detail, quota_metrics) = match quota_cache.lock() {
                Ok(cache) => quota_health(&cache, now_ms()),
                Err(_) => (
                    Some("quota cache lock poisoned".to_string()),
                    json!({ "lastError": "cache poisoned" }),
                ),
            };
            for detail in [
                (!pricing_ready).then(|| {
                    "routing pricing not ready: awaiting first billing-plane load".to_string()
                }),
                quota_detail,
            ]
            .into_iter()
            .flatten()
            {
                report.status = HealthStatus::Degraded;
                report.detail = Some(match report.detail.take() {
                    Some(existing) => format!("{existing}; {detail}"),
                    None => detail,
                });
            }
            report.metrics = Some(json!({
                "pricingReady": pricing_ready,
                "brocaReachability": broca_metrics,
                "fusiformCatalog": catalog_health
                    .lock()
                    .map(|health| health.snapshot(now_ms()))
                    .unwrap_or_else(|_| json!({ "reach": "never-reached", "lastError": "health lock poisoned" })),
                "quota": quota_metrics,
                "consumerDeclarations": routing_v1::consumers::health(),
            }));
            let body = serde_json::to_vec(&ModuleControlResponse::from(report))
                .map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                frame.header.ver,
                FrameType::Response,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|e| ModuleError::Message(e.to_string()))?;
            send(writer, response).await
        }
    }
}

/// The method envelope, decoded first so an unknown method reports `unknown_method`
/// regardless of whether the (method-specific) params are present/valid. Only once
/// the method is confirmed do we decode the typed `SelectParams`.
#[derive(Debug, Deserialize)]
pub(super) struct MethodEnvelope {
    pub(super) method: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct MethodParamsBody<P> {
    pub(super) params: P,
}

/// Send a `{ result: { ok: true } }` ack for a successful write op.
pub(super) async fn send_ok(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
) -> Result<(), ModuleError> {
    let body =
        serde_json::to_vec(&json!({ "result": { "ok": true } })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, response).await
}

/// Send a key-pool reply: a `{"result": ...}` response, or an error frame
/// carrying the typed refusal code and its detail (for an exhausted pool, every
/// key with its reason and reset).
pub(super) async fn send_pool_reply(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    reply: Result<serde_json::Value, key_pool::PoolRefusal>,
) -> Result<(), ModuleError> {
    let (ty, body) = match reply {
        Ok(result) => (
            FrameType::Response,
            serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?,
        ),
        Err(refusal) => (
            FrameType::Error,
            serde_json::to_vec(&ErrorBody {
                code: refusal.code,
                message: refusal.message,
                detail: refusal.detail,
            })
            .map_err(ModuleError::Json)?,
        ),
    };
    let frame = Frame::build_with_version(
        ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

pub(super) async fn send_route_error(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    code: &str,
    message: &str,
) -> Result<(), ModuleError> {
    send_route_error_body(
        writer,
        ver,
        channel,
        epoch,
        corr,
        ErrorBody {
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
        },
    )
    .await
}

pub(super) async fn send_route_error_body(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    error: ErrorBody,
) -> Result<(), ModuleError> {
    let body = serde_json::to_vec(&error).map_err(ModuleError::Json)?;
    let frame = Frame::build_with_version(
        ver,
        FrameType::Error,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

/// Send an account-selection reply: a `{"result": ...}` response, or an error
/// frame whose code is the typed refusal code and whose detail repeats that
/// code with the retry time, decision id and per-account exclusions.
pub(super) async fn send_account_reply(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    reply: account_selection::OpReply,
) -> Result<(), ModuleError> {
    let (frame_type, body) = match reply {
        account_selection::OpReply::Result(result) => (
            FrameType::Response,
            serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?,
        ),
        account_selection::OpReply::Error {
            code,
            message,
            detail,
        } => (
            FrameType::Error,
            serde_json::to_vec(&ErrorBody {
                code,
                message,
                detail,
            })
            .map_err(ModuleError::Json)?,
        ),
    };
    let frame = Frame::build_with_version(
        ver,
        frame_type,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ModuleError::Message(e.to_string()))?;
    send(writer, frame).await
}

pub(super) async fn send(writer: &mpsc::Sender<Frame>, frame: Frame) -> Result<(), ModuleError> {
    writer
        .send(frame)
        .await
        .map_err(|_| ModuleError::Message("egress channel closed".into()))
}

pub(super) fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}
