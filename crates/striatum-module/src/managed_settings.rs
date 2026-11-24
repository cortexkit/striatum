//! Routing's settings hooks and the channel-0 session that drives them.

mod projects;
mod session;
#[cfg(test)]
#[path = "managed_settings/test_daemon/tests.rs"]
pub(crate) mod test_daemon;
#[cfg(test)]
mod tests;
pub(super) use projects::{project_view, RequestViews};
pub(super) use session::{Session, PROJECTS_CAPABILITY, WAITING};

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;
use subc_client_rs::config::{
    ApplyKind, ConfigDeclareRequest, ConfigReviewRefusal, ConfigReviewReply, ConfigReviewRequest,
    ConfigStatus, ConfigView, FieldChange, Layer, LayerValue, RecordedValue, Schema,
    ValidationContext,
};
use tokio::{io::AsyncRead, sync::mpsc};

use crate::{
    background::read_next_frame,
    config_overrides,
    route_transport::{
        clear_route_epoch, handle_control_request_with_readiness, send_pong, send_route_error_body,
        RouteEpochs,
    },
    startup_settings::check_config_at,
    BrocaCredentials, ErrorBody, Frame, FrameType, ModuleError, SharedCatalogHealth,
    SharedQuotaCache,
};

struct Settings {
    module_id: String,
    path: PathBuf,
}

fn emitted_schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(&config_overrides::routing_config_schema())
            .expect("the routing settings schema is generated JSON")
    })
}

fn parse_layer_document(document: &str) -> Result<Value, String> {
    let json = subc_jsonc::jsonc_to_json(document)?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}

pub(super) fn validate_user_file(module_id: &str, document: Option<&str>) -> Result<(), String> {
    let user_layer = document
        .map(parse_layer_document)
        .transpose()?
        .map(|value| LayerValue::new(Layer::User, value));
    let tuple = Schema::new(emitted_schema().clone())
        .map_err(|error| error.to_string())?
        .review_tuple(
            user_layer.as_slice(),
            &ValidationContext::new(module_id, None),
        )
        .map_err(|error| error.to_string())?;
    // The managed view includes schema defaults, so check that routing can
    // also load the merged settings it would get from the daemon.
    let effective = serde_json::to_string(&tuple.effective).map_err(|error| error.to_string())?;
    config_overrides::validate_review_document(&effective).map_err(|(_, reason)| reason)
}

impl Settings {
    fn config_declaration(&self) -> Result<ConfigDeclareRequest, ModuleError> {
        let path = self.path.to_str().ok_or_else(|| {
            ModuleError::Message("the routing settings path is not valid UTF-8".into())
        })?;
        Ok(ConfigDeclareRequest::new(
            emitted_schema().clone(),
            path,
            vec![],
        ))
    }

    fn config_review(&self, request: ConfigReviewRequest) -> ConfigReviewReply {
        static SCHEMA: OnceLock<Result<Schema, ConfigReviewRefusal>> = OnceLock::new();
        match SCHEMA.get_or_init(|| {
            Schema::new(emitted_schema().clone()).map_err(|error| {
                ConfigReviewRefusal::new(
                    "schema_invalid",
                    vec![error.path.clone()],
                    vec![error.to_string()],
                )
            })
        }) {
            Ok(schema) => review(schema, &self.module_id, request),
            Err(error) => ConfigReviewReply::new(vec![], vec![error.clone()]),
        }
    }

    fn config_apply(&self, view: &ConfigView) -> Result<(), Vec<String>> {
        if view.project_id.is_some()
            || !matches!(
                view.status,
                ConfigStatus::Active | ConfigStatus::Pending | ConfigStatus::Unstamped
            )
            || view.layers.iter().any(|layer| {
                layer.module_id != self.module_id
                    || layer.layer.known_layer() != Some(Layer::User)
                    || layer.project_id.is_some()
                    || layer.workspace_id.is_some()
            })
        {
            return Err(vec![
                "config.get returned an invalid global routing settings view".into(),
            ]);
        }
        config_overrides::apply_managed(&self.path, &view.effective).map_err(|error| vec![error])
    }

    fn config_files(path: &Path) -> Result<(), ModuleError> {
        check_config_at(path)
    }
}

fn review(schema: &Schema, module_id: &str, request: ConfigReviewRequest) -> ConfigReviewReply {
    match review_layers(schema, module_id, request) {
        Ok(fields) => ConfigReviewReply::new(fields, vec![]),
        Err(refusal) => ConfigReviewReply::new(vec![], vec![refusal]),
    }
}

fn review_layers(
    schema: &Schema,
    module_id: &str,
    request: ConfigReviewRequest,
) -> Result<Vec<FieldChange>, ConfigReviewRefusal> {
    let mut layers = Vec::new();
    let mut raw_user = None;
    let project = request
        .layers
        .iter()
        .find_map(|layer| layer.project_id.clone());
    let invalid_document = |reason: String| {
        let mut refusal =
            ConfigReviewRefusal::new("value_invalid", vec![String::new()], vec![reason]);
        refusal.project_id.clone_from(&project);
        refusal
    };
    for snapshot in request.layers {
        let layer = snapshot
            .layer
            .known_layer()
            .filter(|layer| {
                let scope = snapshot.project_id.as_deref();
                snapshot.module_id == module_id
                    && scope == project.as_deref()
                    && match layer {
                        Layer::User => snapshot.workspace_id.is_none(),
                        Layer::Workspace => scope.is_some() && snapshot.workspace_id.is_some(),
                        Layer::Project => scope.is_some(),
                        _ => false,
                    }
            })
            .ok_or_else(|| {
                let mut refusal = ConfigReviewRefusal::new(
                    "layer_forbidden",
                    vec![String::new()],
                    vec!["routing settings snapshots disagree on module or project scope".into()],
                );
                refusal.project_id.clone_from(&project);
                refusal
            })?;
        if let Some(bytes) = snapshot.bytes {
            let document =
                String::from_utf8(bytes).map_err(|error| invalid_document(error.to_string()))?;
            let value = parse_layer_document(&document).map_err(invalid_document)?;
            layers.push(LayerValue::new(layer, value));
            if layer == Layer::User {
                raw_user = Some(document);
            }
        }
    }
    let tuple = schema
        .review_tuple(&layers, &ValidationContext::new(module_id, project.clone()))
        .map_err(|error| {
            let mut refusal = ConfigReviewRefusal::new(
                "value_invalid",
                vec![error.path.clone()],
                vec![error.to_string()],
            );
            refusal.project_id.clone_from(&project);
            refusal
        })?;
    let old = request.base.as_ref();
    // Use the SDK's field boundaries, including arrays that are replaced as a
    // unit. Include old source paths so removed fields can also be reported.
    let paths: BTreeSet<_> = tuple
        .sources
        .keys()
        .chain(old.into_iter().flat_map(|view| view.sources.keys()))
        .collect();
    let fields: Vec<_> = paths
        .into_iter()
        .filter_map(|path| {
            let before = old.and_then(|view| view.effective.pointer(path));
            let after = tuple.effective.pointer(path);
            let old_source = source_at(old.map(|view| &view.sources), path);
            let new_source = source_at(Some(&tuple.sources), path);
            if before == after && old_source == new_source {
                return None;
            }
            let mut field = FieldChange::new(
                module_id,
                path.as_str(),
                RecordedValue::new(before.cloned().unwrap_or(Value::Null)),
                RecordedValue::new(after.cloned().unwrap_or(Value::Null)),
                ApplyKind::Live,
            );
            field.project_id.clone_from(&project);
            field.old_source = old_source;
            field.new_source = new_source;
            Some(field)
        })
        .collect();
    // Runtime restrictions and enrollment are evaluated against the global
    // configuration. A project list cannot add enrollment or invalidate it.
    if project.is_some() {
        return Ok(fields);
    }
    let effective = serde_json::to_string(&tuple.effective)
        .map_err(|error| invalid_document(error.to_string()))?;
    // Test both forms: the original text can contain repeated keys that do
    // not survive decoding into the merge tuple, and the merged defaults can
    // interact with fields the operator supplied.
    for document in raw_user.as_deref().into_iter().chain([effective.as_str()]) {
        config_overrides::validate_review_document(document).map_err(|(mut paths, reason)| {
            if paths.is_empty() {
                paths.extend(
                    fields
                        .iter()
                        .filter(|field| {
                            field.new_source == Layer::User || field.old_source == Layer::User
                        })
                        .map(|field| field.path.clone()),
                );
            }
            ConfigReviewRefusal::new("value_invalid", paths, vec![reason])
        })?;
    }
    Ok(fields)
}

fn source_at(sources: Option<&BTreeMap<String, Layer>>, path: &str) -> Layer {
    sources
        .and_then(|sources| sources.get(path))
        .copied()
        .unwrap_or(Layer::Default)
}

type FrameRead<R> = (R, Result<Option<Frame>, ModuleError>);
type NextFrame<'a, R> = Pin<Box<dyn Future<Output = FrameRead<R>> + Send + 'a>>;

async fn read_owned<R: AsyncRead + Unpin>(mut reader: R) -> FrameRead<R> {
    let frame = read_next_frame(&mut reader).await;
    (reader, frame)
}

/// Keep the same partially read frame when settings arrive mid-read. Cancelling
/// a stream read after consuming its header would corrupt the next frame.
pub(super) async fn wait_until_ready<'a, R: AsyncRead + Unpin + Send + 'a>(
    session: &mut Session,
    mut reader: R,
    writer: &mpsc::Sender<Frame>,
    epochs: &mut RouteEpochs,
    catalog_health: &SharedCatalogHealth,
    quota_cache: &SharedQuotaCache,
) -> Result<Option<NextFrame<'a, R>>, ModuleError> {
    let credentials = Arc::new(Mutex::new(BrocaCredentials::default()));
    loop {
        let mut next = Box::pin(read_owned(reader));
        let (returned_reader, frame) = tokio::select! {
            biased;
            ready = session.ready() => {
                ready?;
                return Ok(Some(next));
            }
            read = &mut next => read,
        };
        reader = returned_reader;
        let Some(frame) = frame? else { return Ok(None) };
        if session.dispatch(&frame) {
            continue;
        }
        let header = frame.header;
        match header.ty {
            FrameType::Goodbye if header.channel == 0 => return Ok(None),
            FrameType::Goodbye => clear_route_epoch(epochs, header.channel, header.epoch),
            FrameType::Ping if header.channel == 0 => send_pong(writer, &frame).await?,
            FrameType::Request if header.channel == 0 => {
                handle_control_request_with_readiness(
                    frame,
                    writer,
                    epochs,
                    catalog_health,
                    &credentials,
                    quota_cache,
                    Some(WAITING),
                )
                .await?
            }
            FrameType::Request if epochs.get(&header.channel) == Some(&header.epoch) => {
                send_route_error_body(
                    writer,
                    header.ver,
                    header.channel,
                    header.epoch,
                    header.corr,
                    ErrorBody {
                        code: "routing_not_ready".into(),
                        message: format!("{WAITING}; retry shortly"),
                        detail: Some(
                            serde_json::json!({"retryable": true, "retry_after_ms": 1000}),
                        ),
                    },
                )
                .await?;
            }
            _ => {}
        }
    }
}
