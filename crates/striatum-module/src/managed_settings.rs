//! Routing's settings hooks and the channel-0 session that drives them.

mod session;
#[cfg(test)]
mod tests;
pub(super) use session::{Session, WAITING};

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
    fn invalid_document(reason: impl Into<String>) -> ConfigReviewRefusal {
        ConfigReviewRefusal::new("value_invalid", vec![String::new()], vec![reason.into()])
    }
    let mut layers = Vec::new();
    let mut raw_user = None;
    for snapshot in request.layers {
        if snapshot.module_id != module_id
            || snapshot.layer.known_layer() != Some(Layer::User)
            || snapshot.project_id.is_some()
            || snapshot.workspace_id.is_some()
        {
            return Err(ConfigReviewRefusal::new(
                "layer_forbidden",
                vec![String::new()],
                vec!["routing only accepts a global user layer".into()],
            ));
        }
        if let Some(bytes) = snapshot.bytes {
            let document =
                String::from_utf8(bytes).map_err(|error| invalid_document(error.to_string()))?;
            let json = subc_jsonc::jsonc_to_json(&document).map_err(invalid_document)?;
            let value =
                serde_json::from_str(&json).map_err(|error| invalid_document(error.to_string()))?;
            layers.push(LayerValue::new(Layer::User, value));
            raw_user = Some(document);
        }
    }
    let tuple = schema
        .review_tuple(&layers, &ValidationContext::new(module_id, None))
        .map_err(|error| {
            ConfigReviewRefusal::new(
                "value_invalid",
                vec![error.path.clone()],
                vec![error.to_string()],
            )
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
            field.old_source = old_source;
            field.new_source = new_source;
            Some(field)
        })
        .collect();
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
