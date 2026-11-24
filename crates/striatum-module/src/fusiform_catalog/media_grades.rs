use super::{
    CatalogSnapshot, CatalogWire, FusiformCatalogConsumer, FusiformCatalogError, MediaGrade, Value,
};
use std::collections::BTreeMap;
use std::{
    sync::{Arc, Once},
    time::Duration,
};
use tokio::time::Instant;

static READ_WARNING: Once = Once::new();
static UNSUPPORTED_WARNING: Once = Once::new();
const RETRY_AFTER: Duration = Duration::from_secs(30);

#[cfg(test)]
tokio::task_local! {
    static CALLS: Arc<std::sync::atomic::AtomicUsize>;
}

#[cfg(test)]
pub(super) fn count_call() {
    let _ = CALLS.try_with(|calls| calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
}

#[cfg(test)]
pub(crate) async fn count_calls<T>(future: impl std::future::Future<Output = T>) -> (T, usize) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = CALLS.scope(calls.clone(), future).await;
    (result, calls.load(std::sync::atomic::Ordering::SeqCst))
}

#[cfg(test)]
pub(crate) fn inherit_calls<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let calls = CALLS.try_with(Clone::clone).ok();
    async move {
        match calls {
            Some(calls) => CALLS.scope(calls, future).await,
            None => future.await,
        }
    }
}

#[derive(serde::Deserialize)]
struct Reply {
    grades: Vec<Row>,
}

#[derive(serde::Deserialize)]
struct Row {
    model: String,
    kind: String,
    #[serde(default)]
    grade: Value,
    graded_by: String,
}

fn decode(bytes: &[u8]) -> Result<LocalGrades, FusiformCatalogError> {
    if let Some((code, message)) = super::decode::refusal(bytes, true) {
        let reason = FusiformCatalogError::Refused { code, message };
        return if unsupported_prefix(&reason) {
            Ok(LocalGrades::unsupported_prefix())
        } else {
            Err(reason)
        };
    }
    let reply: Reply = serde_json::from_value(super::unwrap_tool_result(bytes)?)
        .map_err(FusiformCatalogError::Json)?;
    let mut result = LocalGrades::default();
    for row in reply.grades {
        if row.graded_by != "operator"
            || !KINDS.contains(&row.kind.as_str())
            || !row.model.starts_with("local:")
        {
            continue;
        }
        if let Some(grade) = row
            .grade
            .as_f64()
            .filter(|grade| grade.is_finite() && *grade > 0.0)
        {
            result
                .values
                .entry(row.model)
                .or_default()
                .insert(row.kind, grade);
        }
    }
    Ok(result)
}

fn unsupported_prefix(reason: &FusiformCatalogError) -> bool {
    // This reader sends only prefix, never model. A schema refusal therefore
    // means the producer cannot serve the prefix query; serde's text is unstable.
    matches!(reason, FusiformCatalogError::Refused { code, .. } if code == "bad_request")
}

impl FusiformCatalogConsumer {
    pub(super) async fn read_local_grades(&mut self) -> Result<LocalGrades, FusiformCatalogError> {
        let body = serde_json::to_vec(
            &serde_json::json!({"name":"quality.grades", "arguments":{"prefix":"local:"}}),
        )
        .map_err(FusiformCatalogError::Json)?;
        match self.call(body).await {
            Ok(reply) => decode(&reply),
            Err(reason) if unsupported_prefix(&reason) => Ok(LocalGrades::unsupported_prefix()),
            Err(reason) => Err(reason),
        }
    }
}

pub(super) async fn refresh_snapshot<C: CatalogWire>(
    client: &mut C,
) -> Result<Arc<CatalogSnapshot>, FusiformCatalogError> {
    let mut snapshot = client.catalog_get().await?;
    let previous = client.credentials().and_then(|credentials| {
        credentials
            .lock()
            .ok()?
            .catalog_snapshot
            .as_ref()
            .map(|snapshot| {
                (
                    snapshot.cursor.catalog_version,
                    snapshot.local_grades.clone(),
                )
            })
    });
    let unsupported_this_version = previous.as_ref().is_some_and(|(version, grades)| {
        *version == snapshot.cursor.catalog_version && grades.unsupported_prefix
    });
    let previous = previous.map(|(_, grades)| grades).unwrap_or_default();
    let grades = if unsupported_this_version {
        previous
    } else {
        refresh(client, previous).await
    };
    // Publish catalog rows and caller-local grades together so selection never
    // has to fetch grades or combine separately updated caches.
    Arc::make_mut(&mut snapshot).local_grades = grades;
    Ok(snapshot)
}

async fn refresh<C: CatalogWire>(client: &mut C, mut previous: LocalGrades) -> LocalGrades {
    match client.local_grades().await {
        Ok(grades) => {
            if grades.unsupported_prefix {
                UNSUPPORTED_WARNING.call_once(|| eprintln!("[striatum] Fusiform quality.grades does not accept prefix; caller-local grades are unknown"));
            }
            grades
        }
        Err(reason) => {
            READ_WARNING.call_once(|| eprintln!("[striatum] Fusiform local grades unavailable; keeping the last good grades: {reason}"));
            previous.unsupported_prefix = false;
            previous.retry_at = Some(Instant::now() + RETRY_AFTER);
            previous
        }
    }
}

pub(super) async fn retry_if_due<C: CatalogWire>(
    client: &mut C,
    credentials: Option<&crate::SharedBrocaCredentials>,
) -> Result<(), FusiformCatalogError> {
    let Some(credentials) = credentials else {
        return Ok(());
    };
    let snapshot = credentials
        .lock()
        .map_err(|_| super::error("credential cache poisoned"))?
        .catalog_snapshot
        .as_ref()
        .filter(|snapshot| {
            snapshot
                .local_grades
                .retry_at
                .is_some_and(|at| at <= Instant::now())
        })
        .cloned();
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let grades = refresh(client, snapshot.local_grades.clone()).await;
    let mut updated = (*snapshot).clone();
    updated.local_grades = grades;
    let mut credentials = credentials
        .lock()
        .map_err(|_| super::error("credential cache poisoned"))?;
    // An on-demand catalog apply may have published a different snapshot during the read.
    if credentials
        .catalog_snapshot
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &snapshot))
    {
        credentials.catalog_snapshot = Some(Arc::new(updated));
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LocalGrades {
    values: BTreeMap<String, MediaGrade>,
    unsupported_prefix: bool,
    pub(super) retry_at: Option<Instant>,
}

impl LocalGrades {
    pub(super) fn unsupported_prefix() -> Self {
        Self {
            unsupported_prefix: true,
            ..Default::default()
        }
    }

    #[cfg(test)]
    pub(super) fn from_test_grade(route: &str, kind: &str, grade: f64) -> Self {
        Self {
            values: BTreeMap::from([(route.into(), BTreeMap::from([(kind.into(), grade)]))]),
            ..Default::default()
        }
    }

    #[cfg(test)]
    pub(super) fn from_test_entries(entries: &[(&str, &str, f64)]) -> Self {
        let mut result = Self::default();
        for (route, kind, grade) in entries {
            result
                .values
                .entry((*route).into())
                .or_default()
                .insert((*kind).into(), *grade);
        }
        result
    }
}

impl CatalogSnapshot {
    pub(crate) fn local_media_grade(&self, kind: &str, route: &str) -> Option<f64> {
        self.local_grades
            .values
            .get(&format!("local:{route}"))?
            .get(grade_kind(kind)?)
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn with_local_media_grade(mut self, route: &str, kind: &str, grade: f64) -> Self {
        self.local_grades
            .values
            .entry(route.to_owned())
            .or_default()
            .insert(kind.to_owned(), grade);
        self
    }
}

const KINDS: [&str; 4] = ["transcription", "speech", "image", "video"];

pub(super) fn grade_kind(kind: &str) -> Option<&str> {
    match kind {
        "transcribe" | "stream_transcribe" => Some("transcription"),
        "synthesize" => Some("speech"),
        "image" | "video" | "decision" => Some(kind),
        _ => None,
    }
}

pub(super) fn catalog_row(entry: &Value) -> MediaGrade {
    let decision_kind = matches!(
        entry["model.kind"].as_str(),
        Some("decision" | "classifier")
    )
    .then_some("decision");
    let mut grades: MediaGrade = KINDS
        .into_iter()
        .chain(decision_kind)
        .filter_map(|kind| {
            entry
                .get(format!("quality.grade.{kind}"))
                .and_then(Value::as_f64)
                .filter(|grade| grade.is_finite() && *grade > 0.0)
                .map(|grade| (kind.to_owned(), grade))
        })
        .collect();
    if let Some(grade) = search_only_grade(entry) {
        grades.insert("search".into(), grade);
    }
    grades
}

fn search_only_grade(entry: &Value) -> Option<f64> {
    // Search grades belong only to search rows, unlike cross-type media grades.
    (entry["model.type"].as_str() == Some("search"))
        .then(|| {
            entry["quality.grade.search"]
                .as_f64()
                .filter(|grade| grade.is_finite() && *grade > 0.0)
        })
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn catalog_fixture_port_changes_only_the_media_grade_key() {
        use sha2::{Digest, Sha256};
        let bytes = include_str!("../../fixtures/served-payloads.json");
        assert_eq!(
            bytes
                .matches("\"quality.grade.transcription\": 87.500")
                .count(),
            1
        );
        let original = bytes.replace(
            "\"quality.grade.transcription\": 87.500",
            "\"quality.grade\": 87.500",
        );
        // Reversing the key rename restores every byte, including numeric
        // spelling, in the original producer capture.
        assert_eq!(
            format!("{:x}", Sha256::digest(original.as_bytes())),
            "afeac184c2481f67afe3c7dee03b5a96c069cfa86ffd7fb037f9674c5de4aa46"
        );
    }

    #[test]
    fn search_grade_is_per_kind_operator_only_and_search_row_only() {
        for (model_type, key, graded_by, grade, expected) in [
            (
                "search",
                "quality.grade.search",
                "operator",
                json!(91),
                Some(91.0),
            ),
            ("chat", "quality.grade.search", "operator", json!(91), None),
            ("search", "quality.grade", "operator", json!(91), None),
            ("search", "quality.grade.search", "creator", json!(91), None),
            ("search", "quality.grade.search", "operator", json!(0), None),
            (
                "search",
                "quality.grade.search",
                "operator",
                json!(-1),
                None,
            ),
            (
                "search",
                "quality.grade.search",
                "operator",
                json!("Infinity"),
                None,
            ),
        ] {
            let snapshot = snapshot(
                json!({"existence":"present", "model.type":model_type, key: grade}),
                graded_by,
            );
            assert_eq!(
                snapshot.media_grade("search", "google/gemini-4-hydrogen"),
                expected
            );
            assert_eq!(
                snapshot.media_grade("transcribe", "google/gemini-4-hydrogen"),
                None
            );
        }
    }

    #[test]
    fn decision_grade_is_operator_only_and_requires_a_decision_or_classifier_row() {
        for (kind, graded_by, grade, expected) in [
            ("chat", "operator", json!(91), None),
            ("decision", "operator", json!(91), Some(91.0)),
            ("classifier", "operator", json!(83), Some(83.0)),
            ("decision", "creator", json!(91), None),
            ("decision", "operator", json!(0), None),
        ] {
            let snapshot = snapshot(
                json!({"existence":"present", "model.kind":kind, "quality.grade.decision":grade}),
                graded_by,
            );
            assert_eq!(
                snapshot.media_grade("decision", "google/gemini-4-hydrogen"),
                expected,
                "{kind} / {graded_by}"
            );
        }
    }

    #[test]
    fn local_grade_serializer_samples_decode_and_remain_byte_pinned() {
        let samples = [
            (
                include_bytes!("../../fixtures/quality-grades-prefix-v1/local-empty.json")
                    .as_slice(),
                "40df85311d94ce03d87573c79d993e2c933ab960dd5a4645d18ced3a31c3168a",
            ),
            (
                include_bytes!("../../fixtures/quality-grades-prefix-v1/local-two-entries.json")
                    .as_slice(),
                "3de8894c93f50f93983f36ef9a60048f95d4fca4b37b8edc487b158d90ecc898",
            ),
            (
                include_bytes!(
                    "../../fixtures/quality-grades-prefix-v1/prefix-and-model-refused.json"
                )
                .as_slice(),
                "fc534fd9411c447af4e7ae8a889066ab1ab4736f98e1d037e39d6fe461f6983e",
            ),
            (
                include_bytes!(
                    "../../fixtures/quality-grades-prefix-v1/prefix-unsupported-older.json"
                )
                .as_slice(),
                "c6a66747e16f7548fda8da1d9f939abd119cabae672265526e116ee025a44185",
            ),
        ];
        use sha2::{Digest, Sha256};
        for (index, (bytes, digest)) in samples.into_iter().enumerate() {
            assert_eq!(format!("{:x}", Sha256::digest(bytes)), digest);
            let sample: Value = serde_json::from_slice(bytes).unwrap();
            let payload = match index {
                0 | 1 => &sample["response"],
                2 => &sample["error"],
                _ => &sample["refusal"],
            };
            let decoded = super::decode(&serde_json::to_vec(payload).unwrap()).unwrap();
            match index {
                0 => assert_eq!(decoded, Default::default()),
                1 => assert_eq!(
                    decoded,
                    super::LocalGrades::from_test_entries(&[
                        ("local:apple/phone-transcriber", "transcription", 59.0),
                        ("local:example/offline-speaker", "speech", 72.5),
                    ])
                ),
                _ => {
                    assert_eq!(payload["code"], "bad_request");
                    assert_eq!(decoded, super::LocalGrades::unsupported_prefix());
                }
            }
        }
    }

    #[test]
    fn local_grade_decoder_enforces_operator_kind_and_positive_number_checks() {
        for grade in [
            Value::Null,
            json!(0),
            json!(-1),
            json!("NaN"),
            json!("Infinity"),
        ] {
            let decoded = super::decode(&serde_json::to_vec(&json!({"grades":[{
                "model":"local:apple/phone-transcriber", "kind":"transcription", "grade":grade, "graded_by":"operator"
            }]})).unwrap()).unwrap();
            assert_eq!(decoded, Default::default());
        }
        for (kind, by, model) in [
            ("speech", "creator", "local:apple/m"),
            ("realtime", "operator", "local:apple/m"),
            ("transcription", "operator", "apple/m"),
        ] {
            let decoded = super::decode(
                &serde_json::to_vec(&json!({"grades":[{
                    "model":model, "kind":kind, "grade":59, "graded_by":by
                }]}))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(decoded, Default::default());
        }
    }

    fn snapshot(row: Value, graded_by: &str) -> Arc<CatalogSnapshot> {
        decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version": 1,
                "resolved_at_ms": 1,
                "quality_scores": {"graded_by": graded_by, "graded_at_ms": 1},
                "models": {"google/gemini-4-hydrogen": row}
            }))
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn chat_row_transcription_grade_is_independent_of_row_type() {
        let snapshot = snapshot(
            json!({
                "existence": "present", "model.type": "chat",
                "quality.grade.transcription": 97
            }),
            "operator",
        );
        assert_eq!(
            snapshot.media_grade("transcribe", "google/gemini-4-hydrogen"),
            Some(97.0)
        );
        assert_eq!(
            snapshot.media_grade("synthesize", "google/gemini-4-hydrogen"),
            None
        );
    }

    #[test]
    fn speech_and_non_operator_grades_do_not_grade_transcription() {
        let speech = snapshot(
            json!({
                "existence": "present", "model.type": "transcription", "quality.grade.speech": 97
            }),
            "operator",
        );
        assert_eq!(
            speech.media_grade("transcribe", "google/gemini-4-hydrogen"),
            None
        );
        let non_operator = snapshot(
            json!({
                "existence": "present", "quality.grade.transcription": 97
            }),
            "creator",
        );
        assert_eq!(
            non_operator.media_grade("transcribe", "google/gemini-4-hydrogen"),
            None
        );
    }

    #[test]
    fn bare_and_non_media_kind_grades_are_retired() {
        let snapshot = snapshot(
            json!({
                "existence": "present", "model.type": "transcription", "quality.grade": 97,
                "quality.grade.realtime": 98, "quality.grade.search": 99
            }),
            "operator",
        );
        for kind in [
            "transcribe",
            "stream_transcribe",
            "synthesize",
            "image",
            "video",
            "realtime",
            "search",
        ] {
            assert_eq!(
                snapshot.media_grade(kind, "google/gemini-4-hydrogen"),
                None,
                "{kind}"
            );
        }
    }
}
