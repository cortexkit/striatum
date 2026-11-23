//! Decode each model independently, releasing its JSON tree before reading the
//! next model. Only the finished, immutable catalog crosses publication boundaries.

use super::*;
use serde::de::{IgnoredAny, SeqAccess, Visitor};

// RawValue borrows the payload from the wire buffer; it never copies the body or
// constructs a JSON tree just to remove the tool response's {"result": ...} envelope.
type Fields<'a> = BTreeMap<String, &'a serde_json::value::RawValue>;

fn payload(bytes: &[u8]) -> Result<(&[u8], Fields<'_>), FusiformCatalogError> {
    let fields: Fields<'_> = serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)?;
    if fields.len() == 1 {
        if let Some(result) = fields.get("result") {
            let bytes = result.get().as_bytes();
            let fields = serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)?;
            return Ok((bytes, fields));
        }
    }
    Ok((bytes, fields))
}

pub(super) fn tool_reply<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, FusiformCatalogError> {
    let (bytes, _) = payload(bytes)?;
    serde_json::from_slice(bytes).map_err(FusiformCatalogError::Json)
}

fn field<T: serde::de::DeserializeOwned>(
    fields: &Fields<'_>,
    name: &str,
) -> Result<T, FusiformCatalogError> {
    let raw = fields
        .get(name)
        .ok_or_else(|| error(format!("missing field `{name}`")))?;
    serde_json::from_str(raw.get()).map_err(FusiformCatalogError::Json)
}

fn optional<T: serde::de::DeserializeOwned + Default>(
    fields: &Fields<'_>,
    name: &str,
) -> Result<T, FusiformCatalogError> {
    if fields.contains_key(name) {
        field(fields, name)
    } else {
        Ok(T::default())
    }
}

pub(super) fn refusal(bytes: &[u8], error_frame: bool) -> Option<(String, String)> {
    let (_, fields) = payload(bytes).ok()?;
    let nested;
    let refusal = if let Some(error) = fields.get("error") {
        nested = serde_json::from_str::<Fields<'_>>(error.get()).ok()?;
        &nested
    } else if error_frame {
        &fields
    } else {
        return None;
    };
    Some((
        field(refusal, "code").ok()?,
        field(refusal, "message").unwrap_or_default(),
    ))
}

pub(super) fn status(bytes: &[u8]) -> Result<CatalogStatusReply, FusiformCatalogError> {
    let (_, fields) = payload(bytes)?;
    Ok(CatalogStatusReply {
        catalog_version: field(&fields, "catalog_version")?,
    })
}

#[derive(Debug)]
pub(super) struct CatalogPage {
    source: Option<String>,
    catalog_version: i64,
    resolved_at_ms: i64,
    models: ModelRows,
    pub(super) next_page_token: Option<String>,
    curated: Vec<CatalogCuratedModel>,
    login_methods: Vec<CatalogCuratedModel>,
    quality_scores: Option<QualityScores>,
}

// These disclosures are validated on every page, but routing does not use them.
// Do not keep their bodies around while the following pages are fetched.
#[derive(Debug, Default)]
struct Disclosures;

impl<'de> Deserialize<'de> for Disclosures {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ArrayVisitor;
        impl<'de> Visitor<'de> for ArrayVisitor {
            type Value = Disclosures;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an array of catalog disclosures")
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Disclosures)
            }
        }
        deserializer.deserialize_seq(ArrayVisitor)
    }
}

#[derive(Debug, Default)]
struct ModelRows(BTreeMap<String, Box<ModelRow>>);

impl<'de> Deserialize<'de> for ModelRows {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Borrowing the row bodies keeps last-wins duplicate keys and the same
        // sorted diagnostic order as the former Value map. Only one row's JSON
        // tree is materialized, then released, while the typed rows accumulate.
        let raw = Fields::deserialize(deserializer)?;
        let mut rows = BTreeMap::new();
        for (key, value) in raw {
            let entry = serde_json::from_str(value.get()).map_err(serde::de::Error::custom)?;
            let row = Box::new(ModelRow::decode(&key, &entry));
            rows.insert(key, row);
        }
        Ok(ModelRows(rows))
    }
}

#[derive(Debug)]
struct ModelRow {
    decoded: Result<(FusiformFactUpdate, ModelRates), String>,
    decommissioned: Option<String>,
    grade: Option<striatum_store::OperatorQuality>,
    media_grade: MediaGrade,
    video: Option<VideoFacts>,
    media_rates: BTreeMap<String, MediaRate>,
    audio: Option<AudioInputPrice>,
    kind: Option<String>,
    wire_api: bool,
    wire_family: Option<String>,
}

impl ModelRow {
    fn decode(key: &str, entry: &Value) -> Self {
        let canonical = || {
            entry
                .get("model.canonical_id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .unwrap_or(key)
                .to_owned()
        };
        let decoded = match key
            .split_once('/')
            .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        {
            Some((provider, model)) => decode_model_entry(key, provider, model, entry, None),
            None => Err("key is not provider/model".to_string()),
        };
        let readable = decoded.is_ok();
        Self {
            decoded,
            decommissioned: (entry.get("quality.decommissioned").and_then(Value::as_bool)
                == Some(true))
            .then(canonical),
            grade: entry
                .get("quality.elo")
                .and_then(Value::as_f64)
                .zip(entry.get("quality.eq").and_then(Value::as_f64))
                .map(|(elo, eq)| striatum_store::OperatorQuality {
                    model: canonical(),
                    elo,
                    eq,
                }),
            media_grade: media_grades::catalog_row(entry),
            video: (readable && entry.get("model.type").and_then(Value::as_str) == Some("video"))
                .then(|| VideoFacts::decode(key, entry)),
            media_rates: if readable {
                entry
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter(|(key, _)| key.starts_with("rate."))
                    .map(|(rate, value)| (rate.clone(), MediaRate::decode(key, rate, value)))
                    .collect()
            } else {
                BTreeMap::new()
            },
            audio: readable.then(|| AudioInputPrice::decode(key, entry)),
            kind: readable
                .then(|| {
                    entry
                        .get("model.kind")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten(),
            wire_api: readable && row_serves_wire_api(entry),
            wire_family: readable
                .then(|| {
                    entry
                        .get("wire.family")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten(),
        }
    }
}

impl CatalogPage {
    pub(super) fn read(bytes: &[u8], paged: bool) -> Result<Self, FusiformCatalogError> {
        // The old Value map accepted repeated top-level keys with last-wins
        // semantics. Borrowing raw fields preserves that without a document tree.
        let (_, fields) = payload(bytes)?;
        let source = if paged {
            Some(field(&fields, "source")?)
        } else {
            None
        };
        let next_page_token = if paged {
            for name in ["withheld", "uncertain", "aliased"] {
                let _: Disclosures = optional(&fields, name)?;
            }
            optional(&fields, "next_page_token")?
        } else {
            None
        };
        let catalog_version = field(&fields, "catalog_version")?;
        let resolved_at_ms = field(&fields, "resolved_at_ms")?;
        let curated = optional(&fields, "curated")?;
        let login_methods = optional(&fields, "login_methods")?;
        let quality_scores = optional(&fields, "quality_scores")?;
        let models = field(&fields, "models")?;
        Ok(Self {
            source,
            catalog_version,
            resolved_at_ms,
            models,
            next_page_token,
            curated,
            login_methods,
            quality_scores,
        })
    }

    pub(super) fn append(&mut self, mut page: Self) -> Result<(), FusiformCatalogError> {
        if self.source != page.source
            || self.catalog_version != page.catalog_version
            || self.resolved_at_ms != page.resolved_at_ms
            || matches!((&self.quality_scores, &page.quality_scores),
                (Some(first), Some(next)) if first.graded_by != next.graded_by)
        {
            return Err(error("Fusiform catalog page identity changed during read"));
        }
        if page
            .models
            .0
            .keys()
            .any(|key| self.models.0.contains_key(key))
        {
            return Err(error("Fusiform catalog pages repeated a model"));
        }
        match (&mut self.quality_scores, page.quality_scores.take()) {
            (Some(first), Some(next)) => {
                first.graded_at_ms = first.graded_at_ms.max(next.graded_at_ms)
            }
            (scores @ None, Some(next)) => *scores = Some(next),
            _ => {}
        }
        self.models.0.append(&mut page.models.0);
        self.curated.append(&mut page.curated);
        self.login_methods.append(&mut page.login_methods);
        self.next_page_token = page.next_page_token;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Arc<CatalogSnapshot>, FusiformCatalogError> {
        let documented: BTreeSet<_> = self
            .login_methods
            .iter()
            .map(|row| row.model.clone())
            .collect();
        let mut auth_methods = BTreeMap::new();
        for row in self.curated.into_iter().chain(self.login_methods) {
            let mut methods = row.auth_methods;
            methods.sort();
            methods.dedup();
            if auth_methods.insert(row.model.clone(), methods).is_some() {
                return Err(error(format!(
                    "duplicate Fusiform auth-method model: {}",
                    row.model
                )));
            }
        }
        let mut snapshot = CatalogSnapshot {
            cursor: FusiformCatalogCursor {
                catalog_version: self.catalog_version,
                resolved_at_ms: self.resolved_at_ms,
            },
            facts: Vec::with_capacity(self.models.0.len()),
            rates: BTreeMap::new(),
            wire_apis: BTreeMap::new(),
            wire_api_models: BTreeSet::new(),
            model_kinds: BTreeMap::new(),
            audio_input_prices: BTreeMap::new(),
            media_grades: BTreeMap::new(),
            local_grades: Default::default(),
            media_rates: BTreeMap::new(),
            video_facts: BTreeMap::new(),
            quality_scores: self.quality_scores,
            grades: BTreeMap::new(),
            decommissioned: BTreeMap::new(),
            retained: BTreeSet::new(),
        };
        let operator = snapshot
            .quality_scores
            .as_ref()
            .is_some_and(|scores| scores.graded_by == "operator");
        let mut unreadable = Vec::new();
        for (key, row) in self.models.0 {
            let row = *row;
            if let Some(model) = row.decommissioned {
                snapshot.decommissioned.insert(key.clone(), model);
            }
            if operator {
                if let Some(grade) = row.grade {
                    snapshot.grades.insert(key.clone(), grade);
                }
                snapshot.media_grades.insert(key.clone(), row.media_grade);
            }
            match row.decoded {
                Ok((mut fact, rates)) => {
                    fact.auth_methods = auth_methods.remove(&key);
                    fact.login_methods_documented = documented.contains(&key);
                    snapshot.facts.push(fact);
                    snapshot.rates.insert(key.clone(), Arc::new(rates));
                    snapshot.media_rates.insert(key.clone(), row.media_rates);
                    if let Some(audio) = row.audio {
                        snapshot.audio_input_prices.insert(key.clone(), audio);
                    }
                    if let Some(video) = row.video {
                        snapshot.video_facts.insert(key.clone(), video);
                    }
                    if let Some(kind) = row.kind {
                        snapshot.model_kinds.insert(key.clone(), kind);
                    }
                    if row.wire_api {
                        snapshot.wire_api_models.insert(key.clone());
                    }
                    if let Some(family) = row.wire_family {
                        snapshot.wire_apis.insert(key, family);
                    }
                }
                Err(reason) => {
                    if let Some((provider, model)) = key
                        .split_once('/')
                        .filter(|(p, m)| !p.is_empty() && !m.is_empty())
                    {
                        snapshot
                            .retained
                            .insert((provider.to_owned(), model.to_owned()));
                    }
                    unreadable.push((key, reason));
                }
            }
        }
        report_unreadable_entries_once(self.catalog_version, &unreadable);
        super::speed_twins::validate_bases(&mut snapshot.facts);
        Ok(Arc::new(snapshot))
    }
}
