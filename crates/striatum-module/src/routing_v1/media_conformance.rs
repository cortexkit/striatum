//! Translate the kit's abstract media facts into catalog and policy inputs.

use super::*;

#[derive(Default)]
pub(super) struct Inputs {
    pub facts: cortexkit_role_routing_conformance::SelectionFacts,
    pub costs: Vec<cortexkit_role_routing_conformance::EngineFacts>,
    policies: std::collections::BTreeSet<String>,
}

impl Inputs {
    pub fn install(&mut self, h: &Harness) -> Result<(), String> {
        let mut kinds = std::collections::BTreeMap::new();
        for slot in super::super::consumers::engine_declarations().values() {
            for engine in &slot.engines {
                kinds.insert(
                    format!("{}/{}", engine.provider, engine.model),
                    engine.kind.as_str().to_owned(),
                );
            }
        }
        for cost in &self.costs {
            kinds.insert(
                format!("{}/{}", cost.provider, cost.model),
                cost.kind.as_str().to_owned(),
            );
        }
        for slot in super::super::consumers::search_declarations().values() {
            for route in &slot.routes {
                kinds.insert(
                    format!("{}/{}", route.provider, route.model),
                    "search".into(),
                );
            }
        }
        let mut models: serde_json::Map<String, Value> = h
            .store()
            .get_facts()
            .map_err(|error| error.to_string())?
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    json!({"existence":"present","capability.tool_call":true}),
                )
            })
            .collect();
        for (key, kind) in &kinds {
            let row = models
                .entry(key.clone())
                .or_insert_with(|| json!({"existence":"present"}));
            row["model.type"] = json!(match kind.as_str() {
                "transcribe" | "stream_transcribe" => "transcription",
                "synthesize" => "speech",
                kind => kind,
            });
        }
        for (key, fact) in &self.facts.media {
            let row = models
                .entry(key.clone())
                .or_insert_with(|| json!({"existence":"present"}));
            if let Some(grade) = fact.grade {
                row["quality.grade"] = json!(grade);
            }
            if let Some(limit) = fact.max_output_duration_ms {
                row["limit.output_duration_ms"] = json!(limit);
            }
            if !fact.aspect_ratios.is_empty() {
                row["capability.aspect_ratios"] = json!(fact.aspect_ratios);
            }
            if let Some(audio) = fact.audio {
                row["capability.output_audio"] = json!(audio);
            }
            if let Some(price) = &fact.price {
                for (key, rate) in &price.rates {
                    // The kit names the media wire's role/unit pair. Fusiform
                    // names input audio as a rate role with seconds as its unit.
                    let key = match key.as_str() {
                        "input.per_audio_second" => "rate.input_audio.per_second".to_owned(),
                        key => format!("rate.{key}"),
                    };
                    row[&key] = catalog_rate(&rate.rate)?;
                    // The wire's absent minimum is zero, not an unknown
                    // catalog minimum that would disable equal-rate ties.
                    if rate.minimum.is_none() {
                        row[&key]["minimum_quantity"] = json!({"kind":"none"});
                    }
                    if let Some(quantity) =
                        rate.minimum.as_ref().and_then(serde_json::Number::as_u64)
                    {
                        row[&key]["minimum_quantity"] = if quantity == 0 {
                            json!({"kind":"none"})
                        } else {
                            json!({"kind":"minimum","quantity":quantity})
                        };
                    }
                    if let Some(quantity) = rate.step.as_ref().and_then(serde_json::Number::as_u64)
                    {
                        row[&key]["increment"] = json!({"kind":"quantum","quantity":quantity});
                    }
                }
            }
        }
        for fact in &self.costs {
            let key = format!("{}/{}", fact.provider, fact.model);
            let Some(cost) = fact.cost else {
                continue;
            };
            let rate = match fact.kind.as_str() {
                "transcribe" | "stream_transcribe" => "rate.input_audio.per_second",
                "synthesize" => "rate.input.per_character",
                _ => continue,
            };
            let row = models
                .entry(key)
                .or_insert_with(|| json!({"existence":"present"}));
            if row.get(rate).is_none() {
                row[rate] = catalog_rate(&cost.into())?;
            }
        }
        for (key, fact) in &self.facts.search {
            let row = models
                .entry(key.clone())
                .or_insert_with(|| json!({"existence":"present"}));
            row["model.type"] = json!("search");
            if let Some(grade) = fact.grade {
                row["quality.grade"] = json!(grade);
            }
            if let Some(price) = &fact.price {
                for (key, rate) in &price.rates {
                    row[&format!("rate.{key}")] = catalog_rate(&rate.rate)?;
                }
            }
        }
        let snapshot = crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version":1,"resolved_at_ms":1,
                "quality_scores":{"graded_by":"operator","graded_at_ms":1},"models":models
            }))
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        h.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
        h.edit_config(|config| {
            for key in &self.policies {
                if let Some(model) = config["models"][key].as_object_mut() {
                    model.remove("not_for");
                }
            }
            for (key, fact) in &self.facts.media {
                if !fact.not_for.is_empty() {
                    config["models"][key]["not_for"] = json!(fact.not_for);
                }
            }
            for (key, fact) in &self.facts.search {
                if !fact.not_for.is_empty() {
                    config["models"][key]["not_for"] = json!(fact.not_for);
                }
            }
        });
        self.policies = self
            .facts
            .media
            .iter()
            .filter(|(_, fact)| !fact.not_for.is_empty())
            .map(|(key, _)| key.clone())
            .chain(
                self.facts
                    .search
                    .iter()
                    .filter(|(_, fact)| !fact.not_for.is_empty())
                    .map(|(key, _)| key.clone()),
            )
            .collect();
        Ok(())
    }
}

fn catalog_rate(rate: &serde_json::Number) -> Result<Value, String> {
    // Preserve decimal fixture rates exactly as the catalog's mantissa/exponent
    // pair, rather than rounding them through a floating-point scaling factor.
    let text = rate.to_string();
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map_or((text.as_str(), 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap())
        });
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len() as i32);
    let units = mantissa
        .replace('.', "")
        .parse::<i64>()
        .map_err(|error| error.to_string())?;
    Ok(
        json!({"state":"priced","currency":"USD","units":units,"exponent":exponent-fraction,"per":1}),
    )
}
