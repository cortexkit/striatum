use std::collections::{BTreeSet, HashSet};
use std::sync::{Mutex, OnceLock};

use serde_json::Value;
use striatum_core::model_facts::{SpeedTwin, SpeedTwinMode};
use striatum_store::FusiformFactUpdate;

fn report_once(provider: &str, model: &str, reason: &str) {
    type Warning = (String, String, String);
    static WARNED: OnceLock<Mutex<HashSet<Warning>>> = OnceLock::new();
    if let Ok(mut warned) = WARNED.get_or_init(Default::default).lock() {
        if warned.insert((provider.to_owned(), model.to_owned(), reason.to_owned())) {
            eprintln!("[striatum] ignoring model.mode_of on {provider}/{model}: {reason}");
        }
    }
}

pub(super) fn decode(provider: &str, model: &str, entry: &Value) -> Option<SpeedTwin> {
    let mark = entry.get("model.mode_of").filter(|mark| !mark.is_null())?;
    let mode = mark.get("mode").and_then(Value::as_str);
    let Some(tier) = mode.and_then(SpeedTwinMode::parse) else {
        report_once(provider, model, &format!("unknown mode {mode:?}"));
        return None;
    };
    let Some(model_id) = mark
        .get("model_id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
    else {
        report_once(provider, model, "missing base model_id");
        return None;
    };
    Some(SpeedTwin {
        mode: tier,
        model_id: model_id.to_owned(),
    })
}

pub(super) fn validate_bases(facts: &mut [FusiformFactUpdate]) {
    if !facts.iter().any(|fact| fact.mode_of.is_some()) {
        return;
    }
    // Pages can separate a twin from its base. Validate against the complete
    // snapshot, and don't follow a chain of tier rows or a self-reference.
    let bases: BTreeSet<_> = facts
        .iter()
        .filter(|fact| fact.present && fact.mode_of.is_none())
        .map(|fact| (fact.provider_id.as_str(), fact.model_id.as_str()))
        .collect();
    let invalid: Vec<_> = facts
        .iter()
        .enumerate()
        .filter_map(|(index, fact)| {
            let twin = fact.mode_of.as_ref()?;
            (!bases.contains(&(fact.provider_id.as_str(), twin.model_id.as_str()))).then_some(index)
        })
        .collect();
    for index in invalid {
        let fact = &mut facts[index];
        if let Some(twin) = fact.mode_of.take() {
            report_once(
                &fact.provider_id,
                &fact.model_id,
                &format!("no base model {:?} on the same provider", twin.model_id),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::fusiform_catalog::{decode::CatalogPage, decode_catalog_get};

    #[test]
    fn speed_twin_catalog_guards_keep_invalid_marks_as_ordinary_rows() {
        let catalog = json!({"catalog_version":42, "resolved_at_ms":123, "models":{
            "p/base":{"existence":"present"},
            "other/remote":{"existence":"present"},
            "p/missing-twin":{"existence":"present", "model.mode_of":{"mode":"fast", "model_id":"missing"}},
            "p/cross-twin":{"existence":"present", "model.mode_of":{"mode":"fast", "model_id":"remote"}},
            "p/unknown-mode":{"existence":"present", "model.mode_of":{"mode":"standard", "model_id":"base"}},
            "p/ordinary-highspeed":{"existence":"present"}
        }});
        for _ in 0..2 {
            let snapshot = decode_catalog_get(catalog.to_string()).unwrap();
            let (facts, _) = snapshot.parts();
            assert_eq!(facts.len(), 6);
            assert!(facts
                .iter()
                .all(|fact| fact.present && fact.mode_of.is_none()));
        }
    }

    #[test]
    fn speed_twin_catalog_resolves_a_base_on_another_page() {
        let first = json!({"source":"models.dev", "catalog_version":43, "resolved_at_ms":123, "models":{
            "p/twin":{"existence":"present", "model.mode_of":{"mode":"ultrafast", "model_id":"base"}}
        }});
        let second = json!({"source":"models.dev", "catalog_version":43, "resolved_at_ms":123, "models":{
            "p/base":{"existence":"present"}
        }});
        let mut page = CatalogPage::read(first.to_string().as_bytes(), true).unwrap();
        page.append(CatalogPage::read(second.to_string().as_bytes(), true).unwrap())
            .unwrap();
        let snapshot = page.finish().unwrap();
        assert_eq!(
            snapshot.speed_twin_ids().collect::<Vec<_>>(),
            [("p", "twin")]
        );
        let twin = snapshot
            .parts()
            .0
            .iter()
            .find(|fact| fact.model_id == "twin")
            .unwrap();
        assert_eq!(
            twin.mode_of,
            Some(SpeedTwin {
                mode: SpeedTwinMode::Ultrafast,
                model_id: "base".into()
            })
        );
    }
}
