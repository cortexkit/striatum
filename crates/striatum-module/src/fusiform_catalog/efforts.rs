use serde_json::Value;
use striatum_store::CatalogEfforts;

#[cfg(test)]
#[path = "efforts_tests.rs"]
mod tests;

pub(crate) const EFFORT_ORDER: [&str; 7] =
    ["none", "minimal", "low", "medium", "high", "xhigh", "max"];
pub(crate) const WORKLOADS: [&str; 5] = ["reason", "code", "synthesize", "lookup", "quick"];

pub(crate) fn rank(effort: &str) -> Option<usize> {
    EFFORT_ORDER.iter().position(|known| *known == effort)
}

pub(super) fn decode(
    route: &str,
    row: &Value,
    mut offered: Option<Vec<String>>,
) -> Option<CatalogEfforts> {
    // This filtered copy is the preview's own: a workload minimum can only be
    // compared against efforts with a known rank. Live selection keeps checking
    // a pinned effort against the full published list, unknown names included.
    if let Some(list) = &mut offered {
        list.retain(|effort| {
            if rank(effort).is_some() {
                return true;
            }
            tracing::warn!(route, effort, "ignoring unknown catalog reasoning option");
            false
        });
    }
    let valid = |key: &str, effort: &str| {
        let threshold = key.ends_with(".effort_minimum");
        if rank(effort).is_some()
            && (threshold
                || offered
                    .as_ref()
                    .is_some_and(|list| list.iter().any(|e| e == effort)))
        {
            return true;
        }
        tracing::warn!(
            route,
            key,
            effort,
            ?offered,
            "ignoring invalid catalog effort"
        );
        false
    };
    let read = |key: &str, value: &Value| {
        if value.is_null() {
            return None;
        }
        match value.as_str() {
            Some(effort) => valid(key, effort).then(|| effort.to_owned()),
            None => {
                tracing::warn!(route, key, %value, "ignoring non-string catalog effort");
                None
            }
        }
    };
    let mut facts = CatalogEfforts {
        graded: row
            .get("quality.graded_effort")
            .and_then(|v| read("quality.graded_effort", v)),
        vendor_default: row
            .get("capability.reasoning_default")
            .and_then(|v| read("capability.reasoning_default", v)),
        ..Default::default()
    };
    for (key, value) in row.as_object().into_iter().flatten() {
        for (prefix, grades) in [
            ("quality.elo.effort.", &mut facts.elo),
            ("quality.eq.effort.", &mut facts.eq),
        ] {
            if let Some(effort) = key.strip_prefix(prefix).filter(|effort| valid(key, effort)) {
                if let Some(grade) = value.as_f64() {
                    grades.insert(effort.to_owned(), grade);
                }
            }
        }
        if let Some((workload, field)) = key
            .strip_prefix("workload.")
            .and_then(|key| key.split_once('.'))
            .filter(|(workload, _)| WORKLOADS.contains(workload))
        {
            match field {
                "effort_default" => {
                    if let Some(default) = read(key, value) {
                        facts
                            .workloads
                            .entry(workload.to_owned())
                            .or_default()
                            .default = Some(default);
                    }
                }
                // A minimum is a threshold, not an assertion that the row offers it.
                "effort_minimum" => {
                    if let Some(minimum) = read(key, value) {
                        facts
                            .workloads
                            .entry(workload.to_owned())
                            .or_default()
                            .minimum = Some(minimum);
                    }
                }
                "excluded" if value.as_bool() == Some(true) => {
                    facts
                        .workloads
                        .entry(workload.to_owned())
                        .or_default()
                        .excluded = true;
                }
                _ => {}
            }
        }
    }
    facts.offered = offered;
    (facts != CatalogEfforts::default()).then_some(facts)
}
