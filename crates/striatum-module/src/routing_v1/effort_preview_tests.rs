use super::*;
use crate::fusiform_catalog::efforts::EFFORT_ORDER;
use striatum_store::CatalogWorkload;

#[test]
fn catalog_effort_order_matches_the_published_threshold_order() {
    assert_eq!(
        EFFORT_ORDER,
        ["none", "minimal", "low", "medium", "high", "xhigh", "max"]
    );
    for (level, effort) in ["none", "minimal", "low", "medium", "high", "xhigh", "max"]
        .iter()
        .enumerate()
    {
        assert_eq!(rank(effort), Some(level));
    }
    assert_eq!(rank("extreme"), None);
}

#[test]
fn catalog_effort_chain_prefers_pin_workload_graded_vendor_then_none() {
    let mut model = CatalogEfforts {
        offered: Some(
            ["low", "medium", "high", "xhigh"]
                .map(String::from)
                .to_vec(),
        ),
        graded: Some("high".into()),
        vendor_default: Some("xhigh".into()),
        workloads: [(
            "code".into(),
            CatalogWorkload {
                default: Some("medium".into()),
                ..Default::default()
            },
        )]
        .into(),
        ..Default::default()
    };
    let check = |model: &CatalogEfforts, pin: Option<&str>, variant: Option<&str>, source: &str| {
        let result = resolve(
            Some(model),
            Some("code"),
            pin.is_some(),
            pin,
            1200.0,
            1400.0,
        )
        .unwrap();
        let wire = json!(result);
        assert_eq!(wire["variant"], json!(variant));
        assert_eq!(wire["effort_source"], json!(source));
    };
    check(&model, Some("low"), Some("low"), "pin");
    check(&model, None, Some("medium"), "workload_default");
    model.workloads.clear();
    check(&model, None, Some("high"), "graded");
    model.graded = None;
    check(&model, None, Some("xhigh"), "vendor_default");
    model.vendor_default = None;
    check(&model, None, None, "none");
}

#[test]
fn catalog_effort_grade_falls_back_per_metric_and_names_the_measured_effort() {
    let model = CatalogEfforts {
        graded: Some("high".into()),
        elo: [("low".into(), 1100.0)].into(),
        ..Default::default()
    };
    let choice = resolve(Some(&model), None, true, Some("low"), 1200.0, 1400.0).unwrap();
    assert_eq!(
        json!(choice.grade),
        json!({"elo":1100.0,"eq":1400.0,"elo_effort":"low","eq_effort":"high"})
    );
}

#[test]
fn unknown_workload_warnings_are_bounded_and_dedupe_recent_names() {
    let consumer = "workload-dedupe-regression";
    for n in 0..300 {
        warn_unknown_workload(consumer, &format!("unknown-{n}"));
    }
    assert!(WARNED_UNKNOWN_WORKLOADS.lock().unwrap().len() <= 256);
    let _ = crate::take_operator_warnings();
    warn_unknown_workload(consumer, "unknown-299");
    assert!(crate::take_operator_warnings()
        .iter()
        .all(|w| !w.contains(consumer)));
}
