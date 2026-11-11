use super::*;
use serde_json::json;

fn groups(value: serde_json::Value) -> SubstrateGroups {
    serde_json::from_value(value).unwrap()
}

#[test]
fn resolution_walks_every_workload_consumer_all_rule_before_broca() {
    let mut value = json!({
        "sidekick.librarian": {
            "vendor/model/high": "pi", "vendor": "opencode", "default": "pi"
        },
        "sidekick": {
            "vendor/model": "opencode", "vendor": "pi", "default": "opencode"
        },
        "all": {
            "vendor/model": "pi", "vendor": "opencode", "default": "pi"
        }
    });
    for (group, key, expected) in [
        ("sidekick.librarian", "vendor/model/high", Harness::Pi),
        ("sidekick.librarian", "vendor", Harness::Opencode),
        ("sidekick.librarian", "default", Harness::Pi),
        ("sidekick", "vendor/model", Harness::Opencode),
        ("sidekick", "vendor", Harness::Pi),
        ("sidekick", "default", Harness::Opencode),
        ("all", "vendor/model", Harness::Pi),
        ("all", "vendor", Harness::Opencode),
        ("all", "default", Harness::Pi),
    ] {
        assert_eq!(
            resolve(
                &groups(value.clone()),
                "vendor/model/high",
                Some(Consumer::Sidekick),
                Some("sidekick.librarian"),
            ),
            expected,
            "{group}.{key} must win before the remaining rules"
        );
        value[group].as_object_mut().unwrap().remove(key);
    }
    assert_eq!(
        resolve(
            &groups(value),
            "vendor/model/high",
            Some(Consumer::Sidekick),
            Some("sidekick.librarian"),
        ),
        Harness::Broca
    );
}

#[test]
fn longest_prefix_wins_and_model_keys_cover_variant_routes() {
    let policy = groups(json!({"all": {
        "vendor": "broca",
        "vendor/org/model": "opencode",
        "vendor/org/model/high": "pi",
        "default": "pi"
    }}));
    for (route, expected) in [
        ("vendor/org/model", Harness::Opencode),
        ("vendor/org/model/low", Harness::Opencode),
        ("vendor/org/model/high", Harness::Pi),
        ("vendor/org/model/high/extra", Harness::Pi),
        ("vendor/org/model/highest", Harness::Opencode),
        ("vendor/org/model-extra", Harness::Broca),
        ("vendor-extra/org/model", Harness::Pi),
    ] {
        assert_eq!(resolve(&policy, route, None, None), expected, "{route}");
    }
}

#[test]
fn partial_mason_group_falls_to_all_and_no_workload_starts_at_consumer() {
    let policy = groups(json!({
        "mason": {"vendor/special": "pi"},
        "sidekick": {"default": "broca"},
        "sidekick.librarian": {"default": "opencode"},
        "all": {"default": "opencode"}
    }));
    assert_eq!(
        resolve(
            &policy,
            "vendor/other",
            Some(Consumer::Mason),
            Some("mason")
        ),
        Harness::Opencode
    );
    assert_eq!(
        resolve(&policy, "vendor/special/high", Some(Consumer::Mason), None),
        Harness::Pi
    );
    assert_eq!(
        resolve(&policy, "vendor/other", Some(Consumer::Sidekick), None),
        Harness::Broca
    );
    assert_eq!(
        resolve(&policy, "vendor/other", None, None),
        Harness::Opencode
    );
}

#[test]
fn panels_start_at_athena_and_workloads_have_no_builtin_defaults() {
    let policy = groups(json!({
        "athena": {"default": "pi"},
        "hire": {"default": "opencode"},
        "persona_seat": {"default": "pi"},
        "all": {"default": "broca"}
    }));
    for (workload, expected) in [
        ("athena", Harness::Pi),
        ("hire", Harness::Opencode),
        ("persona_seat", Harness::Pi),
        ("sidekick.librarian", Harness::Broca),
    ] {
        assert_eq!(
            resolve(&policy, "vendor/model", None, Some(workload)),
            expected,
            "{workload}"
        );
    }
    assert_eq!(
        resolve(
            &SubstrateGroups::default(),
            "vendor/model",
            Some(Consumer::Sidekick),
            Some("sidekick.librarian"),
        ),
        Harness::Broca
    );
    for harness in [Harness::Broca, Harness::Opencode, Harness::Pi] {
        assert_eq!(serde_json::to_value(harness).unwrap(), harness.as_str());
    }
}
