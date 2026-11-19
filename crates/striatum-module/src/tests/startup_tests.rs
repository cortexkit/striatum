//! Startup tests cover one-shot commands and the registered operation surface.

use std::ffi::OsString;

use super::*;
use crate::operator_commands::{capture_account_quota_reply, parse_startup_mode, StartupMode};

#[tokio::test(flavor = "current_thread")]
async fn account_quota_capture_keeps_current_thread_worker_free() {
    let scratch = cortexkit_test_support::ScratchDir::new("routing-quota-capture-blocked");
    let path = scratch.join("usage-get-all.json");
    assert!(std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap()
        .success());
    let body = b"{ \"providers\": [] }\n\0unfiltered bytes".to_vec();

    // The writer is polled first and cannot open the FIFO until the sibling
    // future runs on the same async worker and starts a blocking reader.
    let (captured, saved) = tokio::join!(
        biased;
        capture_account_quota_reply(scratch.path().to_path_buf(), body.clone()),
        async {
            tokio::task::spawn_blocking(move || std::fs::read(path))
                .await.unwrap().unwrap()
        },
    );
    captured.unwrap();
    assert_eq!(saved, body);
}

#[tokio::test(flavor = "current_thread")]
async fn account_quota_capture_preserves_bytes_and_io_errors() {
    let scratch = cortexkit_test_support::ScratchDir::new("routing-quota-capture-bytes");
    let directory = scratch.join("capture");
    let path = directory.join("usage-get-all.json");
    let body = b"{ \"providers\": [] }\n\0unfiltered bytes".to_vec();
    capture_account_quota_reply(directory.clone(), body.clone())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), body);

    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let expected = std::fs::write(&path, &body).unwrap_err().to_string();
    let error = capture_account_quota_reply(directory, body.clone())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), format!("capture usage: {expected}"));

    let not_directory = scratch.join("file");
    std::fs::write(&not_directory, b"file").unwrap();
    let expected = std::fs::create_dir_all(&not_directory)
        .unwrap_err()
        .to_string();
    let error = capture_account_quota_reply(not_directory, body)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), format!("capture usage: {expected}"));
}

#[test]
fn startup_refuses_removed_managed_settings_flag() {
    let error =
        parse_startup_mode(["--subc", "connection.json", "--managed-settings"].map(OsString::from))
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("--managed-settings has been removed"),
        "{error}"
    );
}

#[test]
fn config_check_uses_startup_loader_and_refuses_unknown_config() {
    assert_eq!(
        parse_startup_mode([OsString::from("--check-config")]).unwrap(),
        StartupMode::CheckConfig
    );
    assert!(
        parse_startup_mode([OsString::from("--check-config"), OsString::from("--subc=x")]).is_err()
    );
    let scratch = cortexkit_test_support::ScratchDir::new("routing-config-check");
    let path = scratch.join("striatum.jsonc");
    check_config_at(&path).unwrap();
    let invalid = cortexkit_test_support::ScratchDir::new("routing-config-check-invalid");
    let invalid_path = invalid.join("striatum.jsonc");
    crate::test_config::write(&invalid_path, "{\"not_a_routing_setting\":true}").unwrap();
    assert!(check_config_at(&invalid_path).is_err());
}

#[test]
fn config_check_refuses_old_shape_and_names_converter() {
    let scratch = cortexkit_test_support::ScratchDir::new("routing-config-check-explicit");
    let path = scratch.join("striatum.jsonc");
    crate::test_config::write_raw(
        &path,
        r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
    )
    .unwrap();
    let error = check_config_at(&path).unwrap_err().to_string();
    assert!(
        error.contains("model_routing") && error.contains("ck-striatum config-convert"),
        "{error}"
    );
}

#[test]
fn manifest_declares_wire_version_and_build_provenance_contract() {
    let provenance = subc_protocol::manifest::build_provenance_from_source(
        BuildGitShaSource::Git {
            revision: "0123456789abcdef0123456789abcdef01234567",
            tree_state: GitTreeState::Clean,
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        provenance.wire_crate_version.as_deref(),
        Some(subc_protocol::SUBC_PROTOCOL_CRATE_VERSION)
    );
    assert_eq!(
        provenance.build_git_sha.as_deref(),
        Some("0123456789abcdef0123456789abcdef01234567")
    );

    let dirty = subc_protocol::manifest::build_provenance_from_source(
        BuildGitShaSource::Git {
            revision: "0123456789abcdef0123456789abcdef01234567",
            tree_state: GitTreeState::Dirty,
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(dirty.build_git_sha, None);
    assert_eq!(
        dirty.build_git_sha_absence_reason,
        Some(subc_protocol::manifest::BuildGitShaAbsenceReason::DeclinedDirty)
    );
}

#[test]
fn declared_provenance_follows_build_inputs() {
    let provenance = declared_provenance().unwrap();
    use subc_protocol::manifest::BuildGitShaAbsenceReason;
    if env!("PREFRONTAL_BUILD_GIT_SHA") == "unknown" {
        assert_eq!(provenance.build_git_sha, None);
        assert_eq!(
            provenance.build_git_sha_absence_reason,
            Some(BuildGitShaAbsenceReason::NeverDerived)
        );
    } else if env!("PREFRONTAL_BUILD_GIT_DIRTY") == "true" {
        assert_eq!(provenance.build_git_sha, None);
        assert_eq!(
            provenance.build_git_sha_absence_reason,
            Some(BuildGitShaAbsenceReason::DeclinedDirty)
        );
    } else {
        assert_eq!(
            provenance.build_git_sha.as_deref(),
            Some(env!("PREFRONTAL_BUILD_GIT_SHA"))
        );
    }
    assert_eq!(
        provenance.wire_crate_version.as_deref(),
        Some(subc_protocol::SUBC_PROTOCOL_CRATE_VERSION)
    );
    assert_eq!(
        provenance.build_lock_digest.as_deref().map(str::len),
        Some(64)
    );
}

#[test]
fn default_manifest_registers_striatum() {
    assert_eq!(DEFAULT_MODULE_ID, "striatum");
    let printed: serde_json::Value =
        serde_json::from_str(&printed_manifest_json(DEFAULT_MODULE_ID).unwrap()).unwrap();
    assert_eq!(printed["manifest"]["module_id"], "striatum");
}

#[test]
fn print_manifest_carries_the_whole_manifest_and_the_operation_kinds() {
    let printed: serde_json::Value =
        serde_json::from_str(&printed_manifest_json("print-manifest-test").unwrap()).unwrap();
    let object = printed.as_object().expect("printed manifest is an object");
    assert_eq!(
        object.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["manifest", "operation_kinds"]
    );

    // The whole manifest HELLO declares, so the deploy script can read the
    // installed build commit at manifest.provenance.build_git_sha.
    let printed_manifest = &printed["manifest"];
    assert_eq!(
        printed_manifest,
        &serde_json::to_value(manifest("print-manifest-test")).unwrap()
    );
    assert_eq!(printed_manifest["module_id"], "print-manifest-test");
    let provenance = &printed_manifest["provenance"];
    assert_eq!(
        provenance["wire_crate_version"],
        subc_protocol::SUBC_PROTOCOL_CRATE_VERSION
    );
    if env!("PREFRONTAL_BUILD_GIT_SHA") == "unknown" {
        assert!(provenance["build_git_sha"].is_null());
        assert_eq!(provenance["build_git_sha_absence_reason"], "never_derived");
    } else if env!("PREFRONTAL_BUILD_GIT_DIRTY") == "true" {
        assert!(provenance["build_git_sha"].is_null());
        assert_eq!(provenance["build_git_sha_absence_reason"], "declined_dirty");
    } else {
        assert_eq!(
            provenance["build_git_sha"],
            env!("PREFRONTAL_BUILD_GIT_SHA")
        );
    }
    assert_eq!(
        provenance["build_lock_digest"].as_str().map(str::len),
        Some(64)
    );

    // The op-kinds generator reads this list: every served operation once,
    // sorted by name, as exactly { name, kind }.
    let kinds = printed["operation_kinds"]
        .as_array()
        .expect("operation kinds printed");
    let names = kinds
        .iter()
        .map(|operation| operation["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let mut expected = served_operations()
        .into_iter()
        .map(|operation| operation.name)
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(names, expected);
    let route_select = kinds
        .iter()
        .find(|operation| operation["name"] == ROUTE_SELECT_OP)
        .expect("route.select printed");
    assert_eq!(
        route_select,
        &serde_json::json!({ "name": ROUTE_SELECT_OP, "kind": "mutate" })
    );
}

#[test]
fn manifest_exposes_policy_query_alongside_selection() {
    let manifest = manifest(DEFAULT_MODULE_ID);
    let operations = manifest
        .provides
        .iter()
        .find_map(|role| match role {
            ProviderRole::ManagementSurface { operations, .. } => Some(operations),
            _ => None,
        })
        .unwrap();
    assert!(operations
        .iter()
        .any(|operation| operation.name == ROUTE_RESOLVE_POLICY_OP));
    assert!(operations
        .iter()
        .any(|operation| operation.name == ROUTE_MODEL_FACT_OP));
    for operation_name in [ROUTE_EXPLAIN_OP, ROUTE_DECISIONS_LIST_OP] {
        assert!(operations.iter().any(|operation| {
            operation.name == operation_name && operation.kind == ManagementOperationKind::Query
        }));
    }
}

#[tokio::test]
async fn removed_routing_available_models_text_is_unknown_method() {
    let rig = Rig::new("removed-available-models-wire");
    let store = Arc::new(rig.open_store());
    for params in [
        json!({"availableModels":["wireco/model"],"include":"all"}),
        json!({}),
        json!({"limit":"invalid"}),
        serde_json::Value::Null,
    ] {
        let response = run_route_wire_request(
            &store,
            json!({"method":"routing.available_models_text","params":params}),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error, "{params}");
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "unknown_method", "{params}: {body}");
    }
    assert!(!handles("routing.available_models_text"));
    assert!(served_operations()
        .iter()
        .all(|operation| operation.name != "routing.available_models_text"));
    assert!(manifest(DEFAULT_MODULE_ID).provides.iter().all(|role| {
        match role {
            ProviderRole::ManagementSurface { operations, .. } => operations
                .iter()
                .all(|operation| operation.name != "routing.available_models_text"),
            _ => true,
        }
    }));
}

#[tokio::test]
async fn removed_opencode_plugin_operations_are_unknown_methods() {
    let rig = Rig::new("removed-opencode-plugin-wire");
    let store = Arc::new(rig.open_store());
    let operations = served_operations();
    for method in [
        "route.resolve_model",
        "route.model_capabilities",
        "route.agent_model_requirements",
    ] {
        assert!(!handles(method), "{method}");
        assert!(operations.iter().all(|op| op.name != method), "{method}");
        for request in [
            json!({"method":method}),
            json!({"method":method,"params":null}),
            json!({"method":method,"params":{"action":"pipeline","providerID":"wireco","modelID":"model"}}),
        ] {
            let response = run_route_wire_request(&store, request).await;
            assert_eq!(response.header.ty, FrameType::Error, "{method}");
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(body["code"], "unknown_method", "{method}: {body}");
            assert_eq!(body["message"], format!("unknown method '{method}'"));
        }
    }
}

#[test]
fn census_gated_routing_operations_remain_registered() {
    // Without a fleet-wide fourteen-day zero-call window and an empty
    // caller census, these methods and their legacy shapes must stay.
    let operations = served_operations();
    for method in [
        ROUTE_COOLDOWN_STATUS_OP,
        ROUTE_MODEL_FACT_OP,
        ROUTE_MODEL_FACTS_OP,
        ROUTE_SET_DECISION_OUTCOME_OP,
        ROUTE_RECORD_USAGE_OP,
        ROUTE_RECORD_COOLDOWN_OP,
        ROUTE_SELECT_OP,
        ROUTE_SELECT_PANEL_OP,
        ROUTE_EXPLAIN_OP,
        account_selection::ACCOUNT_FOR_OP,
        account_selection::ACCOUNT_OBSERVE_OP,
        account_selection::ACCOUNT_STATUS_OP,
        "route.pool_acquire",
        "route.pool_renew",
        "route.pool_release",
        "route.pool_wall",
    ] {
        assert!(handles(method), "{method}");
        assert!(operations.iter().any(|op| op.name == method), "{method}");
    }
}

#[test]
fn account_status_is_registered_as_a_query() {
    let operations = served_operations();
    let operation = operations
        .iter()
        .find(|operation| operation.name == account_selection::ACCOUNT_STATUS_OP)
        .expect("account status must be registered");
    assert_eq!(operation.kind, ManagementOperationKind::Query);
    assert!(account_selection::handles(&operation.name));
    assert!(handles(&operation.name));
}

#[test]
fn handles_covers_every_manifest_operation() {
    let operations = manifest(DEFAULT_MODULE_ID)
        .provides
        .iter()
        .find_map(|role| match role {
            ProviderRole::ManagementSurface { operations, .. } => Some(operations.clone()),
            _ => None,
        })
        .expect("routing module must expose a management surface");
    for operation in &operations {
        assert!(
            handles(&operation.name),
            "manifest op {} missing from handles allowlist",
            operation.name
        );
    }
    assert!(handles(ROUTE_LIST_INFERRED_OP));
    assert!(handles(ROUTE_LIST_UNSCORED_OP));
    assert!(handles(ROUTE_CATALOG_TARGETS_OP));
    assert!(!handles("route.unknown"));
}

#[tokio::test]
async fn removed_aa_seed_operation_is_not_served() {
    let rig = Rig::new("removed-aa-seed");
    let store = Arc::new(rig.open_store());
    assert!(!handles("route.model_seed_aa"));
    assert!(served_operations()
        .iter()
        .all(|operation| operation.name != "route.model_seed_aa"));
    let response = run_route_wire_request(
        &store,
        json!({
            "method": "route.model_seed_aa",
            "params": { "providerID": "custom", "modelID": "aa-live" }
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "unknown_method");
}
