//! Exercise startup before a daemon connection: bad operator policy must stop
//! the real process rather than silently permit models the operator excluded.

#[path = "../../src/config_convert.rs"]
mod config_convert;
#[path = "../../src/test_config.rs"]
mod test_config;

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::atomic::{AtomicU64, Ordering};

struct Home(PathBuf);

impl Home {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = cortexkit_test_support::scratch_root().join(format!(
            "routing-startup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.join("xdg/cortexkit")).unwrap();
        std::fs::create_dir_all(path.join(".config/cortexkit")).unwrap();
        Self(path)
    }

    fn config_dir(&self, xdg: bool) -> PathBuf {
        self.0.join(if xdg {
            "xdg/cortexkit"
        } else {
            ".config/cortexkit"
        })
    }

    fn run(&self, xdg: bool) -> Output {
        self.run_with_args(
            xdg,
            &[
                "--subc",
                self.0.join("missing-connection-file").to_str().unwrap(),
            ],
        )
    }

    fn run_with_args(&self, xdg: bool, args: &[&str]) -> Output {
        let binary =
            cortexkit_test_support::stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
        let mut command = cortexkit_test_support::fenced_command(&binary, &self.0);
        command.env("HOME", &self.0).env_remove("XDG_CONFIG_HOME");
        if xdg {
            command.env("XDG_CONFIG_HOME", self.0.join("xdg"));
        }
        command.args(args).output().unwrap()
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn stderr(output: Output) -> String {
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

fn invalid_startup(document: &str, token: &str) {
    let home = Home::new();
    test_config::write_raw(home.config_dir(true).join("striatum.jsonc"), document).unwrap();
    let message = stderr(home.run(true));
    assert!(message.contains(token), "{message}");
    assert!(message.contains("striatum.jsonc"), "{message}");
    assert!(!message.contains("missing-connection-file"), "{message}");
}

// The in-process `refusal_case!` contract tests and old-shape tests in
// `config_overrides` check every refusal message, including the converter
// remedy, on first load and on reload. One binary spawn here is
// enough to prove that a refusal stops startup before the daemon connection.
#[test]
fn old_explicit_only_providers_refuses_startup_with_converter() {
    invalid_startup(
        r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
        "ck-striatum config-convert",
    );
}

#[test]
fn absent_routing_file_with_xdg_does_not_read_home_file() {
    let home = Home::new();
    test_config::write_raw(home.config_dir(false).join("striatum.jsonc"), "not json").unwrap();
    let message = stderr(home.run(true));
    assert!(
        message.contains(Path::new("missing-connection-file").to_str().unwrap()),
        "valid empty policy must reach daemon connection: {message}"
    );
}

#[test]
fn striatum_startup_and_check_config_name_the_settings_file() {
    for xdg in [false, true] {
        for primary_exists in [false, true] {
            let home = Home::new();
            let directory = home.config_dir(xdg);
            let primary = directory.join("striatum.jsonc");
            if primary_exists {
                test_config::write_raw(&primary, "{}").unwrap();
            }
            let startup = stderr(home.run(xdg));
            assert!(startup.contains("missing-connection-file"), "{startup}");
            let output = home.run_with_args(xdg, &["--check-config"]);
            let report = String::from_utf8(output.stderr).unwrap();
            assert!(output.status.success(), "{report}");
            assert!(
                report.contains(&format!("config check passed: {}", primary.display())),
                "{report}"
            );
        }
    }
}

#[test]
fn striatum_invalid_settings_file_refuses_startup_and_check_config() {
    let home = Home::new();
    let primary = home.config_dir(true).join("striatum.jsonc");
    test_config::write_raw(&primary, "not json").unwrap();
    for output in [
        home.run(true),
        home.run_with_args(true, &["--check-config"]),
    ] {
        let message = stderr(output);
        assert!(
            message.contains(&format!("invalid config in {}", primary.display())),
            "{message}"
        );
        assert!(!message.contains("missing-connection-file"), "{message}");
    }
}

#[test]
fn one_shot_output_modes_add_no_stderr_logging() {
    let home = Home::new();
    for mode in ["--print-manifest", "--print-config-schema"] {
        let output = home.run_with_args(true, &[mode]);
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        assert!(
            !output.stdout.is_empty(),
            "{mode} must still print its output"
        );
    }
}

#[test]
fn check_config_binary_prints_five_anonymous_count_lines_offline() {
    let home = Home::new();
    let primary = home.config_dir(true).join("striatum.jsonc");
    test_config::write_raw(
        &primary,
        r#"{
        "providers":{"private-provider":{"subscription":true,"pin_only":false}},
        "runners":[{"module":"private-runner","routes":["private-provider"]}]
    }"#,
    )
    .unwrap();
    let binary =
        cortexkit_test_support::stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
    let run = |data: Option<&Path>| {
        let mut command = cortexkit_test_support::fenced_command(&binary, &home.0);
        command
            .env("XDG_CONFIG_HOME", home.0.join("xdg"))
            .env_remove("HOME")
            .env_remove("XDG_DATA_HOME")
            .arg("--check-config");
        if let Some(path) = data {
            command.env("XDG_DATA_HOME", path);
        }
        let output = command.output().unwrap();
        let text = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{text}");
        text.lines()
            .filter(|line| line.contains('='))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let data = home.0.join("data");
    for counts in [run(None), run(Some(&data))] {
        assert_eq!(counts.len(), 5, "{counts:?}");
        assert_eq!(
            &counts[..3],
            [
                "subscription_rules=1",
                "exclusion_rules=1",
                "runner_route_rules=1"
            ]
        );
        assert!(counts[3].starts_with("catalog_facts=unavailable ("));
        assert!(counts[4].starts_with("vault_roster_credentials=unavailable ("));
        assert!(!counts.join("\n").contains("private"));
    }
}
