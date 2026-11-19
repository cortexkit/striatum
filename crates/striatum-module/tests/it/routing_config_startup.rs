//! Exercise startup and offline checks for the routing file. When the daemon
//! does not manage settings, bad operator policy must stop the module before it
//! can serve routes.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};
use tokio::{process::Child, time::timeout};

use super::managed_settings::{launch_with_ack, DEADLINE};

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
        self.command(xdg).args(args).output().unwrap()
    }

    async fn run_with_file_settings(&self) -> Output {
        let (child, _peer, _) = launch_with_ack(&self.0, self.command(true), vec![]).await;
        wait_for_output(child).await
    }

    async fn assert_file_settings_serve(&self, xdg: bool) {
        let (child, mut peer, hello) = launch_with_ack(&self.0, self.command(xdg), vec![]).await;
        let control_frame = |ty, corr| {
            Frame::build_with_version(
                hello.header.ver,
                ty,
                Flags::new(false, Priority::Passive, false),
                0,
                0,
                corr,
                vec![],
            )
            .unwrap()
        };
        let reply = timeout(DEADLINE, async {
            write_frame(&mut peer, &control_frame(FrameType::Ping, 800)).await?;
            read_frame(&mut peer).await
        })
        .await
        .expect("routing did not answer PING");
        let pong = match reply {
            Ok(Some(frame)) => frame,
            other => {
                let output = wait_for_output(child).await;
                panic!(
                    "expected PONG, got {other:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        };
        assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 800));
        write_frame(&mut peer, &control_frame(FrameType::Goodbye, 0))
            .await
            .unwrap();
        let output = wait_for_output(child).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn command(&self, xdg: bool) -> Command {
        let binary =
            cortexkit_test_support::stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
        let mut command = cortexkit_test_support::fenced_command(&binary, &self.0);
        command.env("HOME", &self.0).env_remove("XDG_CONFIG_HOME");
        if xdg {
            command.env("XDG_CONFIG_HOME", self.0.join("xdg"));
        }
        command
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

async fn wait_for_output(child: Child) -> Output {
    timeout(DEADLINE, child.wait_with_output())
        .await
        .expect("routing did not exit")
        .unwrap()
}

fn stderr(output: Output) -> String {
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

async fn invalid_startup(document: &str, token: &str) {
    let home = Home::new();
    std::fs::write(home.config_dir(true).join("striatum.jsonc"), document).unwrap();
    let message = stderr(home.run_with_file_settings().await);
    assert!(message.contains(token), "{message}");
    assert!(message.contains("striatum.jsonc"), "{message}");
    assert!(!message.contains("missing-connection-file"), "{message}");
}

// The in-process `refusal_case!` contract tests and old-shape tests in
// `config_overrides` check every refusal message, including the converter
// remedy, on first load and on reload. This spawn verifies that file mode
// refuses to serve after an invalid startup document.
#[tokio::test]
async fn old_explicit_only_providers_refuses_startup_with_converter() {
    invalid_startup(
        r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
        "ck-striatum config-convert",
    )
    .await;
}

#[tokio::test]
async fn absent_routing_file_with_xdg_does_not_read_home_file() {
    let home = Home::new();
    std::fs::write(home.config_dir(false).join("striatum.jsonc"), "not json").unwrap();
    home.assert_file_settings_serve(true).await;
}

#[tokio::test]
async fn striatum_startup_and_check_config_name_the_settings_file() {
    for xdg in [false, true] {
        for primary_exists in [false, true] {
            let home = Home::new();
            let directory = home.config_dir(xdg);
            let primary = directory.join("striatum.jsonc");
            if primary_exists {
                std::fs::write(&primary, "{}").unwrap();
            }
            home.assert_file_settings_serve(xdg).await;
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

#[tokio::test]
async fn removed_model_naming_refuses_startup_and_check_config() {
    for document in [
        r#"{"model_naming":{}}"#,
        r#"{"model_naming":null}"#,
        r#"{"model_routing":{"model_naming":{}}}"#,
        r#"{"model_routing":{"model_naming":null}}"#,
    ] {
        let home = Home::new();
        let primary = home.config_dir(true).join("striatum.jsonc");
        std::fs::write(&primary, document).unwrap();
        for output in [
            home.run_with_file_settings().await,
            home.run_with_args(true, &["--check-config"]),
        ] {
            let message = stderr(output);
            assert!(
                message.contains("config_key_removed: model_naming"),
                "{message}"
            );
            assert!(message.contains("removed"), "{message}");
            assert!(message.contains("striatum.jsonc"), "{message}");
            assert!(!message.contains("missing-connection-file"), "{message}");
        }
    }
}

#[tokio::test]
async fn striatum_invalid_settings_file_refuses_startup_and_check_config() {
    let home = Home::new();
    let primary = home.config_dir(true).join("striatum.jsonc");
    std::fs::write(&primary, "not json").unwrap();
    for output in [
        home.run_with_file_settings().await,
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
fn connection_failure_takes_precedence_over_invalid_settings_file() {
    let home = Home::new();
    std::fs::write(home.config_dir(true).join("striatum.jsonc"), "not json").unwrap();
    let message = stderr(home.run(true));
    assert!(message.contains("reading connection file"), "{message}");
    assert!(message.contains("missing-connection-file"), "{message}");
    assert!(!message.contains("invalid config"), "{message}");
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
    std::fs::write(
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
