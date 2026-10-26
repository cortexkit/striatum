//! Exercise startup before a daemon connection: bad operator policy must stop
//! the real process rather than silently permit models the operator excluded.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

struct Home(PathBuf);

impl Home {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = prefrontal_test_support::scratch_root().join(format!(
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
        let binary = prefrontal_test_support::stage_test_binary(Path::new(env!(
            "CARGO_BIN_EXE_ck-prefrontal-routing"
        )));
        let mut command = Command::new(binary);
        command.env("HOME", &self.0).env_remove("XDG_CONFIG_HOME");
        if xdg {
            command.env("XDG_CONFIG_HOME", self.0.join("xdg"));
        }
        command
            .args([
                "--subc",
                self.0.join("missing-connection-file").to_str().unwrap(),
            ])
            .output()
            .unwrap()
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

fn renamed_refusal(xdg: bool) {
    for both in [false, true] {
        let home = Home::new();
        let directory = home.config_dir(xdg);
        let old = directory.join("alfonso-routing.jsonc");
        let new = directory.join("prefrontal-routing.jsonc");
        std::fs::write(&old, "old settings must never be read").unwrap();
        if both {
            std::fs::write(&new, "{}").unwrap();
        }
        let message = stderr(home.run(xdg));
        assert!(message.contains("config_renamed"), "{message}");
        assert!(
            message.contains(&old.display().to_string())
                && message.contains(&new.display().to_string()),
            "{message}"
        );
        assert!(
            message.contains(if both { "delete" } else { "move" }),
            "{message}"
        );
        assert!(
            !message.contains("missing-connection-file"),
            "configuration must refuse before daemon connection: {message}"
        );
    }
}

#[test]
fn config_renamed_resolves_xdg_paths_at_startup() {
    renamed_refusal(true);
}

#[test]
fn config_renamed_resolves_home_paths_at_startup() {
    renamed_refusal(false);
}

fn invalid_startup(document: &str, token: &str) {
    let home = Home::new();
    std::fs::write(
        home.config_dir(true).join("prefrontal-routing.jsonc"),
        document,
    )
    .unwrap();
    let message = stderr(home.run(true));
    assert!(message.contains(token), "{message}");
    assert!(!message.contains("missing-connection-file"), "{message}");
}

// The in-process `refusal_case!` contract tests in `config_overrides` check
// every refusal message on first load and on reload. One binary spawn here is
// enough to prove that a refusal stops startup before the daemon connection.
#[test]
fn explicit_only_providers_refuses_startup_with_the_exclude_remedy() {
    invalid_startup(
        r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"]}}"#,
        "config_key_removed: model_routing.explicit_only_providers; list the provider in model_routing.exclude.all; pins still work",
    );
}
#[test]
fn absent_routing_file_with_xdg_does_not_read_home_legacy_file() {
    let home = Home::new();
    std::fs::write(home.config_dir(false).join("alfonso-routing.jsonc"), "{}").unwrap();
    let message = stderr(home.run(true));
    assert!(!message.contains("config_renamed"), "{message}");
    assert!(
        message.contains(Path::new("missing-connection-file").to_str().unwrap()),
        "valid empty policy must reach daemon connection: {message}"
    );
}
