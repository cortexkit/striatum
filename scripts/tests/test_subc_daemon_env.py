import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATE = runpy.run_path(str(ROOT / "scripts/subc-daemon-env.py"))


class SubcDaemonEnvGate(unittest.TestCase):
    def check_source(self, source, relative="crates/probe.rs"):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="subc-env-gate-", dir=scratch) as directory:
            root = Path(directory)
            probe = root / relative
            probe.parent.mkdir(parents=True, exist_ok=True)
            probe.write_text(source)
            return GATE["violations"](root)

    def test_refuses_spawn_by_subc_named_program_even_when_fenced(self):
        # Even with fenced_subc_daemon_env applied, building the Command by hand
        # is refused: tests must launch the daemon through SubcDaemonBinary.
        for constructor in ["Command::new(&subc_core)", "Command::new(subc_core)", "tokio::process::Command::new(&ck_subc)", "std::process::Command::new(&subc)"]:
            for fence in ["", ".envs(cortexkit_test_support::fenced_subc_daemon_env(&rig))"]:
                with self.subTest(constructor=constructor, fence=fence):
                    source = f"fn rig() {{\n let child = {constructor}{fence}.env(\"SUBC_PORT\", \"0\").spawn().unwrap();\n}}"
                    errors = self.check_source(source)
                    self.assertEqual(len(errors), 1)
                    self.assertIn("crates/probe.rs:2:", errors[0])
                    self.assertIn("SubcDaemonBinary", errors[0])

    def test_refuses_locating_the_binary_by_name(self):
        # A rig that builds or joins the binary path itself can spawn it under
        # any variable name, so naming the binary is refused on its own.
        for literal in ['"ck-subc"', '"debug/ck-subc"', '"ck-subc.exe"', 'r#"target/debug/ck-subc"#']:
            with self.subTest(literal=literal):
                source = f"fn rig() {{\n    let bin = target.join({literal});\n    let child = Command::new(&bin).spawn();\n}}"
                errors = self.check_source(source)
                self.assertEqual(len(errors), 1, errors)
                self.assertIn("crates/probe.rs:2:", errors[0])

    def test_accepts_helper_spawn(self):
        source = '''
fn rig() {
    let subc_core = cortexkit_test_support::SubcDaemonBinary::build();
    let child = tokio::process::Command::from(subc_core.command(&rig))
        .env("SUBC_PORT", "0")
        .spawn()
        .expect("spawn ck-subc");
}
'''
        self.assertEqual(self.check_source(source), [])

    def test_ignores_prose_comments_and_non_subc_programs(self):
        source = '''
fn rig() {
    // Command::new(&subc_core).spawn(); cargo build --bin "ck-subc"
    /* nested /* "ck-subc" */ still a comment "debug/ck-subc" */
    let quote = '"';
    let text = "builds and spawns a real ck-subc daemon";
    let warm = std::process::Command::new(&binary).arg("--version");
}
'''
        self.assertEqual(self.check_source(source), [])
        helper = 'const SUBC_DAEMON_BIN: &str = "ck-subc";\nfn f() { std::process::Command::new(&subc_path); }\n'
        # The daemon helper is published externally; no local source is exempt.
        self.assertEqual(len(self.check_source(helper, "crates/probe/src/lib.rs")), 2)


if __name__ == "__main__":
    unittest.main()
