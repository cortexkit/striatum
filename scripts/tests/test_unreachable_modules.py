#!/usr/bin/env python3
"""Fixture tests for the Rust module reachability census."""
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "unreachable-modules.py"


class UnreachableModulesTest(unittest.TestCase):
    def run_gate(
        self, files: dict[str, str], allowlist: str = ""
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(prefix="unreachable-modules-") as temporary:
            fixture = Path(temporary)
            self.write(fixture, "scripts/unreachable-modules.py", SCRIPT.read_text())
            self.write(fixture, "scripts/unreachable-modules.allow", allowlist)
            for relative, contents in files.items():
                self.write(fixture, relative, contents)
            subprocess.run(
                ["git", "init", "-q"], cwd=fixture, check=True, capture_output=True
            )
            subprocess.run(
                ["git", "add", "."], cwd=fixture, check=True, capture_output=True
            )
            return subprocess.run(
                ["python3", "scripts/unreachable-modules.py"],
                cwd=fixture,
                text=True,
                capture_output=True,
                check=False,
            )

    @staticmethod
    def write(root: Path, relative: str, contents: str) -> None:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    @staticmethod
    def wiring_fixture(declaration: str) -> dict[str, str]:
        return {
            "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
            "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
            "crates/prefrontal-routing-module/src/main.rs": (
                f"{declaration}\nfn main() {{}}\n"
            ),
            "crates/prefrontal-routing-module/src/bridge.rs": (
                "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
            ),
        }

    def test_test_only_wiring_is_rejected(self) -> None:
        test_only = self.run_gate(
            self.wiring_fixture("#[cfg(test)]\nmod bridge;")
        )
        self.assertNotEqual(test_only.returncode, 0, test_only.stdout + test_only.stderr)
        self.assertIn("prefrontal-routing-core/feature", test_only.stderr)

        enclosed = self.wiring_fixture(
            '#[cfg(test)]\nmod tests {\n    #[path = "bridge.rs"]\n    mod bridge;\n}'
        )
        enclosed_result = self.run_gate(enclosed)
        self.assertNotEqual(
            enclosed_result.returncode,
            0,
            enclosed_result.stdout + enclosed_result.stderr,
        )
        self.assertIn("prefrontal-routing-core/feature", enclosed_result.stderr)

        file_test_only = self.wiring_fixture("mod bridge;")
        file_test_only["crates/prefrontal-routing-module/src/bridge.rs"] = (
            "#![cfg(test)]\n"
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        )
        file_result = self.run_gate(file_test_only)
        self.assertNotEqual(
            file_result.returncode, 0, file_result.stdout + file_result.stderr
        )
        self.assertIn("prefrontal-routing-core/feature", file_result.stderr)

        production = self.run_gate(self.wiring_fixture("mod bridge;"))
        self.assertEqual(production.returncode, 0, production.stdout + production.stderr)
        self.assertIn("every declared module is reachable", production.stdout)

    def test_binary_links_its_own_library_tree(self) -> None:
        # A binary can name its own library crate rather than declaring its
        # modules directly. Production wiring through that library counts too.
        def fixture(lib_declaration: str) -> dict[str, str]:
            return {
                "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
                "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
                "crates/prefrontal-routing-module/src/main.rs": (
                    "use prefrontal_routing_module::runtime;\nfn main() { runtime::run(); }\n"
                ),
                "crates/prefrontal-routing-module/src/lib.rs": (
                    f"pub mod runtime;\n{lib_declaration}\n"
                ),
                "crates/prefrontal-routing-module/src/runtime.rs": "pub fn run() {}\n",
                "crates/prefrontal-routing-module/src/bridge.rs": (
                    "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
                ),
            }

        linked = self.run_gate(fixture("pub mod bridge;"))
        self.assertEqual(linked.returncode, 0, linked.stdout + linked.stderr)
        self.assertIn("every declared module is reachable", linked.stdout)

        test_only = self.run_gate(fixture("#[cfg(test)]\nmod bridge;"))
        self.assertNotEqual(test_only.returncode, 0, test_only.stdout + test_only.stderr)
        self.assertIn("prefrontal-routing-core/feature", test_only.stderr)

    @staticmethod
    def library_root_fixture(root_code: str) -> dict[str, str]:
        return {
            "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
            "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
            "crates/prefrontal-routing-store/src/lib.rs": root_code,
            "crates/prefrontal-routing-module/src/main.rs": (
                "fn main() { prefrontal_routing_store::run(); }\n"
            ),
        }

    def test_library_root_references_are_reachable(self) -> None:
        result = self.run_gate(self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        ))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_library_root_references_are_transitive(self) -> None:
        files = self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        )
        files.update({
            "crates/prefrontal-routing-module/src/lib.rs": "pub mod bridge;\n",
            "crates/prefrontal-routing-module/src/bridge.rs": (
                "pub fn run() { prefrontal_routing_store::run(); }\n"
            ),
            "crates/prefrontal-routing-module/src/main.rs": (
                "fn main() { prefrontal_routing_module::bridge::run(); }\n"
            ),
        })
        result = self.run_gate(files)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_library_root_reexports_remain_reachable(self) -> None:
        files = self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::run(); }\n"
        )
        files["crates/prefrontal-routing-core/src/lib.rs"] += "pub use feature::run;\n"
        result = self.run_gate(files)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_test_only_library_root_references_are_rejected(self) -> None:
        for test_code in [
            "#[cfg(test)]\nfn check() { prefrontal_routing_core::feature::run(); }\n",
            "#[cfg(test)] mod tests {\n"
            "    fn check() { prefrontal_routing_core::feature::run(); }\n}\n",
            "#[cfg(test)]\nuse prefrontal_routing_core::feature;\n",
            "#[cfg(test)]\npub use prefrontal_routing_core::feature;\n",
            "struct Store;\n#[cfg(test)]\n#[allow(dead_code)]\nimpl Store {\n"
            "    fn check() { prefrontal_routing_core::feature::run(); }\n}\n",
        ]:
            with self.subTest(test_code=test_code):
                result = self.run_gate(self.library_root_fixture(
                    test_code + "pub fn run() {}\n"
                ))
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_library_root_production_after_test_item_remains_reachable(self) -> None:
        # Literal and comment braces must not close the test module early or
        # swallow the production function that follows it.
        result = self.run_gate(self.library_root_fixture(
            "#[cfg(test)]\nmod tests {\n"
            '    const PROSE: &str = r#" } { "#;\n'
            "    // }\n"
            "    fn check() {}\n}\n"
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        ))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_library_root_local_module_path_expressions_are_reachable(self) -> None:
        result = self.run_gate(
            {
                "crates/prefrontal-routing-store/src/lib.rs": """#[cfg(test)]
mod tests {
    const PROSE: &str = r#" } { "#;
    fn check(conn: &()) { super::elicitation::test_only(conn); }
}
pub mod elicitation;
pub struct Store;
impl Store {
    pub fn open(store: &Store) {
        let _sql = r#"CREATE TABLE records (
            payload TEXT DEFAULT '{}',
            note TEXT DEFAULT 'quoted'
        );
        -- braces and quotes in SQL must not hide the call: { "quoted" }
        -- elicitation::sql_only(); is text, not production wiring
        "#;
        let _options = ["atomic", "durable", "shared"];
        let _namespace = "elicitation";
        if _options.is_empty() {
            return;
        }
        let _opening = store
            .refuse_store_ahead(|| Ok(()))
            .with_conn(|conn| {
                let _transaction_label = "opening";
                conn.execute_batch("PRAGMA foreign_keys = ON;");
                elicitation::deletion_open(conn)
            });
    }
}
""",
                "crates/prefrontal-routing-store/src/elicitation.rs": (
                    "pub fn deletion_open(_conn: &()) {}\n"
                ),
                "crates/prefrontal-routing-module/src/main.rs": (
                    "fn main() { prefrontal_routing_store::run(); }\n"
                ),
            }
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_library_root_non_code_references_are_rejected(self) -> None:
        result = self.run_gate(self.library_root_fixture(
            "// prefrontal_routing_core::feature::run();\n"
            'const PROSE: &str = "prefrontal_routing_core::feature::run();";\n'
            "pub fn run() {}\n"
        ))
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_library_named_only_by_test_code_is_not_reached(self) -> None:
        files = self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        )
        files["crates/prefrontal-routing-module/src/main.rs"] = (
            "#[cfg(test)]\nfn check() { prefrontal_routing_store::run(); }\n"
            "fn main() {}\n"
        )
        result = self.run_gate(files)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_unlinked_library_root_does_not_seed_references(self) -> None:
        files = self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        )
        files["crates/prefrontal-routing-module/src/main.rs"] = "fn main() {}\n"
        result = self.run_gate(files)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_library_root_does_not_reach_all_declared_modules(self) -> None:
        files = self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        )
        files["crates/prefrontal-routing-store/src/lib.rs"] += "pub mod dormant;\n"
        files["crates/prefrontal-routing-store/src/dormant.rs"] = "pub fn run() {}\n"
        result = self.run_gate(files, "prefrontal-routing-store/dormant # not wired\n")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(
            "allowlisted unreachable module prefrontal-routing-store/dormant",
            result.stdout,
        )

    def test_allowlisted_reachable_module_is_refused(self) -> None:
        result = self.run_gate(self.library_root_fixture(
            "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
        ), "prefrontal-routing-core/feature # not wired\n")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("allowlisted modules are now REACHABLE", result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)
    def test_binary_links_its_own_production_library(self) -> None:
        files = {
            "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
            "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
            "crates/prefrontal-routing-module/src/main.rs": (
                "use prefrontal_routing_module::module;\nfn main() { module::run(); }\n"
            ),
            "crates/prefrontal-routing-module/src/lib.rs": "pub mod module;\n",
            "crates/prefrontal-routing-module/src/module.rs": (
                "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
            ),
        }
        linked = self.run_gate(files)
        self.assertEqual(linked.returncode, 0, linked.stdout + linked.stderr)
        self.assertIn("every declared module is reachable", linked.stdout)

        files["crates/prefrontal-routing-module/src/lib.rs"] = "#[cfg(test)]\npub mod module;\n"
        test_only = self.run_gate(files)
        self.assertNotEqual(test_only.returncode, 0, test_only.stdout + test_only.stderr)
        self.assertIn("prefrontal-routing-core/feature", test_only.stderr)

        files["crates/prefrontal-routing-module/src/lib.rs"] = "pub mod module;\n"
        files["crates/prefrontal-routing-module/src/main.rs"] = "fn main() {}\n"
        unlinked = self.run_gate(files)
        self.assertNotEqual(unlinked.returncode, 0, unlinked.stdout + unlinked.stderr)
        self.assertIn("prefrontal-routing-core/feature", unlinked.stderr)

    def test_duplicate_declaration_is_refused(self) -> None:
        result = self.run_gate(
            {
                "crates/prefrontal-routing-module/src/main.rs": (
                    "mod hires;\n"
                    '#[path = "hires.rs"]\n'
                    "mod host_hires;\n"
                    "fn main() {}\n"
                ),
                "crates/prefrontal-routing-module/src/hires.rs": "pub fn run() {}\n",
            }
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("crates/prefrontal-routing-module/src/hires.rs", result.stderr)
        self.assertIn("crates/prefrontal-routing-module/src/main.rs:1", result.stderr)
        self.assertIn("crates/prefrontal-routing-module/src/main.rs:3", result.stderr)
        self.assertIn(
            "declared twice; a file compiles once per declaration and its tests run once per path",
            result.stderr,
        )

    def test_path_declaration_resolves_file_and_seeds_references(self) -> None:
        result = self.run_gate(
            {
                "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
                "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
                "crates/prefrontal-routing-module/src/main.rs": (
                    '#[path = "sub/x.rs"]\nmod bridge;\nfn main() {}\n'
                ),
                "crates/prefrontal-routing-module/src/sub/x.rs": (
                    "pub fn run() { prefrontal_routing_core::feature::run(); }\n"
                ),
            }
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_undeclared_file_is_refused(self) -> None:
        result = self.run_gate(
            {
                "crates/prefrontal-routing-module/src/main.rs": "fn main() {}\n",
                "crates/prefrontal-routing-module/src/orphan.rs": "pub fn run() {}\n",
            }
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("no `mod` declaration", result.stderr)
        self.assertIn("crates/prefrontal-routing-module/src/orphan.rs", result.stderr)

    def test_include_resolves_file_without_a_mod_declaration(self) -> None:
        files = self.wiring_fixture('include!("bridge.rs");')
        result = self.run_gate(files)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("every declared module is reachable", result.stdout)

        for source in [
            '// include!("bridge.rs");\nfn main() {}\n',
            'const PROSE: &str = r#"\ninclude!("bridge.rs");\n"#;\nfn main() {}\n',
        ]:
            files["crates/prefrontal-routing-module/src/main.rs"] = source
            not_code = self.run_gate(files)
            self.assertNotEqual(not_code.returncode, 0, not_code.stdout + not_code.stderr)
            self.assertIn("no `mod` declaration", not_code.stderr)

    def test_manifest_dir_include_resolves_from_the_crate_root(self) -> None:
        include = 'include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/nested_tests.rs"));'
        files = {
            "crates/prefrontal-routing-module/Cargo.toml": "[package]\nname = \"m\"\n",
            "crates/prefrontal-routing-module/src/main.rs": "mod sub;\nfn main() {}\n",
            "crates/prefrontal-routing-module/src/sub/mod.rs": f"{include}\n",
            "crates/prefrontal-routing-module/src/nested_tests.rs": "pub fn run() {}\n",
        }
        result = self.run_gate(files)
        self.assertNotIn("no `mod` declaration", result.stderr, result.stdout + result.stderr)

        # The same path resolved from the including file's directory does not
        # exist, so a checker that ignored the crate root would refuse the file.
        del files["crates/prefrontal-routing-module/Cargo.toml"]
        unresolved = self.run_gate(files)
        self.assertIn("no `mod` declaration", unresolved.stderr)

    def test_test_only_include_does_not_wire_production(self) -> None:
        result = self.run_gate(self.wiring_fixture(
            '#[cfg(test)]\nmod tests {\n    include!("bridge.rs");\n}'
        ))
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("no `mod` declaration", result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_allowlisted_unreachable_module_prints_reason(self) -> None:
        reason = "waiting for a production composition owner"
        result = self.run_gate(
            {
                "crates/prefrontal-routing-core/src/lib.rs": "pub mod dormant;\n",
                "crates/prefrontal-routing-core/src/dormant.rs": "pub fn run() {}\n",
                "crates/prefrontal-routing-module/src/main.rs": "fn main() {}\n",
            },
            f"prefrontal-routing-core/dormant  # {reason}\n",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(reason, result.stdout)
        self.assertIn("every declared module is reachable", result.stdout)

    def test_test_support_gated_module_is_test_only(self) -> None:
        for gate in [
            '#[cfg(feature = "test-support")]',
            '#[cfg(any(test, feature = "test-support"))]',
        ]:
            with self.subTest(gate=gate):
                result = self.run_gate({
                    "crates/prefrontal-routing-core/src/lib.rs": f"{gate}\npub mod fixtures;\n",
                    "crates/prefrontal-routing-core/src/fixtures.rs": "pub fn seed() {}\n",
                    "crates/prefrontal-routing-module/src/main.rs": "fn main() {}\n",
                })
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("every declared module is reachable", result.stdout)

        # A string or comment that spells the gate is not an attribute: the
        # production reference after it still wires the module.
        for text in [
            'const GATE: &str = r#"#[cfg(feature = "test-support")]"#;\n',
            '// #[cfg(feature = "test-support")]\n',
        ]:
            with self.subTest(text=text):
                result = self.run_gate({
                    "crates/prefrontal-routing-core/src/lib.rs": "pub mod feature;\n",
                    "crates/prefrontal-routing-core/src/feature.rs": "pub fn run() {}\n",
                    "crates/prefrontal-routing-module/src/main.rs": (
                        f"{text}fn main() {{ prefrontal_routing_core::feature::run(); }}\n"
                    ),
                })
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("every declared module is reachable", result.stdout)

    def test_test_support_gated_wiring_is_rejected(self) -> None:
        result = self.run_gate(
            self.wiring_fixture('#[cfg(feature = "test-support")]\nmod bridge;')
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("prefrontal-routing-core/feature", result.stderr)

    def test_production_enabled_test_support_is_refused(self) -> None:
        files = {
            "crates/prefrontal-routing-core/src/lib.rs": (
                '#[cfg(feature = "test-support")]\npub mod fixtures;\n'
            ),
            "crates/prefrontal-routing-core/src/fixtures.rs": "pub fn seed() {}\n",
            "crates/prefrontal-routing-module/src/main.rs": "fn main() {}\n",
            "crates/prefrontal-routing-core/Cargo.toml": (
                '[features]\ntest-support = []\n'
            ),
            "crates/prefrontal-routing-module/Cargo.toml": (
                '[features]\ntest-support = ["prefrontal-routing-core/test-support"]\n\n'
                '[dependencies]\nprefrontal-test-support = { path = "../x" }\n\n'
                '[dev-dependencies]\n'
                'prefrontal-routing-core = { path = "../prefrontal-routing-core", features = ["test-support"] }\n'
            ),
        }
        allowed = self.run_gate(files)
        self.assertEqual(allowed.returncode, 0, allowed.stdout + allowed.stderr)

        for manifest in [
            '[dependencies]\nprefrontal-routing-core = { path = "../prefrontal-routing-core", features = ["test-support"] }\n',
            '[features]\ndefault = [\n    "prefrontal-routing-core/test-support",\n]\n',
            '[target.\'cfg(unix)\'.dependencies]\nprefrontal-routing-core = { path = "../prefrontal-routing-core", features = ["test-support"] }\n',
        ]:
            with self.subTest(manifest=manifest):
                refused = self.run_gate(
                    {**files, "crates/prefrontal-routing-module/Cargo.toml": manifest}
                )
                self.assertNotEqual(refused.returncode, 0, refused.stdout + refused.stderr)
                self.assertIn("test-support feature is enabled outside dev-dependencies", refused.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
