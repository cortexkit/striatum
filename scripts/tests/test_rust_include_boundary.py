import runpy
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD = runpy.run_path(str(ROOT / "scripts/rust-include-boundary.py"))


class RustIncludeBoundaryTests(unittest.TestCase):
    def scan_source(self, source, *, test_file=False):
        scratch = Path(tempfile.gettempdir()) / "prefrontal-tests"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="include-boundary-", dir=scratch) as directory:
            root = Path(directory)
            crate = root / "crates/probe"
            source_dir = crate / ("tests" if test_file else "src")
            source_dir.mkdir(parents=True)
            (crate / "Cargo.toml").write_text("[package]\nname='probe'\nversion='0.1.0'\n")
            source_path = source_dir / "lib.rs"
            source_path.write_text(source)
            return GUARD["scan"](root)

    def test_external_production_include_reds(self):
        count, errors = self.scan_source(
            'const AGENT: &str = include_str!("../../../packages/opencode/agent.ts");'
        )
        self.assertEqual(count, 1)
        self.assertEqual(len(errors), 1)
        self.assertIn("include_str! resolves outside crates/ and test-vectors/", errors[0])

    def test_raw_literals_do_not_hide_includes(self):
        _, errors = self.scan_source('''
const CONTEXT: &str = r#"{"role":"mason"}"#;
const AGENT: &str = include_str!(r#"../../../packages/opencode/agent.ts"#);
''')
        self.assertEqual(len(errors), 1)
        self.assertIn("include_str! resolves outside crates/ and test-vectors/", errors[0])

    def test_test_only_includes_are_ignored(self):
        _, cfg_errors = self.scan_source('''
#[cfg(test)]
mod source_contract {
    #[test]
    fn producer_is_present() {
        include_str!("../../../packages/opencode/agent.ts");
    }
}
''')
        _, integration_errors = self.scan_source(
            'const AGENT: &str = include_str!("../../../packages/opencode/agent.ts");',
            test_file=True,
        )
        self.assertEqual(cfg_errors, [])
        self.assertEqual(integration_errors, [])

    def test_a_file_marked_test_only_is_skipped_but_an_inline_block_is_not(self):
        _, file_errors = self.scan_source('''//! Shared test samples.
#![allow(dead_code)]
#![cfg(test)]
const AGENT: &[u8] = include_bytes!(concat!("../../../packages/", "agent.ts"));
''')
        _, inline_errors = self.scan_source('''
const AGENT: &str = include_str!("../../../packages/opencode/agent.ts");
mod inner {
    #![cfg(test)]
}
''')
        self.assertEqual(file_errors, [])
        self.assertEqual(len(inline_errors), 1)

    def test_allowed_inputs_and_literals(self):
        _, errors = self.scan_source('''
const LOCAL: &str = include_str!("../fixtures/local.txt");
const VECTOR: &str = include_str!("../../../test-vectors/example.json");
const GENERATED: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/generated.bin"));
// include_str!("../../../packages/comment-only.ts")
const EXAMPLE: &str = "include_str!(\\\"../../../packages/string-only.ts\\\")";
''')
        self.assertEqual(errors, [])

    def test_repository_include_boundary(self):
        self.assertEqual(GUARD["violations"](ROOT), [])

    def test_dynamic_production_path_fails_closed(self):
        _, errors = self.scan_source('const GENERATED: &str = include_str!(env!("CUSTOM_PATH"));')
        self.assertEqual(len(errors), 1)
        self.assertIn("cannot resolve production include_str! path", errors[0])

    MACRO = '''
macro_rules! asset {
    ($id:literal, $version:literal) => {
        include_str!(concat!("../", $id, "/", $version, "/script.js"))
    };
}
'''

    def test_macro_include_is_checked_at_each_invocation(self):
        _, allowed = self.scan_source(self.MACRO + 'const A: &str = asset!("flows/watch", 3);\n')
        self.assertEqual(allowed, [])
        _, errors = self.scan_source(
            self.MACRO
            + 'const A: &str = asset!("flows/watch", 3);\n'
            + 'const B: &str = asset!("../../../packages/core", 1);\n'
        )
        self.assertEqual(len(errors), 1)
        self.assertIn("(via asset! at line 8)", errors[0])
        self.assertIn("include_str! resolves outside crates/ and test-vectors/", errors[0])

    def test_macro_include_without_mappable_invocation_fails_closed(self):
        _, uninvoked = self.scan_source(self.MACRO)
        self.assertEqual(len(uninvoked), 1)
        self.assertIn("cannot resolve production include_str! path", uninvoked[0])
        _, repeated = self.scan_source('''
macro_rules! assets {
    ($($id:literal),*) => {
        [$(include_str!(concat!("../", $id))),*]
    };
}
const A: [&str; 1] = assets!("flows/watch");
''')
        self.assertEqual(len(repeated), 1)
        self.assertIn("cannot resolve production include_str! path", repeated[0])

    def test_repeated_literal_includes_check_every_filename(self):
        macro = '''
macro_rules! samples {
    ($($name:literal),+ $(,)?) => {
        const SAMPLES: &[&[u8]] = &[$(
            include_bytes!(concat!("../../../test-vectors/", $name)),
        )+];
    };
}
'''
        _, allowed = self.scan_source(macro + 'samples!("one.json", "two.json",);')
        self.assertEqual(allowed, [])
        _, errors = self.scan_source(
            macro + 'samples!("one.json", "../packages/core/contract.json",);'
        )
        self.assertEqual(len(errors), 1)
        self.assertIn("include_bytes! resolves outside crates/ and test-vectors/", errors[0])

    def test_repeated_literal_include_with_unknown_argument_fails_closed(self):
        _, errors = self.scan_source('''
macro_rules! samples {
    ($($name:literal),+ $(,)?) => {
        const SAMPLES: &[&[u8]] = &[$(include_bytes!(concat!("../", $name))),+];
    };
}
samples!("one.json", env!("CUSTOM_PATH"));
''')
        self.assertEqual(len(errors), 1)
        self.assertIn("cannot resolve production include_bytes! path", errors[0])


if __name__ == "__main__":
    unittest.main()
