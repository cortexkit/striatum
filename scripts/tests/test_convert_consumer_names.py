import contextlib
import io
from pathlib import Path
import runpy
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CONVERTER = runpy.run_path(str(ROOT / "scripts/convert-consumer-names.py"))
convert = CONVERTER["convert"]
Jsonc = CONVERTER["Jsonc"]


class ConvertConsumerNames(unittest.TestCase):
    def test_comments_and_nonconsumer_strings_are_byte_identical(self):
        source = '''{\r
  // mason, sidekick and lab_planner are words in a comment.\r
  "consumers": {"mason": {"variants": {"mason/model": "sidekick"}, "below_bar": "refuse"}},\r
  "providers": {"mason": {"only_for": ["sidekick"], "models": ["mason"]}},\r
  "models": {"p/model": {"not_for": ["athena.gather"], "pin_only_for": ["flow"]}},\r
  "default_variants": {"mason": "sidekick"} /* athena */\r
}\r
'''
        expected = source.replace('"consumers": {"mason"', '"consumers": {"prefrontal.mason"')
        expected = expected.replace('["sidekick"]', '["prefrontal.sidekick"]')
        expected = expected.replace('["athena.gather"]', '["prefrontal.athena.gather"]')
        expected = expected.replace('["flow"]', '["basal.flow"]')
        converted, counts, unknown = convert(source)
        self.assertEqual(converted, expected)
        self.assertEqual(counts, {"mason": 1, "sidekick": 1, "athena.gather": 1, "flow": 1})
        self.assertEqual(unknown, [])

    def test_drops_retired_names_but_preserves_their_comments(self):
        for source in [
            '{"consumers": {"lab_planner": {/* keep */ "pin": {"model": "p/m"}}, "mason": {}}}',
            '{"consumers": {"mason": {}, "lab_planner": {}}}',
            '{"consumers": {"lab_planner": {}}}',
            '{"models": {"p/m": {"not_for": ["lab_planner", /* keep */ "mason", "mc_dream_docs"]}}}',
        ]:
            with self.subTest(source=source):
                converted, _, unknown = convert(source)
                self.assertEqual(unknown, [])
                self.assertNotIn('"lab_planner"', converted)
                self.assertNotIn('"mc_dream_docs"', converted)
                if "/* keep */" in source:
                    self.assertIn("/* keep */", converted)
                Jsonc(converted)

    def test_verify_is_copied_to_both_siblings(self):
        source = '{"consumers":{"mc_dream_verify":{/* once */ "below_bar":"refuse"}},"models":{"p/m":{"only_for":["mc_dream_verify"]}}}'
        converted, _, _ = convert(source)
        value = Jsonc(converted).root.value
        self.assertEqual(value["consumers"], {
            "magic-context.dream.verify": {"below_bar": "refuse"},
            "magic-context.dream.verify-broad": {"below_bar": "refuse"},
        })
        self.assertEqual(value["models"]["p/m"]["only_for"], ["magic-context.dream.verify", "magic-context.dream.verify-broad"])
        self.assertEqual(converted.count("/* once */"), 1)

    def test_all_approved_pairs_match_the_checked_in_twin(self):
        fixture = ROOT / "crates/striatum-module/fixtures"
        source = (fixture / "consumer-names-old.jsonc").read_text()
        expected = (fixture / "consumer-names-new.jsonc").read_text()
        converted, _, unknown = convert(source)
        self.assertEqual(unknown, [])
        self.assertEqual(converted, expected)
        self.assertEqual(convert(expected)[0], expected)

    def test_unknown_names_in_each_position_stop_conversion(self):
        for source in [
            '{"consumers":{"surprise":{}}}',
            '{"providers":{"p":{"only_for":["surprise"]}}}',
            '{"models":{"p/m":{"not_for":["surprise"],"pin_only_for":["other"]}}}',
        ]:
            with self.subTest(source=source):
                converted, _, unknown = convert(source)
                self.assertIsNone(converted)
                self.assertIn("surprise", unknown)

    def test_collision_and_duplicate_keys_are_refused(self):
        for source in [
            '{"consumers":{"mason":{},"prefrontal.mason":{}}}',
            '{"consumers":{"mason":{},"mason":{}}}',
            '{"consumers":{"mc_dream_verify":{},"magic-context.dream.verify-broad":{}}}',
        ]:
            with self.subTest(source=source), self.assertRaises(ValueError):
                convert(source)

    def test_builtins_and_operator_media_consumer_are_unchanged(self):
        source = '{"consumers":{"head":{},"classifier":{},"media.smoke":{},"runners":{}}}'
        self.assertEqual(convert(source)[0], source)

    def test_cli_is_dry_run_and_never_overwrites(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            source = Path(directory) / "settings.jsonc"
            output = Path(directory) / "converted.jsonc"
            original = b'{"consumers":{"mason":{}}}\r\n'
            source.write_bytes(original)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(CONVERTER["main"]([str(source)]), 0)
                self.assertFalse(output.exists())
                self.assertEqual(CONVERTER["main"]([str(source), "--write", str(source)]), 1)
                self.assertEqual(CONVERTER["main"]([str(source), "--write", str(output)]), 0)
                first = output.read_bytes()
                self.assertEqual(CONVERTER["main"]([str(source), "--write", str(output)]), 1)
                self.assertEqual(output.read_bytes(), first)
                source.write_text('{"consumers":{"unknown":{}}}')
                blocked = Path(directory) / "blocked.jsonc"
                self.assertEqual(CONVERTER["main"]([str(source), "--write", str(blocked)]), 1)
                self.assertFalse(blocked.exists())
            self.assertEqual(first, original.replace(b'"mason"', b'"prefrontal.mason"'))


if __name__ == "__main__":
    unittest.main()
