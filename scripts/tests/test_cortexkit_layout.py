import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/cortexkit-layout.py"


class CortexkitLayoutTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(tempfile.gettempdir()) / "striatum-tests"
        scratch.mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix="cortexkit-layout-", dir=scratch)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = os.environ.copy()
        self.env.update({"HOME": str(self.root), "XDG_CONFIG_HOME": str(self.root),
                         "GIT_CONFIG_NOSYSTEM": "1"})
        self.git("init", "-q", "--template=")

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, env=self.env,
                              capture_output=True, text=True, check=True)

    def add(self, path, content="content\n"):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        self.git("add", "--", path)

    def check(self, cwd=None):
        return subprocess.run([sys.executable, str(CHECK)], cwd=cwd or self.root,
                              env=self.env, capture_output=True, text=True)

    def test_allowed_tree_passes(self):
        for path in (".cortexkit/WORKER.md", ".cortexkit/skills/x/SKILL.md",
                     ".cortexkit/rules/r.md", ".cortexkit/release-notes/n.md",
                     ".cortexkit/striatum.jsonc", ".cortexkit/skills/x/a space.md"):
            self.add(path)
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unignored_tracked_file_is_rejected_even_though_ci_misses_it(self):
        self.add(".gitignore", ".cortexkit/*\n!.cortexkit/notes.md\n")
        self.add(".cortexkit/notes.md")
        old_listing = self.git("ls-files", "-ci", "--exclude-standard", "--", ".cortexkit")
        self.assertNotIn(".cortexkit/notes.md", old_listing.stdout)
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn("tracked outside the .cortexkit allowlist:", result.stderr)
        self.assertIn(".cortexkit/notes.md", result.stderr)

    def test_nested_jsonc_is_rejected(self):
        self.add(".cortexkit/x/y.jsonc")
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn(".cortexkit/x/y.jsonc", result.stderr)

    def test_git_failure_is_not_a_pass(self):
        outside = self.root / "outside"
        outside.mkdir()
        # A separate ceiling prevents Git from finding the fixture repository above it.
        self.env["GIT_CEILING_DIRECTORIES"] = str(self.root)
        result = self.check(outside)
        self.assertEqual(result.returncode, 2)
        self.assertIn("BLIND: git ls-files failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
