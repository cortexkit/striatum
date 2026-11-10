"""Check signing policy with a fake codesign; never sign or place a real binary."""
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("verify_module_signing", ROOT / "script/verify-module-signing.py")
signing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signing)


class RoutingSigningTests(unittest.TestCase):
    def verify(self, identifier):
        results = [subprocess.CompletedProcess([], 0), subprocess.CompletedProcess(
            [], 0, stdout=b"", stderr=f"Identifier={identifier}\nflags=0x10000(runtime)\n".encode()
        )]
        with patch.object(signing.subprocess, "run", side_effect=results):
            signing.verify("scratch-binary", "routing")

    def test_accepts_striatum_identifier_for_routing(self):
        self.verify("ck-striatum")

    def test_refuses_retired_routing_identifier(self):
        with self.assertRaisesRegex(ValueError, "expected signing identifier ck-striatum"):
            self.verify("ck-prefrontal-routing")

    def test_refuses_identifier_with_valid_name_only_as_a_prefix(self):
        with self.assertRaisesRegex(ValueError, "expected signing identifier ck-striatum"):
            self.verify("ck-striatum-other")


if __name__ == "__main__":
    unittest.main()
