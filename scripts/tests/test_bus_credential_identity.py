import pathlib
import runpy
import unittest

check = runpy.run_path(str(pathlib.Path(__file__).resolve().parents[1] / "bus-credential-identity.py"))["violations"]


class BusCredentialIdentity(unittest.TestCase):
    def test_rejects_identity_override(self):
        self.assertEqual(check("CallOptions {\n consumer_identity: Some(principal),\n}"), [2])

    def test_ignores_comments_and_strings(self):
        self.assertEqual(check('// consumer_identity\nlet note = "consumer_identity";'), [])
