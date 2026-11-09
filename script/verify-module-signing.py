#!/usr/bin/env python3
"""Check hardened runtime signing without additional entitlements."""
import argparse
import plistlib
import re
import subprocess
import sys

def verify(binary, kind):
    subprocess.run(["codesign", "--verify", "--strict", binary], check=True)
    result = subprocess.run(
        ["codesign", "-dv", "--entitlements", "-", "--xml", binary],
        capture_output=True, check=True,
    )
    text = (result.stdout + result.stderr).decode()
    if not re.search(r"flags=0x[0-9a-fA-F]+\([^\n)]*\bruntime\b", text):
        raise ValueError("missing hardened runtime flag")
    # Explicit XML avoids codesign's human-readable dictionary display.
    entitlements = plistlib.loads(result.stdout) if result.stdout.strip() else {}
    if "com.apple.security.get-task-allow" in entitlements:
        raise ValueError("get-task-allow is forbidden, even when false")
    if entitlements:
        raise ValueError("entitlements must be empty")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("kind", choices=["routing"])
    args = parser.parse_args()
    try:
        verify(args.binary, args.kind)
        print(f"signature gate passed: {args.binary} ({args.kind})")
    except (ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
