#!/usr/bin/env bash
# Repository source censuses run before the standalone routing suite.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check

python3 -m unittest scripts/tests/test_unreachable_modules.py
python3 scripts/unreachable-modules.py
python3 -m unittest scripts/tests/test_test_binary_fold.py
python3 scripts/test-binary-fold.py
python3 -m unittest scripts/tests/test_rust_include_boundary.py
python3 scripts/rust-include-boundary.py
python3 -m unittest scripts/tests/test_test_scratch.py
python3 scripts/test-scratch.py
python3 -m unittest scripts/tests/test_test_self_exec.py
python3 scripts/test-self-exec.py
python3 scripts/test-dev-binaries.py
python3 -m unittest scripts/tests/test_subc_daemon_env.py
python3 scripts/subc-daemon-env.py
python3 -m unittest scripts/tests/test_write_transactions.py
python3 scripts/write-transactions.py
python3 -m unittest scripts/tests/test_convert_consumer_names.py

# The tests build with test-support features, which hide code that only
# compiles there. Check the shipped binary as it is built for deploy.
cargo check --locked -p striatum-module --bin ck-striatum
cargo nextest run --locked --lib --bins --tests
printf 'gate summary: source checks and routing tests passed\n'
