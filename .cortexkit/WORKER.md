# Worker guide

Standing rules for workers in the routing repository. Your brief can override any of them; where it doesn't, these apply without asking. "Your head" below is the agent that launched you: it answers your questions, reviews your delivery, and merges it.

Run every command from the worktree root. Run one cargo command at a time, never several in parallel: they fight over the same target directory lock.

## Building and testing

- **Builds and tests run on remote Linux build servers,** through the `runon` parameter of your bash tool (it names the server and the CPUs and memory the job reserves). Use `runon: "linux,4c"` for `cargo check` and focused tests, and `runon: "linux,8c,16gb"` for the full gate. Add `--locked` to cargo commands.
- **Focused tests:** filter by name, for example `cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(/account_selection::/)'`. A filtered run that takes more than about 15 minutes means the filter is too broad.
- **Full gate:** `scripts/rust-test-gate.sh`. It passed when it prints `gate summary: source checks and routing tests passed` and an `N tests run: N passed` line with no failures. Run it on your committed tree when your brief asks for it, and quote both lines in your report.
- **Remote jobs can't change tracked files.** Run `cargo fmt --all` locally in your worktree, not in a remote job. Regenerate samples and goldens locally, or in a remote job that names each changed file in `return_paths`.
- **Local compiles:** if you must compile locally (only to regenerate samples), build into `$HOME/.cache/ck-worker-target-<your task id>`, never the checkout's `target/`. That directory links to an external drive, and each new test program built there raises a macOS permission prompt that stalls the build. Delete the folder when you are done.

## Proving a change

- **A bug fix or behavior change needs a test that fails without it.** Prove it: temporarily restore the old source, run the test and record the red output, then restore your source byte for byte. Keep that output in your report.
- **A performance change** needs a counter or structural check, not a timing assertion. Measure speed with paired runs in one build job, alternating old and new.

## Existing expectations

These changes to existing tests are pre-approved:
- regenerating a recorded sample, golden file or pinned value (a stored digest or byte count a test compares against) whose bytes come out identical;
- pointing a check at a renamed or replaced table while still comparing every field, with every expected value unchanged.

Any other change to an existing expected value, fixture or assertion comes to your head first, with the old line, the new line and the reason. Never raise a timeout, loosen an assertion, mark a test ignored, shrink a test's inputs, or weaken a gate check. A test that times out or flakes is diagnosed to its cause.

## Code rules

- **File size:** hand-written source files stay under 5,000 lines. The files already over it are listed in `scripts/source-size.allow` and may not grow; put new code in a new module. Only the standard `@generated` marker exempts a file.
- **Store writes** open their transaction through the helpers in `crates/striatum-store/src/transactions.rs`, which take SQLite's write lock up front. `scripts/write-transactions.py`, run by the gate, rejects any other transaction opening, because a transaction that reads first and writes later can fail when another writer gets in between.
- **Migrations** take the next free number in `crates/striatum-store/migrations`. Never edit a migration that has been applied; write a new one. Your head rehearses migrations on a copy of the live store.
- **Settings schema:** regenerate it with `ck-striatum --print-config-schema`; never edit it by hand.
- **Tests** take scratch paths from `cortexkit-test-support` and never touch the live store or `$HOME`. You have no access to the live store; ask your head for a read-only export when you need live data.
- **Comments** explain why the code is the way it is, for an engineer with none of your context. Never cite task ids, audit finding numbers or plan labels.

## Reports and delivery

- Write reports under `.cortexkit/internal-docs/investigations/` (never `reports/`) and scripts under `.cortexkit/scripts/`. They stay uncommitted; they reach your head with the delivery.
- Commit your code on your task branch. Never `git push`; your head merges and pushes.
