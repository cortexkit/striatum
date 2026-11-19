use super::*;

// Store callback boundaries are implemented by module writer commands, not
// by calling the old store transaction wrappers from a request. These are
// writer obligations, never zero-write reader exclusions.
const CALLBACK_BOUNDARIES: [(&str, &str); 3] = [
    (
        "direct_account_transaction",
        "module_adapter::execute_with_error",
    ),
    ("account_transaction", "module_adapter::execute"),
    ("account_repair", "module_adapter::repair"),
];

// Selection grants directly to a decision. The focused handoff test checks the
// branch of `AccountTx::bind_decision_probe` that moves an existing authorization
// between owners. Routing does not persist a probe claim before the decision ID
// exists, so only the focused test creates such a claim.
const NATIVE_TRANSFER: &str = "account_tx::fence_rechecks::transfer_probe_authorization";

pub(super) fn require_focused_transfer(
    expected: &mut BTreeSet<String>,
    observed: &BTreeSet<String>,
) {
    assert!(
        expected.remove(NATIVE_TRANSFER),
        "missing focused writer declaration: {NATIVE_TRANSFER}"
    );
    assert!(
        !observed.contains(NATIVE_TRANSFER),
        "routing replay exercised the focused handoff: {NATIVE_TRANSFER}"
    );
}

pub(super) fn assert_focused_transfer_wrote(step: &[striatum_store::writer_entry_trace::Entry]) {
    assert!(
        step.iter().any(|entry| entry.name == NATIVE_TRANSFER
            && entry.account_rows_written.is_some_and(|rows| rows > 0)),
        "focused handoff did not write through {NATIVE_TRANSFER}: {step:?}"
    );
}

pub(super) fn assert_reconciled(trace: &striatum_store::writer_entry_trace::Trace) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/account-writer-entry-points.txt");
    let text = std::fs::read_to_string(path).unwrap();
    let mut declarations = declared_adapters();
    declarations.extend(declared_mutable_commands());
    declarations.extend(metadata::native_declarations());
    let observed: BTreeSet<_> = trace
        .entries()
        .into_iter()
        .map(|entry| entry.name.to_owned())
        .collect();
    let mut catalogued = BTreeSet::new();
    for name in text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        assert!(
            name.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
            "unrecognised canonical writer entry: {name}"
        );
        assert!(
            catalogued.insert(name),
            "duplicate canonical writer entry: {name}"
        );
        let matching: BTreeSet<String> = if let Some((_, command)) = CALLBACK_BOUNDARIES
            .iter()
            .find(|(boundary, _)| *boundary == name)
        {
            assert!(
                declarations.contains(*command),
                "callback boundary lost its writer command: {name} => {command}"
            );
            [(*command).to_owned()].into_iter().collect()
        } else {
            declarations
                .iter()
                .filter(|declaration| declaration.rsplit("::").next() == Some(name))
                .cloned()
                .collect()
        };
        assert!(
            !matching.is_empty(),
            "canonical store writer has no discovered module command or native producer: {name}"
        );
        // The read-only store entry remains in the canonical
        // call-site gate. Its zero-write trace guard is checked separately.
        let writing: Vec<_> = matching
            .iter()
            .filter(|candidate| {
                !read_only::entries().any(|(reader, _)| *reader == candidate.as_str())
            })
            .collect();
        if writing.is_empty() {
            assert_eq!(
                name, "pool_key_states",
                "new canonical non-writer needs review: {name}"
            );
            continue;
        }
        assert!(writing.iter().any(|candidate|observed.contains(candidate.as_str())),
            "canonical writer entry was not observed through its real producer: {name} => {writing:?}");
    }
    assert!(
        !catalogued.is_empty(),
        "the independent store writer catalogue was not read"
    );
    for (boundary, _) in CALLBACK_BOUNDARIES {
        assert!(
            catalogued.contains(boundary),
            "stale callback-boundary mapping: {boundary}"
        );
    }
}
