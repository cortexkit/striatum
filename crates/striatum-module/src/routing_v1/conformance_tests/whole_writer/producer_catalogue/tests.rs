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
