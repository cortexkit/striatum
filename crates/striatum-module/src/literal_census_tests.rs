//! Literal census: routing never names a provider, model or runner in code.
//!
//! Every provider-specific fact reaches routing from Fusiform's catalog, the
//! vault's credential listing, Insula readings, a runner's declaration or
//! operator config. This census holds that rule in place: it reads every Rust
//! source under the `src/` trees of striatum-core,
//! striatum-store and striatum-module, collects their
//! string literals, and fails when one names a listed id.
//!
//! The listed ids are:
//! - the provider ids, model ids and `provider/model` keys of the Fusiform
//!   catalog fixture `fixtures/fusiform-catalog-census.json`;
//! - the vault provider ids (`provider_ids`) of the credential-listing
//!   fixtures `fixtures/claustrum-list-scoped.json` and the
//!   `credential_listing` of `fixtures/routing-baseline-2026-10-24.json`;
//! - the runner ids `broca`, `opencode` and `pi`.
//!
//! Header names are not ids and are not listed.
//!
//! Out of scope are test code (files whose names end in `_tests.rs`, modules
//! and items compiled only under `#[cfg(test)]`, and `#[test]` functions), data
//! files such as `data/config_convert_defaults.json` (only `.rs` files are
//! read), and the items in [`EXEMPTIONS`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Runner ids. Runners declare themselves; routing code never names one.
const RUNNER_IDS: &[&str] = &["broca", "opencode", "pi"];

/// One item the census does not read.
struct Exemption {
    /// Path relative to the workspace `crates/` directory.
    file: &'static str,
    item: ExemptItem,
}

enum ExemptItem {
    /// A function, with the type of the `impl` block holding it for methods.
    Function {
        impl_type: Option<&'static str>,
        name: &'static str,
    },
    /// A `const` item.
    Const(&'static str),
    /// The whole module that `file` holds.
    Module,
}

/// The only items the census does not read.
const EXEMPTIONS: &[Exemption] = &[
    // Local exhaustion evidence still parses raw Anthropic header names until
    // runners send normalized readings.
    Exemption {
        file: "striatum-module/src/account_selection.rs",
        item: ExemptItem::Function {
            impl_type: None,
            name: "header_fences",
        },
    },
    // The runner names fixed by the legacy route.select, route.select_panel
    // and account wire shapes, which today's core sends and decision rows
    // store. The module is deleted with those shapes (board item 22616ba9).
    Exemption {
        file: "striatum-module/src/legacy_wire.rs",
        item: ExemptItem::Module,
    },
    // A test-only module (declared under `#[cfg(test)]`) that copies a test
    // program off an external drive before a test runs it again; it names no
    // provider or runner.
    Exemption {
        file: "striatum-store/src/test_exe.rs",
        item: ExemptItem::Module,
    },
    // Model-family names, not provider references: the stored and wire
    // spellings of each grand family (some equal a provider id, such as
    // `deepseek`), and the historical store spellings (`anthropic`, `openai`)
    // that the versioned family normalization migration still accepts.
    Exemption {
        file: "striatum-core/src/model_facts.rs",
        item: ExemptItem::Function {
            impl_type: Some("GrandFamily"),
            name: "as_str",
        },
    },
    Exemption {
        file: "striatum-core/src/model_facts.rs",
        item: ExemptItem::Const("GRAND_FAMILY_NORMALIZATIONS"),
    },
];

/// Crates whose `src/` trees the census reads.
const SCANNED_CRATES: &[&str] = &["striatum-core", "striatum-store", "striatum-module"];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the routing module sits under crates/")
        .to_path_buf()
}

fn fixture(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parsing {}: {error}", path.display()))
}

/// Which source an id was listed from; the controls plant one of each.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum IdClass {
    CatalogProvider,
    CatalogModel,
    CatalogKey,
    VaultProvider,
    Runner,
}

/// Every listed id with the classes it was listed under.
fn listed_ids() -> BTreeMap<String, BTreeSet<IdClass>> {
    let mut ids: BTreeMap<String, BTreeSet<IdClass>> = BTreeMap::new();
    let mut add = |id: &str, class: IdClass| {
        let id = id.trim();
        if !id.is_empty() {
            ids.entry(id.to_ascii_lowercase())
                .or_default()
                .insert(class);
        }
    };

    let catalog = fixture("fusiform-catalog-census.json");
    let models = catalog["response"]["models"]
        .as_object()
        .expect("the catalog fixture carries response.models");
    assert!(!models.is_empty(), "the catalog fixture lists no models");
    for key in models.keys() {
        // Fusiform keys a served model by `provider/model`; the model id may
        // itself hold a `/`, so the key splits once, as fusiform_catalog.rs does.
        let (provider, model) = key
            .split_once('/')
            .unwrap_or_else(|| panic!("catalog key {key:?} is not provider/model"));
        add(key, IdClass::CatalogKey);
        add(provider, IdClass::CatalogProvider);
        add(model, IdClass::CatalogModel);
    }

    let mut vault = Vec::new();
    collect_provider_ids(&fixture("claustrum-list-scoped.json"), &mut vault);
    collect_provider_ids(
        &fixture("routing-baseline-2026-10-24.json")["credential_listing"],
        &mut vault,
    );
    assert!(
        !vault.is_empty(),
        "the credential listings carry no provider_ids"
    );
    for id in &vault {
        add(id, IdClass::VaultProvider);
    }

    for id in RUNNER_IDS {
        add(id, IdClass::Runner);
    }
    ids
}

fn collect_provider_ids(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "provider_ids" {
                    out.extend(
                        value
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_string),
                    );
                } else {
                    collect_provider_ids(value, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_provider_ids(item, out);
            }
        }
        _ => {}
    }
}

/// A string literal's contents and the byte offset where they start.
struct Literal {
    at: usize,
    text: String,
}

/// A lexed source file: its string literals, and a copy of the source with
/// every comment, string and character literal blanked to spaces (newlines
/// kept) so braces and attributes can be found without lexing again.
struct Lexed {
    literals: Vec<Literal>,
    code: Vec<u8>,
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn blank(code: &mut [u8], from: usize, to: usize) {
    for byte in &mut code[from..to] {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

fn lex(source: &str) -> Lexed {
    let bytes = source.as_bytes();
    let mut code = bytes.to_vec();
    let mut literals = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let rest = &bytes[i..];
        let after_ident = i > 0 && is_ident_byte(bytes[i - 1]);
        if rest.starts_with(b"//") {
            let end = rest
                .iter()
                .position(|&b| b == b'\n')
                .map_or(bytes.len(), |n| i + n);
            blank(&mut code, i, end);
            i = end;
        } else if rest.starts_with(b"/*") {
            let start = i;
            let mut depth = 0usize;
            while i < bytes.len() {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            blank(&mut code, start, i);
        } else if let Some(hashes) = (!after_ident).then(|| raw_string_open(rest)).flatten() {
            // r"..", r#".."#, br"..", cr"..": no escapes; ends at a quote
            // followed by the same number of hashes.
            let (prefix, hashes) = hashes;
            let body = i + prefix;
            let mut close = vec![b'"'];
            close.extend(std::iter::repeat_n(b'#', hashes));
            let end = find(bytes, body, &close).expect("unterminated raw string");
            literals.push(Literal {
                at: body,
                text: source[body..end].to_string(),
            });
            blank(&mut code, i, end + close.len());
            i = end + close.len();
        } else if bytes[i] == b'"'
            || (!after_ident && (rest.starts_with(b"b\"") || rest.starts_with(b"c\"")))
        {
            let body = if bytes[i] == b'"' { i + 1 } else { i + 2 };
            let mut j = body;
            while bytes[j] != b'"' {
                j += if bytes[j] == b'\\' { 2 } else { 1 };
            }
            literals.push(Literal {
                at: body,
                text: source[body..j].to_string(),
            });
            blank(&mut code, i, j + 1);
            i = j + 1;
        } else if bytes[i] == b'\'' {
            i = skip_char_literal(source, &mut code, i);
        } else {
            i += 1;
        }
    }
    Lexed { literals, code }
}

/// The opening of a raw string at the start of `rest`: the length of its
/// prefix through the opening quote, and its number of hashes.
fn raw_string_open(rest: &[u8]) -> Option<(usize, usize)> {
    let mut at = match rest {
        [b'r', ..] => 1,
        [b'b' | b'c', b'r', ..] => 2,
        _ => return None,
    };
    let hashes = rest[at..].iter().take_while(|&&b| b == b'#').count();
    at += hashes;
    (rest.get(at) == Some(&b'"')).then_some((at + 1, hashes))
}

fn find(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes[from..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|n| from + n)
}

/// Skip a character literal starting at the quote at `i`, or only the quote
/// when it opens a lifetime or a loop label.
fn skip_char_literal(source: &str, code: &mut [u8], i: usize) -> usize {
    let bytes = source.as_bytes();
    if bytes.get(i + 1) == Some(&b'\\') {
        // An escape: `'\n'`, `'\''`, `'\u{..}'`. The escaped byte never ends it.
        let end = find(bytes, i + 3, b"'").expect("unterminated character literal");
        blank(code, i, end + 1);
        return end + 1;
    }
    let Some(next) = source[i + 1..].chars().next() else {
        return i + 1;
    };
    let close = i + 1 + next.len_utf8();
    if bytes.get(close) == Some(&b'\'') {
        blank(code, i, close + 1);
        close + 1
    } else {
        i + 1
    }
}

/// The index just past the `}` matching the `{` at `open`.
fn matching_close(code: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    for (offset, &byte) in code[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return open + offset + 1;
                }
            }
            _ => {}
        }
    }
    code.len()
}

fn skip_whitespace(code: &[u8], mut at: usize) -> usize {
    while at < code.len() && code[at].is_ascii_whitespace() {
        at += 1;
    }
    at
}

/// An attribute `#[..]` or `#![..]` at `at`: its end and its text with all
/// whitespace removed.
fn attribute_at(code: &[u8], at: usize) -> Option<(usize, String)> {
    let open = if code[at..].starts_with(b"#[") {
        at + 1
    } else if code[at..].starts_with(b"#![") {
        at + 2
    } else {
        return None;
    };
    let mut depth = 0usize;
    for (offset, &byte) in code[open..].iter().enumerate() {
        match byte {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    let end = open + offset + 1;
                    let text = code[open + 1..end - 1]
                        .iter()
                        .filter(|byte| !byte.is_ascii_whitespace())
                        .map(|&byte| byte as char)
                        .collect();
                    return Some((end, text));
                }
            }
            _ => {}
        }
    }
    None
}

fn is_test_gate(attribute: &str) -> bool {
    matches!(attribute, "cfg(test)" | "test" | "tokio::test")
}

/// Where the item starting at `at` ends. An item with a body ends after its
/// matching `}`; `const`, `static`, `use`, `type` and `let` items end at their
/// `;`; a field or variant ends at its `,` or at the enclosing `}`.
fn item_end(code: &[u8], at: usize) -> usize {
    let text = String::from_utf8_lossy(&code[at..code.len().min(at + 64)]).into_owned();
    let mut words = strip_visibility(&text)
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|word| !word.is_empty());
    let first = words.next().unwrap_or("");
    let statement_like = matches!(first, "static" | "use" | "type" | "let")
        || (first == "const" && !matches!(words.next(), Some("fn" | "unsafe" | "async")));
    let mut depth = 0isize;
    let mut i = at;
    while i < code.len() {
        match code[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b'{' if statement_like => depth += 1,
            b'{' if depth == 0 => return matching_close(code, i),
            b'}' if statement_like && depth > 0 => depth -= 1,
            b'}' if depth == 0 => return i,
            b';' | b',' if depth == 0 => return i + 1,
            _ => {}
        }
        i += 1;
    }
    code.len()
}

/// `mod NAME;` at `at`, after any visibility: the module name.
fn module_declaration(code: &[u8], at: usize, end: usize) -> Option<String> {
    let text = String::from_utf8_lossy(&code[at..end]);
    let name = strip_visibility(&text)
        .strip_prefix("mod")?
        .trim()
        .strip_suffix(';')?;
    Some(name.trim().to_string())
}

/// `text` without leading whitespace and a leading `pub` or `pub(..)`.
fn strip_visibility(text: &str) -> &str {
    let text = text.trim_start();
    let Some(rest) = text.strip_prefix("pub") else {
        return text;
    };
    if rest.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return text;
    }
    let rest = rest.trim_start();
    match rest.strip_prefix('(') {
        Some(scoped) => scoped
            .split_once(')')
            .map_or("", |(_, rest)| rest.trim_start()),
        None => rest,
    }
}

/// What one file's structure keeps out of the census: byte ranges, and the
/// files of modules it declares under a test gate.
struct Excluded {
    ranges: Vec<(usize, usize)>,
    whole_file: bool,
    test_module_files: Vec<String>,
}

fn test_gated_code(rel_path: &str, source: &str, code: &[u8]) -> Excluded {
    let mut excluded = Excluded {
        ranges: Vec::new(),
        whole_file: false,
        test_module_files: Vec::new(),
    };
    let mut i = 0;
    while i < code.len() {
        let Some((after, text)) = (code[i] == b'#').then(|| attribute_at(code, i)).flatten() else {
            i += 1;
            continue;
        };
        if code[i..].starts_with(b"#!") {
            excluded.whole_file |= text == "cfg(test)";
            i = after;
            continue;
        }
        // A run of attributes on one item: gated when any is a test gate.
        let start = i;
        let mut gated = is_test_gate(&text);
        let mut path = attribute_path(&source[i..after]);
        let mut item = skip_whitespace(code, after);
        while let Some((after, text)) = attribute_at(code, item) {
            gated |= is_test_gate(&text);
            path = path.or_else(|| attribute_path(&source[item..after]));
            item = skip_whitespace(code, after);
        }
        if !gated {
            i = item;
            continue;
        }
        let end = item_end(code, item);
        if let Some(name) = module_declaration(code, item, end) {
            excluded
                .test_module_files
                .extend(module_files(rel_path, &name, path.as_deref()));
        }
        excluded.ranges.push((start, end));
        i = end;
    }
    excluded
}

/// The file named by `#[path = "..."]`, read from the attribute's source
/// text (the blanked copy has lost the string).
fn attribute_path(attribute: &str) -> Option<String> {
    let inner = attribute.strip_prefix("#[")?.strip_suffix(']')?.trim();
    let value = inner.strip_prefix("path")?.trim_start().strip_prefix('=')?;
    Some(value.trim().trim_matches('"').to_string())
}

/// The files that may hold module `name` declared in `rel_path`.
fn module_files(rel_path: &str, name: &str, path_attribute: Option<&str>) -> Vec<String> {
    let file = Path::new(rel_path);
    let dir = file.parent().expect("a source file sits in a directory");
    if let Some(path) = path_attribute {
        return vec![normalize(&dir.join(path))];
    }
    let stem = file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("");
    let module_dir = if matches!(stem, "main" | "lib" | "mod") {
        dir.to_path_buf()
    } else {
        dir.join(stem)
    };
    vec![
        normalize(&module_dir.join(format!("{name}.rs"))),
        normalize(&module_dir.join(name).join("mod.rs")),
    ]
}

fn normalize(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn word_at(code: &[u8], at: usize, word: &[u8]) -> bool {
    code[at..].starts_with(word)
        && (at == 0 || !is_ident_byte(code[at - 1]))
        && code
            .get(at + word.len())
            .is_none_or(|&byte| !is_ident_byte(byte))
}

/// Byte ranges of every `impl` block whose self type is `type_name`.
fn impl_blocks(code: &[u8], type_name: &str) -> Vec<(usize, usize)> {
    let mut blocks = Vec::new();
    for at in 0..code.len() {
        if !word_at(code, at, b"impl") {
            continue;
        }
        let Some(open) = code[at..].iter().position(|&b| b == b'{' || b == b';') else {
            continue;
        };
        let open = at + open;
        if code[open] != b'{' {
            continue;
        }
        let header = String::from_utf8_lossy(&code[at + 4..open]).into_owned();
        // `impl<T> Trait for Type<T>` names its self type after `for`.
        let self_type = header
            .rsplit_once(" for ")
            .map_or(header.as_str(), |(_, ty)| ty);
        let self_type = self_type.trim_start();
        let self_type = if self_type.starts_with('<') {
            self_type.split_once('>').map_or("", |(_, ty)| ty)
        } else {
            self_type
        };
        let ident = self_type
            .trim()
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
            .next()
            .unwrap_or("");
        if ident.rsplit("::").next() == Some(type_name) {
            blocks.push((open, matching_close(code, open)));
        }
    }
    blocks
}

/// Byte ranges of every `fn name` inside `within`, signature through body.
fn function_ranges(code: &[u8], name: &str, within: (usize, usize)) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let (from, to) = within;
    for at in from..to {
        if !word_at(code, at, b"fn") {
            continue;
        }
        let name_at = skip_whitespace(code, at + 2);
        if !word_at(code, name_at, name.as_bytes()) {
            continue;
        }
        let mut depth = 0isize;
        let mut i = name_at;
        while i < to {
            match code[i] {
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b'{' if depth == 0 => {
                    ranges.push((at, matching_close(code, i)));
                    break;
                }
                b';' if depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
    }
    ranges
}

/// Byte ranges of every `const NAME` item, through its `;`.
fn const_ranges(code: &[u8], name: &str) -> Vec<(usize, usize)> {
    (0..code.len())
        .filter(|&at| word_at(code, at, b"const"))
        .filter(|&at| word_at(code, skip_whitespace(code, at + 5), name.as_bytes()))
        .map(|at| (at, item_end(code, at)))
        .collect()
}

/// The exempt item ranges in one file. Each exemption must name exactly one
/// item, so a rename or a second same-named item fails loudly instead of
/// silently widening or dropping the exemption.
fn exempt_ranges(rel_path: &str, code: &[u8]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    for exemption in EXEMPTIONS.iter().filter(|e| e.file == rel_path) {
        let (found, described) = match exemption.item {
            ExemptItem::Function { impl_type, name } => {
                let scopes = match impl_type {
                    Some(type_name) => impl_blocks(code, type_name),
                    None => vec![(0, code.len())],
                };
                let found = scopes
                    .into_iter()
                    .flat_map(|scope| function_ranges(code, name, scope))
                    .collect::<Vec<_>>();
                let owner = impl_type.map(|ty| format!("{ty}::")).unwrap_or_default();
                (found, format!("function {owner}{name}"))
            }
            ExemptItem::Const(name) => (const_ranges(code, name), format!("const {name}")),
            ExemptItem::Module => (vec![(0, code.len())], "module".to_string()),
        };
        assert_eq!(
            found.len(),
            1,
            "{rel_path}: exempt {described} must exist exactly once"
        );
        ranges.extend(found);
    }
    ranges
}

/// Whether `literal` (already lowercased) names `id` as a whole token. Ids are
/// built from ASCII letters, digits, `-`, `_` and `.`, so an id only counts
/// when no such character extends it on either side; a `.` that ends a
/// sentence is not part of a token.
fn names_id(literal: &str, id: &str) -> bool {
    let bytes = literal.as_bytes();
    let extends = |at: usize| -> bool {
        let Some(&byte) = bytes.get(at) else {
            return false;
        };
        is_ident_byte(byte)
            || byte == b'-'
            || (byte == b'.'
                && at > 0
                && bytes[at - 1].is_ascii_alphanumeric()
                && bytes.get(at + 1).is_some_and(u8::is_ascii_alphanumeric))
    };
    literal
        .match_indices(id)
        .any(|(at, _)| (at == 0 || !extends(at - 1)) && !extends(at + id.len()))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Finding {
    file: String,
    line: usize,
    id: String,
    literal: String,
}

/// Every listed id named by a string literal in `files` (relative path and
/// source), outside test code and the exempt functions.
fn census(files: &[(String, String)], ids: &BTreeMap<String, BTreeSet<IdClass>>) -> Vec<Finding> {
    let lexed = files
        .iter()
        .map(|(path, source)| (path, source, lex(source)))
        .collect::<Vec<_>>();
    let mut test_modules = BTreeSet::new();
    let mut structure = BTreeMap::new();
    for (path, source, lexed) in &lexed {
        let excluded = test_gated_code(path, source, &lexed.code);
        test_modules.extend(excluded.test_module_files.iter().cloned());
        structure.insert(path.as_str(), excluded);
    }
    let in_test_module = |path: &str| {
        test_modules.iter().any(|module| {
            path == module
                || path.starts_with(&format!("{}/", module.trim_end_matches(".rs")))
                || module
                    .strip_suffix("/mod.rs")
                    .is_some_and(|dir| path.starts_with(&format!("{dir}/")))
        })
    };

    let mut findings = Vec::new();
    for (path, source, lexed) in &lexed {
        let excluded = &structure[path.as_str()];
        if excluded.whole_file || in_test_module(path) {
            continue;
        }
        let mut ranges = excluded.ranges.clone();
        ranges.extend(exempt_ranges(path, &lexed.code));
        for literal in &lexed.literals {
            if ranges
                .iter()
                .any(|&(from, to)| (from..to).contains(&literal.at))
            {
                continue;
            }
            let lowered = literal.text.to_ascii_lowercase();
            for id in ids.keys() {
                if names_id(&lowered, id) {
                    findings.push(Finding {
                        file: path.to_string(),
                        line: source[..literal.at].matches('\n').count() + 1,
                        id: id.clone(),
                        literal: literal.text.clone(),
                    });
                }
            }
        }
    }
    findings.sort();
    findings
}

/// The Rust sources the census reads: every `.rs` file under the scanned
/// crates' `src/` trees except `*_tests.rs` files, keyed by their path under
/// `crates/`.
fn scanned_files() -> Vec<(String, String)> {
    let root = crates_dir();
    let mut files = Vec::new();
    for krate in SCANNED_CRATES {
        let mut pending = vec![root.join(krate).join("src")];
        while let Some(dir) = pending.pop() {
            let entries = std::fs::read_dir(&dir)
                .unwrap_or_else(|error| panic!("listing {}: {error}", dir.display()));
            for entry in entries {
                let path = entry.expect("directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !name.ends_with(".rs") || name.ends_with("_tests.rs") {
                    continue;
                }
                let rel = normalize(path.strip_prefix(&root).expect("under crates/"));
                let source = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
                files.push((rel, source));
            }
        }
    }
    files.sort();
    files
}

fn render(findings: &[Finding]) -> String {
    findings
        .iter()
        .map(|f| {
            format!(
                "  {}:{} names {:?} in {:?}",
                f.file, f.line, f.id, f.literal
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn routing_sources_name_no_provider_model_or_runner() {
    let ids = listed_ids();
    let files = scanned_files();
    // An exemption whose file is gone would hide nothing, but it would also
    // stop saying what the exempt code is; keep the list true.
    for exemption in EXEMPTIONS {
        assert!(
            files.iter().any(|(path, _)| path == exemption.file),
            "exempt file {} is not among the scanned sources",
            exemption.file
        );
    }
    let findings = census(&files, &ids);
    assert!(
        findings.is_empty(),
        "routing code names a provider, model or runner id in a string literal; \
         take the fact from the catalog, the vault, a runner declaration or \
         operator config instead:\n{}",
        render(&findings)
    );
}

/// The first listed id carrying `class`, preferring one listed under that
/// class alone so a control proves that source is read.
fn id_of_class(ids: &BTreeMap<String, BTreeSet<IdClass>>, class: IdClass) -> String {
    ids.iter()
        .find(|(_, classes)| classes.len() == 1 && classes.contains(&class))
        .or_else(|| ids.iter().find(|(_, classes)| classes.contains(&class)))
        .map(|(id, _)| id.clone())
        .unwrap_or_else(|| panic!("no id listed as {class:?}"))
}

/// The census of the real sources with `planted` appended to one file,
/// minus the findings the real sources already have.
fn planted_findings(file: &str, planted: &str) -> Vec<Finding> {
    let ids = listed_ids();
    let real = scanned_files();
    let baseline = census(&real, &ids);
    let mut files = real;
    let (_, source) = files
        .iter_mut()
        .find(|(path, _)| path == file)
        .unwrap_or_else(|| panic!("{file} is not among the scanned sources"));
    source.push('\n');
    source.push_str(planted);
    source.push('\n');
    census(&files, &ids)
        .into_iter()
        .filter(|finding| !baseline.contains(finding))
        .collect()
}

fn plant_const(id: &str) -> String {
    format!("const CENSUS_PLANT: &str = \"{id}\";")
}

#[test]
fn the_listed_ids_come_from_every_source() {
    let ids = listed_ids();
    assert!(ids.contains_key("ollama-cloud"), "ollama-cloud is listed");
    for runner in RUNNER_IDS {
        assert!(ids[*runner].contains(&IdClass::Runner));
    }
    for class in [
        IdClass::CatalogProvider,
        IdClass::CatalogModel,
        IdClass::CatalogKey,
        IdClass::VaultProvider,
    ] {
        id_of_class(&ids, class);
    }
    // Header names are not ids, even where they start with a provider id.
    assert!(!ids.keys().any(|id| id.contains("ratelimit")));
}

#[test]
fn a_planted_literal_of_each_class_fails() {
    let ids = listed_ids();
    let plants = [
        (
            "striatum-core/src/launch_policy.rs",
            "ollama-cloud".to_string(),
        ),
        ("striatum-module/src/config_convert.rs", "broca".to_string()),
        (
            "striatum-module/src/quota_consumer.rs",
            id_of_class(&ids, IdClass::VaultProvider),
        ),
        (
            "striatum-store/src/lib.rs",
            id_of_class(&ids, IdClass::CatalogModel),
        ),
        (
            "striatum-module/src/account_selection.rs",
            id_of_class(&ids, IdClass::CatalogKey),
        ),
        (
            "striatum-module/src/main.rs",
            id_of_class(&ids, IdClass::CatalogProvider),
        ),
    ];
    for (file, id) in plants {
        let found = planted_findings(file, &plant_const(&id));
        assert!(
            !found.is_empty() && found.iter().all(|finding| finding.file == file),
            "planting {id:?} in {file}: {found:?}"
        );
        assert!(found.iter().any(|finding| finding.id == id), "{found:?}");
    }
}

#[test]
fn a_planted_id_inside_prose_or_a_route_key_fails() {
    for literal in [
        "no account for opencode-go today",
        "anthropic/*",
        "Broca said: quota",
        "runner pi.",
    ] {
        let found = planted_findings(
            "striatum-core/src/launch_policy.rs",
            &format!("const CENSUS_PLANT: &str = {literal:?};"),
        );
        assert_eq!(found.len(), 1, "planting {literal:?}: {found:?}");
    }
    for literal in [
        "anthropic-ratelimit-unified-5h-status",
        "api key",
        "spinner",
    ] {
        let found = planted_findings(
            "striatum-core/src/launch_policy.rs",
            &format!("const CENSUS_PLANT: &str = {literal:?};"),
        );
        assert!(found.is_empty(), "planting {literal:?}: {found:?}");
    }
}

#[test]
fn every_scanned_file_is_read_except_test_modules() {
    // One plant at the end of every file: a file whose plant is not found is
    // one the census never reads, and only test modules may be such files.
    let ids = listed_ids();
    let mut files = scanned_files();
    for (_, source) in &mut files {
        source.push('\n');
        source.push_str(&plant_const("broca"));
        source.push('\n');
    }
    let flagged = census(&files, &ids)
        .into_iter()
        .filter(|finding| finding.literal == "broca")
        .map(|finding| finding.file)
        .collect::<BTreeSet<_>>();
    let unread = files
        .iter()
        .map(|(path, _)| path.as_str())
        .filter(|path| !flagged.contains(*path))
        .collect::<Vec<_>>();
    let exempt_modules = EXEMPTIONS
        .iter()
        .filter(|exemption| matches!(exemption.item, ExemptItem::Module))
        .map(|exemption| exemption.file)
        .collect::<BTreeSet<_>>();
    for path in &unread {
        assert!(
            path.ends_with("/tests.rs")
                || path.ends_with("/test_config.rs")
                || exempt_modules.contains(path),
            "the census does not read {path}, which is not a test module"
        );
    }
    for crate_name in SCANNED_CRATES {
        assert!(
            flagged
                .iter()
                .any(|path| path.starts_with(&format!("{crate_name}/src/"))),
            "the census reads no file of {crate_name}"
        );
    }
    for required in [
        "striatum-core/src/launch_policy.rs",
        "striatum-module/src/config_convert.rs",
        "striatum-module/src/main.rs",
    ] {
        assert!(flagged.contains(required), "the census reads {required}");
    }
}

#[test]
fn test_code_exempt_functions_and_data_files_pass() {
    // Inside an exempt function, and beside one.
    let exempt = census(
        &[
            (
                "striatum-module/src/account_selection.rs".to_string(),
                "fn header_fences() { let _ = \"anthropic\"; }\n\
                 fn other() { let _ = \"anthropic\"; }\n"
                    .to_string(),
            ),
            (
                "striatum-core/src/model_facts.rs".to_string(),
                "impl GrandFamily { fn as_str() { let _ = \"anthropic\"; } }\n\
                 const GRAND_FAMILY_NORMALIZATIONS: &[&str] = &[\"anthropic\"];\n"
                    .to_string(),
            ),
        ],
        &listed_ids(),
    );
    assert_eq!(exempt.len(), 1, "{exempt:?}");
    assert_eq!(exempt[0].line, 2);

    // Test-gated modules, items and #[test] functions.
    for planted in [
        "#[cfg(test)]\nmod census_plant { const X: &str = \"broca\"; }",
        "#[cfg(test)]\nconst CENSUS_PLANT: &str = \"broca\";",
        "#[cfg(test)]\nfn census_plant() -> &'static str { \"broca\" }",
        "#[test]\nfn census_plant() { assert_eq!(\"broca\", \"broca\"); }",
        "// \"broca\" in a comment\n/// and \"opencode\" in a doc comment",
    ] {
        let found = planted_findings("striatum-core/src/launch_policy.rs", planted);
        assert!(found.is_empty(), "planting {planted:?}: {found:?}");
    }

    // The converter's data file names ids and is not a Rust source.
    let defaults = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/config_convert_defaults.json");
    let text = std::fs::read_to_string(&defaults).expect("converter defaults");
    assert!(text.contains("ollama-cloud"));
    assert!(
        scanned_files()
            .iter()
            .all(|(path, _)| path.ends_with(".rs")),
        "only Rust sources are scanned"
    );
}
