//! Read entry declarations independently of the generator and its trace hooks.
//! Literal/comment masking keeps JSON, SQL and documentation braces from
//! changing the enclosing Rust implementation a method belongs to.

use std::collections::BTreeSet;
use std::path::Path;

fn blank(bytes: &mut [u8], start: usize, end: usize) {
    for byte in &mut bytes[start..end] {
        if *byte != b'\n' && *byte != b'\r' {
            *byte = b' ';
        }
    }
}

fn mask(source: &str) -> String {
    let raw = source.as_bytes();
    let mut result = raw.to_vec();
    let mut index = 0;
    while index < raw.len() {
        let start = index;
        if raw[index..].starts_with(b"//") {
            index = source[index..]
                .find('\n')
                .map_or(raw.len(), |end| index + end);
        } else if raw[index..].starts_with(b"/*") {
            let mut depth = 1;
            index += 2;
            while index < raw.len() && depth > 0 {
                if raw[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if raw[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            assert_eq!(depth, 0, "unterminated Rust comment");
        } else if raw[index] == b'r' {
            let mut quote = index + 1;
            while quote < raw.len() && raw[quote] == b'#' {
                quote += 1;
            }
            if quote < raw.len() && raw[quote] == b'"' {
                let delimiter = format!("\"{}", "#".repeat(quote - index - 1));
                let content = quote + 1;
                index = content
                    + source[content..]
                        .find(&delimiter)
                        .expect("unterminated raw Rust literal")
                    + delimiter.len();
            }
        } else if raw[index] == b'"' {
            index += 1;
            loop {
                assert!(index < raw.len(), "unterminated Rust string");
                if raw[index] == b'\\' {
                    index += 2;
                } else if raw[index] == b'"' {
                    index += 1;
                    break;
                } else {
                    index += 1;
                }
            }
        } else if raw[index] == b'\'' && index + 1 < raw.len() {
            let content = index + 1;
            let end = if raw[content] == b'\\' {
                source[content + 1..]
                    .find('\'')
                    .map(|offset| content + 1 + offset)
            } else {
                let next = content + source[content..].chars().next().unwrap().len_utf8();
                (raw.get(next) == Some(&b'\'')).then_some(next)
            };
            if let Some(end) = end {
                index = end + 1;
            }
        }
        if index == start {
            index += 1;
        } else {
            blank(&mut result, start, index);
        }
    }
    String::from_utf8(result).unwrap()
}

fn close(code: &[u8], opening: usize) -> usize {
    let mut depth = 0;
    for (offset, byte) in code[opening..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return opening + offset;
                }
            }
            _ => {}
        }
    }
    panic!("unterminated Rust implementation or method");
}

fn test_attribute_before(code: &str, start: usize) -> bool {
    let before = code[..start].trim_end();
    if !before.ends_with(']') {
        return false;
    }
    let Some(open) = before.rfind("#[") else {
        return false;
    };
    let attribute = &before[open..];
    attribute.contains("cfg")
        && attribute
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .any(|word| word == "test")
}

fn file_declarations(source: &str, module: &str) -> BTreeSet<String> {
    let code = mask(source);
    let mut names = BTreeSet::new();
    for (start, _) in code.match_indices("impl") {
        let line = code[..start].rfind('\n').map_or(0, |offset| offset + 1);
        if !matches!(code[line..start].trim(), "" | "unsafe") {
            continue;
        }
        let Some(open) = code[start..].find('{').map(|offset| start + offset) else {
            continue;
        };
        let header = &code[start..open];
        if !header
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .any(|word| word == "AccountTx")
            || test_attribute_before(&code, start)
        {
            continue;
        }
        let end = close(code.as_bytes(), open);
        for (offset, _) in code[open + 1..end].match_indices("fn ") {
            let function = open + 1 + offset;
            let line = code[..function].rfind('\n').map_or(0, |offset| offset + 1);
            if code[line..function].split_whitespace().next() != Some("pub")
                || test_attribute_before(&code, line)
            {
                continue;
            }
            let depth = code[open + 1..function].bytes().fold(0_i32, |depth, byte| {
                depth
                    + match byte {
                        b'{' => 1,
                        b'}' => -1,
                        _ => 0,
                    }
            });
            if depth != 0 {
                continue;
            }
            let name: String = code[function + 3..]
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
                .collect();
            assert!(
                !name.is_empty(),
                "unrecognised public AccountTx declaration in {module}"
            );
            assert!(
                names.insert(format!("account_tx::{module}::{name}")),
                "duplicate public AccountTx declaration: {module}::{name}"
            );
        }
    }
    names
}

fn collect(root: &Path, directory: &Path, names: &mut BTreeSet<String>) {
    let mut paths = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == "tests" || name == "legacy")
            {
                continue;
            }
            collect(root, &path, names);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let filename = path.file_name().unwrap().to_string_lossy();
            if filename == "tests.rs" || filename.ends_with("_tests.rs") {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let module = relative.trim_end_matches(".rs").replace('/', "::");
            for declaration in file_declarations(&std::fs::read_to_string(&path).unwrap(), &module)
            {
                assert!(
                    names.insert(declaration),
                    "duplicate AccountTx declaration across source files"
                );
            }
        }
    }
}

pub(super) fn native_declarations() -> BTreeSet<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../striatum-store/src");
    let mut declarations = BTreeSet::new();
    collect(&root, &root, &mut declarations);
    assert!(
        !declarations.is_empty(),
        "no native AccountTx declarations were read"
    );
    declarations
}

#[test]
fn literal_braces_and_test_implementations_are_not_public_native_entries() {
    let source = r###"
impl<'a> crate::AccountTx<'a> {
    pub fn changed(&self) {
        let sql=r#"JSON { } impl AccountTx { pub fn fake() {} }"#;
        let character='{';
        /* nested /* braces { } */ comment */
    }
    pub(crate) fn internal(&self) {}
    #[cfg(test)]
    pub fn test_only(&self) {}
    pub fn read(&self) {}
}
#[cfg(test)]
impl AccountTx<'_> {
    pub fn another_test(&self) {}
}
"###;
    assert_eq!(
        file_declarations(source, "fixture"),
        [
            "account_tx::fixture::changed".into(),
            "account_tx::fixture::read".into()
        ]
        .into_iter()
        .collect()
    );
}
