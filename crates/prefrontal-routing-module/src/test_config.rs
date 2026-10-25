//! Shared config writer for routing tests. Legacy fixtures deliberately pass
//! through the real converter, so tests exercise the same cutover as operators.

use std::path::Path;

pub fn document(document: &str) -> String {
    let value = subc_jsonc::jsonc_to_json(document)
        .ok()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok());
    if value.as_ref().is_some_and(|value| {
        value.get("model_routing").is_some() || value.get("account_selection").is_some()
    }) {
        super::config_convert::convert(document).unwrap_or_else(|_| document.to_owned())
    } else {
        match value {
            Some(mut value) if value.is_object() => {
                // Selection fixtures that predate runner configuration used
                // Broca implicitly. State that test reach explicitly; an
                // explicitly empty array still tests an unconfigured registry.
                value
                    .as_object_mut()
                    .unwrap()
                    .entry("runners")
                    .or_insert_with(|| serde_json::json!([{"module": "broca", "via_vault": true}]));
                value.to_string()
            }
            _ => document.to_owned(),
        }
    }
}

pub fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> std::io::Result<()> {
    let bytes = contents.as_ref();
    let text = std::str::from_utf8(bytes)
        .map(document)
        .unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned());
    std::fs::write(path, text)
}

pub fn write_raw(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> std::io::Result<()> {
    std::fs::write(path, contents)
}
