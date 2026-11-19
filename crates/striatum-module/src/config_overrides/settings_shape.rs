//! Reject outdated settings spellings, or translate them when conversion is requested.

use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Site {
    Root,
    Panels,
    Panel,
    Models,
    Model,
    Providers,
    Provider,
    Accounts,
    Other,
}

impl Site {
    fn child(self, key: &str) -> Self {
        match (self, key) {
            (Self::Root, "panels") => Self::Panels,
            (Self::Root, "models") => Self::Models,
            (Self::Root, "providers") => Self::Providers,
            (Self::Panels, _) => Self::Panel,
            (Self::Models, _) => Self::Model,
            (Self::Providers, _) => Self::Provider,
            (Self::Root | Self::Provider, "accounts") => Self::Accounts,
            _ => Self::Other,
        }
    }

    fn renames(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Panel => &[("extraBy", "extra_by"), ("topK", "top_k")],
            Self::Model => &[
                ("context_window", "context_window_tokens"),
                ("speed", "output_tokens_per_second"),
            ],
            Self::Accounts => &[("spend_budget", "spend_budget_usd")],
            _ => &[],
        }
    }

    fn duration(self, key: &str, value: &Value) -> Option<String> {
        (self == Self::Accounts
            && matches!(
                key,
                "max_hold" | "hold_when_reset_within" | "unknown_reset_retry"
            ))
        .then(|| value.as_number().map(|n| format!("{n}s")))
        .flatten()
    }
}

fn pointer(path: &str, key: &str) -> String {
    format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Edit {
    Remove(String),
    Rename { from: String, to: String },
    Duration(String),
}

pub(super) fn validate(value: &mut Value) -> Result<(), String> {
    visit(value, Site::Root, "", &mut None)
}

/// Convert the most recent pre-managed settings shape. Paths use JSON Pointer;
/// array removals are reported in descending index order so they can be replayed.
#[allow(dead_code, reason = "reserved for managed settings conversion")]
pub(crate) fn convert(mut value: Value) -> Result<(Value, Vec<Edit>), String> {
    let mut edits = Some(Vec::new());
    visit(&mut value, Site::Root, "", &mut edits)?;
    Ok((value, edits.unwrap()))
}

fn null_error(path: &str) -> String {
    if path.is_empty() {
        "invalid routing config: expected an object, not null".into()
    } else {
        format!("invalid routing config: invalid type at {path}: null is not allowed; omit the field instead")
    }
}

// Without an edit list, report the first outdated setting without changing it.
fn visit(
    value: &mut Value,
    site: Site,
    path: &str,
    edits: &mut Option<Vec<Edit>>,
) -> Result<(), String> {
    match value {
        Value::Object(fields) => {
            for &(old, new) in site.renames() {
                let Some(existing) = fields.get(old) else {
                    continue;
                };
                let from = pointer(path, old);
                let to = pointer(path, new);
                let Some(edits) = edits else {
                    return Err(format!("old routing config key {from}; use {to}"));
                };
                if fields.contains_key(new) {
                    return Err(format!(
                        "cannot rename {from} to {to}: both keys are present"
                    ));
                }
                if !existing.is_null() {
                    let value = fields.remove(old).unwrap();
                    fields.insert(new.into(), value);
                    edits.push(Edit::Rename { from, to });
                }
            }
            if let Some(edits) = edits {
                fields.retain(|key, value| {
                    if value.is_null() {
                        edits.push(Edit::Remove(pointer(path, key)));
                        false
                    } else {
                        true
                    }
                });
            }
            for (key, child) in fields {
                let child_path = pointer(path, key);
                if let Some(text) = site.duration(key, child) {
                    if let Some(edits) = edits {
                        *child = Value::String(text);
                        edits.push(Edit::Duration(child_path));
                    } else {
                        return Err(format!(
                            "{child_path} must use a duration string; use {text:?}"
                        ));
                    }
                } else {
                    visit(child, site.child(key), &child_path, edits)?;
                }
            }
        }
        Value::Array(items) => {
            for index in (0..items.len()).rev() {
                let child_path = pointer(path, &index.to_string());
                if let Some(edits) = edits.as_mut().filter(|_| items[index].is_null()) {
                    items.remove(index);
                    edits.push(Edit::Remove(child_path));
                } else {
                    visit(&mut items[index], Site::Other, &child_path, edits)?;
                }
            }
        }
        Value::Null => return Err(null_error(path)),
        _ => {}
    }
    Ok(())
}
