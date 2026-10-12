//! Provider, model, and optional variant accepted by Broca's `session.send`,
//! plus the credential pin a launch target may need on every send.

use serde::{Deserialize, Serialize};

/// Broca's `session.send` `auth` object (`AuthParams` in broca-wire):
/// `{"method": "apikey"}` or `{"method": "apikey", "account": "work"}`. Broca
/// builds the credential id as `method:provider[:account]` from it and the
/// send's model provider; an absent account names the bare `method:provider`
/// credential.
///
/// Broca does not carry a per-send auth selection over to later runs of the
/// same session, so every send that may start a run must repeat it. A send
/// without it falls back to the provider's default credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerAuthPin {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

impl RunnerAuthPin {
    /// Split a vault credential id `method:provider[:account]` into the
    /// provider Broca serves it under and the pin that selects it. `None` for an
    /// id without both a method and a provider segment.
    pub fn from_credential_id(credential_id: &str) -> Option<(String, Self)> {
        let mut parts = credential_id.splitn(3, ':');
        let method = parts.next().filter(|part| !part.is_empty())?;
        let provider = parts.next().filter(|part| !part.is_empty())?;
        let account = match parts.next() {
            Some("") => return None,
            other => other.map(str::to_string),
        };
        Some((
            provider.to_string(),
            Self {
                method: method.to_string(),
                account,
            },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerModel {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The credential pin this launch target needs on every send that may
    /// start a run. It is not part of Broca's `model` object: the sender lifts
    /// it into the send's top-level `auth` field. Routing sets it for providers
    /// that exist only as a vault credential mapping (see `pinned`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<RunnerAuthPin>,
}

impl RunnerModel {
    /// Broca translates model aliases (including priority-tier names), so this
    /// conversion changes only the provider name and leaves model IDs unchanged.
    pub fn broca(provider: &str, model: &str, variant: Option<&str>) -> Self {
        Self {
            provider: match provider {
                "opencode-go" => "opencode",
                other => other,
            }
            .to_string(),
            model: model.to_string(),
            variant: variant.map(str::to_string),
            auth: None,
        }
    }

    /// The launch target for a routing provider that Broca knows only through
    /// one vault credential (for example `anthropic-platform`, mapped to
    /// `apikey:anthropic:work`): the model goes to the credential's provider and
    /// every send pins that credential. `None` when the id does not parse.
    pub fn pinned(credential_id: &str, model: &str, variant: Option<&str>) -> Option<Self> {
        let (provider, pin) = RunnerAuthPin::from_credential_id(credential_id)?;
        Some(Self {
            auth: Some(pin),
            ..Self::broca(&provider, model, variant)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credential_ids_split_into_provider_and_pin() {
        let bare =
            RunnerModel::pinned("apikey:anthropic", "claude-opus-4-1", Some("high")).unwrap();
        assert_eq!(bare.provider, "anthropic");
        assert_eq!(bare.variant.as_deref(), Some("high"));
        assert_eq!(
            serde_json::to_value(&bare.auth).unwrap(),
            json!({"method": "apikey"})
        );
        let labelled =
            RunnerModel::pinned("apikey:anthropic:work", "claude-opus-4-1", None).unwrap();
        assert_eq!(labelled.provider, "anthropic");
        assert_eq!(
            serde_json::to_value(&labelled.auth).unwrap(),
            json!({"method": "apikey", "account": "work"})
        );
        // An account label may itself contain colons; only the first two split.
        assert_eq!(
            RunnerAuthPin::from_credential_id("apikey:anthropic:team:a")
                .unwrap()
                .1
                .account
                .as_deref(),
            Some("team:a")
        );
        for broken in ["", "apikey", "apikey:", ":anthropic", "apikey:anthropic:"] {
            assert_eq!(
                RunnerAuthPin::from_credential_id(broken),
                None,
                "{broken:?}"
            );
        }
    }

    #[test]
    fn ordinary_launch_targets_carry_no_pin_and_keep_their_wire_shape() {
        let runner = RunnerModel::broca("anthropic", "claude-opus-4-1", None);
        assert_eq!(runner.auth, None);
        assert_eq!(
            serde_json::to_value(&runner).unwrap(),
            json!({"provider": "anthropic", "model": "claude-opus-4-1"})
        );
        assert_eq!(
            RunnerModel::broca("opencode-go", "glm", None).provider,
            "opencode"
        );
    }
}
