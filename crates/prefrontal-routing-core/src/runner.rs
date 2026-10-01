//! Provider, model, and optional variant accepted by Broca's `session.send`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerModel {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
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
        }
    }
}
