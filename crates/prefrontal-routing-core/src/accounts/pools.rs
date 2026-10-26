//! Durable quota-pool identities and the account class frozen at selection.

use serde::{Deserialize, Serialize};

use super::{FenceScope, ModelFamily};

/// Scope supplied by quota evidence, independent of a provider's spelling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PoolScope {
    EveryModel,
    Family(ModelFamily),
    Models(std::collections::BTreeSet<String>),
}

/// A named pool remains distinct from every other name, even at equal length.
/// `None` for the length means unknown, not an aggregate of known windows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PoolKey {
    pub window_minutes: Option<u32>,
    pub scope: PoolScope,
    pub name: Option<String>,
}

impl From<FenceScope> for PoolKey {
    fn from(scope: FenceScope) -> Self {
        let (window_minutes, scope) = match scope {
            FenceScope::General(window) => (Some(window.window_minutes()), PoolScope::EveryModel),
            FenceScope::Family(family) | FenceScope::UnknownScope(family) => {
                (None, PoolScope::Family(family))
            }
        };
        Self {
            window_minutes,
            scope,
            name: None,
        }
    }
}

/// Authority recorded once with a selection. Account lookup must not infer it
/// again from a consumer's current configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowedAccountClass {
    PricedPerTokenOnly,
    #[default]
    NotPayPerToken,
}

impl AllowedAccountClass {
    pub fn from_pay_per_token(value: bool) -> Self {
        if value {
            Self::PricedPerTokenOnly
        } else {
            Self::NotPayPerToken
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PricedPerTokenOnly => "priced_per_token_only",
            Self::NotPayPerToken => "not_pay_per_token",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "priced_per_token_only" => Ok(Self::PricedPerTokenOnly),
            "not_pay_per_token" => Ok(Self::NotPayPerToken),
            _ => Err(format!("unknown allowed account class: {value}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::GeneralWindow;

    #[test]
    fn legacy_scopes_have_unnamed_pool_identities() {
        for (window, minutes) in [
            (GeneralWindow::FiveHour, 300),
            (GeneralWindow::SevenDay, 10080),
            (GeneralWindow::Reported(90), 90),
        ] {
            assert_eq!(
                PoolKey::from(FenceScope::General(window)),
                PoolKey {
                    window_minutes: Some(minutes),
                    scope: PoolScope::EveryModel,
                    name: None,
                }
            );
        }
        for scope in [
            FenceScope::Family(ModelFamily::Opus),
            FenceScope::UnknownScope(ModelFamily::Opus),
        ] {
            assert_eq!(
                PoolKey::from(scope),
                PoolKey {
                    window_minutes: None,
                    scope: PoolScope::Family(ModelFamily::Opus),
                    name: None,
                }
            );
        }
    }
}
