//! Credential-specific rates. The catalog's API prices are not evidence of
//! subscription billing; a plane is attached only by the catalog consumer.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BillingRate {
    Priced { rate: f64 },
    NotBilled,
    BilledAs { key: String },
    Unpriced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillingPlane {
    pub provider_id: String,
    pub auth_method: String,
    pub kind: String,
    /// How routing identified the credential that serves this route.
    pub selection_reason: String,
    /// Exact fact keys, including any above-context suffix. Alias targets must
    /// be resolved here, never against an unrelated API catalog row.
    pub rates: BTreeMap<String, BillingRate>,
}

/// Gateway credentials use the upstream API's metered rates, not subscription quota.
pub fn is_api_metered_auth_method(method: &str) -> bool {
    matches!(method, "apikey" | "gateway")
}

impl BillingPlane {
    /// Only API-balance charges with a resolved output price can be budgeted in
    /// dollars. A plan may publish token rates without charging an API balance.
    pub fn is_api_metered(&self) -> bool {
        is_api_metered_auth_method(&self.auth_method)
            && matches!(self.kind.as_str(), "upstream" | "no_coverage fallback")
            && self.rate("rate.output").is_some()
    }

    pub fn rate(&self, key: &str) -> Option<f64> {
        let mut key = key;
        // A producer alias cycle is unpriced, rather than a selection-path loop.
        for _ in 0..=self.rates.len() {
            match self.rates.get(key)? {
                BillingRate::Priced { rate } => {
                    return (rate.is_finite() && *rate >= 0.0).then_some(*rate);
                }
                BillingRate::NotBilled => return Some(0.0),
                BillingRate::BilledAs { key: target } => key = target,
                BillingRate::Unpriced => return None,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_metered_requires_api_auth_direct_plane_and_resolved_output_price() {
        let mut plane = BillingPlane {
            provider_id: "provider".into(),
            auth_method: "apikey".into(),
            kind: "upstream".into(),
            selection_reason: "roster".into(),
            rates: BTreeMap::from([("rate.output".into(), BillingRate::Priced { rate: 2.0 })]),
        };
        assert!(plane.is_api_metered());
        plane.auth_method = "gateway".into();
        assert!(plane.is_api_metered());
        plane.auth_method = "apikey".into();
        plane.kind = "no_coverage fallback".into();
        assert!(plane.is_api_metered());
        for kind in ["quota_proxy", "unpriced", "refused: unavailable", "unknown"] {
            plane.kind = kind.into();
            assert!(!plane.is_api_metered(), "{kind}");
        }
        plane.kind = "upstream".into();
        for auth in ["oauth", "chatgpt", "antigravity", "ambiguous"] {
            plane.auth_method = auth.into();
            assert!(!plane.is_api_metered(), "{auth}");
        }
        plane.auth_method = "apikey".into();
        for rate in [
            BillingRate::Unpriced,
            BillingRate::Priced { rate: -1.0 },
            BillingRate::Priced { rate: f64::NAN },
            BillingRate::Priced {
                rate: f64::INFINITY,
            },
            BillingRate::BilledAs {
                key: "missing".into(),
            },
        ] {
            plane.rates.insert("rate.output".into(), rate);
            assert!(!plane.is_api_metered());
        }
        plane
            .rates
            .insert("rate.output".into(), BillingRate::NotBilled);
        assert!(plane.is_api_metered());
        plane.rates.clear();
        assert!(!plane.is_api_metered());
    }

    #[test]
    fn billed_as_uses_the_exact_target_and_not_billed_is_free() {
        let plane = BillingPlane {
            provider_id: "openai".into(),
            auth_method: "chatgpt".into(),
            kind: "quota_proxy".into(),
            selection_reason: "roster".into(),
            rates: BTreeMap::from([
                ("rate.input".into(), BillingRate::Priced { rate: 2.5 }),
                (
                    "rate.input.above_context.272000".into(),
                    BillingRate::Priced { rate: 5.0 },
                ),
                (
                    "rate.cache_write".into(),
                    BillingRate::BilledAs {
                        key: "rate.input".into(),
                    },
                ),
                (
                    "rate.cache_write.above_context.272000".into(),
                    BillingRate::BilledAs {
                        key: "rate.input.above_context.272000".into(),
                    },
                ),
                ("rate.reasoning".into(), BillingRate::NotBilled),
            ]),
        };
        assert_eq!(plane.rate("rate.cache_write"), Some(2.5));
        assert_eq!(
            plane.rate("rate.cache_write.above_context.272000"),
            Some(5.0)
        );
        assert_eq!(plane.rate("rate.reasoning"), Some(0.0));
        assert_eq!(plane.rate("rate.output"), None);
    }

    #[test]
    fn missing_targets_and_alias_cycles_are_unpriced() {
        let plane = BillingPlane {
            provider_id: "openai".into(),
            auth_method: "chatgpt".into(),
            kind: "quota_proxy".into(),
            selection_reason: "roster".into(),
            rates: BTreeMap::from([
                (
                    "missing".into(),
                    BillingRate::BilledAs {
                        key: "absent".into(),
                    },
                ),
                (
                    "cycle".into(),
                    BillingRate::BilledAs {
                        key: "cycle".into(),
                    },
                ),
            ]),
        };
        assert_eq!(plane.rate("missing"), None);
        assert_eq!(plane.rate("cycle"), None);
    }

    #[test]
    fn explicit_unpriced_reasoning_does_not_borrow_the_output_rate() {
        let mut fact = crate::model_facts_seed()[0].clone();
        fact.billing_plane = Some(BillingPlane {
            provider_id: fact.provider_id.clone(),
            auth_method: "oauth".into(),
            kind: "quota_proxy".into(),
            selection_reason: "roster".into(),
            rates: BTreeMap::from([
                ("rate.output".into(), BillingRate::Priced { rate: 2.0 }),
                ("rate.reasoning".into(), BillingRate::Unpriced),
            ]),
        });
        assert_eq!(fact.basket_rate(crate::BasketDimension::Reasoning), None);
        fact.billing_plane
            .as_mut()
            .unwrap()
            .rates
            .remove("rate.reasoning");
        assert_eq!(
            fact.basket_rate(crate::BasketDimension::Reasoning),
            Some(2.0)
        );
    }
}
