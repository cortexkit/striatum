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

impl BillingPlane {
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
