use std::collections::HashMap;
use striatum_core::{make_capacity_key, ModelFact};

pub(super) struct TierCatalog<'a> {
    facts: HashMap<String, &'a ModelFact>,
    #[cfg(test)]
    lookups: std::sync::atomic::AtomicUsize,
}

impl<'a> TierCatalog<'a> {
    pub(super) fn new(facts: &'a [ModelFact]) -> Self {
        let mut by_capacity = HashMap::with_capacity(facts.len());
        for fact in facts {
            // A tier belongs to the base model. Keep the first fact when its
            // capacity is shared by several catalog variants.
            by_capacity
                .entry(make_capacity_key(&fact.provider_id, &fact.model_id))
                .or_insert(fact);
        }
        Self {
            facts: by_capacity,
            #[cfg(test)]
            lookups: Default::default(),
        }
    }

    pub(super) fn get(&self, capacity_key: &str) -> Option<&'a ModelFact> {
        #[cfg(test)]
        self.lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.facts.get(capacity_key).copied()
    }
}

#[cfg(test)]
#[test]
fn tier_catalog_lookup_count_scales_linearly_and_preserves_first_fact() {
    for count in [32, 64] {
        let mut facts: Vec<_> = (0..count)
            .map(|model| crate::tests::fact("provider", &format!("model-{model}")))
            .collect();
        let mut first_variant = facts[0].clone();
        first_variant.variant = Some("high".into());
        facts.insert(0, first_variant);
        let catalog = TierCatalog::new(&facts);
        for model in 0..count {
            let key = format!("provider/model-{model}");
            let fact = catalog.get(&key).unwrap();
            assert!(std::ptr::eq(
                fact,
                &facts[if model == 0 { 0 } else { model + 1 }]
            ));
        }
        assert!(catalog.get("provider/missing").is_none());
        assert_eq!(
            catalog.lookups.load(std::sync::atomic::Ordering::Relaxed),
            count + 1,
            "catalog size {count}"
        );
    }
}
