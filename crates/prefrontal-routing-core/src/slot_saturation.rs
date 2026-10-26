//! Caller concurrency slots as a per-decision admission input.
//!
//! The caller that launches the selected model (prefrontal-core's task manager)
//! admits a launch only while the model's slot count is below its configured
//! limit; a launch at the limit waits in a queue until a running task finishes.
//! Routing never saw those counts, so it kept choosing a model whose slots were
//! full while an equally adequate model on another provider had room.
//!
//! A caller may now send the slot counts it holds at request time. A candidate
//! whose slot is full is passed over for THIS decision only, in favour of the
//! next adequate candidate in the normal preference order. Nothing is stored:
//! this is not a wall or a cooldown, and the next decision reads fresh counts.
//! When every adequate candidate is full, the normal winner is kept (the caller
//! queues it, as before) and the decision says so. Saturation never widens the
//! candidate set, so it can never pick a model that would not otherwise qualify.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Rejection reason for a candidate passed over because its concurrency slot
/// was full when this decision was made. Distinct from quota and cooldown
/// reasons: it is read from the caller's live counts and never persisted.
pub const REASON_PROVIDER_SATURATED: &str = "provider_saturated";

/// One concurrency slot as the caller counted it when it asked for a route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConcurrencySlotState {
    /// The caller's slot key, `provider/model`. It is the same string as a
    /// route's capacity key, which is how a candidate finds its slot.
    pub key: String,
    /// Tasks currently holding a slot under this key.
    pub running: u32,
    /// The limit the caller admits against. Zero means unlimited, matching the
    /// caller's own configuration convention, and is never saturated.
    pub limit: u32,
    /// Tasks already waiting for this slot. Informational only.
    #[serde(default)]
    pub queued: u32,
}

impl ConcurrencySlotState {
    /// True when the caller would queue a new launch on this slot.
    pub fn is_saturated(&self) -> bool {
        self.limit > 0 && self.running >= self.limit
    }

    /// The provider part of the slot key.
    pub fn provider(&self) -> &str {
        self.key.split('/').next().unwrap_or(&self.key)
    }

    /// `anthropic/claude-opus-4-8 50/40`, plus the queue length when non-zero.
    pub fn describe(&self) -> String {
        let mut text = format!("{} {}/{}", self.key, self.running, self.limit);
        if self.queued > 0 {
            text.push_str(&format!(", {} queued", self.queued));
        }
        text
    }
}

/// The caller's slot counts for one decision, indexed by slot key.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConcurrencySlots {
    by_key: HashMap<String, ConcurrencySlotState>,
    preferred_capacity_key: Option<String>,
}

impl ConcurrencySlots {
    pub fn new(states: impl IntoIterator<Item = ConcurrencySlotState>) -> Self {
        Self {
            by_key: states
                .into_iter()
                .map(|state| (state.key.clone(), state))
                .collect(),
            preferred_capacity_key: None,
        }
    }

    /// Prefer a freed capacity only after the router has built its quality shortlist.
    /// Counts remain truthful, and a full or unqualified preference changes nothing.
    pub fn with_preferred_capacity(mut self, capacity_key: Option<String>) -> Self {
        self.preferred_capacity_key = capacity_key;
        self
    }

    /// The slot for `capacity_key` when it is full; `None` when it has room or
    /// the caller reported nothing for it (an unreported slot holds no tasks).
    pub fn saturated(&self, capacity_key: &str) -> Option<&ConcurrencySlotState> {
        self.by_key
            .get(capacity_key)
            .filter(|state| state.is_saturated())
    }
}

/// A candidate passed over for this decision because its slot was full.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkippedSaturatedRoute {
    pub route_key: String,
    pub slot: ConcurrencySlotState,
}

/// What slot saturation did to one selection. Absent from a selection that
/// saturation did not change.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotSaturation {
    /// Better-ranked candidates passed over, in preference order.
    pub skipped: Vec<SkippedSaturatedRoute>,
    /// Set when every adequate candidate was full: the normal winner was kept
    /// and the caller will queue it on this slot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_on: Option<ConcurrencySlotState>,
}

impl SlotSaturation {
    /// One explanation line per skipped candidate, or the queue notice.
    pub fn explanation_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .skipped
            .iter()
            .map(|skipped| {
                format!(
                    "route {} skipped: {}: {}",
                    skipped.route_key,
                    REASON_PROVIDER_SATURATED,
                    skipped.slot.describe()
                )
            })
            .collect();
        if let Some(slot) = &self.queued_on {
            lines.push(format!(
                "all adequate providers saturated; queued on {} ({}/{})",
                slot.key, slot.running, slot.limit
            ));
        }
        lines
    }
}

/// Pick the winner from candidates already sorted best-first, each given as
/// `(route_key, capacity_key)`. Returns the index of the first candidate whose
/// slot has room. When every candidate is full, returns index 0 with
/// `queued_on` set. With no slot counts, or when the best candidate has room,
/// returns index 0 and no saturation record: the decision is unchanged.
pub fn pick_unsaturated(
    ordered: &[(&str, &str)],
    slots: Option<&ConcurrencySlots>,
) -> (usize, Option<SlotSaturation>) {
    let Some(slots) = slots else {
        return (0, None);
    };
    if let Some(preferred) = slots.preferred_capacity_key.as_deref() {
        if let Some(index) = ordered.iter().position(|(_, capacity)| *capacity == preferred
            && slots.saturated(capacity).is_none()) {
            return (index, None);
        }
    }
    let mut skipped = Vec::new();
    for (index, (route_key, capacity_key)) in ordered.iter().enumerate() {
        match slots.saturated(capacity_key) {
            Some(slot) => skipped.push(SkippedSaturatedRoute {
                route_key: (*route_key).to_string(),
                slot: slot.clone(),
            }),
            None if index == 0 => return (0, None),
            None => {
                return (
                    index,
                    Some(SlotSaturation {
                        skipped,
                        queued_on: None,
                    }),
                )
            }
        }
    }
    match skipped.into_iter().next() {
        Some(first) => (
            0,
            Some(SlotSaturation {
                skipped: Vec::new(),
                queued_on: Some(first.slot),
            }),
        ),
        None => (0, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(key: &str, running: u32, limit: u32) -> ConcurrencySlotState {
        ConcurrencySlotState {
            key: key.to_string(),
            running,
            limit,
            queued: 0,
        }
    }

    #[test]
    fn a_slot_is_saturated_at_and_above_its_limit_and_never_when_unlimited() {
        assert!(!slot("a/m", 39, 40).is_saturated());
        assert!(slot("a/m", 40, 40).is_saturated());
        assert!(slot("a/m", 50, 40).is_saturated());
        assert!(!slot("a/m", 50, 0).is_saturated());
    }

    #[test]
    fn no_slot_counts_keep_the_first_candidate_and_record_nothing() {
        assert_eq!(
            pick_unsaturated(&[("a/m", "a/m"), ("b/m", "b/m")], None),
            (0, None)
        );
    }

    #[test]
    fn a_full_first_candidate_yields_to_the_next_with_room() {
        let slots = ConcurrencySlots::new([slot("a/m", 50, 40)]);
        let (index, saturation) =
            pick_unsaturated(&[("a/m/high", "a/m"), ("b/m", "b/m")], Some(&slots));
        assert_eq!(index, 1);
        let saturation = saturation.expect("a skip is recorded");
        assert_eq!(saturation.skipped.len(), 1);
        assert_eq!(saturation.skipped[0].route_key, "a/m/high");
        assert_eq!(saturation.queued_on, None);
        assert_eq!(
            saturation.explanation_lines(),
            vec!["route a/m/high skipped: provider_saturated: a/m 50/40".to_string()]
        );
    }

    #[test]
    fn every_candidate_full_keeps_the_first_and_names_its_queue() {
        let slots = ConcurrencySlots::new([slot("a/m", 50, 40), slot("b/m", 3, 3)]);
        let (index, saturation) = pick_unsaturated(&[("a/m", "a/m"), ("b/m", "b/m")], Some(&slots));
        assert_eq!(index, 0);
        let saturation = saturation.expect("the queue is recorded");
        assert!(saturation.skipped.is_empty());
        assert_eq!(
            saturation.explanation_lines(),
            vec!["all adequate providers saturated; queued on a/m (50/40)".to_string()]
        );
    }
}
