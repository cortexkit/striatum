//! Job estimates keep catalog units intact and never treat an unknown charge as free.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use cortexkit_role_routing::select_media::{UnitPrice, UnitRate};
use serde_json::{Number, Value};

use super::wire;
use crate::fusiform_catalog::{CatalogSnapshot, RateQuantityKind};

pub(super) struct Estimate {
    pub cost: f64,
    pub price: Option<UnitPrice>,
    pub basis: &'static str,
    pub minimum: Option<f64>,
    quantity_known: bool,
}

impl Estimate {
    pub fn detail(&self) -> Value {
        if self.quantity_known {
            serde_json::json!({"basis": self.basis, "estimated_task_usd": self.cost})
        } else {
            serde_json::json!({"basis": self.basis, "unit_rate": self.cost})
        }
    }
}

pub(super) fn estimate(
    catalog: &CatalogSnapshot,
    request: &wire::SelectEngineRequest,
    input_characters: Option<u64>,
    provider: &str,
    model: &str,
) -> Option<Estimate> {
    let route = format!("{provider}/{model}");
    if request.kind.as_str() == "search" {
        let rates = catalog.media_rates(&route)?;
        let mut unit_rates = BTreeMap::new();
        let mut cost = 0.0;
        for (key, rate) in rates {
            if !matches!(
                key.as_str(),
                "rate.request.per_request" | "rate.output.per_result"
            ) {
                return None;
            }
            let amount = rate.unit_price?;
            cost += amount;
            unit_rates.insert(key.strip_prefix("rate.")?.to_owned(), amount);
        }
        if unit_rates.is_empty() || !cost.is_finite() {
            return None;
        }
        return Some(Estimate {
            cost,
            price: unit_price(unit_rates, None),
            basis: "metered",
            minimum: None,
            quantity_known: false,
        });
    }
    if matches!(
        request.kind,
        wire::Kind::Transcribe | wire::Kind::StreamTranscribe
    ) {
        let unit_rate = catalog
            .media_rates(&route)
            .and_then(|rates| rates.get("rate.input_audio.per_second"))
            .and_then(|rate| rate.unit_price);
        if request.requirements.duration_ms.is_none() {
            // Every known unit-only transcription cost uses input audio seconds.
            // Ordering linear rates therefore needs no invented job duration.
            let rate = unit_rate?;
            return Some(Estimate {
                cost: rate,
                price: unit_price(
                    BTreeMap::from([("input.per_audio_second".into(), rate)]),
                    None,
                ),
                basis: "metered",
                quantity_known: false,
                minimum: catalog
                    .media_rates(&route)?
                    .get("rate.input_audio.per_second")?
                    .minimum
                    .map(|quantity| quantity as f64 * rate),
            });
        }
        // Both route.select and route.select_engine must estimate the same
        // transcription job identically, ignoring output tokens and quantity rules.
        let cost = catalog.engine_cost(request, provider, model)?;
        let price = catalog
            .media_rates(&route)
            .and_then(|rates| rates.get("rate.input_audio.per_second"))
            .and_then(|rate| rate.unit_price)
            .and_then(|rate| {
                unit_price(
                    BTreeMap::from([("input_audio.per_second".into(), rate)]),
                    Some(cost),
                )
            });
        return Some(Estimate {
            cost,
            price,
            basis: "metered",
            quantity_known: true,
            minimum: None,
        });
    }

    let rates = catalog.media_rates(&route)?;
    let quantity = input_characters.or(request.requirements.input_bytes);
    // Stated minima can break ties between unit-only quotes. A measured job
    // still fails closed on quantity rules this estimator cannot apply.
    // Scan every rate before measuring, so even a row with another unmeasurable
    // charge reports each unsupported quantum or floor once, not on every job.
    let mut unsupported = false;
    for (key, rate) in rates {
        if rate.increment != RateQuantityKind::Unspecified
            || (rate.minimum_quantity != RateQuantityKind::Unspecified
                && rate.minimum != Some(0)
                && (quantity.is_some() || rate.minimum.is_none()))
        {
            report_quantity_once(&route, key);
            unsupported = true;
        }
    }
    if unsupported {
        return None;
    }

    let mut cost = 0.0;
    let mut unit_rates = BTreeMap::new();
    for (key, rate) in rates {
        if matches!(rate.state.as_deref(), Some("stated_zero" | "not_billed")) {
            continue;
        }
        // The explicit Unicode scalar count is more precise than UTF-8 bytes.
        // Older callers can still provide bytes as a character-count ceiling;
        // neither count supplies a quantity for a per-token rate.
        let measured = match (request.kind.as_str(), key.as_str()) {
            ("synthesize", "rate.input.per_character") => quantity,
            _ => return None,
        };
        let rate = rate.unit_price?;
        // All known unit-only speech costs use input characters, so they are
        // comparable without guessing the text length. Other units stay unknown.
        cost += rate * measured.map_or(1.0, |quantity| quantity as f64);
        unit_rates.insert(key.strip_prefix("rate.")?.to_owned(), rate);
    }
    // No measured charge is not evidence of a free job. In particular a row
    // containing only known-zero exceptions must not win as an invented zero.
    if unit_rates.is_empty() || !cost.is_finite() {
        return None;
    }
    Some(Estimate {
        cost,
        price: unit_price(unit_rates, quantity.map(|_| cost)),
        basis: if input_characters.is_some() {
            "input_characters"
        } else {
            "bytes_as_characters_ceiling"
        },
        quantity_known: quantity.is_some(),
        minimum: if quantity.is_none() {
            rates
                .get("rate.input.per_character")
                .and_then(|rate| rate.minimum.zip(rate.unit_price))
                .map(|(quantity, rate)| quantity as f64 * rate)
        } else {
            None
        },
    })
}

fn unit_price(rates: BTreeMap<String, f64>, cost: Option<f64>) -> Option<UnitPrice> {
    let rates = rates
        .into_iter()
        .map(|(key, rate)| Some((key, UnitRate::new(Number::from_f64(rate)?))))
        .collect::<Option<_>>()?;
    let mut price = UnitPrice::metered(rates);
    if let Some(cost) = cost {
        price = price.with_estimated_task_usd(Number::from_f64(cost)?);
    }
    Some(price)
}

fn report_quantity_once(route: &str, rate: &str) {
    static REPORTED: OnceLock<Mutex<BTreeSet<(String, String)>>> = OnceLock::new();
    let Ok(mut reported) = REPORTED.get_or_init(Default::default).lock() else {
        return;
    };
    if reported.insert((route.into(), rate.into())) {
        eprintln!("[striatum] media price for {route} has unsupported increment or minimum_quantity on {rate}; route stays unpriced");
    }
}
