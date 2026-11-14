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
}

impl Estimate {
    pub fn detail(&self) -> Value {
        serde_json::json!({"basis": self.basis, "estimated_task_usd": self.cost})
    }
}

pub(super) fn estimate(
    catalog: &CatalogSnapshot,
    request: &wire::SelectEngineRequest,
    provider: &str,
    model: &str,
) -> Option<Estimate> {
    let route = format!("{provider}/{model}");
    if matches!(
        request.kind,
        wire::Kind::Transcribe | wire::Kind::StreamTranscribe
    ) {
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
                    cost,
                )
            });
        return Some(Estimate {
            cost,
            price,
            basis: "metered",
        });
    }

    let rates = catalog.media_rates(&route)?;
    // Scan every rate before measuring, so even a row with another unmeasurable
    // charge reports each unsupported quantum or floor once, not on every job.
    let mut unsupported = false;
    for (key, rate) in rates {
        if rate.increment != RateQuantityKind::Unspecified
            || rate.minimum_quantity != RateQuantityKind::Unspecified
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
        // This closed pair is the only non-transcription unit served by the
        // pinned catalog with a request quantity. Bytes are a ceiling on UTF-8
        // characters, not an exact character count; tokens have no conversion.
        let quantity = match (request.kind.as_str(), key.as_str()) {
            ("synthesize", "rate.input.per_character") => request.requirements.input_bytes? as f64,
            _ => return None,
        };
        let rate = rate.unit_price?;
        cost += rate * quantity;
        unit_rates.insert(key.strip_prefix("rate.")?.to_owned(), rate);
    }
    // No measured charge is not evidence of a free job. In particular a row
    // containing only known-zero exceptions must not win as an invented zero.
    if unit_rates.is_empty() || !cost.is_finite() {
        return None;
    }
    Some(Estimate {
        cost,
        price: unit_price(unit_rates, cost),
        basis: "bytes_as_characters_ceiling",
    })
}

fn unit_price(rates: BTreeMap<String, f64>, cost: f64) -> Option<UnitPrice> {
    let rates = rates
        .into_iter()
        .map(|(key, rate)| Some((key, UnitRate::new(Number::from_f64(rate)?))))
        .collect::<Option<_>>()?;
    Some(UnitPrice::metered(rates).with_estimated_task_usd(Number::from_f64(cost)?))
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
