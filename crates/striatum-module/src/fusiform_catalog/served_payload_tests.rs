use super::super::{
    decode, decode_catalog_get, decode_catalog_status, decode_plane_get, PlaneCatalog, PlaneReply,
};
use serde::Deserialize;
use serde_json::{value::RawValue, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const GOLDEN: &[u8] = include_bytes!("../../fixtures/served-payloads-710f929f.json");
const ANTIGRAVITY_GOLDEN: &[u8] = include_bytes!("../../fixtures/served-payloads-dd928ca5.json");

#[derive(Deserialize)]
struct Case<'a> {
    request: Value,
    #[serde(borrow)]
    response: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

fn check(request: &Value, response: &[u8], error: bool) -> Result<(), String> {
    let arguments = &request["arguments"];
    match request["name"].as_str().unwrap() {
        "catalog.get" if arguments["auth_method"].is_string() => {
            let reply = decode_plane_get(response).map_err(|err| err.to_string())?;
            match &reply {
                PlaneReply::Served(page) if !error => {
                    if arguments["provider_id"] != page.plane.provider_id
                        || arguments["auth_method"] != page.plane.auth_method
                    {
                        return Err("Fusiform replied on a different billing plane".into());
                    }
                    if page.plane.provider_id == "opencode-go" {
                        let catalog = PlaneCatalog::default().with_test_plane(response);
                        for (route, family) in [
                            ("opencode-go/deepseek-v4-flash", "openai.chat_completions"),
                            ("opencode-go/qwen3.7-plus", "anthropic.messages"),
                        ] {
                            assert_eq!(catalog.wire_api(route, "apikey"), Some(Some(family)));
                            assert_eq!(catalog.row_serves_wire_api(route, "apikey"), Some(true));
                        }
                    }
                }
                PlaneReply::Refused { code, message } if error => {
                    let expected: Value = serde_json::from_slice(response).unwrap();
                    assert_eq!(expected["code"], *code);
                    assert_eq!(expected["message"], *message);
                }
                other => return Err(format!("wrong plane reply variant: {other:?}")),
            }
        }
        "catalog.get" => {
            assert!(!error);
            let snapshot = if arguments["paged"] == true {
                let page = decode::CatalogPage::read(response, true).map_err(|e| e.to_string())?;
                assert!(page.next_page_token.is_some());
                page.finish()
            } else {
                decode_catalog_get(response)
            }
            .map_err(|err| err.to_string())?;
            let payload: Value = serde_json::from_slice(response).unwrap();
            let models = payload["models"].as_object().unwrap();
            // A reply limited to selected facts can omit existence. Do not apply those rows.
            let partial_rows = models
                .values()
                .filter(|row| row.get("existence").is_none())
                .count();
            assert_eq!(snapshot.retained.len(), partial_rows);
            assert_eq!(snapshot.facts.len(), models.len() - partial_rows);
        }
        "catalog.status" => {
            assert!(!error);
            decode_catalog_status(response).map_err(|err| err.to_string())?;
        }
        "quality.grades" => {
            assert!(!error);
            let grades = super::decode(response).map_err(|err| err.to_string())?;
            let expected = if arguments["prefix"] == "local:" {
                super::LocalGrades::from_test_entries(&[(
                    "local:apple/phone-transcriber",
                    "transcription",
                    90.0,
                )])
            } else {
                Default::default()
            };
            assert_eq!(grades, expected);
        }
        other => unreachable!("unselected tool {other}"),
    }
    Ok(())
}

fn check_golden(golden: &[u8], methods: &[&str]) -> BTreeMap<String, usize> {
    let cases: BTreeMap<String, Case<'_>> = serde_json::from_slice(golden).unwrap();
    let mut failures = Vec::new();
    let mut checked = BTreeMap::new();
    for (name, case) in cases {
        // Routing never sends the by-model grade-history query.
        if name == "quality.grades — history" {
            continue;
        }
        let (requests, replies) = if let Some(requests) = case.request.as_array() {
            let replies: Vec<&RawValue> =
                serde_json::from_str(case.response.unwrap().get()).unwrap();
            assert_eq!(requests.len(), replies.len(), "{name}");
            (requests.as_slice(), replies)
        } else {
            (
                std::slice::from_ref(&case.request),
                vec![case.response.or(case.error).unwrap()],
            )
        };
        for (index, (request, reply)) in requests.iter().zip(replies).enumerate() {
            let method = request["name"].as_str().unwrap();
            if !methods.contains(&method) {
                continue;
            }
            *checked.entry(method.to_owned()).or_insert(0) += 1;
            if let Err(error) = check(request, reply.get().as_bytes(), case.error.is_some()) {
                failures.push(format!("{name} (exchange {}): {error}", index + 1));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    checked
}

#[test]
fn producer_served_payloads_reader_matrix() {
    assert_eq!(
        format!("{:x}", Sha256::digest(GOLDEN)),
        "cdb5589a6d054ff5a0e1887dcaa7f78025f0b96ab0903f7eb0a74d6404eb2ce9"
    );
    assert_eq!(
        check_golden(GOLDEN, &["catalog.get", "catalog.status", "quality.grades"]),
        [
            ("catalog.get".to_owned(), 121),
            ("catalog.status".to_owned(), 3),
            ("quality.grades".to_owned(), 2)
        ]
        .into()
    );
}

#[test]
fn antigravity_golden_catalog_get_reader_matrix() {
    assert_eq!(
        format!("{:x}", Sha256::digest(ANTIGRAVITY_GOLDEN)),
        "e1d000b295b29ed53db4effb134586d0391624eba9ee39891378e0fc316a1ac6"
    );
    assert_eq!(
        check_golden(ANTIGRAVITY_GOLDEN, &["catalog.get"]),
        [("catalog.get".to_owned(), 126)].into()
    );
}
