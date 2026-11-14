//! Line-oriented stderr logging. The daemon supplies timestamps and the module prefix.

use std::fmt::{self, Write as _};
use std::io::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

pub(crate) fn install() {
    // A thread-local test dispatcher still takes precedence over this fallback.
    let _ = tracing::subscriber::set_global_default(StderrSubscriber::from_env());
}

struct StderrSubscriber {
    filter: Filter,
    next_span: AtomicU64,
}

impl StderrSubscriber {
    fn from_env() -> Self {
        Self {
            filter: Filter::parse(
                std::env::var("STRIATUM_LOG")
                    .ok()
                    .or_else(|| std::env::var("RUST_LOG").ok())
                    .as_deref()
                    .unwrap_or("warn"),
            ),
            next_span: AtomicU64::new(1),
        }
    }
}

// Spans need identities for instrumentation, but are not printed: these logs are
// event records, not span lifecycle records. Event fields carry their own context.
impl Subscriber for StderrSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.filter.enabled(metadata)
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = EventLine::default();
        event.record(&mut visitor);
        // Hold stderr's lock for the whole record, including the newline, so
        // concurrent requests cannot interleave fragments of separate events.
        let _ = writeln!(
            io::stderr().lock(),
            "{} {}{}",
            event.metadata().level(),
            visitor.message,
            visitor.fields
        );
    }
}

/// Accept global levels and comma-separated target=level directives. The longest
/// matching target prefix wins; STRIATUM_LOG overrides RUST_LOG. Invalid directives
/// are ignored, and targets without an explicit directive keep the WARN default.
struct Filter {
    default: LevelFilter,
    targets: Vec<(String, LevelFilter)>,
}

impl Filter {
    fn parse(spec: &str) -> Self {
        let mut filter = Self {
            default: LevelFilter::WARN,
            targets: Vec::new(),
        };
        for directive in spec.split(',').map(str::trim) {
            if let Some((target, level)) = directive.split_once('=') {
                if let Some(level) = parse_level(level.trim()) {
                    if !target.trim().is_empty() {
                        filter.targets.push((target.trim().to_owned(), level));
                    }
                }
            } else if let Some(level) = parse_level(directive) {
                filter.default = level;
            }
        }
        filter
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= self.level_for(metadata.target())
    }

    fn level_for(&self, target: &str) -> LevelFilter {
        self.targets
            .iter()
            .enumerate()
            .filter(|(_, (prefix, _))| target.starts_with(prefix))
            .max_by_key(|(index, (prefix, _))| (prefix.len(), *index))
            .map_or(self.default, |(_, (_, level))| *level)
    }
}

fn parse_level(value: &str) -> Option<LevelFilter> {
    match value.to_ascii_lowercase().as_str() {
        "off" => Some(LevelFilter::OFF),
        "error" => Some(LevelFilter::ERROR),
        "warn" => Some(LevelFilter::WARN),
        "info" => Some(LevelFilter::INFO),
        "debug" => Some(LevelFilter::DEBUG),
        "trace" => Some(LevelFilter::TRACE),
        _ => None,
    }
}

#[derive(Default)]
struct EventLine {
    message: String,
    fields: String,
}

impl EventLine {
    fn field(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = escape_line(&value);
        } else {
            let _ = write!(self.fields, " {}={}", field.name(), value);
        }
    }
}

impl Visit for EventLine {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.field(field, value.to_owned());
        } else {
            self.field(field, field_value(value));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.field(field, rendered);
        } else {
            // Debug strings already include quotes; display wrappers and other
            // Debug values may still need quoting to keep a field on one line.
            let rendered = if rendered.starts_with('"') && rendered.ends_with('"') {
                escape_line(&rendered)
            } else {
                field_value(&rendered)
            };
            self.field(field, rendered);
        }
    }
}

fn field_value(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '"' | '\\'))
    {
        format!("{value:?}")
    } else {
        value.to_owned()
    }
}

fn escape_line(value: &str) -> String {
    value.replace('\r', "\\r").replace('\n', "\\n")
}

#[cfg(test)]
#[path = "logging/tests.rs"]
mod subprocess_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_default_to_warn_and_honor_global_and_target_levels() {
        let default = Filter::parse("");
        assert_eq!(default.level_for("striatum_store"), LevelFilter::WARN);
        let filter = Filter::parse(
            "error,striatum_store=debug,striatum_store::blocks=trace,striatum_module=off",
        );
        assert_eq!(filter.level_for("other"), LevelFilter::ERROR);
        assert_eq!(filter.level_for("striatum_store"), LevelFilter::DEBUG);
        assert_eq!(
            filter.level_for("striatum_store::blocks"),
            LevelFilter::TRACE
        );
        assert_eq!(filter.level_for("striatum_module"), LevelFilter::OFF);
        assert_eq!(Filter::parse("info").level_for("other"), LevelFilter::INFO);
        assert_eq!(
            Filter::parse("bad,striatum_store=nope").level_for("striatum_store"),
            LevelFilter::WARN
        );
        assert_eq!(
            Filter::parse("x=info,x=debug").level_for("x"),
            LevelFilter::DEBUG
        );
    }

    #[test]
    fn field_strings_are_quoted_and_line_breaks_are_escaped() {
        assert_eq!(field_value("plain"), "plain");
        assert_eq!(field_value("two words"), r#""two words""#);
        assert_eq!(field_value("line\nbreak"), r#""line\nbreak""#);
        assert_eq!(field_value(""), r#""""#);
        assert_eq!(escape_line("line\r\nbreak"), r#"line\r\nbreak"#);
    }
}
