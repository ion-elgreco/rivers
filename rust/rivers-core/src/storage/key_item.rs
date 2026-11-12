//! One entry of a dimension in a partition key: a value, or a span of time windows.

use anyhow::{Result, bail};
use jiff::{SignedDuration, civil};
use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

use crate::timegrid::{TimeGrid, cron_ticks, cron_ticks_from, parse_cron};
use crate::util::parse_key_datetime;

/// One entry of a dimension: one value, or consecutive time windows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum KeyItem {
    Value(String),
    Span(KeySpan),
}

/// The distance from one window of a span to the next.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SpanStep {
    Cron(String),
    Seconds(i64),
}

/// Consecutive time windows, from `first` to `last`, both included. A span
/// holds the step between its windows and not the partition definition, so a
/// later change of the definition does not change the windows it names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeySpan {
    first: String,
    last: String,
    step: SpanStep,
    fmt: String,
    count: usize,
}

/// The time of `key`, which must be written exactly as `fmt` writes it.
fn exact_time(key: &str, fmt: &str) -> Result<civil::DateTime> {
    let time = parse_key_datetime(key, fmt)
        .map_err(|e| anyhow::anyhow!("'{key}' is not a time in the format '{fmt}': {e}"))?;
    let written = time.strftime(fmt).to_string();
    if written != key {
        bail!("'{key}' is not in the exact format '{fmt}' (expected '{written}')");
    }
    Ok(time)
}

/// Whole steps of `seconds` from `first` to `time`; `None` between two steps.
fn steps_between(first: civil::DateTime, time: civil::DateTime, seconds: i64) -> Option<usize> {
    let step = i128::from(seconds).checked_mul(1_000_000_000)?;
    let distance = time.duration_since(first).as_nanos();
    if step <= 0 || distance < 0 || distance % step != 0 {
        return None;
    }
    usize::try_from(distance / step).ok()
}

impl KeySpan {
    /// The span from `first` to `last`. Both must be windows of `step`,
    /// written exactly as `fmt` writes them.
    pub fn new(first: &str, last: &str, step: SpanStep, fmt: &str) -> Result<Self> {
        let from = exact_time(first, fmt)?;
        let to = exact_time(last, fmt)?;
        if to < from {
            bail!("the span {first}..{last} ends before it starts");
        }

        let count = match &step {
            SpanStep::Seconds(seconds) => {
                let steps = steps_between(from, to, *seconds).ok_or_else(|| {
                    anyhow::anyhow!(
                        "{last} is not a whole number of {seconds} s steps after {first}"
                    )
                })?;
                steps + 1
            }
            SpanStep::Cron(expr) => {
                let mut count = 0usize;
                for tick in cron_ticks_from(expr, from)? {
                    if (count == 0 && tick != from) || tick > to {
                        bail!(
                            "the span {first}..{last} does not start and end on the schedule '{expr}'"
                        );
                    }
                    count += 1;
                    if tick == to {
                        break;
                    }
                }
                count
            }
        };

        Ok(Self {
            first: first.to_string(),
            last: last.to_string(),
            step,
            fmt: fmt.to_string(),
            count,
        })
    }

    /// A span as storage holds it. The count of a cron span is not walked
    /// again: a read must not cost one step per window.
    fn stored(
        first: String,
        last: String,
        step: SpanStep,
        fmt: String,
        count: usize,
    ) -> Result<Self> {
        let from = exact_time(&first, &fmt)?;
        let to = exact_time(&last, &fmt)?;
        let fits = match &step {
            SpanStep::Seconds(seconds) => steps_between(from, to, *seconds) == count.checked_sub(1),
            SpanStep::Cron(expr) => parse_cron(expr).is_ok() && count >= 1 && from <= to,
        };
        if !fits {
            bail!("the stored span {first}..{last} does not hold {count} windows");
        }
        Ok(Self {
            first,
            last,
            step,
            fmt,
            count,
        })
    }

    /// The number of windows.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn first(&self) -> &str {
        &self.first
    }

    pub fn last(&self) -> &str {
        &self.last
    }

    fn display(&self) -> String {
        format!("{}..{}", self.first, self.last)
    }

    /// The windows from place `skip` on, at most `take` of them, in time order.
    fn windows(&self, skip: usize, take: usize) -> Vec<String> {
        let take = take.min(self.count.saturating_sub(skip));
        let Ok(first) = parse_key_datetime(&self.first, &self.fmt) else {
            return Vec::new();
        };
        let write = |time: civil::DateTime| time.strftime(&self.fmt).to_string();

        match &self.step {
            SpanStep::Seconds(seconds) => (skip..skip + take)
                .filter_map(|place| {
                    let offset = seconds.checked_mul(i64::try_from(place).ok()?)?;
                    first.checked_add(SignedDuration::from_secs(offset)).ok()
                })
                .map(write)
                .collect(),
            SpanStep::Cron(expr) => match cron_ticks_from(expr, first) {
                Ok(ticks) => ticks.skip(skip).take(take).map(write).collect(),
                Err(_) => Vec::new(),
            },
        }
    }

    /// The span in a form that answers [`SpanWindows::contains`] fast.
    pub(super) fn prepared(&self) -> Option<SpanWindows<'_>> {
        Some(SpanWindows {
            fmt: &self.fmt,
            first: parse_key_datetime(&self.first, &self.fmt).ok()?,
            last: parse_key_datetime(&self.last, &self.fmt).ok()?,
            step: PreparedStep::of(&self.step)?,
        })
    }
}

/// The windows of a span, with its times and schedule already parsed.
pub(super) struct SpanWindows<'a> {
    fmt: &'a str,
    first: civil::DateTime,
    last: civil::DateTime,
    step: PreparedStep,
}

/// A step with its schedule parsed.
enum PreparedStep {
    Seconds(i64),
    Cron(Box<croner::Cron>),
}

impl PreparedStep {
    fn of(step: &SpanStep) -> Option<Self> {
        match step {
            SpanStep::Seconds(seconds) => (*seconds > 0).then_some(Self::Seconds(*seconds)),
            SpanStep::Cron(expr) => Some(Self::Cron(Box::new(parse_cron(expr).ok()?))),
        }
    }

    /// How many of the first `values` are consecutive windows, each written
    /// exactly as `fmt` writes it.
    fn windows_in_a_row(&self, values: &[String], fmt: &str) -> usize {
        let Some(first) = values.first().and_then(|value| exact_time(value, fmt).ok()) else {
            return 0;
        };
        let is_next =
            |(time, value): &(civil::DateTime, &String)| time.strftime(fmt).to_string() == **value;

        match self {
            Self::Seconds(seconds) => (0i64..)
                .map_while(|place| {
                    let offset = seconds.checked_mul(place)?;
                    first.checked_add(SignedDuration::from_secs(offset)).ok()
                })
                .zip(values)
                .take_while(is_next)
                .count(),
            Self::Cron(cron) => cron_ticks(cron, first)
                .zip(values)
                .take_while(is_next)
                .count(),
        }
    }
}

impl SpanWindows<'_> {
    /// Whether `value` is one of the windows.
    pub(super) fn contains(&self, value: &str) -> bool {
        let Ok(time) = exact_time(value, self.fmt) else {
            return false;
        };
        if time < self.first || time > self.last {
            return false;
        }
        match &self.step {
            PreparedStep::Seconds(seconds) => steps_between(self.first, time, *seconds).is_some(),
            PreparedStep::Cron(cron) => cron.is_time_matching(&time).unwrap_or(false),
        }
    }
}

impl SpanStep {
    /// The step of `grid`. `None` for a grid that a span cannot follow: an
    /// interval that is not a whole number of seconds.
    fn of_grid(grid: &TimeGrid) -> Option<Self> {
        match (grid.interval_seconds, &grid.cron_schedule) {
            (Some(seconds), _) => {
                let whole = seconds >= 1.0 && seconds.fract() == 0.0 && seconds < i64::MAX as f64;
                whole.then_some(Self::Seconds(seconds as i64))
            }
            (None, Some(expr)) => Some(Self::Cron(expr.clone())),
            (None, None) => None,
        }
    }
}

/// A row of windows becomes a span from this length on.
const MIN_SPAN_WINDOWS: usize = 3;

/// The `values` of a time dimension as items. Three or more values in a row
/// that are consecutive windows of `grid`, each written exactly as its
/// format writes it, become one span; the items name the same values in the
/// same order. `None` when no span can be written.
///
/// A window must read back from its key as the same time. A partition
/// definition whose format does not do this is refused when it is made.
pub fn items_with_spans(values: &[String], grid: &TimeGrid) -> Option<Vec<KeyItem>> {
    if values.len() < MIN_SPAN_WINDOWS {
        return None;
    }
    let step = SpanStep::of_grid(grid)?;
    let walk = PreparedStep::of(&step)?;

    let mut items = Vec::new();
    let mut has_span = false;
    let mut place = 0;
    while place < values.len() {
        let row = walk.windows_in_a_row(&values[place..], &grid.fmt);
        if row >= MIN_SPAN_WINDOWS {
            items.push(KeyItem::Span(KeySpan {
                first: values[place].clone(),
                last: values[place + row - 1].clone(),
                step: step.clone(),
                fmt: grid.fmt.clone(),
                count: row,
            }));
            has_span = true;
            place += row;
        } else {
            items.push(KeyItem::Value(values[place].clone()));
            place += 1;
        }
    }
    has_span.then_some(items)
}

impl KeyItem {
    /// The number of values the item names.
    pub fn len(&self) -> usize {
        match self {
            Self::Value(_) => 1,
            Self::Span(span) => span.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn display(&self) -> String {
        match self {
            Self::Value(value) => value.clone(),
            Self::Span(span) => span.display(),
        }
    }
}

/// The number of values that `items` name.
pub(super) fn count(items: &[KeyItem]) -> usize {
    items
        .iter()
        .map(KeyItem::len)
        .fold(0usize, usize::saturating_add)
}

/// The values of `items` from place `skip` on, at most `take` of them, in order.
pub(super) fn values(items: &[KeyItem], mut skip: usize, take: usize) -> Vec<String> {
    let mut out = Vec::new();
    for item in items {
        if out.len() >= take {
            break;
        }
        let len = item.len();
        if skip >= len {
            skip -= len;
            continue;
        }
        match item {
            KeyItem::Value(value) => out.push(value.clone()),
            KeyItem::Span(span) => out.extend(span.windows(skip, take - out.len())),
        }
        skip = 0;
    }
    out
}

/// `items` as a reader sees them: `a,b` and `first..last` for a span.
pub(super) fn display(items: &[KeyItem]) -> String {
    items
        .iter()
        .map(KeyItem::display)
        .collect::<Vec<_>>()
        .join(",")
}

impl SurrealValue for KeyItem {
    fn kind_of() -> Kind {
        Kind::Either(vec![Kind::String, Kind::Object])
    }

    fn into_value(self) -> Value {
        let span = match self {
            Self::Value(value) => return value.into_value(),
            Self::Span(span) => span,
        };

        let mut map = std::collections::BTreeMap::new();
        map.insert("first".to_string(), span.first.into_value());
        map.insert("last".to_string(), span.last.into_value());
        map.insert("fmt".to_string(), span.fmt.into_value());
        map.insert("count".to_string(), (span.count as i64).into_value());
        match span.step {
            SpanStep::Cron(expr) => map.insert("cron".to_string(), expr.into_value()),
            SpanStep::Seconds(seconds) => map.insert("seconds".to_string(), seconds.into_value()),
        };
        Value::Object(map.into())
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        if let Ok(value) = String::from_value(value.clone()) {
            return Ok(Self::Value(value));
        }

        let mut map = <std::collections::BTreeMap<String, Value>>::from_value(value)?;
        let mut text = |field: &str| {
            map.remove(field)
                .ok_or_else(|| SurrealError::internal(format!("a key span has no '{field}'")))
                .and_then(String::from_value)
        };
        let first = text("first")?;
        let last = text("last")?;
        let fmt = text("fmt")?;
        let step = match map.remove("cron") {
            Some(expr) => SpanStep::Cron(String::from_value(expr)?),
            None => SpanStep::Seconds(
                map.remove("seconds")
                    .ok_or_else(|| SurrealError::internal("a key span has no step".to_string()))
                    .and_then(i64::from_value)?,
            ),
        };
        let count = map
            .remove("count")
            .ok_or_else(|| SurrealError::internal("a key span has no 'count'".to_string()))
            .and_then(i64::from_value)?;
        let count = usize::try_from(count)
            .map_err(|_| SurrealError::internal(format!("a key span holds {count} windows")))?;

        KeySpan::stored(first, last, step, fmt, count)
            .map(Self::Span)
            .map_err(|e| SurrealError::internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
