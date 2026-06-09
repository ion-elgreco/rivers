use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDateTime;

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Parse a partition key against `fmt`, defaulting the time fields the
/// format omits to zero. A plain `NaiveDateTime::parse_from_str` errors when
/// the format lacks any time field (date-only fmts) — and a date-only
/// fallback silently DROPS the hour for formats like hourly's
/// `%Y-%m-%dT%H:00`, which carry an hour but no minute.
pub fn parse_key_datetime(key: &str, fmt: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    use chrono::format::{Parsed, StrftimeItems, parse};
    let mut parsed = Parsed::new();
    parse(&mut parsed, key, StrftimeItems::new(fmt))?;
    if parsed.hour_div_12().is_none() && parsed.hour_mod_12().is_none() {
        parsed.set_hour(0)?;
    }
    if parsed.minute().is_none() {
        parsed.set_minute(0)?;
    }
    let date = parsed.to_naive_date()?;
    let time = parsed.to_naive_time()?;
    Ok(date.and_time(time))
}
