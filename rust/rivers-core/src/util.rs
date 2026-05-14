use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDateTime;

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Parse a partition key, falling back to date-only at midnight if `fmt` includes a time component.
pub fn parse_key_datetime(key: &str, fmt: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(key, fmt).or_else(|_| {
        chrono::NaiveDate::parse_from_str(key, fmt).map(|d| {
            d.and_hms_opt(0, 0, 0)
                .expect("00:00:00 is a valid NaiveTime")
        })
    })
}
