use std::time::{SystemTime, UNIX_EPOCH};

use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::TimeZone;
use jiff::{Timestamp, civil};

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Local wall-clock datetime for a nanosecond Unix timestamp.
pub fn local_datetime(nanos: i64) -> civil::DateTime {
    Timestamp::from_nanosecond(nanos as i128)
        .expect("i64 nanoseconds always fall inside jiff's timestamp range")
        .to_zoned(TimeZone::system())
        .datetime()
}

/// Parse a partition key against `fmt`. Formats that pin a whole instant
/// (`%s`) or a full date go straight through; coarse formats that omit
/// calendar fields — monthly `%Y-%m`, yearly `%Y` — fall back to a pass that
/// defaults the missing fields to the window start. Missing time fields need
/// no such pass: jiff already defaults them to zero, so hourly's
/// `%Y-%m-%dT%H:00` keeps its hour.
pub fn parse_key_datetime(key: &str, fmt: &str) -> Result<civil::DateTime, jiff::Error> {
    let mut tm = BrokenDownTime::parse(fmt, key)?;
    if let Some(ts) = tm.timestamp() {
        return Ok(ts.to_zoned(TimeZone::UTC).datetime());
    }
    let Err(short) = tm.to_datetime() else {
        return tm.to_datetime();
    };
    // Only reached when the calendar fields are short. A week-date or
    // day-of-year format resolves on the first try, so the defaults below
    // can't clobber it.
    if tm.month().is_none() {
        tm.set_month(Some(1))?;
    }
    if tm.day().is_none() {
        tm.set_day(Some(1))?;
    }
    tm.to_datetime().map_err(|_| short)
}

#[cfg(test)]
mod tests {
    use super::parse_key_datetime;
    use jiff::civil::date;

    #[test]
    fn date_only_fmt_defaults_to_midnight() {
        assert_eq!(
            parse_key_datetime("2024-01-05", "%Y-%m-%d").unwrap(),
            date(2024, 1, 5).at(0, 0, 0, 0)
        );
    }

    #[test]
    fn hourly_fmt_preserves_the_hour() {
        // `%H:00` carries an hour but no minute — the hour must survive.
        assert_eq!(
            parse_key_datetime("2024-11-03T05:00", "%Y-%m-%dT%H:00").unwrap(),
            date(2024, 11, 3).at(5, 0, 0, 0)
        );
    }

    #[test]
    fn month_only_fmt_defaults_to_first_day() {
        assert_eq!(
            parse_key_datetime("2024-03", "%Y-%m").unwrap(),
            date(2024, 3, 1).at(0, 0, 0, 0)
        );
    }

    #[test]
    fn year_only_fmt_defaults_to_january_first() {
        assert_eq!(
            parse_key_datetime("2024", "%Y").unwrap(),
            date(2024, 1, 1).at(0, 0, 0, 0)
        );
    }

    #[test]
    fn full_datetime_fmt_round_trips() {
        assert_eq!(
            parse_key_datetime("2024-01-05T06:07:08", "%Y-%m-%dT%H:%M:%S").unwrap(),
            date(2024, 1, 5).at(6, 7, 8, 0)
        );
    }

    #[test]
    fn epoch_fmt_derives_time_from_the_timestamp() {
        // `%s` carries the whole instant; the zero-defaults for omitted
        // hour/minute fields must not clobber it.
        assert_eq!(
            parse_key_datetime("1704067200", "%s").unwrap(),
            date(2024, 1, 1).at(0, 0, 0, 0)
        );
        assert_eq!(
            parse_key_datetime("1704110645", "%s").unwrap(),
            date(2024, 1, 1).at(12, 4, 5, 0)
        );
    }

    #[test]
    fn trailing_garbage_rejected() {
        assert!(parse_key_datetime("2024-01-05XYZ", "%Y-%m-%d").is_err());
    }
}
