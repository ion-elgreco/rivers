use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDateTime;

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
pub fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Parse a partition key against `fmt`. Fully-specified formats (including
/// timestamp-derived ones like `%s`) go through `parse_from_str`; formats
/// that omit time fields — date-only, or hourly's `%Y-%m-%dT%H:00` which
/// carries an hour but no minute — fall back to a `Parsed` pass that
/// defaults the missing fields to zero instead of dropping them.
pub fn parse_key_datetime(key: &str, fmt: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    use chrono::format::{Parsed, StrftimeItems, parse};
    NaiveDateTime::parse_from_str(key, fmt).or_else(|e| {
        let mut parsed = Parsed::new();
        parse(&mut parsed, key, StrftimeItems::new(fmt))?;
        if parsed.timestamp().is_some() {
            // The instant came from the timestamp; zero-defaults would
            // conflict with it — surface the original error instead.
            return Err(e);
        }
        // Coarse fmts (monthly `%Y-%m`, yearly `%Y`) omit calendar fields;
        // default them to the window start unless week/ordinal info already
        // pins the date.
        let has_week_info = parsed.isoweek().is_some()
            || parsed.week_from_sun().is_some()
            || parsed.week_from_mon().is_some()
            || parsed.ordinal().is_some();
        if parsed.month().is_none() && !has_week_info {
            parsed.set_month(1)?;
        }
        if parsed.day().is_none() && !has_week_info && parsed.weekday().is_none() {
            parsed.set_day(1)?;
        }
        if parsed.hour_div_12().is_none() && parsed.hour_mod_12().is_none() {
            parsed.set_hour(0)?;
        }
        if parsed.minute().is_none() {
            parsed.set_minute(0)?;
        }
        let date = parsed.to_naive_date()?;
        let time = parsed.to_naive_time()?;
        Ok(date.and_time(time))
    })
}

#[cfg(test)]
mod tests {
    use super::parse_key_datetime;
    use chrono::NaiveDate;

    fn dt(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
    }

    #[test]
    fn date_only_fmt_defaults_to_midnight() {
        assert_eq!(
            parse_key_datetime("2024-01-05", "%Y-%m-%d").unwrap(),
            dt(2024, 1, 5, 0, 0, 0)
        );
    }

    #[test]
    fn hourly_fmt_preserves_the_hour() {
        // `%H:00` carries an hour but no minute — the hour must survive.
        assert_eq!(
            parse_key_datetime("2024-11-03T05:00", "%Y-%m-%dT%H:00").unwrap(),
            dt(2024, 11, 3, 5, 0, 0)
        );
    }

    #[test]
    fn month_only_fmt_defaults_to_first_day() {
        assert_eq!(
            parse_key_datetime("2024-03", "%Y-%m").unwrap(),
            dt(2024, 3, 1, 0, 0, 0)
        );
    }

    #[test]
    fn year_only_fmt_defaults_to_january_first() {
        assert_eq!(
            parse_key_datetime("2024", "%Y").unwrap(),
            dt(2024, 1, 1, 0, 0, 0)
        );
    }

    #[test]
    fn full_datetime_fmt_round_trips() {
        assert_eq!(
            parse_key_datetime("2024-01-05T06:07:08", "%Y-%m-%dT%H:%M:%S").unwrap(),
            dt(2024, 1, 5, 6, 7, 8)
        );
    }

    #[test]
    fn epoch_fmt_derives_time_from_the_timestamp() {
        // `%s` carries the whole instant; the zero-defaults for omitted
        // hour/minute fields must not clobber it.
        assert_eq!(
            parse_key_datetime("1704067200", "%s").unwrap(),
            dt(2024, 1, 1, 0, 0, 0)
        );
        assert_eq!(
            parse_key_datetime("1704110645", "%s").unwrap(),
            dt(2024, 1, 1, 12, 4, 5)
        );
    }

    #[test]
    fn trailing_garbage_rejected() {
        assert!(parse_key_datetime("2024-01-05XYZ", "%Y-%m-%d").is_err());
    }
}
