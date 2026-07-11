//! Cron and timezone evaluation: validation, wall-clock occurrence math,
//! and DST-gap resolution.
use std::collections::HashMap;

/// Validate a cron schedule at construction so bad input is rejected up front.
pub fn validate_cron(schedule: &str) -> anyhow::Result<()> {
    crate::timegrid::parse_cron(schedule).map(|_| ())
}

/// Validate an IANA timezone name at construction (parsed via `chrono-tz`).
pub fn validate_timezone(tz: &str) -> anyhow::Result<()> {
    tz.parse::<chrono_tz::Tz>()
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Next cron occurrence strictly after `after`, as a real UTC instant, evaluated
/// against the declared `timezone`'s WALL CLOCK.
pub fn next_cron_occurrence_utc(
    cron: &croner::Cron,
    after: chrono::DateTime<chrono::Utc>,
    timezone: Option<&str>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone;

    let Some(tz) = timezone.and_then(|t| t.parse::<chrono_tz::Tz>().ok()) else {
        return cron.find_next_occurrence(&after, false).ok();
    };

    let resolve_ambiguous = |earliest: chrono::DateTime<chrono_tz::Tz>,
                             latest: chrono::DateTime<chrono_tz::Tz>| {
        let e = earliest.with_timezone(&chrono::Utc);
        if e > after {
            e
        } else {
            latest.with_timezone(&chrono::Utc)
        }
    };

    let naive_after = after.with_timezone(&tz).naive_local();
    let fake_after = chrono::Utc.from_utc_datetime(&naive_after);
    let fake_next = cron.find_next_occurrence(&fake_after, false).ok()?;
    let wall_next = fake_next.naive_utc();

    resolve_wall_instant(&tz, wall_next, resolve_ambiguous)
        .or_else(|| Some(fake_next.max(after + chrono::Duration::minutes(1))))
}

/// Resolve a wall-clock datetime in `tz` to a real UTC instant, walking
/// forward minute-by-minute through a DST gap (up to 4h) when the wall time
/// does not exist. `on_ambiguous` picks the instant when the wall time
/// repeats (fall-back).
pub(crate) fn resolve_wall_instant(
    tz: &chrono_tz::Tz,
    wall: chrono::NaiveDateTime,
    mut on_ambiguous: impl FnMut(
        chrono::DateTime<chrono_tz::Tz>,
        chrono::DateTime<chrono_tz::Tz>,
    ) -> chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone;
    let mut probe = wall;
    for _ in 0..=240 {
        match tz.from_local_datetime(&probe) {
            chrono::LocalResult::Single(dt) => return Some(dt.with_timezone(&chrono::Utc)),
            chrono::LocalResult::Ambiguous(earliest, latest) => {
                return Some(on_ambiguous(earliest, latest));
            }
            chrono::LocalResult::None => {}
        }
        probe += chrono::Duration::minutes(1);
    }
    None
}

/// The first real UTC instant of a wall-clock datetime in `tz`: the earlier
/// instant when the wall time repeats (fall-back), the first instant after
/// the gap when it does not exist (spring-forward).
pub(crate) fn first_real_instant(
    tz: &chrono_tz::Tz,
    wall: chrono::NaiveDateTime,
) -> Option<chrono::DateTime<chrono::Utc>> {
    resolve_wall_instant(tz, wall, |earliest, _| earliest.with_timezone(&chrono::Utc))
}

/// True when a cron occurrence falls within `(prev, now]`, compared as real
/// UTC instants. A wall time that repeats during a DST fall-back counts once,
/// at its first real instant — never twice.
pub(crate) fn cron_tick_between(
    cron_schedule: &str,
    prev_nanos: i64,
    now_nanos: i64,
    timezone: Option<&str>,
) -> bool {
    use chrono::TimeZone;
    use std::cell::RefCell;

    thread_local! {
        static CRON_CACHE: RefCell<HashMap<String, croner::Cron>> = RefCell::new(HashMap::new());
        static TZ_CACHE: RefCell<HashMap<String, Option<chrono_tz::Tz>>> =
            RefCell::new(HashMap::new());
    }

    let prev_secs = prev_nanos / 1_000_000_000;
    let now_secs = now_nanos / 1_000_000_000;

    let tz: Option<chrono_tz::Tz> = timezone.and_then(|t| {
        TZ_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if !cache.contains_key(t) {
                cache.insert(t.to_string(), t.parse().ok());
            }
            cache[t]
        })
    });

    CRON_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(cron_schedule) {
            cache.insert(
                cron_schedule.to_string(),
                crate::timegrid::parse_cron(cron_schedule)
                    .expect("cron schedule validated at construction"),
            );
        }
        let cron = &cache[cron_schedule];
        let (Some(prev), Some(now)) = (
            chrono::DateTime::from_timestamp(prev_secs, 0),
            chrono::DateTime::from_timestamp(now_secs, 0),
        ) else {
            return false;
        };

        let Some(tz) = tz else {
            return cron
                .find_next_occurrence(&prev, false)
                .map(|next| next <= now)
                .unwrap_or(false);
        };

        // Walk wall-clock occurrences from prev's wall projection, mapping
        // each to its first real instant; skip occurrences whose instant is
        // already in the past (the repeated fall-back hour projects the wall
        // clock behind real time).
        let mut fake_cursor = chrono::Utc.from_utc_datetime(&prev.with_timezone(&tz).naive_local());
        for _ in 0..2000 {
            let Ok(fake_next) = cron.find_next_occurrence(&fake_cursor, false) else {
                return false;
            };
            match first_real_instant(&tz, fake_next.naive_utc()) {
                Some(real) if real <= prev => fake_cursor = fake_next,
                Some(real) => return real <= now,
                None => fake_cursor = fake_next,
            }
        }
        false
    })
}
