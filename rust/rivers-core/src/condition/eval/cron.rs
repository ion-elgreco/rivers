//! Cron and timezone evaluation: validation, wall-clock occurrence math,
//! and DST-gap resolution.
use std::collections::HashMap;

use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{SignedDuration, Timestamp, civil};

/// Validate a cron schedule at construction so bad input is rejected up front.
pub fn validate_cron(schedule: &str) -> anyhow::Result<()> {
    crate::timegrid::parse_cron(schedule).map(|_| ())
}

/// Validate an IANA timezone name at construction.
pub fn validate_timezone(tz: &str) -> anyhow::Result<()> {
    TimeZone::get(tz)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Next cron occurrence strictly after `after`, as a real UTC instant, evaluated
/// against the declared `timezone`'s WALL CLOCK.
pub fn next_cron_occurrence_utc(
    cron: &croner::Cron,
    after: Timestamp,
    timezone: Option<&str>,
) -> Option<Timestamp> {
    let Some(tz) = timezone.and_then(|t| TimeZone::get(t).ok()) else {
        let wall = after.to_zoned(TimeZone::UTC).datetime();
        let next = cron.find_next_occurrence(&wall, false).ok()?;
        return wall_as_utc(next);
    };

    let resolve_ambiguous =
        |earliest: Timestamp, latest: Timestamp| if earliest > after { earliest } else { latest };

    let wall_after = after.to_zoned(tz.clone()).datetime();
    let wall_next = cron.find_next_occurrence(&wall_after, false).ok()?;

    resolve_wall_instant(&tz, wall_next, resolve_ambiguous)
        .or_else(|| Some(wall_as_utc(wall_next)?.max(after + SignedDuration::from_mins(1))))
}

/// Read a wall-clock datetime as though it were UTC.
fn wall_as_utc(wall: civil::DateTime) -> Option<Timestamp> {
    TimeZone::UTC.to_timestamp(wall).ok()
}

/// Resolve a wall-clock datetime in `tz` to a real UTC instant. A wall time
/// inside a DST gap resolves to the transition itself — the first instant the
/// clock is valid again. `on_ambiguous` picks the instant when the wall time
/// repeats (fall-back).
pub(crate) fn resolve_wall_instant(
    tz: &TimeZone,
    wall: civil::DateTime,
    mut on_ambiguous: impl FnMut(Timestamp, Timestamp) -> Timestamp,
) -> Option<Timestamp> {
    match tz.to_ambiguous_timestamp(wall).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset.to_timestamp(wall).ok(),
        AmbiguousOffset::Gap { after, .. } => {
            // The post-gap offset maps every wall time in the gap to an
            // instant strictly before the transition, so the next transition
            // is the gap's end.
            let before_gap = after.to_timestamp(wall).ok()?;
            Some(tz.following(before_gap).next()?.timestamp())
        }
        AmbiguousOffset::Fold { before, after } => Some(on_ambiguous(
            before.to_timestamp(wall).ok()?,
            after.to_timestamp(wall).ok()?,
        )),
    }
}

/// The first real UTC instant of a wall-clock datetime in `tz`: the earlier
/// instant when the wall time repeats (fall-back), the first instant after
/// the gap when it does not exist (spring-forward).
pub(crate) fn first_real_instant(tz: &TimeZone, wall: civil::DateTime) -> Option<Timestamp> {
    resolve_wall_instant(tz, wall, |earliest, _| earliest)
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
    use std::cell::RefCell;

    thread_local! {
        static CRON_CACHE: RefCell<HashMap<String, croner::Cron>> = RefCell::new(HashMap::new());
        static TZ_CACHE: RefCell<HashMap<String, Option<TimeZone>>> =
            RefCell::new(HashMap::new());
    }

    let prev_secs = prev_nanos / 1_000_000_000;
    let now_secs = now_nanos / 1_000_000_000;

    let tz: Option<TimeZone> = timezone.and_then(|t| {
        TZ_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if !cache.contains_key(t) {
                cache.insert(t.to_string(), TimeZone::get(t).ok());
            }
            cache[t].clone()
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
        let (Ok(prev), Ok(now)) = (
            Timestamp::from_second(prev_secs),
            Timestamp::from_second(now_secs),
        ) else {
            return false;
        };

        let Some(tz) = tz else {
            let wall = prev.to_zoned(TimeZone::UTC).datetime();
            return cron
                .find_next_occurrence(&wall, false)
                .ok()
                .and_then(wall_as_utc)
                .map(|next| next <= now)
                .unwrap_or(false);
        };

        // Walk wall-clock occurrences from prev's wall projection, mapping
        // each to its first real instant; skip occurrences whose instant is
        // already in the past (the repeated fall-back hour projects the wall
        // clock behind real time).
        let mut cursor = prev.to_zoned(tz.clone()).datetime();
        for _ in 0..2000 {
            let Ok(next_wall) = cron.find_next_occurrence(&cursor, false) else {
                return false;
            };
            match first_real_instant(&tz, next_wall) {
                Some(real) if real <= prev => cursor = next_wall,
                Some(real) => return real <= now,
                None => cursor = next_wall,
            }
        }
        false
    })
}
