//! The ticks of a time grid as days and times of day.
//!
//! The time fields of a cron expression do not depend on the day, and an
//! interval that divides a day gives each day the same times. Each day of
//! such a grid has a tick at each time of day of the grid. A tick is then
//! found by its place with no walk from the start, and a search for a text
//! in the keys looks at each day and at each time of day once, not at each
//! key.

use std::collections::HashMap;

use anyhow::Result;
use jiff::SignedDuration;
use jiff::civil::{Date, DateTime, Time};

use super::{cron_ticks, parse_cron};

const DAY: SignedDuration = SignedDuration::from_hours(24);

/// The most times of day of a grid: one a second.
const TIMES_MOST: usize = 86_400;

/// The longest key that a search reads by its parts: one bit of a word for
/// each place where the text can start.
const KEY_BYTES_MOST: usize = u64::BITS as usize;

/// The fields of each cron nickname.
const NICKNAMES: [(&str, &str); 6] = [
    ("@yearly", "0 0 1 1 *"),
    ("@annually", "0 0 1 1 *"),
    ("@monthly", "0 0 1 * *"),
    ("@weekly", "0 0 * * 0"),
    ("@daily", "0 0 * * *"),
    ("@hourly", "0 * * * *"),
];

/// Keys that hold a text, each with its place in the grid, and the number of
/// all keys that hold it.
pub type Found = (Vec<(usize, String)>, usize);

fn midnight(date: Date) -> DateTime {
    date.at(0, 0, 0, 0)
}

fn days_between(first: Date, last: Date) -> i64 {
    midnight(last).duration_since(midnight(first)).as_secs() / DAY.as_secs()
}

/// The days of a grid that have ticks, ascending.
#[derive(Debug, Clone, PartialEq)]
enum Days {
    /// `count` days in a row from `first`.
    Every {
        first: Date,
        count: usize,
    },
    Listed(Vec<Date>),
}

impl Days {
    fn len(&self) -> usize {
        match self {
            Self::Every { count, .. } => *count,
            Self::Listed(days) => days.len(),
        }
    }

    /// The day at `place`, which is less than `len()`.
    fn get(&self, place: usize) -> Date {
        match self {
            Self::Every { first, .. } => i64::try_from(place)
                .ok()
                .and_then(|days| days.checked_mul(24))
                .and_then(|hours| {
                    midnight(*first)
                        .checked_add(SignedDuration::from_hours(hours))
                        .ok()
                })
                .map_or(Date::MAX, |day| day.date()),
            Self::Listed(days) => days[place],
        }
    }

    fn place(&self, day: Date) -> Option<usize> {
        match self {
            Self::Every { first, count } => usize::try_from(days_between(*first, day))
                .ok()
                .filter(|place| place < count),
            Self::Listed(days) => days.binary_search(&day).ok(),
        }
    }
}

/// The ticks of a time grid whose days all have ticks at the same times.
#[derive(Debug, Clone, PartialEq)]
pub struct DayGrid {
    days: Days,
    /// The times of day of the ticks, ascending. Each day has a tick at each.
    times: Vec<Time>,
    /// The ticks of the first day that are before the start of the grid.
    skip: usize,
    /// The number of ticks.
    len: usize,
}

impl DayGrid {
    fn empty() -> Self {
        Self {
            days: Days::Listed(Vec::new()),
            times: Vec::new(),
            skip: 0,
            len: 0,
        }
    }

    /// The ticks of a time window grid from `start` and before `end`.
    /// `None`: the days of the grid do not have the same ticks, or the grid
    /// is not one that this form reads.
    pub fn of(
        cron_schedule: Option<&str>,
        interval_seconds: Option<f64>,
        start: DateTime,
        end: DateTime,
    ) -> Result<Option<Self>> {
        match (interval_seconds, cron_schedule) {
            (Some(secs), _) => {
                let step = SignedDuration::from_nanos((secs * 1_000_000_000.0) as i64);
                Ok(Self::of_interval(start, step, end))
            }
            (None, Some(expr)) => Self::of_cron(expr, start, end),
            (None, None) => Ok(None),
        }
    }

    /// The ticks `start + n * step` before `end`. `None`: the step does not
    /// divide a day, or it is less than a second.
    fn of_interval(start: DateTime, step: SignedDuration, end: DateTime) -> Option<Self> {
        let (step, day) = (step.as_nanos(), DAY.as_nanos());
        if step <= 0 || day % step != 0 || day / step > TIMES_MOST as i128 {
            return None;
        }
        if end <= start {
            return Some(Self::empty());
        }

        let per_day = (day / step) as usize;
        let first = start.date();
        let since_midnight = start.duration_since(midnight(first)).as_nanos();
        let phase = since_midnight % step;
        let times = (0..per_day as i128)
            .map(|at| {
                let nanos = (phase + at * step) as i64;
                (midnight(first) + SignedDuration::from_nanos(nanos)).time()
            })
            .collect();

        let skip = (since_midnight / step) as usize;
        let len = usize::try_from((end.duration_since(start).as_nanos() - 1) / step + 1).ok()?;
        Some(Self {
            days: Days::Every {
                first,
                count: (skip + len).div_ceil(per_day),
            },
            times,
            skip,
            len,
        })
    }

    /// The ticks of the cron grid `expr` from `start` and before `end`.
    /// `None`: the expression does not have 5 or 6 fields: it has a field
    /// for the year.
    fn of_cron(expr: &str, start: DateTime, end: DateTime) -> Result<Option<Self>> {
        let expr = NICKNAMES
            .iter()
            .find(|(name, _)| expr.trim().eq_ignore_ascii_case(name))
            .map_or(expr, |(_, fields)| fields);

        let fields: Vec<&str> = expr.split_whitespace().collect();
        let day_fields = match fields.len() {
            5 => &fields[2..],
            6 => &fields[3..],
            _ => return Ok(None),
        };
        let cron = parse_cron(expr)?;
        let Some(first) = cron_ticks(&cron, start).next().filter(|tick| *tick < end) else {
            return Ok(Some(Self::empty()));
        };

        // Each day with a tick has the ticks of the first day.
        let first_day = first.date();
        let Ok(next_day) = midnight(first_day).checked_add(DAY) else {
            return Ok(None);
        };
        let times: Vec<Time> = cron_ticks(&cron, midnight(first_day))
            .take_while(|tick| *tick < next_day)
            .take(TIMES_MOST)
            .map(|tick| tick.time())
            .collect();
        let skip = match first_day == start.date() {
            true => times.partition_point(|time| *time < start.time()),
            false => 0,
        };

        let has_tick = |day: &Date| day.to_datetime(times[0]) < end;
        let days = if day_fields.iter().all(|field| *field == "*") {
            let last = match has_tick(&end.date()) {
                true => end.date(),
                false => end.date().yesterday()?,
            };
            Days::Every {
                first: first_day,
                count: days_between(first_day, last) as usize + 1,
            }
        } else {
            // The days come from the day fields alone: one tick a day.
            let at_midnight = vec!["0"; fields.len() - day_fields.len()].join(" ");
            let day_cron = parse_cron(&format!("{at_midnight} {}", day_fields.join(" ")))?;
            Days::Listed(
                cron_ticks(&day_cron, midnight(first_day))
                    .map(|tick| tick.date())
                    .take_while(has_tick)
                    .collect(),
            )
        };

        let last_day = days.get(days.len() - 1);
        let cut = match last_day == end.date() {
            true => times.len() - times.partition_point(|time| *time < end.time()),
            false => 0,
        };
        let len = days.len() * times.len() - skip - cut;
        Ok(Some(Self {
            days,
            times,
            skip,
            len,
        }))
    }

    /// The number of ticks.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The tick at the place `index`, which is less than `len()`.
    pub fn tick(&self, index: usize) -> DateTime {
        let at = index + self.skip;
        self.days
            .get(at / self.times.len())
            .to_datetime(self.times[at % self.times.len()])
    }

    /// The key at the place `index`, which is less than `len()`.
    pub fn key(&self, index: usize, fmt: &str) -> String {
        self.tick(index).strftime(fmt).to_string()
    }

    /// The place of the tick at `time`. `None`: the grid has no tick there.
    pub fn place(&self, time: DateTime) -> Option<usize> {
        let day = self.days.place(time.date())?;
        let of_day = self.times.binary_search(&time.time()).ok()?;
        (day * self.times.len() + of_day)
            .checked_sub(self.skip)
            .filter(|index| *index < self.len)
    }

    /// `limit` keys from the match number `offset` of the keys that hold
    /// `text`, in the order of the grid. `None`: the key format `fmt` has a
    /// part that this search does not read, and each key must be read.
    pub fn find(&self, fmt: &str, text: &str, offset: usize, limit: usize) -> Option<Found> {
        let layout = Layout::of(fmt)?;
        let width = layout.sources.len();
        if width > KEY_BYTES_MOST || !self.writes_keys(&layout, fmt) {
            return None;
        }

        let text = text.as_bytes();
        if self.is_empty() || text.len() > width {
            return Some((Vec::new(), 0));
        }
        if text.is_empty() {
            let page = (offset..self.len.min(offset.saturating_add(limit)))
                .map(|index| (index, self.key(index, fmt)))
                .collect();
            return Some((page, self.len));
        }

        // A key holds the text at a place when the characters that come from
        // the day match and the characters that come from the time match.
        // One bit for each place where the text can start.
        let starts = width - text.len() + 1;
        let held = |key: &[u8], of_time: bool| {
            (0..starts)
                .filter(|start| {
                    text.iter().enumerate().all(|(at, byte)| {
                        let place = start + at;
                        (layout.sources[place] == Source::Time) != of_time || key[place] == *byte
                    })
                })
                .fold(0u64, |held, start| held | 1 << start)
        };

        let mut key = vec![0u8; width];
        let of_times: Vec<u64> = self
            .times
            .iter()
            .map(|time| {
                layout.write_time(*time, &mut key);
                held(&key, true)
            })
            .collect();

        // The ticks of the day at `day` whose key holds the text.
        let per_day = self.times.len();
        let (of_times, in_grid) = (&of_times, &(self.skip..self.skip + self.len));
        let ticks = move |day: usize, of_day: u64| {
            (0..per_day)
                .filter(move |time| of_times[*time] & of_day != 0)
                .map(move |time| day * per_day + time)
                .filter(move |tick| in_grid.contains(tick))
        };

        let last_day = self.days.len() - 1;
        let mut whole_days: HashMap<u64, usize> = HashMap::new();
        let (mut total, mut page) = (0usize, Vec::new());

        for day in 0..self.days.len() {
            layout.write_day(self.days.get(day), &mut key);
            let of_day = held(&key, false);
            if of_day == 0 {
                continue;
            }

            let found = match day == 0 || day == last_day {
                true => ticks(day, of_day).count(),
                false => *whole_days
                    .entry(of_day)
                    .or_insert_with(|| ticks(day, of_day).count()),
            };

            if total + found > offset && page.len() < limit {
                let (before, room) = (offset.saturating_sub(total), limit - page.len());
                page.extend(ticks(day, of_day).skip(before).take(room).map(|tick| {
                    let index = tick - self.skip;
                    (index, self.key(index, fmt))
                }));
            }
            total += found;
        }

        Some((page, total))
    }

    /// Whether `layout` writes the first and the last key of the grid as the
    /// format does: a year of more or fewer than four digits does not fit.
    fn writes_keys(&self, layout: &Layout, fmt: &str) -> bool {
        if self.is_empty() {
            return true;
        }

        [0, self.len - 1].into_iter().all(|index| {
            let tick = self.tick(index);
            let mut key = vec![0u8; layout.sources.len()];
            layout.write_day(tick.date(), &mut key);
            layout.write_time(tick.time(), &mut key);
            key == tick.strftime(fmt).to_string().as_bytes()
        })
    }
}

/// Where a character of a key comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The day of the tick, or the format: the same for each tick of a day.
    Day,
    Time,
}

#[derive(Debug, Clone)]
enum Part {
    Literal(Vec<u8>),
    Year,
    Month,
    DayOfMonth,
    DayOfYear,
    Hour,
    Minute,
    Second,
}

impl Part {
    fn width(&self) -> usize {
        match self {
            Self::Literal(text) => text.len(),
            Self::Year => 4,
            Self::DayOfYear => 3,
            _ => 2,
        }
    }

    fn source(&self) -> Source {
        match self {
            Self::Hour | Self::Minute | Self::Second => Source::Time,
            _ => Source::Day,
        }
    }
}

/// A key format whose parts have a fixed width: each character of a key
/// comes from the day or from the time of day.
struct Layout {
    parts: Vec<Part>,
    /// The source of each character of a key.
    sources: Vec<Source>,
}

fn digits(value: i64, into: &mut [u8]) {
    let mut left = value.unsigned_abs();
    for byte in into.iter_mut().rev() {
        *byte = b'0' + (left % 10) as u8;
        left /= 10;
    }
}

impl Layout {
    /// `None`: the format has a part with no fixed width, or one that this
    /// layout does not know.
    fn of(fmt: &str) -> Option<Self> {
        let literal = |text: &str| Part::Literal(text.as_bytes().to_vec());
        let mut parts: Vec<Part> = Vec::new();
        let mut chars = fmt.chars();

        while let Some(c) = chars.next() {
            if c != '%' {
                parts.push(literal(c.encode_utf8(&mut [0; 4])));
                continue;
            }
            match chars.next()? {
                'Y' => parts.push(Part::Year),
                'm' => parts.push(Part::Month),
                'd' => parts.push(Part::DayOfMonth),
                'j' => parts.push(Part::DayOfYear),
                'H' => parts.push(Part::Hour),
                'M' => parts.push(Part::Minute),
                'S' => parts.push(Part::Second),
                '%' => parts.push(literal("%")),
                'F' => parts.extend([
                    Part::Year,
                    literal("-"),
                    Part::Month,
                    literal("-"),
                    Part::DayOfMonth,
                ]),
                'T' => parts.extend([
                    Part::Hour,
                    literal(":"),
                    Part::Minute,
                    literal(":"),
                    Part::Second,
                ]),
                'R' => parts.extend([Part::Hour, literal(":"), Part::Minute]),
                _ => return None,
            }
        }

        let sources = parts
            .iter()
            .flat_map(|part| std::iter::repeat_n(part.source(), part.width()))
            .collect();
        Some(Self { parts, sources })
    }

    /// Writes the characters of a key that do not come from the time of day.
    fn write_day(&self, day: Date, into: &mut [u8]) {
        let mut at = 0;
        for part in &self.parts {
            let text = &mut into[at..at + part.width()];
            match part {
                Part::Literal(literal) => text.copy_from_slice(literal),
                Part::Year => digits(day.year() as i64, text),
                Part::Month => digits(day.month() as i64, text),
                Part::DayOfMonth => digits(day.day() as i64, text),
                Part::DayOfYear => digits(day.day_of_year() as i64, text),
                Part::Hour | Part::Minute | Part::Second => {}
            }
            at += part.width();
        }
    }

    /// Writes the characters of a key that come from the time of day.
    fn write_time(&self, time: Time, into: &mut [u8]) {
        let mut at = 0;
        for part in &self.parts {
            let text = &mut into[at..at + part.width()];
            match part {
                Part::Hour => digits(time.hour() as i64, text),
                Part::Minute => digits(time.minute() as i64, text),
                Part::Second => digits(time.second() as i64, text),
                _ => {}
            }
            at += part.width();
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;

    /// Every tick of an interval grid, by a walk from the start.
    fn interval_walk(start: DateTime, step: SignedDuration, end: DateTime) -> Vec<DateTime> {
        let mut ticks = Vec::new();
        let mut tick = start;
        while tick < end {
            ticks.push(tick);
            tick = match tick.checked_add(step) {
                Ok(next) => next,
                Err(_) => break,
            };
        }
        ticks
    }

    /// Every tick of a cron grid, by a walk from the start.
    fn cron_walk(expr: &str, start: DateTime, end: DateTime) -> Vec<DateTime> {
        parse_cron(expr)
            .unwrap()
            .iter_from(start, croner::Direction::Forward)
            .take_while(|tick| *tick < end)
            .collect()
    }

    /// The keys that hold `text`, each with its place: the search that reads
    /// each key.
    fn scan(keys: &[String], text: &str) -> Vec<(usize, String)> {
        keys.iter()
            .enumerate()
            .filter(|(_, key)| key.contains(text))
            .map(|(index, key)| (index, key.clone()))
            .collect()
    }

    fn assert_same_ticks(grid: &DayGrid, ticks: &[DateTime], what: &str) {
        assert_eq!(grid.len(), ticks.len(), "{what}: number of ticks");
        for (index, tick) in ticks.iter().enumerate() {
            assert_eq!(grid.tick(index), *tick, "{what}: tick {index}");
            assert_eq!(grid.place(*tick), Some(index), "{what}: place of {tick}");
        }
    }

    /// Texts that some keys hold, and texts that no key holds.
    fn texts(keys: &[String]) -> Vec<String> {
        let mut texts: Vec<String> = ["", "0", "1", "zz", "9999", "T25", "-13-", ":61"]
            .into_iter()
            .map(String::from)
            .collect();
        if keys.is_empty() {
            return texts;
        }

        for key in [&keys[0], &keys[keys.len() / 2], &keys[keys.len() - 1]] {
            for from in 0..key.len() {
                for length in [1, 2, 3, 5, 8] {
                    texts.extend(key.get(from..from + length).map(String::from));
                }
            }
            texts.push(format!("{key}0"));
            texts.push(key.clone());
        }
        texts.sort();
        texts.dedup();
        texts
    }

    fn assert_same_search(grid: &DayGrid, ticks: &[DateTime], fmt: &str, what: &str) {
        let keys: Vec<String> = ticks
            .iter()
            .map(|tick| tick.strftime(fmt).to_string())
            .collect();
        assert_same_search_of(grid, &keys, fmt, &texts(&keys), what);
    }

    fn assert_same_search_of(
        grid: &DayGrid,
        keys: &[String],
        fmt: &str,
        texts: &[String],
        what: &str,
    ) {
        for text in texts {
            let all = scan(keys, text);
            for (offset, limit) in [(0, 100), (7, 3), (100, 100), (5_000, 50), (0, 0)] {
                let found = grid
                    .find(fmt, text, offset, limit)
                    .expect("a format that the search reads");
                let page: Vec<(usize, String)> =
                    all.iter().skip(offset).take(limit).cloned().collect();
                assert_eq!(
                    found,
                    (page, all.len()),
                    "{what}: '{text}' from {offset}, {limit} keys"
                );
            }
        }
    }

    #[test]
    fn an_interval_grid_is_days_and_times() {
        let at = |y, m, d, h, min, s| date(y, m, d).at(h, min, s, 0);
        let cases = [
            (
                at(2024, 1, 1, 0, 0, 0),
                3600,
                at(2024, 3, 1, 0, 0, 0),
                "%Y-%m-%dT%H:%M",
            ),
            (
                at(2024, 1, 1, 0, 30, 0),
                3600,
                at(2024, 1, 4, 0, 0, 0),
                "%Y-%m-%dT%H:%M",
            ),
            (
                at(2024, 1, 1, 5, 7, 0),
                900,
                at(2024, 1, 9, 13, 1, 0),
                "%Y-%m-%dT%H:%M",
            ),
            (
                at(2024, 2, 27, 22, 0, 0),
                86_400,
                at(2025, 3, 2, 21, 0, 0),
                "%Y-%m-%d",
            ),
            (
                at(2024, 1, 1, 0, 0, 0),
                86_400,
                at(2024, 1, 1, 0, 0, 1),
                "%Y%m%d",
            ),
            (
                at(2023, 12, 30, 0, 0, 0),
                21_600,
                at(2024, 1, 3, 6, 0, 0),
                "%Y-%jT%H",
            ),
            (
                at(2024, 1, 1, 0, 0, 0),
                3600,
                at(2024, 1, 1, 0, 0, 0),
                "%Y-%m-%dT%H:%M",
            ),
            (
                at(2024, 1, 1, 0, 0, 0),
                3600,
                at(2023, 1, 1, 0, 0, 0),
                "%Y-%m-%dT%H:%M",
            ),
            (
                at(2024, 5, 31, 12, 0, 0),
                43_200,
                at(2024, 6, 2, 12, 0, 1),
                "%FT%T",
            ),
            (
                at(2024, 1, 1, 0, 0, 0),
                1800,
                at(2024, 1, 3, 0, 0, 0),
                "%F %R",
            ),
        ];

        for (start, secs, end, fmt) in cases {
            let what = format!("{start} every {secs} s to {end}");
            let ticks = interval_walk(start, SignedDuration::from_secs(secs), end);
            let grid = DayGrid::of(None, Some(secs as f64), start, end)
                .unwrap()
                .unwrap_or_else(|| panic!("{what}: the step divides a day"));

            assert_same_ticks(&grid, &ticks, &what);
            assert_same_search(&grid, &ticks, fmt, &what);
        }
    }

    #[test]
    fn a_grid_with_a_tick_each_second_is_days_and_times() {
        let fmt = "%Y-%m-%dT%H:%M:%S";
        let start = date(2024, 1, 1).at(23, 59, 50, 0);
        let end = date(2024, 1, 2).at(0, 2, 5, 0);
        let ticks = interval_walk(start, SignedDuration::from_secs(1), end);
        let grid = DayGrid::of(None, Some(1.0), start, end).unwrap().unwrap();
        let keys: Vec<String> = ticks
            .iter()
            .map(|tick| tick.strftime(fmt).to_string())
            .collect();
        let texts = [
            "",
            "0",
            "5",
            "23:59:5",
            "01-01T23:59:59",
            "02T00:0",
            "00:02:04",
            "00:02:05",
            "2024-01-01T23:59:49",
            "zz",
        ]
        .map(String::from);

        assert_eq!(ticks.len(), 135);
        assert_same_ticks(&grid, &ticks, "a tick each second");
        assert_same_search_of(&grid, &keys, fmt, &texts, "a tick each second");
    }

    #[test]
    fn a_grid_with_a_start_between_seconds_is_days_and_times() {
        let start = date(2024, 1, 1).at(0, 0, 0, 250_000_000);
        let end = date(2024, 1, 4).at(6, 0, 0, 0);
        let ticks = interval_walk(start, SignedDuration::from_hours(6), end);
        let grid = DayGrid::of(None, Some(21_600.0), start, end)
            .unwrap()
            .unwrap();

        assert_same_ticks(&grid, &ticks, "a start at a quarter of a second");
        assert_same_search(
            &grid,
            &ticks,
            "%Y-%m-%dT%H:%M:%S",
            "a start between seconds",
        );
    }

    #[test]
    fn an_interval_that_does_not_divide_a_day_is_not_days_and_times() {
        let start = date(2024, 1, 1).at(0, 0, 0, 0);
        let end = date(2024, 2, 1).at(0, 0, 0, 0);
        let of = |secs: f64| DayGrid::of(None, Some(secs), start, end).unwrap();

        assert_eq!(of(7.0 * 3600.0), None);
        assert_eq!(of(7.0 * 86_400.0), None);
        assert_eq!(of(0.5), None);
        assert_eq!(of(0.0), None);
        assert_eq!(of(-3600.0), None);
    }

    #[test]
    fn a_cron_grid_is_days_and_times() {
        let at = |y, m, d, h, min, s| date(y, m, d).at(h, min, s, 0);
        let cases = [
            (
                "0 * * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 3, 1, 0, 0, 0),
            ),
            (
                "0 0 * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2025, 6, 1, 0, 0, 0),
            ),
            (
                "*/15 * * * *",
                at(2024, 1, 1, 0, 20, 0),
                at(2024, 1, 20, 7, 31, 0),
            ),
            (
                "0 */5 * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 3, 1, 0, 0, 0),
            ),
            (
                "30 2 * * *",
                at(2024, 1, 1, 3, 0, 0),
                at(2024, 6, 1, 2, 30, 0),
            ),
            (
                "30 2 * * *",
                at(2024, 1, 1, 2, 30, 0),
                at(2024, 6, 1, 2, 30, 1),
            ),
            (
                "0 9-17 * * 1-5",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 6, 1, 0, 0, 0),
            ),
            (
                "0 9-17 * * 1-5",
                at(2024, 1, 6, 12, 0, 0),
                at(2024, 3, 8, 11, 0, 0),
            ),
            (
                "*/20 8-18 * * 1-5",
                at(2024, 1, 5, 17, 50, 0),
                at(2024, 2, 5, 8, 20, 0),
            ),
            (
                "0 0 1 * *",
                at(2020, 1, 15, 0, 0, 0),
                at(2026, 1, 1, 0, 0, 0),
            ),
            (
                "0 6 1,15 * *",
                at(2023, 1, 1, 0, 0, 0),
                at(2025, 1, 1, 0, 0, 0),
            ),
            (
                "0 0 * * 0",
                at(2024, 1, 1, 0, 0, 0),
                at(2025, 1, 1, 0, 0, 0),
            ),
            (
                "0 12 13 * 5",
                at(2020, 1, 1, 0, 0, 0),
                at(2022, 1, 1, 0, 0, 0),
            ),
            (
                "0 0 L * *",
                at(2023, 1, 1, 0, 0, 0),
                at(2025, 1, 1, 0, 0, 0),
            ),
            (
                "0 8 * * 1#1",
                at(2023, 1, 1, 0, 0, 0),
                at(2025, 1, 1, 0, 0, 0),
            ),
            (
                "0 0 29 2 *",
                at(2000, 1, 1, 0, 0, 0),
                at(2030, 1, 1, 0, 0, 0),
            ),
            (
                "0 */6 * 1-3,7 *",
                at(2023, 2, 1, 0, 0, 0),
                at(2025, 1, 1, 0, 0, 0),
            ),
            (
                "0 0 ? * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 3, 1, 0, 0, 0),
            ),
            (
                "*/10 * * * * *",
                at(2024, 1, 1, 0, 0, 5),
                at(2024, 1, 1, 3, 0, 0),
            ),
            (
                "0 0 * * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 2, 1, 0, 0, 0),
            ),
            (
                "15,45 30 23 * * 6",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 6, 1, 0, 0, 0),
            ),
            (
                "0  *   * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 1, 3, 0, 0, 0),
            ),
            (
                "0 * * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2024, 1, 1, 0, 0, 0),
            ),
            (
                "0 * * * *",
                at(2024, 1, 1, 0, 0, 0),
                at(2023, 1, 1, 0, 0, 0),
            ),
            (
                "0 0 1 1 *",
                at(2024, 1, 2, 0, 0, 0),
                at(2024, 12, 31, 0, 0, 0),
            ),
        ];

        for (expr, start, end) in cases {
            let what = format!("'{expr}' from {start} to {end}");
            let ticks = cron_walk(expr, start, end);
            let grid = DayGrid::of(Some(expr), None, start, end)
                .unwrap()
                .unwrap_or_else(|| panic!("{what}: 5 or 6 fields"));

            assert_same_ticks(&grid, &ticks, &what);
            assert_same_search(&grid, &ticks, "%Y-%m-%dT%H:%M:%S", &what);
        }
    }

    #[test]
    fn an_end_between_seconds_keeps_the_tick_before_it() {
        let start = date(2024, 1, 1).at(0, 0, 0, 0);
        let end = date(2024, 1, 2).at(5, 0, 0, 1);
        let ticks = cron_walk("0 * * * *", start, end);
        let grid = DayGrid::of(Some("0 * * * *"), None, start, end)
            .unwrap()
            .unwrap();

        assert_eq!(ticks.len(), 30);
        assert_same_ticks(&grid, &ticks, "an end one nanosecond after a tick");
    }

    #[test]
    fn a_cron_nickname_is_days_and_times() {
        let start = date(2023, 11, 20).at(0, 30, 0, 0);
        let end = date(2025, 2, 3).at(0, 0, 0, 0);

        for expr in [
            "@yearly",
            "@annually",
            "@monthly",
            "@weekly",
            "@daily",
            "@hourly",
            " @Daily ",
        ] {
            let ticks = cron_walk(expr, start, end);
            let grid = DayGrid::of(Some(expr), None, start, end)
                .unwrap()
                .unwrap_or_else(|| panic!("{expr}: a nickname"));

            assert!(!ticks.is_empty(), "{expr}");
            assert_same_ticks(&grid, &ticks, expr);
        }
    }

    #[test]
    fn a_cron_expression_with_a_year_is_not_days_and_times() {
        let start = date(2024, 1, 1).at(0, 0, 0, 0);
        let end = date(2026, 1, 1).at(0, 0, 0, 0);
        let of = |expr: &str| DayGrid::of(Some(expr), None, start, end);

        assert_eq!(cron_walk("0 0 12 * * * 2025", start, end).len(), 365);
        assert_eq!(of("0 0 12 * * * 2025").unwrap(), None);
        assert!(of("61 * * * *").is_err());
    }

    #[test]
    fn a_time_that_is_not_a_tick_has_no_place() {
        let start = date(2024, 1, 1).at(6, 0, 0, 0);
        let end = date(2024, 1, 3).at(6, 0, 0, 0);
        let grid = DayGrid::of(Some("0 */6 * * *"), None, start, end)
            .unwrap()
            .unwrap();

        assert_eq!(grid.place(date(2024, 1, 1).at(6, 0, 0, 0)), Some(0));
        assert_eq!(grid.place(date(2024, 1, 3).at(0, 0, 0, 0)), Some(7));
        assert_eq!(grid.place(date(2024, 1, 1).at(0, 0, 0, 0)), None);
        assert_eq!(grid.place(date(2024, 1, 3).at(6, 0, 0, 0)), None);
        assert_eq!(grid.place(date(2024, 1, 2).at(7, 0, 0, 0)), None);
        assert_eq!(grid.place(date(2024, 1, 2).at(6, 0, 0, 1)), None);
        assert_eq!(grid.place(date(2023, 12, 31).at(18, 0, 0, 0)), None);
    }

    #[test]
    fn a_format_that_the_search_does_not_read_is_left_to_a_scan() {
        let start = date(2024, 1, 1).at(0, 0, 0, 0);
        let end = date(2024, 2, 1).at(0, 0, 0, 0);
        let grid = DayGrid::of(None, Some(3600.0), start, end)
            .unwrap()
            .unwrap();

        assert_eq!(grid.find("%Y-%b-%d %H", "Jan", 0, 10), None);
        assert_eq!(grid.find("%s", "17", 0, 10), None);
        assert_eq!(grid.find("%Y-%-m-%d %H", "1", 0, 10), None);
        assert_eq!(grid.find(&"%Y-%m-%dT%H".repeat(5), "1", 0, 10), None);
        assert!(grid.find("%Y-%m-%d %H%%", "1 0", 0, 10).is_some());
    }

    #[test]
    fn a_year_that_is_not_four_digits_is_left_to_a_scan() {
        let start = date(9999, 12, 1).at(0, 0, 0, 0);
        let end = date(9999, 12, 31).at(0, 0, 0, 0);
        let grid = DayGrid::of(None, Some(86_400.0), start, end)
            .unwrap()
            .unwrap();
        assert!(grid.find("%Y-%m-%d", "12", 0, 10).is_some());

        let start = date(-1, 12, 1).at(0, 0, 0, 0);
        let end = date(1, 1, 31).at(0, 0, 0, 0);
        let grid = DayGrid::of(None, Some(86_400.0), start, end)
            .unwrap()
            .unwrap();
        assert_eq!(grid.find("%Y-%m-%d", "12", 0, 10), None);
    }

    #[test]
    fn a_search_of_a_grid_with_a_tick_each_second_reads_no_key() {
        // 315 million keys: a search that reads each key takes minutes.
        let start = date(2015, 1, 1).at(0, 0, 0, 0);
        let end = date(2025, 1, 1).at(0, 0, 0, 0);
        let grid = DayGrid::of(None, Some(1.0), start, end).unwrap().unwrap();
        assert_eq!(grid.len(), 3_653 * 86_400);

        let (page, total) = grid
            .find("%Y-%m-%dT%H:%M:%S", "02-29T23:59:5", 0, 3)
            .unwrap();
        assert_eq!(total, 3 * 10);
        assert_eq!(
            page,
            vec![
                (36_719_990, "2016-02-29T23:59:50".to_string()),
                (36_719_991, "2016-02-29T23:59:51".to_string()),
                (36_719_992, "2016-02-29T23:59:52".to_string()),
            ]
        );

        let (page, total) = grid.find("%Y-%m-%dT%H:%M:%S", "0", 315_619_190, 2).unwrap();
        assert_eq!(total, 315_619_200);
        assert_eq!(page[0], (315_619_190, "2024-12-31T23:59:50".to_string()));
        assert_eq!(
            grid.find("%Y-%m-%dT%H:%M:%S", "zz", 0, 10).unwrap(),
            (vec![], 0)
        );
    }
}
