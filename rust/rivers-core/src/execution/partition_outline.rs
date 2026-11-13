//! The keys of one dimension as the rows of an outline: groups of keys that
//! follow each other, down to the keys.
//!
//! The keys must ascend. A group is then one range of the stored keys, and
//! the store counts its partitions without a read of each key. A split finds
//! the edges of its groups by their place and does not read every key: a
//! dimension can have millions.

use std::ops::Range;

/// The most rows that one row of the outline opens to.
const ROWS_MOST: usize = 500;

/// The keys of one dimension, in ascending order.
pub trait Keys {
    fn len(&self) -> usize;

    /// The key at the place `index`, which is less than `len()`.
    fn key(&self, index: usize) -> String;

    /// The places where `key` is cut into the starts that the keys of a
    /// group share.
    fn cuts(&self, key: &str) -> Vec<usize> {
        cuts(key, &[])
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Keys for [String] {
    fn len(&self) -> usize {
        <[String]>::len(self)
    }

    fn key(&self, index: usize) -> String {
        self[index].clone()
    }
}

impl Keys for Vec<String> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn key(&self, index: usize) -> String {
        self[index].clone()
    }
}

/// A run of keys of one dimension that follow each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGroup {
    /// The start that the keys share (`2024-06`), the key when the group is
    /// one key, or `first … last`.
    pub label: String,
    pub first: String,
    pub last: String,
    /// The number of keys.
    pub keys: usize,
}

/// The groups that a run of keys splits into.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Split {
    pub groups: Vec<KeyGroup>,
    /// The length of the start that the keys of a group share. `None` when
    /// the groups are keys, or parts of equal size.
    pub shared: Option<usize>,
    /// Each group is one key.
    pub of_keys: bool,
}

/// Whether each key sorts after the key before it.
pub fn ascending(keys: &[String]) -> bool {
    keys.windows(2).all(|pair| pair[0] < pair[1])
}

/// The places of the keys from `first` to `last`. Both empty: every key.
pub fn between<K: Keys + ?Sized>(keys: &K, first: &str, last: &str) -> Range<usize> {
    if first.is_empty() && last.is_empty() {
        return 0..keys.len();
    }
    let start = first_place(0..keys.len(), |at| keys.key(at).as_str() >= first);
    let end = first_place(start..keys.len(), |at| keys.key(at).as_str() > last);
    start..end
}

/// The finest split of the keys at `places` with at most `most` groups.
pub fn split<K: Keys + ?Sized>(keys: &K, places: Range<usize>, most: usize) -> Split {
    if places.len() <= most {
        return of_keys(keys, places);
    }

    let mut finest: Option<(usize, Vec<Range<usize>>)> = None;
    let mut above = 1;
    for shared in keys.cuts(&keys.key(places.start)) {
        // More groups than `most`: every later cut has as many or more.
        let Some(runs) = runs_at(keys, places.clone(), shared, most) else {
            break;
        };
        if runs.len() > above {
            above = runs.len();
            finest = Some((shared, runs));
        }
    }

    match finest {
        Some((shared, runs)) => at_level(keys, shared, runs),
        None => parts(keys, places, most),
    }
}

/// The next split of the keys at `places`.
pub fn children<K: Keys + ?Sized>(keys: &K, places: Range<usize>) -> Split {
    if places.is_empty() {
        return of_keys(keys, places);
    }

    for shared in keys.cuts(&keys.key(places.start)) {
        let Some(runs) = runs_at(keys, places.clone(), shared, ROWS_MOST) else {
            break;
        };
        if runs.len() == places.len() {
            break;
        }
        if runs.len() > 1 {
            return at_level(keys, shared, runs);
        }
    }

    if places.len() <= ROWS_MOST {
        of_keys(keys, places)
    } else {
        let most = places.len().div_ceil(ROWS_MOST).min(ROWS_MOST);
        parts(keys, places, most)
    }
}

/// The length of the start that tells a key from the next one: the place
/// where the keys end to differ. `None` when there are fewer than two keys.
pub fn key_length<K: Keys + ?Sized>(keys: &K) -> Option<usize> {
    // One pair of keys can differ early: 23:00 and 00:00 of the next day.
    let sample: Vec<String> = (0..keys.len().min(4)).map(|at| keys.key(at)).collect();
    sample
        .windows(2)
        .map(|pair| {
            keys.cuts(&pair[0])
                .into_iter()
                .find(|shared| start(&pair[0], *shared) != start(&pair[1], *shared))
                .unwrap_or(pair[0].len())
        })
        .max()
}

/// The first place of `places` where `after` holds. `after` holds from that
/// place to the end of `places`.
fn first_place(places: Range<usize>, after: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (places.start, places.end);
    while low < high {
        let middle = low + (high - low) / 2;
        if after(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

/// The places before the end of `key` where it is cut into the starts that
/// the keys of a group share: `of_format`, the cuts of the format of the key,
/// or with none of them each place where a run of digits ends.
pub fn cuts(key: &str, of_format: &[usize]) -> Vec<usize> {
    if !of_format.is_empty() {
        return of_format
            .iter()
            .copied()
            .filter(|cut| *cut < key.len())
            .collect();
    }

    let bytes = key.as_bytes();
    (1..bytes.len())
        .filter(|&at| bytes[at - 1].is_ascii_digit() && !bytes[at].is_ascii_digit())
        .collect()
}

/// The first `shared` bytes of `key`, or the key when it is shorter.
fn start(key: &str, shared: usize) -> &str {
    key.get(..shared).unwrap_or(key)
}

/// The runs of keys at `places` that share a start of `shared` bytes, or
/// `None` when there are more than `most` runs.
fn runs_at<K: Keys + ?Sized>(
    keys: &K,
    places: Range<usize>,
    shared: usize,
    most: usize,
) -> Option<Vec<Range<usize>>> {
    let mut runs = Vec::new();
    let mut at = places.start;
    while at < places.end {
        if runs.len() == most {
            return None;
        }
        let key = keys.key(at);
        let shared_start = start(&key, shared);
        let end = first_place(at + 1..places.end, |next| {
            start(&keys.key(next), shared) != shared_start
        });
        runs.push(at..end);
        at = end;
    }
    Some(runs)
}

fn at_level<K: Keys + ?Sized>(keys: &K, shared: usize, runs: Vec<Range<usize>>) -> Split {
    let groups = runs
        .into_iter()
        .map(|run| {
            let first = keys.key(run.start);
            let label = start(&first, shared).to_string();
            group(keys, run, first, label)
        })
        .collect();
    Split {
        groups,
        shared: Some(shared),
        of_keys: false,
    }
}

fn of_keys<K: Keys + ?Sized>(keys: &K, places: Range<usize>) -> Split {
    let groups = places
        .map(|at| {
            let key = keys.key(at);
            KeyGroup {
                label: key.clone(),
                first: key.clone(),
                last: key,
                keys: 1,
            }
        })
        .collect();
    Split {
        groups,
        shared: None,
        of_keys: true,
    }
}

/// At most `most` parts of the same size, by the place of the keys.
fn parts<K: Keys + ?Sized>(keys: &K, places: Range<usize>, most: usize) -> Split {
    let size = places.len().div_ceil(most.max(1)).max(1);
    let groups = places
        .clone()
        .step_by(size)
        .map(|from| {
            let run = from..(from + size).min(places.end);
            let first = keys.key(run.start);
            let label = if run.len() == 1 {
                first.clone()
            } else {
                format!("{first} … {}", keys.key(run.end - 1))
            };
            group(keys, run, first, label)
        })
        .collect();
    Split {
        groups,
        shared: None,
        of_keys: size == 1,
    }
}

fn group<K: Keys + ?Sized>(keys: &K, run: Range<usize>, first: String, label: String) -> KeyGroup {
    KeyGroup {
        label,
        last: keys.key(run.end - 1),
        first,
        keys: run.len(),
    }
}

/// The marks of an axis along `groups`, at most `most`: the place of a group
/// and the text of its mark.
///
/// A mark is where the start that the groups share changes: `2024-02` along
/// groups of days. Groups with no such start get marks at equal steps.
/// `of_format` holds the cuts of the format of the keys, when it has some.
pub fn marks(groups: &[KeyGroup], of_format: &[usize], most: usize) -> Vec<(usize, String)> {
    let Some(first) = groups.first() else {
        return Vec::new();
    };

    let mut finest = None;
    let mut above = 1;
    for shared in cuts(&first.first, of_format) {
        let mut level: Vec<(usize, String)> = Vec::new();
        for (at, group) in groups.iter().enumerate() {
            let text = start(&group.first, shared);
            if level.last().is_none_or(|(_, last)| last != text) {
                level.push((at, text.to_string()));
            }
        }

        // More marks than `most`: every later cut has as many or more.
        if level.len() > most {
            break;
        }
        if level.len() > above {
            above = level.len();
            finest = Some(level);
        }
    }
    if let Some(level) = finest {
        return level;
    }

    let count = (most / 2).clamp(1, groups.len());
    (0..count)
        .map(|step| {
            let at = step * groups.len() / count;
            let group = &groups[at];
            let text = match group.first.starts_with(&group.label) {
                true => &group.label,
                false => &group.first,
            };
            (at, text.clone())
        })
        .collect()
}

/// The units of time of a key format, from the longest to the shortest.
const UNITS: [&str; 7] = ["year", "month", "week", "day", "hour", "minute", "second"];

/// The fields of the key format `fmt`: the unit of time that ends at each
/// place of a key, and whether every part of the format has a fixed width.
/// `%Y-%m-%d` gives `(4, "year")`, `(7, "month")`, `(10, "day")`.
fn fields(fmt: &str) -> (Vec<(usize, &'static str)>, bool) {
    let mut units = Vec::new();
    let mut at = 0;
    let mut chars = fmt.chars();

    while let Some(c) = chars.next() {
        if c != '%' {
            at += c.len_utf8();
            continue;
        }
        let widths: &[(usize, &str)] = match chars.next() {
            Some('Y') | Some('G') => &[(4, "year")],
            Some('m') => &[(2, "month")],
            Some('V') | Some('U') | Some('W') => &[(2, "week")],
            Some('d') => &[(2, "day")],
            Some('j') => &[(3, "day")],
            Some('H') => &[(2, "hour")],
            Some('M') => &[(2, "minute")],
            Some('S') => &[(2, "second")],
            Some('F') => &[(4, "year"), (3, "month"), (3, "day")],
            Some('T') => &[(2, "hour"), (3, "minute"), (3, "second")],
            Some('R') => &[(2, "hour"), (3, "minute")],
            Some('%') => {
                at += 1;
                continue;
            }
            _ => return (units, false),
        };
        for (width, unit) in widths {
            at += width;
            units.push((at, *unit));
        }
    }
    (units, true)
}

/// The unit of time of the start of `shared` bytes of a key in the format
/// `fmt`, if that start ends with a field of the format.
pub fn time_unit(fmt: &str, shared: usize) -> Option<&'static str> {
    fields(fmt)
        .0
        .into_iter()
        .find(|(at, _)| *at == shared)
        .map(|(_, unit)| unit)
}

/// The cuts of the key format `fmt`: the place where each field of time ends.
/// None when a part of the format has no fixed width.
pub fn time_cuts(fmt: &str) -> Vec<usize> {
    match fields(fmt) {
        (units, true) => units.into_iter().map(|(at, _)| at).collect(),
        (_, false) => Vec::new(),
    }
}

/// Whether the keys of the format `fmt` sort in the order of time: fields of
/// a fixed width, from the year down, each shorter than the one before.
pub fn time_format_ascends(fmt: &str) -> bool {
    let (units, fixed) = fields(fmt);
    let ranks: Vec<usize> = units
        .iter()
        .filter_map(|(_, unit)| UNITS.iter().position(|known| known == unit))
        .collect();
    fixed && ranks.first() == Some(&0) && ranks.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// Every hour of the first `days` days of 2024.
    fn hours(days: u32) -> Vec<String> {
        let mut keys = Vec::new();
        let mut left = days;
        for (month, length) in [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            .into_iter()
            .enumerate()
        {
            for day in 1..=length.min(left) {
                for hour in 0..24 {
                    keys.push(format!("2024-{:02}-{day:02}T{hour:02}:00", month + 1));
                }
            }
            left = left.saturating_sub(length);
        }
        keys
    }

    fn labels(split: &Split) -> Vec<&str> {
        split.groups.iter().map(|g| g.label.as_str()).collect()
    }

    fn whole(keys: &[String], most: usize) -> Split {
        split(keys, 0..keys.len(), most)
    }

    fn below(keys: &[String], first: &str, last: &str) -> Split {
        children(keys, between(keys, first, last))
    }

    #[test]
    fn marks_are_where_the_shared_start_of_the_groups_changes() {
        let keys = hours(91);
        let days = whole(&keys, 400);

        let marks = marks(&days.groups, &[], 12);

        assert_eq!(
            marks,
            vec![
                (0, "2024-01".to_string()),
                (31, "2024-02".to_string()),
                (60, "2024-03".to_string()),
            ]
        );
    }

    #[test]
    fn marks_of_few_groups_name_each_group() {
        let keys = hours(182);
        let months = whole(&keys, 62);

        let marks = marks(&months.groups, &[], 12);

        assert_eq!(marks.len(), 6);
        assert_eq!(marks[5], (5, "2024-06".to_string()));
    }

    #[test]
    fn marks_at_equal_steps_when_the_groups_share_no_start() {
        let zones: Vec<String> = (1..=265).map(|zone| format!("{zone:03}")).collect();
        let keys = whole(&zones, 400);
        let parts = whole(&zones, 62);

        assert_eq!(
            marks(&keys.groups, &[], 12),
            vec![
                (0, "001".to_string()),
                (44, "045".to_string()),
                (88, "089".to_string()),
                (132, "133".to_string()),
                (176, "177".to_string()),
                (220, "221".to_string()),
            ]
        );
        assert_eq!(marks(&parts.groups, &[], 12)[1], (8, "041".to_string()));
        assert_eq!(
            marks(&keys.groups[..1], &[], 12),
            vec![(0, "001".to_string())]
        );
        assert!(marks(&[], &[], 12).is_empty());
    }

    #[test]
    fn hours_split_into_months_days_and_keys() {
        let keys = hours(182);

        let months = whole(&keys, 62);
        assert_eq!(
            labels(&months),
            [
                "2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"
            ]
        );
        assert_eq!(months.shared, Some(7));
        assert_eq!(
            months.groups[1],
            KeyGroup {
                label: "2024-02".to_string(),
                first: "2024-02-01T00:00".to_string(),
                last: "2024-02-29T23:00".to_string(),
                keys: 696,
            }
        );

        let days = whole(&keys, 400);
        assert_eq!(days.groups.len(), 182);
        assert_eq!(days.shared, Some(10));
        assert_eq!(days.groups[0].label, "2024-01-01");
        assert_eq!(days.groups[0].keys, 24);

        let of_june = below(&keys, "2024-06-01T00:00", "2024-06-30T23:00");
        assert_eq!(of_june.groups.len(), 30);
        assert_eq!(of_june.groups[29].label, "2024-06-30");
        assert!(!of_june.of_keys);

        let of_a_day = below(&keys, "2024-06-30T00:00", "2024-06-30T23:00");
        assert_eq!(of_a_day.groups.len(), 24);
        assert_eq!(of_a_day.groups[23].label, "2024-06-30T23:00");
        assert!(of_a_day.of_keys);
    }

    #[test]
    fn a_level_with_one_group_is_left_out() {
        let keys = hours(182);

        // All keys are of 2024: the year is no level, the months are the first.
        let all = below(&keys, "", "");

        assert_eq!(all.groups.len(), 6);
        assert_eq!(all.shared, Some(7));
    }

    #[test]
    fn few_keys_are_not_split() {
        let keys: Vec<String> = hours(90).into_iter().step_by(24).collect();

        let listed = whole(&keys, 120);
        let by_month = whole(&keys, 62);

        assert_eq!(listed.groups.len(), 90);
        assert!(listed.of_keys);
        assert_eq!(labels(&by_month), ["2024-01", "2024-02", "2024-03"]);
    }

    #[test]
    fn keys_with_no_level_split_into_equal_parts() {
        let zones: Vec<String> = (1..=265).map(|zone| format!("{zone:03}")).collect();

        let listed = whole(&zones, 400);
        let in_parts = whole(&zones, 62);

        assert_eq!(listed.groups.len(), 265);
        assert!(listed.of_keys);
        assert_eq!(in_parts.groups.len(), 53);
        assert_eq!(in_parts.shared, None);
        assert!(!in_parts.of_keys);
        assert_eq!(in_parts.groups[0].label, "001 … 005");
        assert_eq!(in_parts.groups[0].keys, 5);
        assert_eq!(in_parts.groups[52].label, "261 … 265");
        assert_eq!(in_parts.groups.iter().map(|g| g.keys).sum::<usize>(), 265);
    }

    #[test]
    fn a_level_with_too_many_groups_is_not_used() {
        // 800 days: the days are more groups than a row opens to.
        let keys: Vec<String> = (0..800)
            .flat_map(|day| (0..2).map(move |half| format!("day-{day:04}T{half}")))
            .collect();

        let all = below(&keys, "", "");

        assert_eq!(all.shared, None);
        assert_eq!(all.groups.len(), 4);
        assert_eq!(all.groups[0].label, "day-0000T0 … day-0199T1");
        assert_eq!(all.groups[0].keys, 400);
    }

    #[test]
    fn many_keys_under_one_row_split_into_parts_that_open_to_keys() {
        let keys: Vec<String> = (0..1_200).map(|i| format!("customer-{i:05}")).collect();

        let all = below(&keys, "customer-00000", "customer-01199");
        let part = &all.groups[1];
        let of_part = below(&keys, &part.first, &part.last);

        assert_eq!(all.groups.len(), 3);
        assert_eq!(part.label, "customer-00400 … customer-00799");
        assert_eq!(of_part.groups.len(), 400);
        assert!(of_part.of_keys);
    }

    #[test]
    fn names_are_listed_as_keys() {
        let keys: Vec<String> = ["eu-central", "eu-west", "us-east", "us-west"]
            .map(String::from)
            .to_vec();

        let all = below(&keys, "eu-central", "us-west");

        assert_eq!(
            labels(&all),
            ["eu-central", "eu-west", "us-east", "us-west"]
        );
        assert!(all.of_keys);
    }

    #[test]
    fn bounds_that_hold_no_key_give_no_group() {
        let keys = hours(2);
        let none = Split {
            of_keys: true,
            ..Split::default()
        };

        assert_eq!(below(&keys, "2025", "2026"), none);
        assert_eq!(below(&keys, "2024-01-02", "2024-01-01"), none);
        assert_eq!(whole(&[], 10), none);
        assert_eq!(
            between(&keys[..], "2024-01-01T23:00", "2024-01-02T00:30"),
            23..25
        );
    }

    #[test]
    fn keys_ascend_when_each_sorts_after_the_one_before() {
        let key = |text: &str| text.to_string();

        assert!(ascending(&[key("2024-01-01"), key("2024-01-02")]));
        assert!(ascending(&[]));
        assert!(!ascending(&[
            key("01/02/2024"),
            key("01/01/2025"),
            key("02/01/2024")
        ]));
        assert!(!ascending(&[key("a"), key("a")]));
    }

    #[test]
    fn the_key_length_is_where_the_keys_end_to_differ() {
        let daily_at_midnight: Vec<String> = hours(40).into_iter().step_by(24).collect();
        let from_eleven_at_night: Vec<String> = hours(2).into_iter().skip(23).collect();
        let zones: Vec<String> = (1..=265).map(|zone| format!("{zone:03}")).collect();

        assert_eq!(key_length(&hours(40)[..]), Some(13));
        assert_eq!(key_length(&daily_at_midnight[..]), Some(10));
        assert_eq!(key_length(&from_eleven_at_night[..]), Some(13));
        assert_eq!(key_length(&zones[..]), Some(3));
        assert_eq!(key_length(&zones[..1]), None);
    }

    /// The keys of a time format, with the cuts of the format.
    struct OfFormat {
        keys: Vec<String>,
        cuts: Vec<usize>,
    }

    impl OfFormat {
        /// Every hour of the first `days` days of 2024, with no separator.
        fn hours(days: u32) -> Self {
            Self {
                keys: hours(days)
                    .into_iter()
                    .map(|key| key.replace(['-', 'T', ':'], "")[..10].to_string())
                    .collect(),
                cuts: time_cuts("%Y%m%d%H"),
            }
        }
    }

    impl Keys for OfFormat {
        fn len(&self) -> usize {
            self.keys.len()
        }

        fn key(&self, index: usize) -> String {
            self.keys[index].clone()
        }

        fn cuts(&self, key: &str) -> Vec<usize> {
            cuts(key, &self.cuts)
        }
    }

    #[test]
    fn keys_with_no_separator_split_at_the_fields_of_their_format() {
        let keys = OfFormat::hours(60);
        assert_eq!(keys.key(0), "2024010100");

        let months = split(&keys, 0..keys.len(), 31);
        let days = children(&keys, between(&keys, "2024020100", "2024022923"));
        let of_a_day = children(&keys, between(&keys, "2024022900", "2024022923"));

        assert_eq!(labels(&months), ["202401", "202402"]);
        assert_eq!(months.shared, Some(6));
        assert_eq!(months.groups[1].keys, 29 * 24);
        assert_eq!(days.groups.len(), 29);
        assert_eq!(days.shared, Some(8));
        assert_eq!(days.groups[28].label, "20240229");
        assert!(of_a_day.of_keys);
        assert_eq!(of_a_day.groups.len(), 24);
        assert_eq!(key_length(&keys), Some(10));
    }

    #[test]
    fn keys_with_no_separator_and_no_cuts_split_into_equal_parts() {
        let keys = OfFormat::hours(60).keys;

        let parts = whole(&keys, 31);

        assert_eq!(parts.shared, None);
        assert_eq!(parts.groups[0].label, "2024010100 … 2024010222");
    }

    #[test]
    fn marks_of_keys_with_no_separator_follow_the_cuts_of_their_format() {
        let keys = OfFormat::hours(91);
        let days = split(&keys, 0..keys.len(), 400);

        assert_eq!(days.groups.len(), 91);
        assert_eq!(
            marks(&days.groups, &keys.cuts, 12),
            vec![
                (0, "202401".to_string()),
                (31, "202402".to_string()),
                (60, "202403".to_string()),
            ]
        );
    }

    #[test]
    fn the_cuts_of_a_format_are_the_ends_of_its_fields() {
        assert_eq!(time_cuts("%Y-%m-%dT%H:%M"), [4, 7, 10, 13, 16]);
        assert_eq!(time_cuts("%Y%m%d%H"), [4, 6, 8, 10]);
        assert_eq!(time_cuts("%FT%T"), [4, 7, 10, 13, 16, 19]);
        assert_eq!(time_cuts("%Y-%b-%d"), [0usize; 0]);

        assert_eq!(cuts("2024010100", &[4, 6, 8, 10]), [4, 6, 8]);
        assert_eq!(
            cuts("2024-01-01T00:00", &[4, 7, 10, 13, 16]),
            [4, 7, 10, 13]
        );
        assert_eq!(cuts("2024-01-01T00:00", &[]), [4, 7, 10, 13]);
        assert_eq!(cuts("v2-2024-01", &[7, 10]), [7]);
        assert_eq!(cuts("customer", &[]), [0usize; 0]);
    }

    /// A dimension of a million keys that counts the keys it gives out.
    struct Seconds {
        read: Cell<usize>,
    }

    impl Keys for Seconds {
        fn len(&self) -> usize {
            1_000_000
        }

        fn key(&self, index: usize) -> String {
            self.read.set(self.read.get() + 1);
            let (day, second) = (index / 86_400, index % 86_400);
            format!(
                "2024-01-{:02}T{:02}:{:02}:{:02}",
                day + 1,
                second / 3_600,
                second / 60 % 60,
                second % 60
            )
        }
    }

    #[test]
    fn a_split_reads_few_keys_of_a_large_dimension() {
        let keys = Seconds { read: Cell::new(0) };

        let hours = split(&keys, 0..keys.len(), 400);
        let of_an_hour = children(
            &keys,
            between(&keys, "2024-01-03T05:00:00", "2024-01-03T05:59:59"),
        );

        assert_eq!(hours.groups.len(), 278);
        assert_eq!(hours.shared, Some(13));
        assert_eq!(hours.groups[277].label, "2024-01-12T13");
        assert_eq!(hours.groups[277].keys, 2_800);
        assert_eq!(of_an_hour.groups.len(), 60);
        assert_eq!(of_an_hour.groups[59].label, "2024-01-03T05:59");
        assert!(
            keys.read.get() < 100_000,
            "{} keys read of a million",
            keys.read.get()
        );
    }

    #[test]
    fn time_units_follow_the_fields_of_the_format() {
        assert_eq!(
            fields("%Y-%m-%dT%H:%M").0,
            [
                (4, "year"),
                (7, "month"),
                (10, "day"),
                (13, "hour"),
                (16, "minute")
            ]
        );
        assert_eq!(fields("%F %T").0[2..4], [(10, "day"), (13, "hour")]);
        assert_eq!(fields("%Y-W%V").0, [(4, "year"), (8, "week")]);
        assert_eq!(fields("%Y %B %d"), (vec![(4, "year")], false));

        assert_eq!(time_unit("%Y-%m-%dT%H:%M", 10), Some("day"));
        assert_eq!(time_unit("%Y-%m-%dT%H:%M", 11), None);
    }

    #[test]
    fn a_time_format_ascends_when_its_fields_go_from_the_year_down() {
        assert!(time_format_ascends("%Y-%m-%dT%H:%M"));
        assert!(time_format_ascends("%Y%m%d"));
        assert!(time_format_ascends("%F %T"));
        assert!(time_format_ascends("%G-W%V"));
        assert!(time_format_ascends("%Y-%j"));

        assert!(!time_format_ascends("%d/%m/%Y"));
        assert!(!time_format_ascends("%m-%d"));
        assert!(!time_format_ascends("%Y %B %d"));
        assert!(!time_format_ascends("%y-%m-%d"));
        assert!(!time_format_ascends("%Y-%-m"));
    }
}
