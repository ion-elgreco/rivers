use std::borrow::Cow;
use std::collections::HashSet;

use surrealdb::types::SurrealValue;

use super::PartitionKey;
use crate::storage::{KeyItem, KeySpan, SpanStep};

const HOUR_FMT: &str = "%Y-%m-%d-%H:%M";

fn hours(first: &str, last: &str) -> KeyItem {
    let step = SpanStep::Cron("0 * * * *".into());
    KeyItem::Span(KeySpan::new(first, last, step, HOUR_FMT).unwrap())
}

fn value(value: &str) -> KeyItem {
    KeyItem::Value(value.into())
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

/// The key of one run: one zone, three hours in a row and one hour apart.
fn zone_run() -> PartitionKey {
    PartitionKey::MultiItems {
        dims: vec![
            ("zone".into(), vec![value("132")]),
            (
                "hour".into(),
                vec![
                    hours("2024-01-01-00:00", "2024-01-01-02:00"),
                    value("2024-01-05-00:00"),
                ],
            ),
        ],
    }
}

fn zone_run_expanded() -> PartitionKey {
    PartitionKey::Multi {
        dims: vec![
            ("zone".into(), strings(&["132"])),
            (
                "hour".into(),
                strings(&[
                    "2024-01-01-00:00",
                    "2024-01-01-01:00",
                    "2024-01-01-02:00",
                    "2024-01-05-00:00",
                ]),
            ),
        ],
    }
}

fn hour_run() -> PartitionKey {
    PartitionKey::SingleItems {
        items: vec![
            value("2023-12-31-00:00"),
            hours("2024-01-01-00:00", "2024-01-01-02:00"),
        ],
    }
}

fn hour_run_expanded() -> PartitionKey {
    PartitionKey::Single {
        keys: strings(&[
            "2023-12-31-00:00",
            "2024-01-01-00:00",
            "2024-01-01-01:00",
            "2024-01-01-02:00",
        ]),
    }
}

/// Two zones and two spans: members must come in the order of the expanded key.
fn two_zones() -> PartitionKey {
    PartitionKey::MultiItems {
        dims: vec![
            (
                "hour".into(),
                vec![
                    hours("2024-01-01-00:00", "2024-01-01-02:00"),
                    hours("2024-03-01-00:00", "2024-03-01-03:00"),
                ],
            ),
            ("zone".into(), vec![value("132"), value("7")]),
        ],
    }
}

fn keys_with_spans() -> Vec<PartitionKey> {
    vec![
        zone_run(),
        hour_run(),
        two_zones(),
        PartitionKey::Set {
            keys: vec![hour_run(), zone_run()],
        },
    ]
}

#[test]
fn a_key_with_a_span_expands_to_the_key_with_each_value() {
    assert_eq!(zone_run().expanded().as_ref(), &zone_run_expanded());
    assert_eq!(hour_run().expanded().as_ref(), &hour_run_expanded());

    let set = PartitionKey::Set {
        keys: vec![hour_run(), zone_run()],
    };
    assert_eq!(
        set.expanded().as_ref(),
        &PartitionKey::Set {
            keys: vec![hour_run_expanded(), zone_run_expanded()],
        }
    );
}

#[test]
fn a_key_without_a_span_expands_to_itself_without_a_copy() {
    for key in [
        hour_run_expanded(),
        zone_run_expanded(),
        PartitionKey::Set {
            keys: vec![hour_run_expanded(), zone_run_expanded()],
        },
    ] {
        assert!(matches!(key.expanded(), Cow::Borrowed(same) if same == &key));
    }
}

#[test]
fn a_key_with_a_span_has_the_members_of_its_expanded_key_in_the_same_order() {
    for key in keys_with_spans() {
        let members = key.expanded().members();
        assert!(!members.is_empty());

        assert_eq!(key.members(), members, "{}", key.to_display());
        assert_eq!(key.member_count(), members.len());
        for (index, member) in members.iter().enumerate() {
            assert_eq!(key.member_at(index).as_ref(), Some(member), "at {index}");
        }
        assert_eq!(key.member_at(members.len()), None);
        for limit in 0..=members.len() + 1 {
            let first: Vec<PartitionKey> = members.iter().take(limit).cloned().collect();
            assert_eq!(key.members_preview(limit), first, "first {limit}");
        }
    }
}

#[test]
fn a_span_displays_as_its_first_and_last_window() {
    assert_eq!(
        zone_run().to_display(),
        "hour=2024-01-01-00:00..2024-01-01-02:00,2024-01-05-00:00|zone=132"
    );
    assert_eq!(
        hour_run().to_display(),
        "2023-12-31-00:00,2024-01-01-00:00..2024-01-01-02:00"
    );
}

#[test]
fn the_json_of_a_key_with_a_span_is_the_json_of_its_expanded_key() {
    for key in keys_with_spans() {
        assert_eq!(key.to_json(), key.expanded().to_json());
        assert_eq!(
            PartitionKey::from_json(&key.to_json()).unwrap(),
            key.expanded().into_owned()
        );
    }
}

#[test]
fn a_stored_key_with_a_span_reads_back_as_it_was() {
    for key in keys_with_spans() {
        let read = PartitionKey::from_value(key.clone().into_value()).unwrap();
        assert_eq!(read, key, "{}", key.to_display());
    }
}

#[test]
fn half_a_year_of_hours_for_one_zone_is_a_small_key() {
    let key = PartitionKey::MultiItems {
        dims: vec![
            ("zone".into(), vec![value("132")]),
            (
                "hour".into(),
                vec![hours("2024-01-01-00:00", "2024-06-30-23:00")],
            ),
        ],
    };

    assert_eq!(key.member_count(), 4368);
    assert_eq!(
        key.to_display(),
        "hour=2024-01-01-00:00..2024-06-30-23:00|zone=132"
    );
    let size = serde_json::to_string(&key).unwrap().len();
    assert!(size < 300, "{size} bytes");
    let expanded = serde_json::to_string(key.expanded().as_ref())
        .unwrap()
        .len();
    assert!(expanded > 80_000, "{expanded} bytes");
}

#[test]
fn keys_with_spans_are_equal_when_they_hold_the_same_items() {
    let PartitionKey::MultiItems { mut dims } = zone_run() else {
        unreachable!();
    };
    dims.reverse();
    let other_order = PartitionKey::MultiItems { dims };

    assert_eq!(zone_run(), other_order);
    let seen: HashSet<PartitionKey> = [zone_run(), other_order, hour_run()].into();
    assert_eq!(seen.len(), 2);

    assert_ne!(hour_run(), zone_run());
    assert_ne!(
        hour_run(),
        PartitionKey::SingleItems {
            items: vec![hours("2024-01-01-00:00", "2024-01-01-02:00")],
        }
    );
}

#[test]
fn a_key_with_a_span_is_not_equal_to_its_expanded_key() {
    // Equality compares how a key is written. Compare `expanded()` to ask
    // whether two keys name the same partitions.
    assert_ne!(zone_run(), zone_run_expanded());
    assert_ne!(hour_run(), hour_run_expanded());
}

#[test]
fn single_displays_bare_value() {
    let pk = PartitionKey::Single {
        keys: vec!["2024-01-01".into()],
    };
    assert_eq!(pk.to_display(), "2024-01-01");
}

#[test]
fn batched_single_joins_values_with_comma() {
    let pk = PartitionKey::Single {
        keys: vec!["a".into(), "b".into()],
    };
    assert_eq!(pk.to_display(), "a,b");
}

#[test]
fn multi_sorts_dims_and_pipe_joins() {
    let pk = PartitionKey::Multi {
        dims: vec![
            ("region".into(), vec!["us".into(), "eu".into()]),
            ("date".into(), vec!["2024-01-01".into()]),
        ],
    };
    assert_eq!(pk.to_display(), "date=2024-01-01|region=us,eu");
}

#[test]
fn member_at_follows_the_order_of_members() {
    let multi = PartitionKey::Multi {
        dims: vec![
            ("region".into(), vec!["us".into(), "eu".into()]),
            ("date".into(), vec!["d1".into(), "d2".into(), "d3".into()]),
            ("kind".into(), vec!["a".into()]),
        ],
    };
    let keys = [
        PartitionKey::Single {
            keys: vec!["a".into(), "b".into(), "c".into()],
        },
        multi.clone(),
        PartitionKey::Multi { dims: vec![] },
        PartitionKey::Set {
            keys: vec![
                PartitionKey::Single {
                    keys: vec!["x".into(), "y".into()],
                },
                multi,
            ],
        },
    ];

    for key in keys {
        let members = key.members();
        for (index, member) in members.iter().enumerate() {
            let at = key.member_at(index).expect("a member at each index");
            assert_eq!(&at, member, "{} at {index}", key.to_display());
            assert_eq!(at.to_json(), member.to_json(), "same dimension order");
        }
        assert_eq!(key.member_at(members.len()), None);
    }
}

#[test]
fn member_at_of_a_dimension_with_no_values_is_none() {
    let key = PartitionKey::Multi {
        dims: vec![
            ("date".into(), vec![]),
            ("region".into(), vec!["us".into()]),
        ],
    };
    assert_eq!(key.member_at(0), None);
}

#[test]
fn set_joins_member_displays() {
    let pk = PartitionKey::Set {
        keys: vec![
            PartitionKey::Single {
                keys: vec!["a".into()],
            },
            PartitionKey::Multi {
                dims: vec![("d".into(), vec!["x".into()])],
            },
        ],
    };
    assert_eq!(pk.to_display(), "a, d=x");
}
