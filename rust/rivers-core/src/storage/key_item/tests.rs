use super::*;
use crate::timegrid::TimeGrid;

const HOUR_FMT: &str = "%Y-%m-%d-%H:%M";

fn hourly_cron(first: &str, last: &str) -> KeySpan {
    KeySpan::new(first, last, SpanStep::Cron("0 * * * *".into()), HOUR_FMT).unwrap()
}

fn hourly_seconds(first: &str, last: &str) -> KeySpan {
    KeySpan::new(first, last, SpanStep::Seconds(3600), HOUR_FMT).unwrap()
}

#[test]
fn a_span_names_each_window_from_first_to_last() {
    for span in [
        hourly_cron("2024-01-01-22:00", "2024-01-02-01:00"),
        hourly_seconds("2024-01-01-22:00", "2024-01-02-01:00"),
    ] {
        assert_eq!(span.len(), 4);
        assert_eq!(
            span.windows(0, usize::MAX),
            [
                "2024-01-01-22:00",
                "2024-01-01-23:00",
                "2024-01-02-00:00",
                "2024-01-02-01:00"
            ]
        );
        assert_eq!(span.windows(1, 2), ["2024-01-01-23:00", "2024-01-02-00:00"]);
        assert_eq!(span.windows(3, 5), ["2024-01-02-01:00"]);
        assert_eq!(span.windows(4, 5), Vec::<String>::new());
    }
}

#[test]
fn a_span_of_half_a_year_of_hours_counts_4368_windows() {
    for span in [
        hourly_cron("2024-01-01-00:00", "2024-06-30-23:00"),
        hourly_seconds("2024-01-01-00:00", "2024-06-30-23:00"),
    ] {
        assert_eq!(span.len(), 4368);
        let windows = span.windows(0, usize::MAX);
        assert_eq!(windows.len(), 4368);
        assert_eq!(windows[0], "2024-01-01-00:00");
        assert_eq!(windows[4367], "2024-06-30-23:00");
        assert_eq!(span.windows(4367, 1), ["2024-06-30-23:00"]);
    }
}

#[test]
fn a_monthly_cron_span_follows_the_calendar() {
    let span = KeySpan::new(
        "2024-01-01",
        "2024-04-01",
        SpanStep::Cron("0 0 1 * *".into()),
        "%Y-%m-%d",
    )
    .unwrap();
    assert_eq!(
        span.windows(0, usize::MAX),
        ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
    );
}

#[test]
fn a_span_must_start_and_end_on_a_window_in_the_exact_format() {
    let cron = || SpanStep::Cron("0 * * * *".into());
    let seconds = || SpanStep::Seconds(3600);

    for (first, last, step) in [
        ("2024-01-01-00:30", "2024-01-01-03:00", cron()),
        ("2024-01-01-00:00", "2024-01-01-03:30", cron()),
        ("2024-01-01-00:00", "2024-01-01-03:30", seconds()),
        ("2024-01-01-03:00", "2024-01-01-00:00", cron()),
        ("2024-01-01-03:00", "2024-01-01-00:00", seconds()),
        ("2024-1-1-00:00", "2024-01-01-03:00", seconds()),
        ("2024-01-01-00:00", "2024-01-01-03:00", SpanStep::Seconds(0)),
        (
            "2024-01-01-00:00",
            "2024-01-01-03:00",
            SpanStep::Cron("not a cron".into()),
        ),
    ] {
        assert!(
            KeySpan::new(first, last, step.clone(), HOUR_FMT).is_err(),
            "{first}..{last} with {step:?}"
        );
    }
}

#[test]
fn a_prepared_span_tells_its_windows_from_other_keys() {
    for span in [
        hourly_cron("2024-01-01-22:00", "2024-01-02-01:00"),
        hourly_seconds("2024-01-01-22:00", "2024-01-02-01:00"),
    ] {
        let windows = span.prepared().unwrap();
        for inside in ["2024-01-01-22:00", "2024-01-02-00:00", "2024-01-02-01:00"] {
            assert!(windows.contains(inside), "{inside}");
        }
        for outside in [
            "2024-01-01-21:00",
            "2024-01-02-02:00",
            "2024-01-01-22:30",
            "2024-1-1-22:00",
            "us",
        ] {
            assert!(!windows.contains(outside), "{outside}");
        }
    }
}

#[test]
fn values_of_items_follow_the_order_of_the_items() {
    let items = [
        KeyItem::Value("2023-12-31-00:00".into()),
        KeyItem::Span(hourly_cron("2024-01-01-00:00", "2024-01-01-02:00")),
        KeyItem::Value("2024-02-01-00:00".into()),
    ];
    let all = [
        "2023-12-31-00:00",
        "2024-01-01-00:00",
        "2024-01-01-01:00",
        "2024-01-01-02:00",
        "2024-02-01-00:00",
    ];

    assert_eq!(count(&items), 5);
    assert_eq!(values(&items, 0, usize::MAX), all);
    for skip in 0..=5 {
        for take in 0..=5 {
            let expected: Vec<&str> = all.iter().copied().skip(skip).take(take).collect();
            assert_eq!(
                values(&items, skip, take),
                expected,
                "skip {skip}, take {take}"
            );
        }
    }
    assert_eq!(
        display(&items),
        "2023-12-31-00:00,2024-01-01-00:00..2024-01-01-02:00,2024-02-01-00:00"
    );
}

#[test]
fn a_stored_item_reads_back_as_it_was() {
    for item in [
        KeyItem::Value("us".into()),
        KeyItem::Span(hourly_cron("2024-01-01-00:00", "2024-06-30-23:00")),
        KeyItem::Span(hourly_seconds("2024-01-01-00:00", "2024-06-30-23:00")),
    ] {
        assert_eq!(
            KeyItem::from_value(item.clone().into_value()).unwrap(),
            item
        );
    }
}

#[test]
fn a_stored_span_that_does_not_fit_is_refused() {
    let stored = |first: &str, last: &str, step: SpanStep, count: usize| {
        KeySpan::stored(first.into(), last.into(), step, HOUR_FMT.into(), count)
    };
    let seconds = || SpanStep::Seconds(3600);

    assert!(stored("2024-01-01-00:00", "2024-01-01-03:00", seconds(), 4).is_ok());
    assert!(stored("2024-01-01-00:00", "2024-01-01-03:00", seconds(), 5).is_err());
    assert!(stored("2024-01-01-00:00", "2024-01-01-03:00", seconds(), 0).is_err());
    assert!(stored("2024-1-1-00:00", "2024-01-01-03:00", seconds(), 4).is_err());
    assert!(
        stored(
            "2024-01-01-00:00",
            "2024-01-01-03:00",
            SpanStep::Cron("x".into()),
            4
        )
        .is_err()
    );
}

fn hourly_grids() -> [TimeGrid; 2] {
    let grid = |cron: bool| TimeGrid {
        cron_schedule: cron.then(|| "0 * * * *".to_string()),
        interval_seconds: (!cron).then_some(3600.0),
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: None,
        fmt: HOUR_FMT.into(),
    };
    [grid(true), grid(false)]
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

fn shown(items: &[KeyItem]) -> String {
    display(items)
}

#[test]
fn three_or_more_windows_in_a_row_become_one_span() {
    let hours = strings(&[
        "2024-01-01-21:00",
        "2024-01-01-22:00",
        "2024-01-01-23:00",
        "2024-01-02-00:00",
        "2024-01-02-01:00",
    ]);

    for grid in hourly_grids() {
        let items = items_with_spans(&hours, &grid).expect("a span");
        assert_eq!(shown(&items), "2024-01-01-21:00..2024-01-02-01:00");
        assert_eq!(count(&items), 5);
        assert_eq!(values(&items, 0, usize::MAX), hours);
    }
}

#[test]
fn windows_apart_stay_values_between_the_spans() {
    let hours = strings(&[
        "2024-01-01-00:00",
        "2024-01-01-01:00",
        "2024-01-01-02:00",
        "2024-01-01-05:00",
        "2024-01-01-07:00",
        "2024-01-01-08:00",
        "2024-01-01-09:00",
        "2024-01-01-20:00",
        "2024-01-01-21:00",
    ]);

    for grid in hourly_grids() {
        let items = items_with_spans(&hours, &grid).expect("two spans");
        assert_eq!(
            shown(&items),
            "2024-01-01-00:00..2024-01-01-02:00,2024-01-01-05:00,\
             2024-01-01-07:00..2024-01-01-09:00,2024-01-01-20:00,2024-01-01-21:00"
        );
        assert_eq!(values(&items, 0, usize::MAX), hours);
    }
}

#[test]
fn fewer_than_three_windows_in_a_row_give_no_span() {
    for grid in hourly_grids() {
        for hours in [
            strings(&[]),
            strings(&["2024-01-01-00:00"]),
            strings(&["2024-01-01-00:00", "2024-01-01-01:00"]),
            strings(&[
                "2024-01-01-00:00",
                "2024-01-01-01:00",
                "2024-01-01-05:00",
                "2024-01-01-06:00",
            ]),
        ] {
            assert_eq!(items_with_spans(&hours, &grid), None, "{hours:?}");
        }
    }
}

#[test]
fn a_value_that_is_not_a_window_in_the_exact_format_stays_a_value() {
    // Sorted as strings, as the planner gives them.
    let keys = strings(&[
        "2024-01-01-00:00",
        "2024-01-01-00:30",
        "2024-01-01-01:00",
        "2024-01-01-02:00",
        "2024-01-01-03:00",
        "2024-01-01-4:00",
        "us",
    ]);

    for grid in hourly_grids() {
        let items = items_with_spans(&keys, &grid).expect("a span");
        assert_eq!(
            shown(&items),
            "2024-01-01-00:00,2024-01-01-00:30,2024-01-01-01:00..2024-01-01-03:00,\
             2024-01-01-4:00,us"
        );
        assert_eq!(values(&items, 0, usize::MAX), keys);
    }
}

#[test]
fn spans_keep_the_sorted_order_when_the_format_does_not_sort_like_time() {
    let grid = TimeGrid {
        cron_schedule: Some("0 0 * * *".into()),
        interval_seconds: None,
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: None,
        fmt: "%d-%m-%Y".into(),
    };
    // Six days in a row, sorted as strings: February comes before the end of
    // January, so one span over all six would change the order.
    let days = strings(&[
        "01-02-2024",
        "02-02-2024",
        "03-02-2024",
        "29-01-2024",
        "30-01-2024",
        "31-01-2024",
    ]);

    let items = items_with_spans(&days, &grid).expect("the rows inside one month");
    assert_eq!(
        shown(&items),
        "01-02-2024..03-02-2024,29-01-2024..31-01-2024"
    );
    assert_eq!(values(&items, 0, usize::MAX), days);
}

#[test]
fn a_daily_grid_gives_a_span_over_days() {
    let grid = TimeGrid {
        cron_schedule: Some("0 0 * * *".into()),
        interval_seconds: None,
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: None,
        fmt: "%Y-%m-%d".into(),
    };
    let days = strings(&["2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01"]);

    let items = items_with_spans(&days, &grid).expect("a span");
    assert_eq!(shown(&items), "2024-02-27..2024-03-01");
    assert_eq!(values(&items, 0, usize::MAX), days);
}

#[test]
fn a_grid_that_a_span_cannot_follow_gives_no_span() {
    let hours = strings(&["2024-01-01-00:00", "2024-01-01-01:00", "2024-01-01-02:00"]);
    let grid = |cron: Option<&str>, seconds: Option<f64>| TimeGrid {
        cron_schedule: cron.map(str::to_string),
        interval_seconds: seconds,
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: None,
        fmt: HOUR_FMT.into(),
    };

    assert_eq!(items_with_spans(&hours, &grid(None, Some(0.5))), None);
    assert_eq!(items_with_spans(&hours, &grid(None, Some(0.0))), None);
    assert_eq!(items_with_spans(&hours, &grid(None, None)), None);
    assert_eq!(
        items_with_spans(&hours, &grid(Some("not a cron"), None)),
        None
    );
    // Another step than the keys have: every second hour.
    assert_eq!(items_with_spans(&hours, &grid(None, Some(7200.0))), None);
}
