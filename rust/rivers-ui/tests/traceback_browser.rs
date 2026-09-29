//! Browser tests for the traceback view: the user's frames open with their
//! source lines and the running expression marked, library frames folded, the
//! cause chain newest first, and the text Python prints one click away.

#![cfg(target_arch = "wasm32")]

mod common;

use common::{click, fresh_mount_target, query_all, query_one, wait_until};
use leptos::mount::mount_to;
use leptos::prelude::*;
use rivers_ui::components::traceback::{RunFailures, TracebackView};
use rivers_ui::types::{
    ChainLink, EventType, ExceptionInfo, MetadataDisplay, RunLog, StoredEvent, Traceback,
    TracebackFrame,
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

const TEXT: &str =
    "Traceback (most recent call last):\n  ...\nRuntimeError: could not read sales\n";

fn frame(filename: &str, function: &str, lineno: u32, in_app: bool) -> TracebackFrame {
    TracebackFrame {
        filename: filename.into(),
        abs_path: format!("/app/{filename}"),
        function: function.into(),
        lineno: Some(lineno),
        colno: None,
        end_lineno: None,
        end_colno: None,
        pre_context: vec![],
        context_line: Some("    pass".into()),
        post_context: vec![],
        in_app,
        repeated: 0,
    }
}

fn exception(exc_type: &str, value: &str, frames: Vec<TracebackFrame>) -> ExceptionInfo {
    ExceptionInfo {
        exc_type: exc_type.into(),
        module: None,
        value: value.into(),
        chain: None,
        frames,
        group: vec![],
        group_omitted: 0,
    }
}

/// A `FileNotFoundError` raised inside pandas, and the `RuntimeError` the
/// user's code raised from it.
fn sample() -> Traceback {
    let read = TracebackFrame {
        pre_context: vec![
            "@rs.Asset".into(),
            "def daily_sales() -> int:".into(),
            "    path = \"/data/sales.csv\"".into(),
            "    try:".into(),
        ],
        context_line: Some("        df = pd.read_csv(path)".into()),
        post_context: vec!["    except FileNotFoundError as e:".into()],
        colno: Some(14),
        end_lineno: Some(42),
        end_colno: Some(30),
        ..frame("assets/sales.py", "daily_sales", 42, true)
    };
    let raise = TracebackFrame {
        context_line: Some("        raise RuntimeError(\"could not read sales\") from e".into()),
        ..frame("assets/sales.py", "daily_sales", 44, true)
    };
    let mut raised = exception("RuntimeError", "could not read sales", vec![raise]);
    raised.chain = Some(ChainLink::Cause);
    Traceback {
        exceptions: vec![
            exception(
                "FileNotFoundError",
                "/data/sales.csv",
                vec![
                    read,
                    frame("pandas/io/parsers/readers.py", "read_csv", 1026, false),
                    frame("pandas/io/common.py", "get_handle", 873, false),
                ],
            ),
            raised,
        ],
        text: TEXT.into(),
    }
}

fn texts(host: &web_sys::HtmlElement, selector: &str) -> Vec<String> {
    query_all(host, selector)
        .iter()
        .filter_map(|e| e.text_content())
        .collect()
}

#[wasm_bindgen_test]
async fn traceback_shows_the_users_code_and_folds_library_frames() {
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, || view! { <TracebackView traceback=sample()/> }).forget();

    // Newest exception first, linked to the one it was raised from.
    assert_eq!(
        texts(&host, ".traceback-exception-head"),
        vec![
            "RuntimeError: could not read sales",
            "FileNotFoundError: /data/sales.csv"
        ]
    );
    assert_eq!(texts(&host, ".traceback-link"), vec!["Caused by"]);

    // Each frame's header leads with its function, then where it is.
    assert_eq!(
        texts(&host, ".traceback-frame--app .traceback-frame-fn"),
        vec!["daily_sales", "daily_sales"]
    );
    assert_eq!(
        texts(&host, ".traceback-frame--app .traceback-frame-where"),
        vec!["assets/sales.py:44", "assets/sales.py:42"]
    );
    assert_eq!(
        texts(&host, ".traceback-frame--app .traceback-frame-dir"),
        vec!["assets/", "assets/"]
    );

    // The user's innermost frame of each exception starts open.
    assert_eq!(
        query_all(&host, "details.traceback-frame--app[open]").len(),
        2
    );
    assert_eq!(
        texts(&host, ".traceback-line-no"),
        vec!["44", "38", "39", "40", "41", "42", "43", "1026", "873"]
    );
    assert_eq!(
        texts(
            &host,
            ".traceback-frame--app .traceback-line--current .traceback-line-no"
        ),
        vec!["44", "42"]
    );
    assert_eq!(
        texts(&host, "mark.traceback-mark"),
        vec!["pd.read_csv(path)"]
    );

    // pandas' frames fold into one closed run.
    let library = query_one(&host, "details.traceback-library");
    assert!(!library.has_attribute("open"));
    assert_eq!(
        texts(&host, ".traceback-library-head"),
        vec!["2 library framespandas"]
    );

    // The text Python prints, one click away, and on the copy button.
    let text = query_one(&host, "pre.traceback-text");
    assert_eq!(text.get_attribute("style").as_deref(), Some("display:none"));
    assert_eq!(
        query_one(&host, ".copyable")
            .get_attribute("data-copy")
            .as_deref(),
        Some(TEXT)
    );
    click(&query_one(&host, ".traceback-text-toggle"), false);
    assert!(
        wait_until(|| text.get_attribute("style").as_deref() == Some("")).await,
        "the text view did not open"
    );
    assert_eq!(text.text_content().unwrap().trim(), TEXT.trim());
}

#[wasm_bindgen_test]
async fn expand_all_and_collapse_all_open_and_close_every_fold() {
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, || view! { <TracebackView traceback=sample()/> }).forget();

    let button = query_one(&host, ".traceback-fold-all");
    let label = || button.text_content().unwrap_or_default();
    let open = || query_all(&host, "details[open]").len();
    // Two frames of the user's code, two library frames, one library run.
    assert_eq!(query_all(&host, "details").len(), 5);
    assert_eq!(open(), 2);
    assert_eq!(label(), "Expand all");

    click(&button, false);
    assert!(wait_until(|| open() == 5).await, "{} of 5 open", open());
    assert_eq!(label(), "Collapse all");

    click(&button, false);
    assert!(wait_until(|| open() == 0).await, "{} still open", open());
    assert_eq!(label(), "Expand all");

    // A frame opened by hand counts, and "Expand all" still opens the rest.
    click(
        &query_one(&host, "details.traceback-frame--app > summary"),
        false,
    );
    assert!(wait_until(|| open() == 1).await, "the frame did not open");
    assert_eq!(label(), "Expand all");
    click(&button, false);
    assert!(wait_until(|| open() == 5).await, "{} of 5 open", open());
}

#[wasm_bindgen_test]
async fn run_failures_list_the_failed_step_with_its_traceback_open() {
    let at = 1_000;
    let events = vec![StoredEvent {
        id: "e1".into(),
        event_type: EventType::StepFailure,
        asset_key: Some("daily_sales".into()),
        run_id: "r1".into(),
        partition_key: None,
        timestamp: at,
        metadata: vec![(
            "error".into(),
            MetadataDisplay::Text("RuntimeError: could not read sales".into()),
        )],
        data_version: None,
    }];
    let logs = vec![RunLog {
        id: "l1".into(),
        run_id: "r1".into(),
        step_key: "daily_sales".into(),
        timestamp: at,
        stdout: None,
        stderr: None,
        logs: None,
        traceback: Some(sample()),
    }];
    let target = fresh_mount_target();
    let host = target.clone();
    mount_to(target, move || {
        let (events, logs) = (events.clone(), logs.clone());
        view! {
            <RunFailures
                step_events=Signal::derive(move || events.clone())
                run_logs=Signal::derive(move || logs.clone())
            />
        }
    })
    .forget();

    assert_eq!(
        texts(&host, ".run-failures .section-header-label"),
        vec!["Failed steps"]
    );
    assert_eq!(
        texts(&host, ".run-failures .section-header-count"),
        vec!["1"]
    );
    assert_eq!(texts(&host, ".run-failure-asset"), vec!["daily_sales"]);
    // The first failure starts open, and its traceback shows the error, so
    // the row does not repeat it.
    assert_eq!(query_all(&host, ".traceback").len(), 1);
    assert!(texts(&host, ".run-failure-error").is_empty());
    assert!(texts(&host, ".run-failure-where").is_empty());

    click(&query_one(&host, ".run-failure-head button"), false);
    assert!(
        wait_until(|| query_all(&host, ".traceback").is_empty()).await,
        "Hide traceback did not close it"
    );
    assert_eq!(
        texts(&host, ".run-failure-error"),
        vec!["RuntimeError: could not read sales"]
    );
    assert_eq!(
        texts(&host, ".run-failure-where"),
        vec!["assets/sales.py:44"]
    );
}
