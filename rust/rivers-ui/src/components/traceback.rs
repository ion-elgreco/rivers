//! A failed step's Python traceback, shown the way an IDE shows one: the
//! exception, each frame of the user's code with the lines around it and the
//! running expression marked, and library frames folded away.

use leptos::prelude::*;

use crate::types::{ChainLink, ExceptionInfo, RunLog, StoredEvent, Traceback, TracebackFrame};

/// Failed steps listed in full above the timeline; the rest are counted.
const FAILURES_SHOWN: usize = 5;

#[component]
pub fn TracebackView(traceback: Traceback) -> impl IntoView {
    let has_frames = !traceback.exceptions.is_empty();
    let show_text = RwSignal::new(!has_frames);
    let mut folds = Vec::new();
    let chain = chain_view(&traceback.exceptions, &mut folds, false);
    let head = traceback.exceptions.last().map(exception_head);
    let can_fold = !folds.is_empty();
    let folds = StoredValue::new(folds);
    let all_open = move || folds.with_value(|fs| fs.iter().all(|f| f.get()));
    let copy_text = traceback.text.clone();
    view! {
        <div class="traceback">
            <div class="traceback-top">
                {head}
                <div class="traceback-bar">
                    {can_fold.then(|| view! {
                        <button
                            class="btn btn-small traceback-fold-all"
                            style=move || if show_text.get() { "display:none" } else { "" }
                            on:click=move |_| {
                                let open = !all_open();
                                folds.with_value(|fs| fs.iter().for_each(|f| f.set(open)));
                            }
                        >
                            {move || if all_open() { "Collapse all" } else { "Expand all" }}
                        </button>
                    })}
                    {has_frames.then(|| view! {
                        <button
                            class="btn btn-small traceback-text-toggle"
                            on:click=move |_| show_text.update(|t| *t = !*t)
                        >
                            {move || if show_text.get() { "Show frames" } else { "Show as text" }}
                        </button>
                    })}
                    <button
                        class="btn btn-small copyable"
                        data-copy=copy_text
                        title="Copy the traceback as Python prints it"
                    >"Copy"</button>
                </div>
            </div>
            <div style=move || if show_text.get() { "display:none" } else { "" }>{chain}</div>
            <pre class="traceback-text" style=move || if show_text.get() { "" } else { "display:none" }>
                {traceback.text}
            </pre>
        </div>
    }
}

/// The failed steps of a run, each with its error and, where the step stored
/// one, its traceback. The first one's traceback starts open.
#[component]
pub fn RunFailures(
    step_events: Signal<Vec<StoredEvent>>,
    run_logs: Signal<Vec<RunLog>>,
) -> impl IntoView {
    let failures = Memo::new(move |_| step_events.with(|events| failed_steps(events)));
    move || {
        let failures = failures.get();
        (!failures.is_empty()).then(|| {
            let total = failures.len();
            let more = total.saturating_sub(FAILURES_SHOWN);
            let steps = failures
                .into_iter()
                .take(FAILURES_SHOWN)
                .enumerate()
                .map(|(i, failure)| view! { <FailedStep failure run_logs open=i == 0/> })
                .collect::<Vec<_>>();
            view! {
                <div class="run-view-panel run-failures">
                    <div class="run-view-panel-header">
                        <span class="run-failures-title">
                            <span class="section-header-label">"Failed steps"</span>
                            <span class="section-header-count">{total}</span>
                        </span>
                    </div>
                    {steps}
                    {(more > 0).then(|| view! {
                        <div class="run-failures-more">
                            {format!("{more} more failed step{} in the events below", plural(more))}
                        </div>
                    })}
                </div>
            }
        })
    }
}

/// One step-level failure: the step, when it failed, and its error message.
#[derive(Debug, Clone, PartialEq)]
pub struct FailedStepInfo {
    pub asset: String,
    pub at: i64,
    pub error: String,
}

/// Each step's last step-level `StepFailure`, in time order. A partition's own
/// failure leaves the step itself standing, so it is left out.
pub fn failed_steps(events: &[StoredEvent]) -> Vec<FailedStepInfo> {
    let mut last: Vec<FailedStepInfo> = Vec::new();
    for e in events {
        if !matches!(e.event_type, crate::types::EventType::StepFailure)
            || e.partition_key.is_some()
        {
            continue;
        }
        let asset = e.asset_key.clone().unwrap_or_default();
        let error = e
            .metadata
            .iter()
            .find(|(k, _)| k == "error")
            .map(|(_, v)| v.as_text())
            .unwrap_or_else(|| "Step failed".to_string());
        last.retain(|f| f.asset != asset);
        last.push(FailedStepInfo {
            asset,
            at: e.timestamp,
            error,
        });
    }
    last.sort_by_key(|f| f.at);
    last
}

#[component]
fn FailedStep(failure: FailedStepInfo, run_logs: Signal<Vec<RunLog>>, open: bool) -> impl IntoView {
    let FailedStepInfo { asset, at, error } = failure;
    let key = asset.clone();
    let traceback = Memo::new(move |_| {
        run_logs.with(|logs| crate::types::traceback_for(logs, &key, at).cloned())
    });
    let open = RwSignal::new(open);
    // An open traceback shows the error and where it happened right below.
    let summary = move || !(open.get() && traceback.with(Option::is_some));
    view! {
        <div class="run-failure">
            <div class="run-failure-head">
                <span class="run-failure-dot"></span>
                <span class="run-failure-asset">{asset}</span>
                {move || summary().then(|| view! {
                    <span class="run-failure-error">{error.clone()}</span>
                    {traceback.with(|tb| tb.as_ref().and_then(failure_location)).map(|at| view! {
                        <span class="run-failure-where">{at}</span>
                    })}
                })}
                {move || traceback.with(Option::is_some).then(|| view! {
                    <button
                        class="btn btn-small run-failure-toggle"
                        on:click=move |_| open.update(|o| *o = !*o)
                    >
                        {move || if open.get() { "Hide traceback" } else { "Show traceback" }}
                    </button>
                })}
            </div>
            {move || open.get().then(|| traceback.get()).flatten().map(|traceback| view! {
                <TracebackView traceback/>
            })}
        </div>
    }
}

/// `file:line` of the user's innermost frame in the exception that failed
/// the step, else of its innermost frame.
pub fn failure_location(traceback: &Traceback) -> Option<String> {
    let frames = &traceback.exceptions.last()?.frames;
    let frame = frames.iter().rev().find(|f| f.in_app).or(frames.last())?;
    Some(match frame.lineno {
        Some(n) => format!("{}:{n}", frame.filename),
        None => frame.filename.clone(),
    })
}

/// The open state of every frame and library run in one traceback, for
/// "Expand all" / "Collapse all".
type Folds = Vec<RwSignal<bool>>;

fn fold(folds: &mut Folds, open: bool) -> RwSignal<bool> {
    let open = RwSignal::new(open);
    folds.push(open);
    open
}

/// Keeps `open` in step with its `<details>` when the user opens or closes it.
fn follow_toggle(open: RwSignal<bool>) -> impl FnMut(leptos::ev::Event) + 'static {
    move |ev| open.set(event_target::<leptos::web_sys::Element>(&ev).has_attribute("open"))
}

/// Newest exception first, each separated from the one it came from.
/// `with_newest_head: false` leaves the newest exception's line out, for a
/// caller that shows it on its own.
fn chain_view(chain: &[ExceptionInfo], folds: &mut Folds, with_newest_head: bool) -> AnyView {
    let newest_first: Vec<&ExceptionInfo> = chain.iter().rev().collect();
    let items = newest_first
        .iter()
        .enumerate()
        .map(|(k, exc)| {
            let link = if k > 0 {
                newest_first[k - 1].chain
            } else {
                None
            };
            view! {
                {link.map(|link| view! { <div class="traceback-link">{link_label(link)}</div> })}
                {exception_view(exc, folds, k > 0 || with_newest_head)}
            }
        })
        .collect::<Vec<_>>();
    view! { <div class="traceback-chain">{items}</div> }.into_any()
}

fn link_label(link: ChainLink) -> &'static str {
    match link {
        ChainLink::Cause => "Caused by",
        ChainLink::Context => "Raised while handling",
    }
}

/// The exception's type and message, as Python's last traceback line shows them.
fn exception_head(exc: &ExceptionInfo) -> AnyView {
    let name = match &exc.module {
        Some(module) => format!("{module}.{}", exc.exc_type),
        None => exc.exc_type.clone(),
    };
    let value = (!exc.value.is_empty()).then(|| format!(": {}", exc.value));
    view! {
        <div class="traceback-exception-head">
            <span class="traceback-exception-type">{name}</span>
            {value}
        </div>
    }
    .into_any()
}

fn exception_view(exc: &ExceptionInfo, folds: &mut Folds, with_head: bool) -> AnyView {
    let total = exc.group.len() + exc.group_omitted as usize;
    let members = exc
        .group
        .iter()
        .enumerate()
        .map(|(i, member)| {
            view! {
                <div class="traceback-member">
                    <div class="traceback-member-label">{format!("Sub-exception {} of {total}", i + 1)}</div>
                    {chain_view(member, folds, true)}
                </div>
            }
        })
        .collect::<Vec<_>>();
    let omitted = exc.group_omitted;
    view! {
        <div class="traceback-exception">
            {with_head.then(|| exception_head(exc))}
            {frames_view(&exc.frames, folds)}
            {members}
            {(omitted > 0).then(|| view! {
                <div class="traceback-member-label">{format!("… and {omitted} more")}</div>
            })}
        </div>
    }
    .into_any()
}

fn frames_view(frames: &[TracebackFrame], folds: &mut Folds) -> AnyView {
    // The user's innermost frame starts open; with none, the innermost frame.
    let open = frames
        .iter()
        .rposition(|f| f.in_app)
        .or(frames.len().checked_sub(1));
    let items = frame_runs(frames)
        .into_iter()
        .map(|run| match run {
            FrameRun::Frame(i) => frame_view(&frames[i], Some(i) == open, folds),
            FrameRun::Library(ix) => {
                let run: Vec<&TracebackFrame> = ix.iter().map(|&i| &frames[i]).collect();
                library_view(&run, folds)
            }
        })
        .collect::<Vec<_>>();
    view! { <div class="traceback-frames">{items}</div> }.into_any()
}

/// A frame, or a run of library frames folded together.
#[derive(Debug, PartialEq)]
pub enum FrameRun {
    Frame(usize),
    Library(Vec<usize>),
}

/// Frames in call order, each run of library frames folded into one. When
/// no frame is the user's own, none is folded.
pub fn frame_runs(frames: &[TracebackFrame]) -> Vec<FrameRun> {
    if !frames.iter().any(|f| f.in_app) {
        return (0..frames.len()).map(FrameRun::Frame).collect();
    }
    let mut runs = Vec::new();
    for (i, frame) in frames.iter().enumerate() {
        if frame.in_app {
            runs.push(FrameRun::Frame(i));
        } else if let Some(FrameRun::Library(run)) = runs.last_mut() {
            run.push(i);
        } else {
            runs.push(FrameRun::Library(vec![i]));
        }
    }
    runs
}

fn library_view(frames: &[&TracebackFrame], folds: &mut Folds) -> AnyView {
    let n = frames.len();
    let open = fold(folds, false);
    let items = frames
        .iter()
        .map(|f| frame_view(f, false, folds))
        .collect::<Vec<_>>();
    view! {
        <details class="traceback-library" open=move || open.get() on:toggle=follow_toggle(open)>
            <summary class="traceback-library-head">
                {format!("{n} library frame{}", plural(n))}
                <span class="traceback-library-packages">{library_packages(frames)}</span>
            </summary>
            {items}
        </details>
    }
    .into_any()
}

/// The packages a run of library frames belongs to, e.g. `pandas, json`.
pub fn library_packages(frames: &[&TracebackFrame]) -> String {
    let mut names: Vec<&str> = Vec::new();
    for frame in frames {
        let first = frame.filename.split('/').next().unwrap_or_default();
        let name = if first.starts_with('<') {
            "python"
        } else {
            first.strip_suffix(".py").unwrap_or(first)
        };
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names.join(", ")
}

fn frame_view(frame: &TracebackFrame, open: bool, folds: &mut Folds) -> AnyView {
    let open = fold(folds, open);
    let class = if frame.in_app {
        "traceback-frame traceback-frame--app"
    } else {
        "traceback-frame"
    };
    let (dir, file) = split_path(&frame.filename);
    let line = frame.lineno.map(|n| format!(":{n}"));
    let peek = frame.context_line.as_deref().map(|l| l.trim().to_string());
    let repeated = frame.repeated;
    view! {
        <details class=class open=move || open.get() on:toggle=follow_toggle(open)>
            <summary class="traceback-frame-head">
                <span class="traceback-frame-fn">{frame.function.clone()}</span>
                <span class="traceback-frame-where" title=frame.abs_path.clone()>
                    <span class="traceback-frame-dir">{dir.to_string()}</span>
                    {file.to_string()}
                    {line}
                </span>
                {peek.map(|p| view! { <span class="traceback-frame-peek">{p}</span> })}
            </summary>
            {code_view(frame)}
        </details>
        {(repeated > 0).then(|| view! {
            <div class="traceback-repeated">
                {format!("Previous frame repeated {repeated} more time{}", plural(repeated as usize))}
            </div>
        })}
    }
    .into_any()
}

/// `path` split after its last `/`: the directory, then the file name.
fn split_path(path: &str) -> (&str, &str) {
    path.rfind('/').map_or(("", path), |i| path.split_at(i + 1))
}

fn code_view(frame: &TracebackFrame) -> AnyView {
    let (Some(lineno), Some(current)) = (frame.lineno, frame.context_line.as_ref()) else {
        return ().into_any();
    };
    let first = lineno.saturating_sub(frame.pre_context.len() as u32);
    let rows = frame
        .pre_context
        .iter()
        .chain(std::iter::once(current))
        .chain(&frame.post_context)
        .enumerate()
        .map(|(i, text)| {
            let n = first + i as u32;
            let (before, marked, after) = split_marked(text, highlight_range(frame, n, text));
            let class = if n == lineno {
                "traceback-line traceback-line--current"
            } else {
                "traceback-line"
            };
            view! {
                <div class=class>
                    <span class="traceback-line-no">{n}</span>
                    <span class="traceback-line-src">
                        {before}
                        {marked.map(|m| view! { <mark class="traceback-mark">{m}</mark> })}
                        {after}
                    </span>
                </div>
            }
        })
        .collect::<Vec<_>>();
    view! { <div class="traceback-code">{rows}</div> }.into_any()
}

/// The characters `[start, end)` of line `n` that the running expression
/// covers, if any. Left out when it covers the whole statement, as Python
/// leaves out its `^^^` marks then.
pub fn highlight_range(frame: &TracebackFrame, n: u32, line: &str) -> Option<(usize, usize)> {
    let (start_line, end_line) = (frame.lineno?, frame.end_lineno?);
    let (col, end_col) = (frame.colno? as usize, frame.end_colno? as usize);
    if n < start_line || n > end_line {
        return None;
    }
    let len = line.chars().count();
    let indent = line.chars().take_while(|c| c.is_whitespace()).count();
    let content_end = len - line.chars().rev().take_while(|c| c.is_whitespace()).count();
    let start = if n == start_line {
        col.saturating_sub(1)
    } else {
        indent
    };
    let end = if n == end_line { end_col } else { content_end };
    let (start, end) = (start.min(len), end.min(len));
    if start >= end || (start_line == end_line && start <= indent && end >= content_end) {
        return None;
    }
    Some((start, end))
}

fn split_marked(text: &str, range: Option<(usize, usize)>) -> (String, Option<String>, String) {
    let Some((start, end)) = range else {
        return (text.to_string(), None, String::new());
    };
    let chars: Vec<char> = text.chars().collect();
    (
        chars[..start].iter().collect(),
        Some(chars[start..end].iter().collect()),
        chars[end..].iter().collect(),
    )
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(in_app: bool, filename: &str) -> TracebackFrame {
        TracebackFrame {
            filename: filename.into(),
            abs_path: format!("/abs/{filename}"),
            function: "f".into(),
            lineno: Some(10),
            colno: None,
            end_lineno: None,
            end_colno: None,
            pre_context: vec![],
            context_line: Some("x = 1".into()),
            post_context: vec![],
            in_app,
            repeated: 0,
        }
    }

    fn located(lineno: u32, colno: u32, end_lineno: u32, end_colno: u32) -> TracebackFrame {
        TracebackFrame {
            lineno: Some(lineno),
            colno: Some(colno),
            end_lineno: Some(end_lineno),
            end_colno: Some(end_colno),
            ..frame(true, "a.py")
        }
    }

    fn event(
        event_type: crate::types::EventType,
        asset: &str,
        at: i64,
        partition: Option<&str>,
        error: &str,
    ) -> StoredEvent {
        StoredEvent {
            id: format!("{asset}-{at}"),
            event_type,
            asset_key: Some(asset.into()),
            run_id: "r1".into(),
            partition_key: partition.map(str::to_string),
            timestamp: at,
            metadata: vec![(
                "error".into(),
                crate::types::MetadataDisplay::Text(error.into()),
            )],
            data_version: None,
        }
    }

    #[test]
    fn failed_steps_keeps_each_steps_last_step_level_failure() {
        use crate::types::EventType as E;
        let events = [
            event(E::StepFailure, "a", 5, None, "first"),
            event(E::StepFailure, "b", 6, Some("2024-01-01"), "one partition"),
            event(E::StepRetry, "c", 6, None, "retried"),
            event(E::StepFailure, "c", 8, None, "gave up"),
            event(E::StepFailure, "a", 9, None, "again"),
        ];
        let got: Vec<(String, i64, String)> = failed_steps(&events)
            .into_iter()
            .map(|f| (f.asset, f.at, f.error))
            .collect();
        assert_eq!(
            got,
            vec![
                ("c".to_string(), 8, "gave up".to_string()),
                ("a".to_string(), 9, "again".to_string()),
            ]
        );
    }

    #[test]
    fn frame_runs_fold_library_frames_between_the_users() {
        let frames = [
            frame(false, "loky/process_executor.py"),
            frame(true, "assets.py"),
            frame(false, "pandas/io/common.py"),
            frame(false, "pandas/io/parsers.py"),
            frame(true, "helpers.py"),
        ];
        assert_eq!(
            frame_runs(&frames),
            vec![
                FrameRun::Library(vec![0]),
                FrameRun::Frame(1),
                FrameRun::Library(vec![2, 3]),
                FrameRun::Frame(4),
            ]
        );
    }

    #[test]
    fn frame_runs_fold_nothing_without_a_frame_of_the_users() {
        let frames = [
            frame(false, "rivers/io.py"),
            frame(false, "json/decoder.py"),
        ];
        assert_eq!(
            frame_runs(&frames),
            vec![FrameRun::Frame(0), FrameRun::Frame(1)]
        );
    }

    #[test]
    fn split_path_keeps_the_directory_apart() {
        assert_eq!(
            split_path("examples/demo/pipeline.py"),
            ("examples/demo/", "pipeline.py")
        );
        assert_eq!(split_path("threading.py"), ("", "threading.py"));
        assert_eq!(
            split_path("<frozen importlib._bootstrap>"),
            ("", "<frozen importlib._bootstrap>")
        );
    }

    #[test]
    fn library_packages_names_each_package_once() {
        let frames = [
            frame(false, "pandas/io/common.py"),
            frame(false, "pandas/core.py"),
            frame(false, "threading.py"),
            frame(false, "<frozen importlib._bootstrap>"),
        ];
        let refs: Vec<&TracebackFrame> = frames.iter().collect();
        assert_eq!(library_packages(&refs), "pandas, threading, python");
    }

    #[test]
    fn highlight_marks_the_expression_on_its_line() {
        // `    return values["missing"]`: the subscript is columns 12..=28.
        let line = r#"    return values["missing"]"#;
        let f = located(10, 12, 10, 28);
        assert_eq!(highlight_range(&f, 10, line), Some((11, 28)));
        assert_eq!(highlight_range(&f, 9, line), None);
        let (before, marked, after) = split_marked(line, highlight_range(&f, 10, line));
        assert_eq!(
            (before.as_str(), marked.as_deref(), after.as_str()),
            ("    return ", Some(r#"values["missing"]"#), "")
        );
    }

    #[test]
    fn highlight_leaves_out_a_whole_statement() {
        let line = "    raise ValueError('x')  ";
        assert_eq!(highlight_range(&located(3, 5, 3, 25), 3, line), None);
    }

    #[test]
    fn highlight_spans_the_lines_of_a_multi_line_expression() {
        let f = located(4, 9, 6, 6);
        assert_eq!(highlight_range(&f, 4, "    x = call("), Some((8, 13)));
        assert_eq!(highlight_range(&f, 5, "        arg,  "), Some((8, 12)));
        assert_eq!(highlight_range(&f, 6, "    )"), Some((4, 5)));
        assert_eq!(highlight_range(&f, 7, "    y"), None);
    }

    #[test]
    fn highlight_needs_python_311_positions() {
        assert_eq!(highlight_range(&frame(true, "a.py"), 10, "x = 1"), None);
    }

    #[test]
    fn failure_location_prefers_the_users_innermost_frame() {
        let mut exc = ExceptionInfo {
            exc_type: "KeyError".into(),
            module: None,
            value: "'b'".into(),
            chain: None,
            frames: vec![
                frame(true, "assets.py"),
                frame(true, "helpers.py"),
                frame(false, "json/decoder.py"),
            ],
            group: vec![],
            group_omitted: 0,
        };
        let tb = |exc: &ExceptionInfo| Traceback {
            exceptions: vec![exc.clone()],
            text: String::new(),
        };
        assert_eq!(
            failure_location(&tb(&exc)).as_deref(),
            Some("helpers.py:10")
        );
        exc.frames.retain(|f| !f.in_app);
        assert_eq!(
            failure_location(&tb(&exc)).as_deref(),
            Some("json/decoder.py:10")
        );
    }
}
