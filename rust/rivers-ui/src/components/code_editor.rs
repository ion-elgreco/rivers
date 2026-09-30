//! A JSON editor: a highlighted `<pre>` under a transparent `<textarea>`,
//! both in one CSS grid cell so they never scroll apart, with issue marks
//! and list and the keys a code editor has (Tab, Enter, pairs).

use leptos::prelude::*;

use crate::config_schema::Candidate;
use crate::json_text::{
    Issue, Slot, Span, TokenKind, context_at, line_col, line_indent, parse, tokenize,
};

/// One run of the highlighted text, or the caret's place in it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Piece {
    Text {
        text: String,
        class: &'static str,
        /// The message of the issue underlining this run.
        mark: Option<String>,
    },
    Caret,
}

fn token_class(kind: TokenKind) -> &'static str {
    match kind {
        TokenKind::Key => "tok-key",
        TokenKind::Str | TokenKind::StrOpen => "tok-str",
        TokenKind::Num => "tok-num",
        TokenKind::Bool | TokenKind::Null => "tok-kw",
        TokenKind::Invalid => "tok-invalid",
        TokenKind::Space => "tok-space",
        _ => "tok-punct",
    }
}

fn next_char_boundary(text: &str, offset: usize) -> usize {
    text[offset..]
        .chars()
        .next()
        .map_or(offset, |c| offset + c.len_utf8())
}

/// `text` cut at every token, issue and the caret, each run classed by
/// its token and marked by the issue covering it. An issue at the end of
/// the text marks one trailing space.
pub fn pieces(text: &str, issues: &[Issue], caret: usize) -> Vec<Piece> {
    let caret = caret.min(text.len());
    let tokens = tokenize(text);
    let marks: Vec<(Span, &str)> = issues
        .iter()
        .map(|issue| {
            let mut span = issue.span;
            if span.start == span.end && span.start < text.len() {
                span.end = next_char_boundary(text, span.start);
            }
            (span, issue.message.as_str())
        })
        .collect();
    let mut cuts = vec![0, text.len(), caret];
    cuts.extend(tokens.iter().flat_map(|t| [t.span.start, t.span.end]));
    cuts.extend(marks.iter().flat_map(|(s, _)| [s.start, s.end]));
    cuts.sort_unstable();
    cuts.dedup();
    let mut out = Vec::new();
    for pair in cuts.windows(2) {
        let (start, end) = (pair[0], pair[1]);
        if start == caret {
            out.push(Piece::Caret);
        }
        let class = tokens
            .iter()
            .find(|t| t.span.start <= start && start < t.span.end)
            .map_or("tok-space", |t| token_class(t.kind));
        let mark = marks
            .iter()
            .find(|(s, _)| s.start <= start && end <= s.end)
            .map(|(_, m)| m.to_string());
        out.push(Piece::Text {
            text: text[start..end].to_string(),
            class,
            mark,
        });
    }
    if caret >= text.len() {
        out.push(Piece::Caret);
    }
    if let Some((_, message)) = marks.iter().find(|(s, _)| s.start >= text.len()) {
        out.push(Piece::Text {
            text: " ".to_string(),
            class: "tok-space",
            mark: Some(message.to_string()),
        });
    }
    out
}

/// What a key does to the text: replace a span, move the caret, or nothing
/// (the browser's default).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edit {
    Insert {
        replace: Span,
        text: String,
        caret: usize,
    },
    Move(usize),
    None,
}

fn char_before(text: &str, offset: usize) -> Option<char> {
    text[..offset].chars().next_back()
}

fn char_after(text: &str, offset: usize) -> Option<char> {
    text[offset..].chars().next()
}

/// Whether `caret` is in a string literal (past its opening quote).
fn in_string(text: &str, caret: usize) -> bool {
    tokenize(text).iter().any(|t| match t.kind {
        TokenKind::Str | TokenKind::Key => t.span.start < caret && caret < t.span.end,
        TokenKind::StrOpen => t.span.start < caret && caret <= t.span.end,
        _ => false,
    })
}

/// The edit `key` makes with the selection `start..end`; `leave` lets Tab
/// through (Escape was pressed before it).
pub fn key_edit(text: &str, start: usize, end: usize, key: &str, leave: bool) -> Edit {
    let selection = Span { start, end };
    let insert = |text: String, caret: usize| Edit::Insert {
        replace: selection,
        text,
        caret,
    };
    match key {
        "Tab" if !leave => insert("  ".to_string(), start + 2),
        "Enter" => {
            let indent = line_indent(text, start);
            let pair = matches!(
                (char_before(text, start), char_after(text, end)),
                (Some('{'), Some('}')) | (Some('['), Some(']'))
            );
            if pair {
                let inserted = format!("\n{indent}  \n{indent}");
                insert(inserted, start + 1 + indent.len() + 2)
            } else {
                let inserted = format!("\n{indent}");
                let caret = start + inserted.len();
                insert(inserted, caret)
            }
        }
        "Backspace" if start == end && start > 0 => {
            let pair = matches!(
                (char_before(text, start), char_after(text, start)),
                (Some('{'), Some('}')) | (Some('['), Some(']')) | (Some('"'), Some('"'))
            );
            if pair {
                Edit::Insert {
                    replace: Span {
                        start: start - 1,
                        end: start + 1,
                    },
                    text: String::new(),
                    caret: start - 1,
                }
            } else {
                Edit::None
            }
        }
        "}" | "]" | "\""
            if start == end
                && char_after(text, start).map(String::from).as_deref() == Some(key) =>
        {
            Edit::Move(start + 1)
        }
        "{" | "[" | "\"" if start == end && !in_string(text, start) => {
            let closer = match key {
                "{" => "}",
                "[" => "]",
                _ => "\"",
            };
            insert(format!("{key}{closer}"), start + 1)
        }
        _ => Edit::None,
    }
}

/// An open completion list: what it replaces and what surrounds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Popup {
    pub options: Vec<Candidate>,
    pub index: usize,
    pub replace: Span,
    pub key_only: bool,
    pub comma_before: bool,
    pub comma_after: bool,
}

/// The completion list at `caret`, or `None`: no slot there, no option
/// matches, or the one match is already typed. Unless `explicit`
/// (Ctrl+Space), it opens only for a typed prefix or right after a colon.
pub fn completion_at(
    text: &str,
    caret: usize,
    options: Vec<Candidate>,
    explicit: bool,
) -> Option<Popup> {
    let caret = caret.min(text.len());
    let ctx = context_at(text, &parse(text), caret);
    let prefix = match &ctx.slot {
        Slot::Key { prefix } | Slot::Value { prefix } => prefix.as_str(),
        Slot::None => return None,
    };
    let after_colon = matches!(ctx.slot, Slot::Value { .. })
        && text[..caret].trim_end_matches(' ').ends_with(':');
    if !explicit && prefix.is_empty() && !after_colon {
        return None;
    }
    let lower = prefix.to_lowercase();
    let options: Vec<Candidate> = options
        .into_iter()
        .filter(|c| c.label.to_lowercase().starts_with(&lower))
        .collect();
    if options.is_empty() || (options.len() == 1 && options[0].label == prefix) {
        return None;
    }
    Some(Popup {
        options,
        index: 0,
        replace: ctx.replace,
        key_only: ctx.key_only,
        comma_before: ctx.comma_before,
        comma_after: ctx.comma_after,
    })
}

/// The edit that accepts `popup.options[index]`: the key alone when the
/// key already has its colon, else the whole `"key": value`, with the comma
/// a neighbouring entry needs.
pub fn completion_edit(popup: &Popup, index: usize) -> Option<Edit> {
    let option = popup.options.get(index)?;
    let (mut text, mut caret) = if popup.key_only {
        let quoted = format!("\"{}\"", option.label);
        let len = quoted.len();
        (quoted, len)
    } else {
        (option.insert.clone(), option.caret)
    };
    if popup.comma_before {
        text.insert_str(0, ", ");
        caret += 2;
    }
    if popup.comma_after {
        text.push(',');
    }
    Some(Edit::Insert {
        replace: popup.replace,
        text,
        caret: popup.replace.start + caret,
    })
}

#[cfg(target_arch = "wasm32")]
mod dom {
    use leptos::prelude::{document, window};
    use leptos::wasm_bindgen::JsCast;
    use leptos::web_sys::{HtmlElement, HtmlTextAreaElement};

    /// Room the list needs under the caret before it is rendered.
    const POPUP_ROOM: f64 = 220.0;

    use crate::json_text::{Span, byte_to_utf16, utf16_to_byte};

    mod js {
        use wasm_bindgen::prelude::wasm_bindgen;
        #[wasm_bindgen]
        extern "C" {
            #[wasm_bindgen(js_namespace = document, js_name = execCommand, catch)]
            pub fn exec_command(
                command: &str,
                show_ui: bool,
                value: &str,
            ) -> Result<bool, wasm_bindgen::JsValue>;
        }
    }

    pub fn value(ta: &HtmlTextAreaElement) -> String {
        ta.value()
    }

    /// The selection as byte offsets.
    pub fn selection(ta: &HtmlTextAreaElement) -> (usize, usize) {
        let text = ta.value();
        let start = ta.selection_start().ok().flatten().unwrap_or(0);
        let end = ta.selection_end().ok().flatten().unwrap_or(start);
        (utf16_to_byte(&text, start), utf16_to_byte(&text, end))
    }

    pub fn set_caret(ta: &HtmlTextAreaElement, byte: usize) {
        let unit = byte_to_utf16(&ta.value(), byte);
        let _ = ta.set_selection_range(unit, unit);
    }

    pub fn set_value(ta: &HtmlTextAreaElement, value: &str) {
        ta.set_value(value);
    }

    pub fn focus(ta: &HtmlTextAreaElement) {
        let _ = ta.focus();
    }

    /// Replace `replace` with `text` and put the caret at `caret` (a byte
    /// offset of the new value). `execCommand` keeps the undo history; it
    /// reports success without editing when the document has no focus, so
    /// the value is checked and set directly when it did not change.
    pub fn insert(ta: &HtmlTextAreaElement, replace: Span, text: &str, caret: usize) -> String {
        let before = ta.value();
        let after = format!(
            "{}{text}{}",
            &before[..replace.start],
            &before[replace.end..]
        );
        let start = byte_to_utf16(&before, replace.start);
        let end = byte_to_utf16(&before, replace.end);
        let _ = ta.focus();
        let _ = ta.set_selection_range(start, end);
        let focused = document()
            .active_element()
            .is_some_and(|el| el.is_same_node(Some(ta.as_ref())));
        let done = focused
            && js::exec_command("insertText", false, text).unwrap_or(false)
            && ta.value() == after;
        if !done {
            ta.set_value(&after);
        }
        let unit = byte_to_utf16(&after, caret);
        let _ = ta.set_selection_range(unit, unit);
        after
    }

    /// Where the list goes in `frame`: under the caret line, or above it
    /// (`true`) when it would not fit in the scrolling ancestor.
    pub fn popup_place(frame: &HtmlElement) -> Option<(f64, f64, bool)> {
        let anchor = frame.query_selector(".code-editor-caret").ok().flatten()?;
        let at = anchor.get_bounding_client_rect();
        let origin = frame.get_bounding_client_rect();
        let limit = frame
            .closest(".modal-body")
            .ok()
            .flatten()
            .map(|body| body.get_bounding_client_rect().bottom())
            .or_else(|| window().inner_height().ok()?.as_f64())
            .unwrap_or(f64::INFINITY);
        let room = frame
            .query_selector(".code-editor-popup")
            .ok()
            .flatten()
            .and_then(|list| list.dyn_into::<HtmlElement>().ok())
            .map_or(POPUP_ROOM, |list| f64::from(list.offset_height()) + 8.0);
        let above = at.bottom() + room > limit;
        let y = if above { at.top() } else { at.bottom() };
        Some((at.left() - origin.left(), y - origin.top(), above))
    }

    /// Scroll the list in `frame` so its `index`th option is in view.
    pub fn reveal(frame: &HtmlElement, index: usize) {
        let Some(list) = frame.query_selector(".code-editor-popup").ok().flatten() else {
            return;
        };
        let selector = format!(".code-editor-option:nth-child({})", index + 1);
        let Some(item) = list
            .query_selector(&selector)
            .ok()
            .flatten()
            .and_then(|e| e.dyn_into::<HtmlElement>().ok())
        else {
            return;
        };
        let top = item.offset_top();
        let bottom = top + item.offset_height();
        let view_top = list.scroll_top();
        if bottom > view_top + list.client_height() {
            list.set_scroll_top(bottom - list.client_height());
        } else if top < view_top {
            list.set_scroll_top(top);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod dom {
    use leptos::web_sys::{HtmlElement, HtmlTextAreaElement};

    use crate::json_text::Span;

    pub fn value(_ta: &HtmlTextAreaElement) -> String {
        String::new()
    }

    pub fn selection(_ta: &HtmlTextAreaElement) -> (usize, usize) {
        (0, 0)
    }

    pub fn set_caret(_ta: &HtmlTextAreaElement, _byte: usize) {}

    pub fn set_value(_ta: &HtmlTextAreaElement, _value: &str) {}

    pub fn focus(_ta: &HtmlTextAreaElement) {}

    pub fn insert(_ta: &HtmlTextAreaElement, _replace: Span, _text: &str, _caret: usize) -> String {
        String::new()
    }

    pub fn popup_place(_frame: &HtmlElement) -> Option<(f64, f64, bool)> {
        None
    }

    pub fn reveal(_frame: &HtmlElement, _index: usize) {}
}

/// The editor over `text`. `issues` underline their spans and list under
/// the text (a click moves the caret there); `on_edit` fires on every
/// change the user makes; `complete` gives the completions for a caret
/// offset, shown while typing and on Ctrl+Space.
#[component]
pub fn CodeEditor(
    text: RwSignal<String>,
    #[prop(into)] issues: Signal<Vec<Issue>>,
    #[prop(into)] label: String,
    on_edit: Callback<()>,
    #[prop(into)] complete: Callback<(String, usize), Vec<Candidate>>,
) -> impl IntoView {
    let textarea = NodeRef::<leptos::html::Textarea>::new();
    let caret = RwSignal::new(0usize);
    // Escape arms the next Tab to leave the editor instead of indenting.
    let escaped = RwSignal::new(false);
    let highlighted = Memo::new(move |_| pieces(&text.get(), &issues.get(), caret.get()));
    let frame = NodeRef::<leptos::html::Div>::new();
    let popup = RwSignal::new(None::<Popup>);
    let popup_at = RwSignal::new((0.0f64, 0.0f64, false));
    let scrolled = RwSignal::new(0u32);

    let read_caret = move || {
        if let Some(ta) = textarea.get_untracked() {
            caret.set(dom::selection(&ta).0);
        }
    };
    // Written only when the DOM differs: a reactive `prop:value` queues its
    // writes, and a queued stale value would undo an edit made in between.
    Effect::new(move || {
        let current = text.get();
        if let Some(ta) = textarea.get_untracked()
            && dom::value(&ta) != current
        {
            dom::set_value(&ta, &current);
        }
    });
    Effect::new(move || {
        let Some(open) = popup.get() else {
            return;
        };
        highlighted.track();
        scrolled.track();
        let Some(frame) = frame.get_untracked() else {
            return;
        };
        if let Some(place) = dom::popup_place(&frame)
            && popup_at.get_untracked() != place
        {
            popup_at.set(place);
        }
        dom::reveal(&frame, open.index);
    });
    let apply = move |edit: Edit| {
        let Some(ta) = textarea.get_untracked() else {
            return;
        };
        match edit {
            Edit::Insert {
                replace,
                text: inserted,
                caret: after,
            } => {
                let value = dom::insert(&ta, replace, &inserted, after);
                text.set(value);
                caret.set(after);
                on_edit.run(());
            }
            Edit::Move(to) => {
                dom::set_caret(&ta, to);
                caret.set(to);
            }
            Edit::None => {}
        }
    };
    let jump = move |to: usize| {
        if let Some(ta) = textarea.get_untracked() {
            dom::focus(&ta);
            dom::set_caret(&ta, to);
            caret.set(to);
        }
    };
    let refresh_popup = move |explicit: bool| {
        let Some(ta) = textarea.get_untracked() else {
            return;
        };
        let value = dom::value(&ta);
        let at = dom::selection(&ta).0;
        let options = complete.run((value.clone(), at));
        popup.set(completion_at(&value, at, options, explicit));
    };
    let accept = move |index: usize| {
        if let Some(edit) = popup
            .get_untracked()
            .and_then(|p| completion_edit(&p, index))
        {
            popup.set(None);
            apply(edit);
        }
    };
    let on_keydown = move |ev: leptos::ev::KeyboardEvent| {
        if ev.is_composing() {
            return;
        }
        let key = ev.key();
        if let Some(open) = popup.get_untracked() {
            let count = open.options.len();
            let handled = match key.as_str() {
                "ArrowDown" => {
                    popup.set(Some(Popup {
                        index: (open.index + 1) % count,
                        ..open
                    }));
                    true
                }
                "ArrowUp" => {
                    popup.set(Some(Popup {
                        index: (open.index + count - 1) % count,
                        ..open
                    }));
                    true
                }
                "Enter" | "Tab" => {
                    accept(open.index);
                    true
                }
                "Escape" => {
                    popup.set(None);
                    true
                }
                _ => false,
            };
            if handled {
                ev.prevent_default();
                return;
            }
        }
        if ev.ctrl_key() && key == " " {
            ev.prevent_default();
            refresh_popup(true);
            return;
        }
        if key == "Escape" {
            escaped.set(true);
            return;
        }
        let leave = escaped.get_untracked();
        escaped.set(false);
        if ev.meta_key() || ev.ctrl_key() || ev.alt_key() {
            return;
        }
        let Some(ta) = textarea.get_untracked() else {
            return;
        };
        let value = dom::value(&ta);
        let (start, end) = dom::selection(&ta);
        let edit = key_edit(&value, start, end, &key, leave);
        if edit != Edit::None {
            ev.prevent_default();
            apply(edit);
        }
    };

    view! {
        <div class="code-editor-frame" node_ref=frame>
        <div
            class="code-editor"
            class:code-editor--invalid=move || !issues.get().is_empty()
            on:scroll=move |_| scrolled.update(|n| *n += 1)
        >
            <pre class="code-editor-hl" aria-hidden="true">
                {move || highlighted.get().into_iter().map(|piece| match piece {
                    Piece::Caret => view! { <span class="code-editor-caret"></span> }.into_any(),
                    Piece::Text { text, class, mark } => view! {
                        <span class=class class:code-editor-mark=mark.is_some() title=mark>{text}</span>
                    }.into_any(),
                }).collect::<Vec<_>>()}
                "\n"
            </pre>
            <textarea
                class="code-editor-text config-editor-text"
                wrap="off"
                spellcheck="false"
                autocapitalize="off"
                aria-label=label
                node_ref=textarea
                on:input=move |ev| {
                    text.set(event_target_value(&ev));
                    read_caret();
                    refresh_popup(false);
                    on_edit.run(());
                }
                on:keydown=on_keydown
                on:keyup=move |_| read_caret()
                on:click=move |_| {
                    read_caret();
                    popup.set(None);
                }
                on:focus=move |_| read_caret()
                on:blur=move |_| popup.set(None)
            >
                {text.get_untracked()}
            </textarea>
        </div>
        {move || popup.get().map(|open| {
            let (x, y, above) = popup_at.get();
            view! {
                <div
                    class="code-editor-popup"
                    class:code-editor-popup--above=above
                    role="listbox"
                    style=format!("left:{x}px;top:{y}px")
                >
                    {open.options.iter().enumerate().map(|(i, option)| {
                        let active = i == open.index;
                        view! {
                            <button
                                type="button"
                                class="code-editor-option"
                                class:code-editor-option--active=active
                                role="option"
                                aria-selected=active.to_string()
                                on:mousedown=move |ev| {
                                    ev.prevent_default();
                                    accept(i);
                                }
                            >
                                <span class="code-editor-option-label">{option.label.clone()}</span>
                                <span class="code-editor-option-detail">{option.detail.clone()}</span>
                            </button>
                        }
                    }).collect::<Vec<_>>()}
                </div>
            }
        })}
        </div>
        <Show when=move || !issues.get().is_empty()>
            <ul class="code-editor-issues">
                {move || issues.get().into_iter().map(|issue| {
                    let (line, col) = line_col(&text.get(), issue.span.start);
                    let at = issue.span.start;
                    view! {
                        <li class="code-editor-issue">
                            <button class="link-btn" on:click=move |_| jump(at)>
                                <span class="code-editor-issue-at">{format!("{line}:{col}")}</span>
                                " "
                                {issue.message}
                            </button>
                        </li>
                    }
                }).collect::<Vec<_>>()}
            </ul>
        </Show>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(start: usize, end: usize) -> Span {
        Span { start, end }
    }

    fn texts(pieces: &[Piece]) -> Vec<(String, &'static str, bool)> {
        pieces
            .iter()
            .filter_map(|p| match p {
                Piece::Text { text, class, mark } => Some((text.clone(), *class, mark.is_some())),
                Piece::Caret => None,
            })
            .collect()
    }

    #[test]
    fn pieces_class_tokens_and_mark_issues() {
        let text = "{\"a\": [1, true]}";
        let issue = Issue {
            span: span(7, 8),
            message: "a[0]: expected string, got number".to_string(),
        };
        let out = pieces(text, &[issue], 0);
        assert_eq!(out[0], Piece::Caret);
        assert_eq!(
            texts(&out),
            vec![
                ("{".to_string(), "tok-punct", false),
                ("\"a\"".to_string(), "tok-key", false),
                (":".to_string(), "tok-punct", false),
                (" ".to_string(), "tok-space", false),
                ("[".to_string(), "tok-punct", false),
                ("1".to_string(), "tok-num", true),
                (",".to_string(), "tok-punct", false),
                (" ".to_string(), "tok-space", false),
                ("true".to_string(), "tok-kw", false),
                ("]".to_string(), "tok-punct", false),
                ("}".to_string(), "tok-punct", false),
            ]
        );
        let marked: Vec<&Piece> = out
            .iter()
            .filter(|p| matches!(p, Piece::Text { mark: Some(_), .. }))
            .collect();
        assert!(
            matches!(marked[0], Piece::Text { mark: Some(m), .. } if m == "a[0]: expected string, got number")
        );
    }

    #[test]
    fn pieces_split_at_the_caret_and_mark_the_end() {
        // The caret inside a key splits it; the pieces still cover the text.
        let out = pieces("{\"key\": 1}", &[], 3);
        assert_eq!(
            out[1..4],
            [
                Piece::Text {
                    text: "\"k".to_string(),
                    class: "tok-key",
                    mark: None
                },
                Piece::Caret,
                Piece::Text {
                    text: "ey\"".to_string(),
                    class: "tok-key",
                    mark: None
                },
            ]
        );
        let joined: String = texts(&out).into_iter().map(|(t, _, _)| t).collect();
        assert_eq!(joined, "{\"key\": 1}");

        // An issue at the end of the text marks a trailing space.
        let eof = Issue {
            span: span(6, 6),
            message: "Expected a value".to_string(),
        };
        let out = pieces("{\"a\": ", &[eof], 6);
        assert_eq!(
            out.last(),
            Some(&Piece::Text {
                text: " ".to_string(),
                class: "tok-space",
                mark: Some("Expected a value".to_string())
            })
        );
        assert_eq!(out[out.len() - 2], Piece::Caret);
        // An empty span before the end marks the next character.
        let here = Issue {
            span: span(6, 6),
            message: "Expected a value".to_string(),
        };
        let out = pieces("{\"a\": }", &[here], 0);
        assert!(matches!(out.last(), Some(Piece::Text { text, mark: Some(_), .. }) if text == "}"));
        assert_eq!(pieces("", &[], 0), vec![Piece::Caret]);
    }

    #[test]
    fn keys_edit_like_a_code_editor() {
        let insert = |start: usize, end: usize, text: &str, caret: usize| Edit::Insert {
            replace: span(start, end),
            text: text.to_string(),
            caret,
        };
        assert_eq!(key_edit("{}", 1, 1, "Tab", false), insert(1, 1, "  ", 3));
        assert_eq!(key_edit("{}", 1, 1, "Tab", true), Edit::None);
        assert_eq!(key_edit("ab", 0, 2, "Tab", false), insert(0, 2, "  ", 2));

        assert_eq!(
            key_edit("  x", 3, 3, "Enter", false),
            insert(3, 3, "\n  ", 6)
        );
        assert_eq!(
            key_edit("{\n  \"a\": {}\n}", 10, 10, "Enter", false),
            insert(10, 10, "\n    \n  ", 15)
        );
        assert_eq!(
            key_edit("[]", 1, 1, "Enter", false),
            insert(1, 1, "\n  \n", 4)
        );

        assert_eq!(key_edit("", 0, 0, "{", false), insert(0, 0, "{}", 1));
        assert_eq!(key_edit("{", 1, 1, "[", false), insert(1, 1, "[]", 2));
        assert_eq!(key_edit("{", 1, 1, "\"", false), insert(1, 1, "\"\"", 2));
        assert_eq!(key_edit("{\"\"}", 2, 2, "\"", false), Edit::Move(3));
        assert_eq!(key_edit("{\"\"}", 3, 3, "}", false), Edit::Move(4));
        assert_eq!(key_edit("[]", 1, 1, "]", false), Edit::Move(2));
        // Inside a string nothing pairs; a selection is left to the browser.
        assert_eq!(key_edit("\"ab", 3, 3, "\"", false), Edit::None);
        assert_eq!(key_edit("\"ab\"", 2, 2, "{", false), Edit::None);
        assert_eq!(key_edit("ab", 0, 2, "{", false), Edit::None);

        assert_eq!(
            key_edit("{\"\"}", 2, 2, "Backspace", false),
            insert(1, 3, "", 1)
        );
        assert_eq!(
            key_edit("{}", 1, 1, "Backspace", false),
            insert(0, 2, "", 0)
        );
        assert_eq!(key_edit("{a}", 2, 2, "Backspace", false), Edit::None);
        assert_eq!(key_edit("{}", 0, 0, "Backspace", false), Edit::None);
        assert_eq!(key_edit("x", 1, 1, "a", false), Edit::None);
    }

    fn option(label: &str, insert: &str) -> Candidate {
        Candidate {
            label: label.to_string(),
            insert: insert.to_string(),
            detail: String::new(),
            caret: insert.len() - usize::from(insert.ends_with("\"\"")),
        }
    }

    fn fields() -> Vec<Candidate> {
        vec![
            option("api_key", "\"api_key\": \"\""),
            option("batch_size", "\"batch_size\": 100"),
            option("mode", "\"mode\": \"fast\""),
        ]
    }

    #[test]
    fn completion_opens_for_a_prefix_a_colon_or_ctrl_space() {
        let labels = |p: Option<Popup>| -> Vec<String> {
            p.map(|p| p.options.into_iter().map(|c| c.label).collect())
                .unwrap_or_default()
        };
        let text = "{\"bat";
        assert_eq!(
            labels(completion_at(text, 5, fields(), false)),
            vec!["batch_size"]
        );
        // Case does not matter; an empty prefix needs Ctrl+Space.
        assert_eq!(
            labels(completion_at("{\"BAT", 5, fields(), false)),
            vec!["batch_size"]
        );
        assert_eq!(completion_at("{", 1, fields(), false), None);
        assert_eq!(
            labels(completion_at("{", 1, fields(), true)),
            vec!["api_key", "batch_size", "mode"]
        );
        // A value opens right after the colon.
        let values = vec![
            option("\"fast\"", "\"fast\""),
            option("\"slow\"", "\"slow\""),
        ];
        assert_eq!(
            labels(completion_at("{\"mode\": ", 9, values.clone(), false)),
            vec!["\"fast\"", "\"slow\""]
        );
        assert_eq!(
            labels(completion_at("{\"mode\": \"s", 11, values.clone(), false)),
            vec!["\"slow\""]
        );
        // Nothing matches, the match is already typed, or there is no slot.
        assert_eq!(completion_at("{\"zz", 4, fields(), true), None);
        assert_eq!(
            completion_at("{\"mode\": \"slow\"", 15, values, false),
            None
        );
        assert_eq!(completion_at("{}", 2, fields(), true), None);
        assert_eq!(completion_at("", 0, fields(), true), None);
    }

    #[test]
    fn completion_edit_replaces_the_prefix_and_adds_commas() {
        let insert = |start: usize, end: usize, text: &str, caret: usize| {
            Some(Edit::Insert {
                replace: span(start, end),
                text: text.to_string(),
                caret,
            })
        };
        let popup = completion_at("{\"bat", 5, fields(), false).unwrap();
        assert_eq!(
            completion_edit(&popup, 0),
            insert(1, 5, "\"batch_size\": 100", 1 + 17)
        );
        assert_eq!(completion_edit(&popup, 7), None);

        // The key alone when its colon is there already.
        let popup = completion_at("{\"bat\": 5}", 3, fields(), false).unwrap();
        assert!(popup.key_only);
        assert_eq!(
            completion_edit(&popup, 0),
            insert(1, 6, "\"batch_size\"", 1 + 12)
        );

        // Commas for the neighbours; the caret lands inside `\"\"`.
        let popup = completion_at("{\"mode\": 1 ", 11, fields(), true).unwrap();
        assert!(popup.comma_before);
        assert_eq!(
            completion_edit(&popup, 0),
            insert(11, 11, ", \"api_key\": \"\"", 11 + 2 + 12)
        );
        let popup = completion_at("{\"mode\": 1}", 1, fields(), true).unwrap();
        assert!(popup.comma_after);
        assert_eq!(
            completion_edit(&popup, 0),
            insert(1, 1, "\"api_key\": \"\",", 1 + 12)
        );
    }
}
