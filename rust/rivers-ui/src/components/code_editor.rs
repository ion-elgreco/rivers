//! A JSON editor: a highlighted `<pre>` under a transparent `<textarea>`,
//! both in one CSS grid cell so they never scroll apart, with issue marks
//! and list and the keys a code editor has (Tab, Enter, pairs).

use leptos::prelude::*;

use crate::json_text::{Issue, Span, TokenKind, line_col, line_indent, tokenize};

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

#[cfg(target_arch = "wasm32")]
mod dom {
    use leptos::prelude::document;
    use leptos::web_sys::HtmlTextAreaElement;

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
}

#[cfg(not(target_arch = "wasm32"))]
mod dom {
    use leptos::web_sys::HtmlTextAreaElement;

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
}

/// The editor over `text`. `issues` underline their spans and list under
/// the text (a click moves the caret there); `on_edit` fires on every
/// change the user makes.
#[component]
pub fn CodeEditor(
    text: RwSignal<String>,
    #[prop(into)] issues: Signal<Vec<Issue>>,
    #[prop(into)] label: String,
    on_edit: Callback<()>,
) -> impl IntoView {
    let textarea = NodeRef::<leptos::html::Textarea>::new();
    let caret = RwSignal::new(0usize);
    // Escape arms the next Tab to leave the editor instead of indenting.
    let escaped = RwSignal::new(false);
    let highlighted = Memo::new(move |_| pieces(&text.get(), &issues.get(), caret.get()));

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
    let on_keydown = move |ev: leptos::ev::KeyboardEvent| {
        if ev.is_composing() {
            return;
        }
        let key = ev.key();
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
        <div class="code-editor" class:code-editor--invalid=move || !issues.get().is_empty()>
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
                    on_edit.run(());
                }
                on:keydown=on_keydown
                on:keyup=move |_| read_caret()
                on:click=move |_| read_caret()
                on:focus=move |_| read_caret()
            >
                {text.get_untracked()}
            </textarea>
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
}
