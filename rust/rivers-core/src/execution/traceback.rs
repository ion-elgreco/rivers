//! A failed step's Python traceback, stored as JSON in the step's `run_logs`
//! row. The executor captures it with Python's `traceback` and `linecache`
//! modules on the machine that ran the step, because the UI server does not
//! have the user's source files. Field names follow Sentry's exception and
//! stack-frame interfaces.

use serde::{Deserialize, Serialize};

/// Source lines kept above and below the running line of an in-app frame.
pub const CONTEXT_LINES: u32 = 5;
/// Largest JSON stored for one traceback. The run page loads all of a run's
/// `run_logs` rows on every refresh.
pub const MAX_JSON_BYTES: usize = 256 * 1024;
/// A frame repeated in a row shows this many times; the rest become a count.
/// Python's own cutoff when it prints a traceback.
pub const REPEAT_CUTOFF: usize = 3;
/// Exception-group members followed per group, and nested groups followed:
/// the defaults of Python's `TracebackException`.
pub const GROUP_WIDTH: usize = 15;
pub const GROUP_DEPTH: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Traceback {
    /// The exception chain, oldest first. The last entry failed the step.
    pub exceptions: Vec<ExceptionInfo>,
    /// The traceback as Python prints it.
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExceptionInfo {
    /// Class name, e.g. `KeyError`.
    #[serde(rename = "type")]
    pub exc_type: String,
    /// Module of the class; `None` for builtins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// `str(exc)`, then one line per note (PEP 678).
    pub value: String,
    /// How this exception is linked to the entry before it in the chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainLink>,
    /// Outermost call first; the last frame raised.
    pub frames: Vec<Frame>,
    /// An exception group's members, each its own chain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group: Vec<Vec<ExceptionInfo>>,
    /// Group members past [`GROUP_WIDTH`] or [`GROUP_DEPTH`].
    #[serde(default, skip_serializing_if = "is_zero")]
    pub group_omitted: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainLink {
    /// Raised `from` the previous exception (`__cause__`).
    Cause,
    /// Raised while handling the previous exception (`__context__`).
    Context,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    /// Path for display: relative to site-packages, the standard library,
    /// or the working directory.
    pub filename: String,
    pub abs_path: String,
    pub function: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineno: Option<u32>,
    /// First column of the running expression, from 1 (Python 3.11+).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub colno: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_lineno: Option<u32>,
    /// Last column of the running expression on `end_lineno`, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_colno: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pre_context: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_line: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_context: Vec<String>,
    /// `false` for rivers, the standard library, and installed packages
    /// other than the step's own.
    pub in_app: bool,
    /// More calls of this same frame right after it, left out as
    /// "[Previous line repeated N more times]" (see [`REPEAT_CUTOFF`]).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub repeated: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl Traceback {
    /// JSON within [`MAX_JSON_BYTES`]. Over the limit, the lines around each
    /// frame go first; then only the text stays, cut in the middle.
    pub fn to_json(mut self) -> String {
        let json = self.encode();
        if json.len() <= MAX_JSON_BYTES {
            return json;
        }
        for_each_exception(&mut self.exceptions, &mut |e| {
            for f in &mut e.frames {
                f.pre_context.clear();
                f.post_context.clear();
            }
        });
        let json = self.encode();
        if json.len() <= MAX_JSON_BYTES {
            return json;
        }
        self.exceptions.clear();
        let text = std::mem::take(&mut self.text);
        let mut keep = text.chars().count();
        loop {
            self.text = cut_middle(&text, keep);
            let json = self.encode();
            if json.len() <= MAX_JSON_BYTES || keep == 0 {
                return json;
            }
            // Every character left out removes at least one byte of JSON.
            keep = keep.saturating_sub(json.len() - MAX_JSON_BYTES);
        }
    }

    fn encode(&self) -> String {
        serde_json::to_string(self).expect("a traceback always serializes")
    }

    /// `true` when no exception in the chain, or in any group, has a frame:
    /// such a traceback adds nothing to the error message.
    pub fn has_no_frames(&self) -> bool {
        fn none(chain: &[ExceptionInfo]) -> bool {
            chain
                .iter()
                .all(|e| e.frames.is_empty() && e.group.iter().all(|m| none(m)))
        }
        none(&self.exceptions)
    }
}

fn for_each_exception(chain: &mut [ExceptionInfo], f: &mut impl FnMut(&mut ExceptionInfo)) {
    for e in chain {
        f(e);
        for member in &mut e.group {
            for_each_exception(member, f);
        }
    }
}

/// `text` cut to `keep` characters, a quarter from the start and the rest
/// from the end, where Python prints the exception.
fn cut_middle(text: &str, keep: usize) -> String {
    let total = text.chars().count();
    if keep >= total {
        return text.to_string();
    }
    let head = keep / 4;
    let start: String = text.chars().take(head).collect();
    let end: String = text.chars().skip(total - (keep - head)).collect();
    format!(
        "{start}\n  [... {} characters omitted ...]\n{end}",
        total - keep
    )
}

/// The frames to keep, as Python prints a traceback: a frame repeated in a
/// row shows [`REPEAT_CUTOFF`] times, and the last one shown counts the rest.
/// `keys` identify each frame (file, line, function); returns
/// `(index, repeated)` pairs.
pub fn fold_repeats<K: PartialEq>(keys: &[K]) -> Vec<(usize, u32)> {
    let mut kept = Vec::new();
    let mut run_start = 0;
    for i in 1..=keys.len() {
        if i < keys.len() && keys[i] == keys[run_start] {
            continue;
        }
        let run = i - run_start;
        let shown = run.min(REPEAT_CUTOFF);
        kept.extend((run_start..run_start + shown).map(|j| (j, 0)));
        if let Some(last) = kept.last_mut() {
            last.1 = (run - shown) as u32;
        }
        run_start = i;
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(line: &str) -> Frame {
        Frame {
            filename: "assets.py".into(),
            abs_path: "/app/assets.py".into(),
            function: "sales".into(),
            lineno: Some(42),
            colno: Some(5),
            end_lineno: Some(42),
            end_colno: Some(10),
            pre_context: vec![line.into(); 5],
            context_line: Some(line.into()),
            post_context: vec![line.into(); 5],
            in_app: true,
            repeated: 0,
        }
    }

    fn exception(frames: Vec<Frame>) -> ExceptionInfo {
        ExceptionInfo {
            exc_type: "KeyError".into(),
            module: None,
            value: "'b'".into(),
            chain: None,
            frames,
            group: vec![],
            group_omitted: 0,
        }
    }

    fn chain_of(exceptions: usize, frames: usize, line: &str) -> Vec<ExceptionInfo> {
        (0..exceptions)
            .map(|_| exception((0..frames).map(|_| frame(line)).collect()))
            .collect()
    }

    #[test]
    fn json_uses_sentry_field_names_and_skips_empty_fields() {
        let mut e = exception(vec![frame("x = d['b']")]);
        e.chain = Some(ChainLink::Cause);
        e.frames[0].repeated = 997;
        let json = Traceback {
            exceptions: vec![e],
            text: "Traceback".into(),
        }
        .to_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let e = &v["exceptions"][0];
        assert_eq!(e["type"], "KeyError");
        assert_eq!(e["chain"], "cause");
        assert!(e.get("module").is_none());
        assert!(e.get("group").is_none());
        assert!(e.get("group_omitted").is_none());
        let f = &e["frames"][0];
        assert_eq!(f["context_line"], "x = d['b']");
        assert_eq!(f["colno"], 5);
        assert_eq!(f["in_app"], true);
        assert_eq!(f["repeated"], 997);
    }

    #[test]
    fn json_round_trips() {
        let mut outer = exception(vec![frame("raise")]);
        outer.group = vec![vec![exception(vec![frame("inner")])]];
        outer.group_omitted = 3;
        let tb = Traceback {
            exceptions: vec![outer],
            text: "Traceback".into(),
        };
        let back: Traceback = serde_json::from_str(&tb.clone().to_json()).unwrap();
        assert_eq!(back, tb);
    }

    #[test]
    fn oversized_json_drops_the_lines_around_each_frame_first() {
        // 2 × 50 frames × 11 lines × 400 chars ≈ 440 KB with them, ≈ 55 KB without.
        let line = "x".repeat(400);
        let tb = Traceback {
            exceptions: chain_of(2, 50, &line),
            text: "Traceback".into(),
        };
        let back: Traceback = serde_json::from_str(&tb.to_json()).unwrap();
        let f = &back.exceptions[1].frames[49];
        assert!(f.pre_context.is_empty() && f.post_context.is_empty());
        assert_eq!(f.context_line.as_deref(), Some(line.as_str()));
        assert_eq!(back.text, "Traceback");
    }

    #[test]
    fn json_still_too_big_keeps_only_the_text() {
        // 10 × 50 frames × 1000 chars ≈ 575 KB even without the lines around them.
        let tb = Traceback {
            exceptions: chain_of(10, 50, &"x".repeat(1000)),
            text: "Traceback".into(),
        };
        let back: Traceback = serde_json::from_str(&tb.to_json()).unwrap();
        assert!(back.exceptions.is_empty());
        assert_eq!(back.text, "Traceback");
    }

    #[test]
    fn text_too_big_is_cut_in_the_middle() {
        let text = format!("Traceback{}\nKeyError: 'b'", "\n  line".repeat(100_000));
        let tb = Traceback {
            exceptions: vec![],
            text,
        };
        let json = tb.to_json();
        assert!(json.len() <= MAX_JSON_BYTES, "{} bytes", json.len());
        let back: Traceback = serde_json::from_str(&json).unwrap();
        assert!(back.text.starts_with("Traceback\n  line"));
        assert!(back.text.ends_with("\nKeyError: 'b'"));
        assert!(back.text.contains("characters omitted ...]"));
    }

    #[test]
    fn has_no_frames_looks_inside_groups() {
        let mut top = exception(vec![]);
        let tb = Traceback {
            exceptions: vec![top.clone()],
            text: String::new(),
        };
        assert!(tb.has_no_frames());
        top.group = vec![vec![exception(vec![frame("inner")])]];
        let tb = Traceback {
            exceptions: vec![top],
            text: String::new(),
        };
        assert!(!tb.has_no_frames());
    }

    #[test]
    fn fold_repeats_keeps_three_of_a_run_and_counts_the_rest() {
        let keys = ["a", "a", "a", "a", "b", "c", "c", "c", "c", "c", "a"];
        assert_eq!(
            fold_repeats(&keys),
            vec![
                (0, 0),
                (1, 0),
                (2, 1),
                (4, 0),
                (5, 0),
                (6, 0),
                (7, 2),
                (10, 0)
            ]
        );
    }

    #[test]
    fn fold_repeats_leaves_short_runs_alone() {
        assert_eq!(
            fold_repeats(&["a", "a", "a", "b"]),
            vec![(0, 0), (1, 0), (2, 0), (3, 0)]
        );
        assert_eq!(fold_repeats::<&str>(&[]), vec![]);
    }
}
