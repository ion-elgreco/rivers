//! JSON text tooling for the config editor: a tokenizer that covers every
//! byte, a parser that keeps a spanned tree past the first error, and the
//! caret context a completion needs. Offsets are bytes; the textarea's
//! UTF-16 offsets convert through [`byte_to_utf16`] and [`utf16_to_byte`].

use std::collections::HashSet;

/// A byte range of the text, end exclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub const fn at(offset: usize) -> Self {
        Span {
            start: offset,
            end: offset,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Colon,
    Comma,
    /// A string followed by `:`.
    Key,
    Str,
    /// A string that reaches the end of its line without a closing quote.
    StrOpen,
    Num,
    Bool,
    Null,
    Space,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// Every byte of `text` is in exactly one token.
pub fn tokenize(text: &str) -> Vec<Token> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let kind = match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => {
                while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
                    i += 1;
                }
                TokenKind::Space
            }
            b'{' => {
                i += 1;
                TokenKind::LBrace
            }
            b'}' => {
                i += 1;
                TokenKind::RBrace
            }
            b'[' => {
                i += 1;
                TokenKind::LBracket
            }
            b']' => {
                i += 1;
                TokenKind::RBracket
            }
            b':' => {
                i += 1;
                TokenKind::Colon
            }
            b',' => {
                i += 1;
                TokenKind::Comma
            }
            b'"' => {
                let (end, closed) = string_end(bytes, i + 1);
                i = end;
                if closed {
                    TokenKind::Str
                } else {
                    TokenKind::StrOpen
                }
            }
            b'-' | b'0'..=b'9' => {
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    i += 1;
                }
                TokenKind::Num
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                match &text[start..i] {
                    "true" | "false" => TokenKind::Bool,
                    "null" => TokenKind::Null,
                    _ => TokenKind::Invalid,
                }
            }
            _ => {
                i += text[start..].chars().next().map_or(1, char::len_utf8);
                TokenKind::Invalid
            }
        };
        tokens.push(Token {
            kind,
            span: Span { start, end: i },
        });
    }
    let mut next = None;
    for tok in tokens.iter_mut().rev() {
        if tok.kind == TokenKind::Str && next == Some(TokenKind::Colon) {
            tok.kind = TokenKind::Key;
        }
        if tok.kind != TokenKind::Space {
            next = Some(tok.kind);
        }
    }
    tokens
}

/// Where a string body starting at `i` ends, and whether a closing quote
/// closed it before the end of its line.
fn string_end(bytes: &[u8], mut i: usize) -> (usize, bool) {
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return (i + 1, true),
            b'\\' => i += 2,
            b'\n' => return (i, false),
            _ => i += 1,
        }
    }
    (bytes.len(), false)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub key: String,
    pub key_span: Span,
    pub colon: Option<Span>,
    pub value: Node,
}

impl Entry {
    fn end(&self) -> usize {
        self.value.span().end.max(self.key_span.end)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    Object {
        span: Span,
        entries: Vec<Entry>,
        closed: bool,
    },
    Array {
        span: Span,
        items: Vec<Node>,
        closed: bool,
    },
    Str {
        span: Span,
        value: String,
    },
    Num {
        span: Span,
        value: f64,
        /// No fractional part: what JSON Schema calls an integer.
        whole: bool,
    },
    Bool {
        span: Span,
        value: bool,
    },
    Null {
        span: Span,
    },
    /// A value that is not there, or text that is not a value.
    Missing {
        span: Span,
    },
}

impl Node {
    pub fn span(&self) -> Span {
        match self {
            Node::Object { span, .. }
            | Node::Array { span, .. }
            | Node::Str { span, .. }
            | Node::Num { span, .. }
            | Node::Bool { span, .. }
            | Node::Null { span } => *span,
            Node::Missing { span } => *span,
        }
    }

    /// The JSON type name, for messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Node::Object { .. } => "object",
            Node::Array { .. } => "array",
            Node::Str { .. } => "string",
            Node::Num { .. } => "number",
            Node::Bool { .. } => "boolean",
            Node::Null { .. } => "null",
            Node::Missing { .. } => "nothing",
        }
    }

    fn is_container(&self) -> bool {
        matches!(self, Node::Object { .. } | Node::Array { .. })
    }

    fn closed(&self) -> bool {
        match self {
            Node::Object { closed, .. } | Node::Array { closed, .. } => *closed,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Issue {
    pub span: Span,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Parsed {
    /// `None` for blank text.
    pub root: Option<Node>,
    /// The first syntax error; the tree past it is a best effort.
    pub error: Option<Issue>,
}

/// Parse `text` without giving up: unclosed containers close at the end,
/// a value that is not there becomes [`Node::Missing`], and only the first
/// error is reported.
pub fn parse(text: &str) -> Parsed {
    let tokens: Vec<Token> = tokenize(text)
        .into_iter()
        .filter(|t| t.kind != TokenKind::Space)
        .collect();
    if tokens.is_empty() {
        return Parsed {
            root: None,
            error: None,
        };
    }
    let mut parser = Parser {
        text,
        tokens,
        pos: 0,
        error: None,
    };
    let root = parser.value();
    if let Some(tok) = parser.peek() {
        parser.fail(tok.span, "Unexpected text after the document");
    }
    Parsed {
        root: Some(root),
        error: parser.error,
    }
}

struct Parser<'a> {
    text: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    error: Option<Issue>,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<Token> {
        self.tokens.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn eof(&self) -> Span {
        Span::at(self.text.len())
    }

    fn here(&self) -> Span {
        self.peek().map_or(self.eof(), |t| t.span)
    }

    fn raw(&self, span: Span) -> &'a str {
        &self.text[span.start..span.end]
    }

    fn fail(&mut self, span: Span, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(Issue {
                span,
                message: message.into(),
            });
        }
    }

    fn string(&mut self, tok: Token) -> String {
        let raw = self.raw(tok.span);
        let body = if tok.kind == TokenKind::StrOpen {
            self.fail(tok.span, "Unterminated string");
            &raw[1..]
        } else {
            &raw[1..raw.len() - 1]
        };
        match unescape(body) {
            Some(value) => value,
            None => {
                self.fail(tok.span, "Invalid escape in string");
                body.to_string()
            }
        }
    }

    fn value(&mut self) -> Node {
        let Some(tok) = self.peek() else {
            self.fail(self.eof(), "Expected a value");
            return Node::Missing { span: self.eof() };
        };
        match tok.kind {
            TokenKind::LBrace => {
                self.bump();
                self.object(tok)
            }
            TokenKind::LBracket => {
                self.bump();
                self.array(tok)
            }
            TokenKind::Key | TokenKind::Str | TokenKind::StrOpen => {
                self.bump();
                let value = self.string(tok);
                Node::Str {
                    span: tok.span,
                    value,
                }
            }
            TokenKind::Num => {
                self.bump();
                match number(self.raw(tok.span)) {
                    Some((value, whole)) => Node::Num {
                        span: tok.span,
                        value,
                        whole,
                    },
                    None => {
                        self.fail(tok.span, "Invalid number");
                        Node::Missing { span: tok.span }
                    }
                }
            }
            TokenKind::Bool => {
                self.bump();
                Node::Bool {
                    span: tok.span,
                    value: self.raw(tok.span) == "true",
                }
            }
            TokenKind::Null => {
                self.bump();
                Node::Null { span: tok.span }
            }
            TokenKind::Invalid => {
                self.bump();
                self.fail(tok.span, "Unexpected text");
                Node::Missing { span: tok.span }
            }
            TokenKind::RBrace
            | TokenKind::RBracket
            | TokenKind::Colon
            | TokenKind::Comma
            | TokenKind::Space => {
                self.fail(tok.span, "Expected a value");
                Node::Missing {
                    span: Span::at(tok.span.start),
                }
            }
        }
    }

    fn object(&mut self, open: Token) -> Node {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        let mut first = true;
        let mut comma: Option<Span> = None;
        loop {
            let Some(tok) = self.peek() else {
                let message = if first || comma.is_some() {
                    "Expected a key"
                } else {
                    "Expected ',' or '}'"
                };
                self.fail(self.eof(), message);
                return Node::Object {
                    span: Span {
                        start: open.span.start,
                        end: self.text.len(),
                    },
                    entries,
                    closed: false,
                };
            };
            match tok.kind {
                TokenKind::RBrace => {
                    if let Some(comma) = comma {
                        self.fail(comma, "Trailing comma");
                    }
                    self.bump();
                    return Node::Object {
                        span: Span {
                            start: open.span.start,
                            end: tok.span.end,
                        },
                        entries,
                        closed: true,
                    };
                }
                TokenKind::Key | TokenKind::Str | TokenKind::StrOpen => {
                    if !first && comma.is_none() {
                        self.fail(tok.span, "Expected ',' or '}'");
                    }
                    self.bump();
                    let key = self.string(tok);
                    if !seen.insert(key.clone()) {
                        self.fail(tok.span, format!("Duplicate key '{key}'"));
                    }
                    let colon = match self.peek() {
                        Some(c) if c.kind == TokenKind::Colon => {
                            self.bump();
                            Some(c.span)
                        }
                        _ => {
                            self.fail(self.here(), "Expected ':'");
                            None
                        }
                    };
                    // Without a colon, a following string reads as the next key.
                    let value =
                        if colon.is_some() || self.peek().is_some_and(|t| starts_value(t.kind)) {
                            self.value()
                        } else {
                            Node::Missing {
                                span: Span::at(self.here().start),
                            }
                        };
                    entries.push(Entry {
                        key,
                        key_span: tok.span,
                        colon,
                        value,
                    });
                    first = false;
                    comma = None;
                    if let Some(c) = self.peek()
                        && c.kind == TokenKind::Comma
                    {
                        self.bump();
                        comma = Some(c.span);
                    }
                }
                _ => {
                    let message = if !first && comma.is_none() {
                        "Expected ',' or '}'"
                    } else {
                        "Expected a key"
                    };
                    self.fail(tok.span, message);
                    self.bump();
                }
            }
        }
    }

    fn array(&mut self, open: Token) -> Node {
        let mut items = Vec::new();
        let mut first = true;
        let mut comma: Option<Span> = None;
        loop {
            let Some(tok) = self.peek() else {
                let message = if first || comma.is_some() {
                    "Expected a value"
                } else {
                    "Expected ',' or ']'"
                };
                self.fail(self.eof(), message);
                return Node::Array {
                    span: Span {
                        start: open.span.start,
                        end: self.text.len(),
                    },
                    items,
                    closed: false,
                };
            };
            match tok.kind {
                TokenKind::RBracket => {
                    if let Some(comma) = comma {
                        self.fail(comma, "Trailing comma");
                    }
                    self.bump();
                    return Node::Array {
                        span: Span {
                            start: open.span.start,
                            end: tok.span.end,
                        },
                        items,
                        closed: true,
                    };
                }
                TokenKind::Colon | TokenKind::Comma | TokenKind::RBrace => {
                    self.fail(tok.span, "Expected a value");
                    self.bump();
                }
                _ => {
                    if !first && comma.is_none() {
                        self.fail(tok.span, "Expected ',' or ']'");
                    }
                    items.push(self.value());
                    first = false;
                    comma = None;
                    if let Some(c) = self.peek()
                        && c.kind == TokenKind::Comma
                    {
                        self.bump();
                        comma = Some(c.span);
                    }
                }
            }
        }
    }
}

fn starts_value(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::LBrace
            | TokenKind::LBracket
            | TokenKind::Num
            | TokenKind::Bool
            | TokenKind::Null
    )
}

/// The value of a JSON number literal and whether it is whole.
fn number(raw: &str) -> Option<(f64, bool)> {
    let b = raw.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => {
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        _ => return None,
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        let digits = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == digits {
            return None;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let digits = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == digits {
            return None;
        }
    }
    if i != b.len() {
        return None;
    }
    let value: f64 = raw.parse().ok()?;
    Some((value, value.fract() == 0.0))
}

/// The body of a JSON string literal (between the quotes) unescaped;
/// `None` for an escape JSON does not have.
pub fn unescape(raw: &str) -> Option<String> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            '/' => out.push('/'),
            'b' => out.push('\u{8}'),
            'f' => out.push('\u{c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                let hi = hex4(&mut chars)?;
                let code = if (0xD800..0xDC00).contains(&hi) {
                    if chars.next()? != '\\' || chars.next()? != 'u' {
                        return None;
                    }
                    let lo = hex4(&mut chars)?;
                    if !(0xDC00..0xE000).contains(&lo) {
                        return None;
                    }
                    0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                } else {
                    hi
                };
                out.push(char::from_u32(code)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

fn hex4(chars: &mut std::str::Chars<'_>) -> Option<u32> {
    let mut value = 0;
    for _ in 0..4 {
        value = value * 16 + chars.next()?.to_digit(16)?;
    }
    Some(value)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathSeg {
    Key(String),
    Index(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Slot {
    /// The caret is in an object key or where one may begin; `prefix` is the
    /// key text before the caret.
    Key {
        prefix: String,
    },
    /// The caret is in a value or where one may begin; `prefix` is the raw
    /// text of the value before the caret.
    Value {
        prefix: String,
    },
    None,
}

/// What surrounds the caret, for completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    /// The object holding a `Key` slot, or the value position of a `Value` slot.
    pub path: Vec<PathSeg>,
    pub slot: Slot,
    /// Keys already in the object of a `Key` slot.
    pub siblings: Vec<String>,
    /// The text a completion replaces: the key or value at the caret, or
    /// nothing at the caret.
    pub replace: Span,
    /// The key at the caret already has its colon: a completion replaces
    /// the key alone.
    pub key_only: bool,
    /// An entry precedes without a comma between it and the caret.
    pub comma_before: bool,
    /// An entry follows without a comma between the caret and it.
    pub comma_after: bool,
}

pub fn context_at(text: &str, parsed: &Parsed, caret: usize) -> Context {
    let caret = caret.min(text.len());
    let mut ctx = Context {
        path: Vec::new(),
        slot: Slot::None,
        siblings: Vec::new(),
        replace: Span::at(caret),
        key_only: false,
        comma_before: false,
        comma_after: false,
    };
    if let Some(root) = &parsed.root {
        locate(text, root, caret, &mut ctx);
    }
    ctx
}

fn inside(span: Span, closed: bool, caret: usize) -> bool {
    span.start < caret && (caret < span.end || !closed)
}

fn touches(span: Span, caret: usize) -> bool {
    span.start <= caret && caret <= span.end
}

fn locate(text: &str, node: &Node, caret: usize, ctx: &mut Context) {
    match node {
        Node::Object {
            span,
            entries,
            closed,
        } if inside(*span, *closed, caret) => {
            for entry in entries {
                if entry.key_span.start < caret && caret <= entry.key_span.end {
                    let prefix = text[entry.key_span.start + 1..caret].trim_end_matches('"');
                    ctx.slot = Slot::Key {
                        prefix: prefix.to_string(),
                    };
                    ctx.replace = entry.key_span;
                    ctx.key_only = entry.colon.is_some();
                    ctx.siblings = entries
                        .iter()
                        .filter(|e| e.key_span != entry.key_span)
                        .map(|e| e.key.clone())
                        .collect();
                    return;
                }
                let value_span = entry.value.span();
                if entry.value.is_container() {
                    if inside(value_span, entry.value.closed(), caret) {
                        ctx.path.push(PathSeg::Key(entry.key.clone()));
                        locate(text, &entry.value, caret, ctx);
                        return;
                    }
                } else if touches(value_span, caret) {
                    ctx.path.push(PathSeg::Key(entry.key.clone()));
                    ctx.slot = Slot::Value {
                        prefix: text[value_span.start..caret].to_string(),
                    };
                    ctx.replace = value_span;
                    return;
                }
                if let Some(colon) = entry.colon
                    && caret >= colon.end
                    && caret <= value_span.start
                {
                    ctx.path.push(PathSeg::Key(entry.key.clone()));
                    ctx.slot = Slot::Value {
                        prefix: String::new(),
                    };
                    return;
                }
            }
            ctx.slot = Slot::Key {
                prefix: String::new(),
            };
            ctx.siblings = entries.iter().map(|e| e.key.clone()).collect();
            let before = entries
                .iter()
                .map(Entry::end)
                .filter(|end| *end <= caret)
                .max();
            let after = entries
                .iter()
                .map(|e| e.key_span.start)
                .filter(|start| *start >= caret)
                .min();
            ctx.comma_before = before.is_some_and(|end| !text[end..caret].contains(','));
            ctx.comma_after = after.is_some_and(|start| !text[caret..start].contains(','));
        }
        Node::Array {
            span,
            items,
            closed,
        } if inside(*span, *closed, caret) => {
            for (index, item) in items.iter().enumerate() {
                let item_span = item.span();
                if item.is_container() {
                    if inside(item_span, item.closed(), caret) {
                        ctx.path.push(PathSeg::Index(index));
                        locate(text, item, caret, ctx);
                        return;
                    }
                } else if touches(item_span, caret) {
                    ctx.path.push(PathSeg::Index(index));
                    ctx.slot = Slot::Value {
                        prefix: text[item_span.start..caret].to_string(),
                    };
                    ctx.replace = item_span;
                    return;
                }
            }
            let ends: Vec<usize> = items.iter().map(|i| i.span().end).collect();
            let index = ends.iter().filter(|end| **end <= caret).count();
            ctx.path.push(PathSeg::Index(index));
            ctx.slot = Slot::Value {
                prefix: String::new(),
            };
            let before = ends.iter().copied().filter(|end| *end <= caret).max();
            let after = items
                .iter()
                .map(|i| i.span().start)
                .filter(|start| *start >= caret)
                .min();
            ctx.comma_before = before.is_some_and(|end| !text[end..caret].contains(','));
            ctx.comma_after = after.is_some_and(|start| !text[caret..start].contains(','));
        }
        _ => {}
    }
}

/// The key span (none for an array item) and value span at `path` under
/// `root`.
pub fn span_at(root: &Node, path: &[PathSeg]) -> Option<(Option<Span>, Span)> {
    let mut node = root;
    let mut key_span = None;
    for seg in path {
        match (seg, node) {
            (PathSeg::Key(key), Node::Object { entries, .. }) => {
                let entry = entries.iter().find(|e| &e.key == key)?;
                key_span = Some(entry.key_span);
                node = &entry.value;
            }
            (PathSeg::Index(index), Node::Array { items, .. }) => {
                node = items.get(*index)?;
                key_span = None;
            }
            _ => return None,
        }
    }
    Some((key_span, node.span()))
}

/// 1-based line and column (in characters) of a byte offset.
pub fn line_col(text: &str, offset: usize) -> (usize, usize) {
    let before = &text[..text.floor_char_boundary(offset)];
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    (
        before.matches('\n').count() + 1,
        before[line_start..].chars().count() + 1,
    )
}

/// The leading whitespace of the line holding `offset`, up to `offset`.
pub fn line_indent(text: &str, offset: usize) -> &str {
    let offset = text.floor_char_boundary(offset);
    let line_start = text[..offset].rfind('\n').map_or(0, |i| i + 1);
    let line = &text[line_start..offset];
    let width = line
        .bytes()
        .take_while(|b| matches!(b, b' ' | b'\t'))
        .count();
    &line[..width]
}

/// The textarea's offset (UTF-16 code units) of a byte offset.
pub fn byte_to_utf16(text: &str, byte: usize) -> u32 {
    text[..text.floor_char_boundary(byte)]
        .encode_utf16()
        .count() as u32
}

/// The byte offset of a textarea offset (UTF-16 code units).
pub fn utf16_to_byte(text: &str, unit: u32) -> usize {
    let mut units = 0;
    for (i, c) in text.char_indices() {
        if units >= unit {
            return i;
        }
        units += c.len_utf16() as u32;
    }
    text.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Vec<TokenKind> {
        tokenize(text).into_iter().map(|t| t.kind).collect()
    }

    fn error(text: &str) -> (String, Span) {
        let issue = parse(text).error.expect("a syntax error");
        (issue.message, issue.span)
    }

    fn span(start: usize, end: usize) -> Span {
        Span { start, end }
    }

    /// The context at the `|` in `marked`.
    fn ctx(marked: &str) -> Context {
        let caret = marked.find('|').expect("a caret mark");
        let text = marked.replacen('|', "", 1);
        context_at(&text, &parse(&text), caret)
    }

    fn key(name: &str) -> PathSeg {
        PathSeg::Key(name.to_string())
    }

    #[test]
    fn tokens_cover_every_byte() {
        for text in [
            "{\"a\": [1, true, null], \"b\": \"x\"}",
            "{\"a\": \"unterminated\n  \"b\": -1.5e3}",
            "tr é @ {\"k\"",
            "",
        ] {
            let tokens = tokenize(text);
            let mut at = 0;
            for tok in &tokens {
                assert_eq!(tok.span.start, at, "{text:?}");
                assert!(tok.span.end > at, "{text:?}");
                at = tok.span.end;
            }
            assert_eq!(at, text.len(), "{text:?}");
        }
    }

    #[test]
    fn tokens_are_classified() {
        use TokenKind::*;
        assert_eq!(
            kinds("{\"a\": [1, true, null], \"b\": \"x\"}"),
            vec![
                LBrace, Key, Colon, Space, LBracket, Num, Comma, Space, Bool, Comma, Space, Null,
                RBracket, Comma, Space, Key, Colon, Space, Str, RBrace
            ]
        );
        assert_eq!(kinds("\"abc\n"), vec![StrOpen, Space]);
        assert_eq!(kinds("\"a\\\"b\": 1"), vec![Key, Colon, Space, Num]);
        assert_eq!(kinds("tr é@"), vec![Invalid, Space, Invalid, Invalid]);
        assert_eq!(kinds("-1.5e+3"), vec![Num]);
    }

    #[test]
    fn a_document_parses_into_a_spanned_tree() {
        let text = "{\"a\": 1, \"b\": [true, null, \"x\"], \"c\": {\"d\": 2.5}}";
        let parsed = parse(text);
        assert_eq!(parsed.error, None);
        let Some(Node::Object {
            span: root_span,
            entries,
            closed: true,
        }) = parsed.root
        else {
            panic!("expected a closed object");
        };
        assert_eq!(root_span, span(0, text.len()));
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, "a");
        assert_eq!(entries[0].key_span, span(1, 4));
        assert_eq!(entries[0].colon, Some(span(4, 5)));
        assert_eq!(
            entries[0].value,
            Node::Num {
                span: span(6, 7),
                value: 1.0,
                whole: true
            }
        );
        let Node::Array { items, closed, .. } = &entries[1].value else {
            panic!("expected an array");
        };
        assert!(closed);
        assert_eq!(items.len(), 3);
        assert_eq!(
            items[2],
            Node::Str {
                span: span(27, 30),
                value: "x".to_string()
            }
        );
        let Node::Object { entries: inner, .. } = &entries[2].value else {
            panic!("expected an object");
        };
        assert!(matches!(
            inner[0].value,
            Node::Num {
                value: 2.5,
                whole: false,
                ..
            }
        ));
    }

    #[test]
    fn blank_text_is_no_document() {
        assert_eq!(
            parse("  \n"),
            Parsed {
                root: None,
                error: None
            }
        );
    }

    #[test]
    fn strings_unescape() {
        let parsed = parse("{\"a\\\"b\": \"\\u00e9\\n\\ud83d\\ude00\"}");
        assert_eq!(parsed.error, None);
        let Some(Node::Object { entries, .. }) = parsed.root else {
            panic!()
        };
        assert_eq!(entries[0].key, "a\"b");
        assert_eq!(
            entries[0].value,
            Node::Str {
                span: span(9, 31),
                value: "é\n😀".to_string()
            }
        );
        assert_eq!(unescape("\\x"), None);
        assert_eq!(unescape("\\ud83d"), None);
        assert_eq!(unescape("a\\/b"), Some("a/b".to_string()));
    }

    #[test]
    fn the_first_syntax_error_is_reported_at_its_span() {
        assert_eq!(
            error("{\"a\": 1,}"),
            ("Trailing comma".to_string(), span(7, 8))
        );
        assert_eq!(
            error("{\"a\": }"),
            ("Expected a value".to_string(), span(6, 7))
        );
        assert_eq!(error("{\"a\" 1}"), ("Expected ':'".to_string(), span(5, 6)));
        assert_eq!(
            error("{\"a\": 1 \"b\": 2}"),
            ("Expected ',' or '}'".to_string(), span(8, 11))
        );
        assert_eq!(
            error("{\"a\": 1"),
            ("Expected ',' or '}'".to_string(), span(7, 7))
        );
        assert_eq!(
            error("{\"a\": "),
            ("Expected a value".to_string(), span(6, 6))
        );
        assert_eq!(error("{"), ("Expected a key".to_string(), span(1, 1)));
        assert_eq!(
            error("{\"a\": \"b"),
            ("Unterminated string".to_string(), span(6, 8))
        );
        assert_eq!(
            error("{\"a\": \"\\x\"}"),
            ("Invalid escape in string".to_string(), span(6, 10))
        );
        assert_eq!(
            error("[1 2]"),
            ("Expected ',' or ']'".to_string(), span(3, 4))
        );
        assert_eq!(error("[1,]"), ("Trailing comma".to_string(), span(2, 3)));
        assert_eq!(
            error("[1,,2]"),
            ("Expected a value".to_string(), span(3, 4))
        );
        assert_eq!(
            error("{\"a\": 1} x"),
            (
                "Unexpected text after the document".to_string(),
                span(9, 10)
            )
        );
        assert_eq!(
            error("{\"a\": 1, \"a\": 2}"),
            ("Duplicate key 'a'".to_string(), span(9, 12))
        );
        assert_eq!(
            error("{\"a\": tr}"),
            ("Unexpected text".to_string(), span(6, 8))
        );
        assert_eq!(
            error("{\"a\": 01}"),
            ("Invalid number".to_string(), span(6, 8))
        );
        assert_eq!(
            error("{\"a\": 1.}"),
            ("Invalid number".to_string(), span(6, 8))
        );
        assert_eq!(error("{,}"), ("Expected a key".to_string(), span(1, 2)));
        assert_eq!(error("{1: 2}"), ("Expected a key".to_string(), span(1, 2)));
    }

    #[test]
    fn the_tree_survives_the_error() {
        let parsed = parse("{\"a\": {\"b\": ");
        assert_eq!(
            parsed.error,
            Some(Issue {
                span: span(12, 12),
                message: "Expected a value".to_string()
            })
        );
        let Some(Node::Object {
            entries,
            closed: false,
            span: root_span,
        }) = parsed.root
        else {
            panic!("expected an unclosed object");
        };
        assert_eq!(root_span, span(0, 12));
        let Node::Object {
            entries: inner,
            closed: false,
            ..
        } = &entries[0].value
        else {
            panic!("expected an unclosed inner object");
        };
        assert_eq!(inner[0].key, "b");
        assert_eq!(inner[0].value, Node::Missing { span: Span::at(12) });

        // A missing comma still yields both entries; a missing colon still
        // takes the value.
        let Some(Node::Object { entries, .. }) = parse("{\"a\": 1 \"b\": 2}").root else {
            panic!()
        };
        assert_eq!(entries.len(), 2);
        let Some(Node::Object { entries, .. }) = parse("{\"a\" 1}").root else {
            panic!()
        };
        assert_eq!(entries[0].colon, None);
        assert!(matches!(entries[0].value, Node::Num { value: 1.0, .. }));
    }

    #[test]
    fn context_in_an_object() {
        let c = ctx("{|");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: String::new()
            }
        );
        assert!(c.path.is_empty() && c.siblings.is_empty());
        assert!(!c.comma_before && !c.comma_after);

        let c = ctx("{\"a\": 1, |}");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: String::new()
            }
        );
        assert_eq!(c.siblings, vec!["a"]);
        assert_eq!(c.replace, Span::at(9));
        assert!(!c.comma_before && !c.comma_after);

        let c = ctx("{\"a\": 1 |}");
        assert!(c.comma_before && !c.comma_after);

        let c = ctx("{\"a\": 1, | \"b\": 2}");
        assert!(!c.comma_before && c.comma_after);
        assert_eq!(c.siblings, vec!["a", "b"]);

        let c = ctx("{\"a\": {}|}");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: String::new()
            }
        );
        assert!(c.comma_before);
    }

    #[test]
    fn context_in_a_key() {
        let c = ctx("{\"bat|");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: "bat".to_string()
            }
        );
        assert_eq!(c.replace, span(1, 5));
        assert!(!c.key_only);

        let c = ctx("{\"x\": 1, \"b|a\": 2}");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: "b".to_string()
            }
        );
        assert_eq!(c.replace, span(9, 13));
        assert!(c.key_only);
        assert_eq!(c.siblings, vec!["x"]);

        let c = ctx("{\"bat\"|: 2}");
        assert_eq!(
            c.slot,
            Slot::Key {
                prefix: "bat".to_string()
            }
        );
        assert!(c.key_only);
    }

    #[test]
    fn context_in_a_value() {
        let c = ctx("{\"a\": |}");
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: String::new()
            }
        );
        assert_eq!(c.path, vec![key("a")]);
        assert_eq!(c.replace, Span::at(6));

        let c = ctx("{\"a\":|}");
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: String::new()
            }
        );
        assert_eq!(c.path, vec![key("a")]);
        assert_eq!(c.replace, Span::at(5));

        let c = ctx("{\"a\": \"fa|st\"}");
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: "\"fa".to_string()
            }
        );
        assert_eq!(c.replace, span(6, 12));

        let c = ctx("{\"a\": |1}");
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: String::new()
            }
        );
        assert_eq!(c.replace, span(6, 7));

        let c = ctx("{\"a\": tr|}");
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: "tr".to_string()
            }
        );
        assert_eq!(c.replace, span(6, 8));

        let c = ctx("{\"a\": {\"x\": |}}");
        assert_eq!(c.path, vec![key("a"), key("x")]);
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: String::new()
            }
        );
    }

    #[test]
    fn context_in_an_array() {
        let c = ctx("{\"tags\": [|]}");
        assert_eq!(c.path, vec![key("tags"), PathSeg::Index(0)]);
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: String::new()
            }
        );

        let c = ctx("{\"tags\": [\"a\", |]}");
        assert_eq!(c.path, vec![key("tags"), PathSeg::Index(1)]);
        assert!(!c.comma_before && !c.comma_after);

        let c = ctx("{\"tags\": [\"a\" |]}");
        assert!(c.comma_before);

        let c = ctx("{\"tags\": [| \"a\"]}");
        assert_eq!(c.path, vec![key("tags"), PathSeg::Index(0)]);
        assert!(c.comma_after);

        let c = ctx("{\"tags\": [\"a|\"]}");
        assert_eq!(c.path, vec![key("tags"), PathSeg::Index(0)]);
        assert_eq!(
            c.slot,
            Slot::Value {
                prefix: "\"a".to_string()
            }
        );
    }

    #[test]
    fn context_outside_the_document() {
        assert_eq!(ctx("|{}").slot, Slot::None);
        assert_eq!(ctx("{}|").slot, Slot::None);
        assert_eq!(ctx("|").slot, Slot::None);
    }

    #[test]
    fn spans_are_found_by_path() {
        let text = "{\"a\": {\"b\": [1, {\"c\": true}]}}";
        let parsed = parse(text);
        let root = parsed.root.as_ref().unwrap();
        let at = |path: &[PathSeg]| span_at(root, path);
        assert_eq!(at(&[]), Some((None, span(0, text.len()))));
        assert_eq!(at(&[key("a")]), Some((Some(span(1, 4)), span(6, 29))));
        assert_eq!(
            at(&[key("a"), key("b"), PathSeg::Index(0)]),
            Some((None, span(13, 14)))
        );
        assert_eq!(
            at(&[key("a"), key("b"), PathSeg::Index(1), key("c")]),
            Some((Some(span(17, 20)), span(22, 26)))
        );
        assert_eq!(at(&[key("zz")]), None);
        assert_eq!(at(&[key("a"), PathSeg::Index(0)]), None);
        assert_eq!(at(&[key("a"), key("b"), PathSeg::Index(5)]), None);
    }

    #[test]
    fn offsets_convert() {
        let text = "{\"ключ\": \"值\", \"e\": \"😀\"}";
        for (i, _) in text.char_indices() {
            assert_eq!(utf16_to_byte(text, byte_to_utf16(text, i)), i);
        }
        assert_eq!(
            byte_to_utf16(text, text.len()),
            text.encode_utf16().count() as u32
        );
        assert_eq!(utf16_to_byte(text, 1000), text.len());
        // Inside a multi-byte char rounds down to its start.
        assert_eq!(byte_to_utf16(text, 3), byte_to_utf16(text, 2));

        assert_eq!(line_col("ab\ncd", 4), (2, 2));
        assert_eq!(line_col("ab\ncd", 0), (1, 1));
        assert_eq!(line_col("é\n", 3), (2, 1));
        assert_eq!(line_col("éa", 3), (1, 3));

        let text = "  x\n    y";
        assert_eq!(line_indent(text, 9), "    ");
        assert_eq!(line_indent(text, 6), "  ");
        assert_eq!(line_indent(text, 4), "");
        assert_eq!(line_indent(text, 3), "  ");
    }
}
