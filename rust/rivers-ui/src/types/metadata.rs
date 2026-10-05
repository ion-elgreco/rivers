use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataDisplay {
    Text(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Url {
        text: String,
        url: String,
    },
    Path(String),
    Json(String),
    Markdown(String),
    CodeBlock {
        code: String,
        language: Option<String>,
    },
    Sql(String),
    Image(String),
    Timestamp(i64),
    Duration(f64),
    DateRange {
        start: i64,
        end: i64,
    },
    Bytes(u64),
    Percentage(f64),
    Schema(Vec<(String, String)>),
    DataVersion(String),
    Null,
}

impl MetadataDisplay {
    /// Deserialize a JSON-encoded MetadataValue from storage.
    /// Falls back to `Text` for internal events (errors, log output) stored as plain strings.
    pub fn from_stored(raw: &str) -> Self {
        serde_json::from_str::<StoredMetadataValue>(raw)
            .map(Into::into)
            .unwrap_or_else(|_| Self::Text(raw.to_string()))
    }

    /// Extract a plain text representation (for log output, error messages, etc.).
    pub fn as_text(&self) -> String {
        match self {
            Self::Text(s)
            | Self::Path(s)
            | Self::Json(s)
            | Self::Markdown(s)
            | Self::Sql(s)
            | Self::Image(s)
            | Self::DataVersion(s) => s.clone(),
            Self::Url { text, .. } => text.clone(),
            Self::CodeBlock { code, .. } => code.clone(),
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format!("{n}"),
            Self::Bool(b) => b.to_string(),
            Self::Timestamp(t) => t.to_string(),
            Self::Duration(d) => format!("{d}s"),
            Self::DateRange { start, end } => format!("{start} - {end}"),
            Self::Bytes(b) => b.to_string(),
            Self::Percentage(p) => format!("{:.1}%", p * 100.0),
            Self::Schema(cols) => cols
                .iter()
                .map(|(n, t)| format!("{n}: {t}"))
                .collect::<Vec<_>>()
                .join(", "),
            Self::Null => String::new(),
        }
    }
}

/// Mirror of Python's `MetadataValue` serde layout for deserialization.
#[derive(Deserialize)]
enum StoredMetadataValue {
    Text {
        value: String,
    },
    Int {
        value: i64,
    },
    Float {
        value: f64,
    },
    Bool {
        value: bool,
    },
    Url {
        value: String,
    },
    Path {
        value: String,
    },
    Json {
        value: String,
    },
    Markdown {
        value: String,
    },
    Timestamp {
        value: f64,
    },
    Null {},
    Bytes {
        value: u64,
    },
    Duration {
        value: f64,
    },
    Sql {
        query: String,
        #[allow(dead_code)]
        dialect: Option<String>,
    },
    CodeBlock {
        code: String,
        language: Option<String>,
    },
    Image {
        value: String,
    },
    Percentage {
        value: f64,
    },
    List {
        values: Vec<StoredMetadataValue>,
    },
    DateRange {
        start: String,
        end: String,
    },
    Schema {
        #[allow(dead_code)]
        ipc_bytes: Vec<u8>,
    },
    DataVersion {
        value: String,
    },
}

/// Nanoseconds for a stored `DateRange` bound. The bounds are serialized
/// wall-clock datetimes with no zone, so they are read as UTC.
fn wall_nanos(wall: &str) -> i64 {
    jiff::civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", wall)
        .and_then(|d| d.to_zoned(jiff::tz::TimeZone::UTC))
        .map(|z| z.timestamp().as_nanosecond() as i64)
        .unwrap_or(0)
}

impl From<StoredMetadataValue> for MetadataDisplay {
    fn from(v: StoredMetadataValue) -> Self {
        match v {
            StoredMetadataValue::Text { value } => Self::Text(value),
            StoredMetadataValue::Int { value } => Self::Int(value),
            StoredMetadataValue::Float { value } => Self::Float(value),
            StoredMetadataValue::Bool { value } => Self::Bool(value),
            StoredMetadataValue::Url { value } => Self::Url {
                text: value.clone(),
                url: value,
            },
            StoredMetadataValue::Path { value } => Self::Path(value),
            StoredMetadataValue::Json { value } => Self::Json(value),
            StoredMetadataValue::Markdown { value } => Self::Markdown(value),
            StoredMetadataValue::Timestamp { value } => Self::Timestamp((value * 1e9) as i64),
            StoredMetadataValue::Null {} => Self::Null,
            StoredMetadataValue::Bytes { value } => Self::Bytes(value),
            StoredMetadataValue::Duration { value } => Self::Duration(value),
            StoredMetadataValue::Sql { query, .. } => Self::Sql(query),
            StoredMetadataValue::CodeBlock { code, language } => Self::CodeBlock { code, language },
            StoredMetadataValue::Image { value } => Self::Image(value),
            StoredMetadataValue::Percentage { value } => Self::Percentage(value),
            StoredMetadataValue::List { values } => {
                let text = values
                    .into_iter()
                    .map(|v| MetadataDisplay::from(v).as_text())
                    .collect::<Vec<_>>()
                    .join(", ");
                Self::Text(format!("[{text}]"))
            }
            StoredMetadataValue::DateRange { start, end } => {
                let s = wall_nanos(&start);
                let e = wall_nanos(&end);
                Self::DateRange { start: s, end: e }
            }
            StoredMetadataValue::Schema { ipc_bytes: _ } => {
                // Arrow IPC decoding requires arrow-ipc, which is SSR-only.
                Self::Text("[Arrow Schema]".to_string())
            }
            StoredMetadataValue::DataVersion { value } => Self::DataVersion(value),
        }
    }
}
