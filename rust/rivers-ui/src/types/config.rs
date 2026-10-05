use serde::{Deserialize, Serialize};

/// A resource a launch document may override: its key and the JSON schema
/// of its class (an instance's current values as the defaults).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceInfo {
    pub key: String,
    pub config_schema: String,
}

/// One step of pydantic's `loc`: a field name or a list index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigLoc {
    Key(String),
    Index(u32),
}

/// One error the definitions found in a launch document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigError {
    /// Where in the document the checked object sits, e.g.
    /// `["assets", "raw_users", "config"]`.
    pub path: Vec<String>,
    /// pydantic's `loc` inside that object; empty for the object itself.
    pub loc: Vec<ConfigLoc>,
    pub message: String,
    /// pydantic's error type (`missing`, `int_parsing`, `value_error`, ...),
    /// `required` for a plain model's field left unset, `exception`, or
    /// `invalid` for a part the definitions refuse.
    pub kind: String,
}
