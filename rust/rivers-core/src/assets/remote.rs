//! References from one code location to an asset owned by another.

use std::fmt;

use anyhow::{bail, ensure};
use serde::{Deserialize, Serialize};

/// A code location named `namespace/name`.
///
/// Serialized as the `"namespace/name"` string. The storage identity behind
/// it is looked up in the code location directory, never stored here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LocationRef {
    namespace: String,
    name: String,
}

impl LocationRef {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> anyhow::Result<Self> {
        let namespace = namespace.into();
        let name = name.into();
        validate_segment("namespace", &namespace)?;
        validate_segment("name", &name)?;
        Ok(Self { namespace, name })
    }

    /// Parse `"name"` (placed in `default_namespace`) or `"namespace/name"`.
    pub fn parse(spec: &str, default_namespace: &str) -> anyhow::Result<Self> {
        match spec.split_once('/') {
            Some((namespace, name)) => Self::new(namespace, name),
            None => Self::new(default_namespace, spec),
        }
        .map_err(|e| anyhow::anyhow!("invalid code location '{spec}': {e}"))
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Check one part (`what`: namespace or name) of a `namespace/name`.
pub fn validate_segment(what: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.is_empty(), "{what} is empty");
    if let Some(c) = value
        .chars()
        .find(|c| *c == '/' || c.is_whitespace() || c.is_control())
    {
        bail!("{what} '{value}' contains {c:?}");
    }
    Ok(())
}

impl fmt::Display for LocationRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

impl TryFrom<String> for LocationRef {
    type Error = anyhow::Error;

    fn try_from(value: String) -> anyhow::Result<Self> {
        let Some((namespace, name)) = value.split_once('/') else {
            bail!("code location '{value}' is not 'namespace/name'");
        };
        Self::new(namespace, name)
    }
}

impl From<LocationRef> for String {
    fn from(value: LocationRef) -> Self {
        value.to_string()
    }
}

/// An asset owned by another code location, as seen from the consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteRef {
    pub code_location: LocationRef,
    /// The asset's key in the owning location.
    pub asset_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bare_name_uses_default_namespace() {
        let loc = LocationRef::parse("warehouse", "dev").unwrap();
        assert_eq!(loc.namespace(), "dev");
        assert_eq!(loc.name(), "warehouse");
        assert_eq!(loc.to_string(), "dev/warehouse");
    }

    #[test]
    fn parse_qualified_name_keeps_namespace() {
        let loc = LocationRef::parse("data-platform/warehouse", "dev").unwrap();
        assert_eq!(loc.namespace(), "data-platform");
        assert_eq!(loc.name(), "warehouse");
    }

    #[test]
    fn parse_rejects_malformed_specs() {
        for spec in ["", "/x", "x/", "a/b/c", "a b", "ns/a\tb"] {
            let err = LocationRef::parse(spec, "dev").unwrap_err().to_string();
            assert!(
                err.starts_with(&format!("invalid code location '{spec}'")),
                "{spec:?}: {err}"
            );
        }
    }

    #[test]
    fn parse_rejects_empty_default_namespace() {
        assert!(LocationRef::parse("warehouse", "").is_err());
    }

    #[test]
    fn serializes_as_qualified_string() {
        let loc = LocationRef::new("team", "wh").unwrap();
        assert_eq!(serde_json::to_string(&loc).unwrap(), r#""team/wh""#);
        let back: LocationRef = serde_json::from_str(r#""team/wh""#).unwrap();
        assert_eq!(back, loc);
    }

    #[test]
    fn deserialize_rejects_unqualified_string() {
        let err = serde_json::from_str::<LocationRef>(r#""wh""#).unwrap_err();
        assert!(err.to_string().contains("is not 'namespace/name'"), "{err}");
    }

    #[test]
    fn remote_ref_round_trips() {
        let remote = RemoteRef {
            code_location: LocationRef::new("team", "wh").unwrap(),
            asset_key: "raw_orders".into(),
        };
        let json = serde_json::to_string(&remote).unwrap();
        assert_eq!(
            json,
            r#"{"code_location":"team/wh","asset_key":"raw_orders"}"#
        );
        assert_eq!(serde_json::from_str::<RemoteRef>(&json).unwrap(), remote);
    }
}
