//! Remote asset — an asset owned by another code location, depended on here.
use std::collections::HashMap;

use pyo3::prelude::*;

use super::decorator::{Asset, PyAsset};
use super::io_handler::IOHandler;
use crate::errors::AssetDefinitionError;
use crate::partitions::{PartitionsDefRef, PartitionsDefinition};

pub struct RemoteAsset {
    /// The local name: unique in this repository, and the name parameters,
    /// `deps=` and `partition_mapping` keys use.
    pub name: Option<String>,
    /// The owning code location as declared: `"name"` or `"namespace/name"`.
    pub code_location: String,
    /// The asset's key in the owning code location.
    pub asset_key: String,
    pub io_handler: Option<IOHandler>,
    /// Mirror of the owner's partitions definition.
    pub partitions_def: Option<PartitionsDefRef>,
    pub metadata: Option<HashMap<String, String>>,
    pub group: Option<String>,
}

/// Dynamic keys live in the owner's scope, so a mirror cannot list them.
pub(crate) fn reject_dynamic_mirror(name: &str, pd: &PartitionsDefinition) -> PyResult<()> {
    if pd.has_dynamic_dimension() {
        return Err(AssetDefinitionError::new_err(format!(
            "remote asset '{name}': a dynamic partitions definition cannot be mirrored; \
             its keys live in the owning code location"
        )));
    }
    Ok(())
}

/// The code location that the remote asset `name` declares; one without a
/// namespace is in `namespace`.
pub(crate) fn parse_code_location(
    name: &str,
    code_location: &str,
    namespace: &str,
) -> PyResult<rivers_core::assets::remote::LocationRef> {
    rivers_core::assets::remote::LocationRef::parse(code_location, namespace)
        .map_err(|e| AssetDefinitionError::new_err(format!("remote asset '{name}': {e}")))
}

/// Python-exposed marker subclass created via `Asset.remote(...)`.
#[pyclass(name = "RemoteAsset", extends=PyAsset, frozen, module = "rivers._core")]
pub struct PyRemoteAsset;

impl PyRemoteAsset {
    fn remote<'a>(slf: &'a Bound<'_, Self>) -> &'a RemoteAsset {
        match slf.as_super().get().inner() {
            Asset::Remote(r) => r,
            _ => unreachable!("RemoteAsset wraps Asset::Remote"),
        }
    }
}

#[pymethods]
impl PyRemoteAsset {
    /// The owning code location as declared: `"name"` or `"namespace/name"`.
    #[getter]
    fn code_location(slf: &Bound<'_, Self>) -> String {
        Self::remote(slf).code_location.clone()
    }

    /// The asset's key in the owning code location.
    #[getter]
    fn asset_key(slf: &Bound<'_, Self>) -> String {
        Self::remote(slf).asset_key.clone()
    }
}
