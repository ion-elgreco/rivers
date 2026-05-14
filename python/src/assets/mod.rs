//! Asset types: single assets, multi-assets, graph assets, external assets, and IO handlers.
pub mod decorator;
pub mod dep_def;
pub mod external_asset;
pub mod graph_asset;
pub mod io_handler;
pub mod io_handler_registry;
pub mod multi_asset;
pub mod self_dependency;
pub mod single_asset;

use decorator::{AssetDef, PyAsset};
use dep_def::DepDef;
use external_asset::PyExternalAsset;
use graph_asset::PyGraphAsset;
use multi_asset::PyMultiAsset;
use self_dependency::PySelfDependency;
use single_asset::PySingleAsset;

use crate::context::asset::PyAssetExecutionContext;

use pyo3::prelude::*;

pub fn register_asset_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "assets", [
        AssetDef as "AssetDef",
        DepDef as "DepDef",
        PyAsset as "Asset",
        PySingleAsset as "SingleAsset",
        PyMultiAsset as "MultiAsset",
        PyGraphAsset as "GraphAsset",
        PyExternalAsset as "ExternalAsset",
        PySelfDependency as "SelfDependency",
        PyAssetExecutionContext as "AssetExecutionContext",
    ])
}
