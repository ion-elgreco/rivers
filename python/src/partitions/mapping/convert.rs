use std::collections::HashMap;
use std::ops::Deref;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::partitions::PyPartitionKey;
use crate::partitions::definition::PartitionsDefinition;
use crate::partitions::key_range::PyPartitionKeyRange;

use super::PartitionMapping;

/// Selector for matching partition keys: either an exact key or a range.
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionKeySelector {
    Key(PyPartitionKey),
    Range(PyPartitionKeyRange),
}

impl PartitionKeySelector {
    pub fn matches(&self, key: &PyPartitionKey, def: Option<&PartitionsDefinition>) -> bool {
        match self {
            Self::Key(k) => k == key,
            Self::Range(range) => range.contains(key, def),
        }
    }
}

impl<'py> FromPyObject<'py, '_> for PartitionKeySelector {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(key) = ob.extract::<PyPartitionKey>() {
            Ok(Self::Key(key))
        } else if let Ok(range) = ob.extract::<PyPartitionKeyRange>() {
            Ok(Self::Range(range))
        } else {
            Err(PyTypeError::new_err(
                "Expected PartitionKey or PartitionKeyRange for PartitionKeySelector",
            ))
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for PartitionKeySelector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Key(k) => Ok(k.into_pyobject(py)?.into_any()),
            Self::Range(r) => Ok(r.into_pyobject(py)?.into_any()),
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &PartitionKeySelector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.clone().into_pyobject(py)
    }
}

/// Newtype around `Box<PartitionMapping>` with manual `FromPyObject`/`IntoPyObject`.
#[derive(Clone, Debug)]
pub struct BoxedMapping(pub Box<PartitionMapping>);

impl PartialEq for BoxedMapping {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'py> FromPyObject<'py, '_> for BoxedMapping {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        let m = ob.extract::<PartitionMapping>()?;
        Ok(BoxedMapping(Box::new(m)))
    }
}

impl<'py> pyo3::IntoPyObject<'py> for BoxedMapping {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok((*self.0).into_pyobject(py)?.into_any())
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &BoxedMapping {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok((*self.0).clone().into_pyobject(py)?.into_any())
    }
}

/// A newtype for `HashMap<String, PartitionMapping>` that accepts keys as `str` or `AssetDef` from Python.
#[derive(Clone, Debug)]
pub struct PartitionMappingDict(pub HashMap<String, PartitionMapping>);

impl Deref for PartitionMappingDict {
    type Target = HashMap<String, PartitionMapping>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'py> pyo3::IntoPyObject<'py> for PartitionMappingDict {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}

impl<'py> pyo3::IntoPyObject<'py> for &PartitionMappingDict {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.clone().into_pyobject(py)
    }
}

impl FromPyObject<'_, '_> for PartitionMappingDict {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        // Iterate a copy: `name` can run user code, and other threads can change the dict.
        let dict = ob.cast::<PyDict>()?.copy()?;
        let mut map = HashMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key_str = if let Ok(s) = key.extract::<String>() {
                s
            } else if let Ok(name_attr) = key.getattr("name") {
                name_attr.extract::<String>().map_err(|_| {
                    PyTypeError::new_err(
                        "partition_mapping keys must be str or AssetDef (object with str 'name' attribute)",
                    )
                })?
            } else {
                return Err(PyTypeError::new_err(
                    "partition_mapping keys must be str or AssetDef",
                ));
            };
            let mapping = value.extract::<PartitionMapping>()?;
            map.insert(key_str, mapping);
        }
        Ok(PartitionMappingDict(map))
    }
}
