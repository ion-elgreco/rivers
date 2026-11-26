//! Migrations in a class-form asset body: `@rs.migration` classmethods,
//! `AssetMigration` attributes and the `migrations = [...]` list.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use super::{definition_error, is_classmethod_or_static, marker, marker_meta, type_name, user_mro};
use crate::assets::migration::{PyAssetMigration, start_at_ns};

const MIGRATION_META_ATTR: &str = "__rivers_migration_meta__";

/// Mark a classmethod in an asset class body as the function of a migration::
///
///     @rs.migration(to=2, breaking=False)
///     @classmethod
///     def add_currency(cls, ctx, warehouse) -> None: ...
#[pyfunction]
#[pyo3(signature = (*, to, breaking=true, retry=None, description=None, start_at=None))]
pub(crate) fn migration(
    py: Python<'_>,
    to: i64,
    breaking: bool,
    retry: Option<Py<PyAny>>,
    description: Option<String>,
    start_at: Option<Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    if let Some(at) = &start_at {
        start_at_ns(to, at)?;
    }
    let meta = PyDict::new(py);
    meta.set_item("to", to)?;
    meta.set_item("breaking", breaking)?;
    meta.set_item("retry", retry)?;
    meta.set_item("description", description)?;
    meta.set_item("start_at", start_at)?;
    marker(py, MIGRATION_META_ATTR, meta.unbind())
}

enum MigrationEntry<'py> {
    Marker(Bound<'py, PyAny>),
    Ready(Bound<'py, PyAny>),
}

fn migration_to(val: &Bound<'_, PyAny>) -> Option<i64> {
    val.cast::<PyAssetMigration>().ok().map(|m| m.get().to)
}

/// Migrations across the MRO: @rs.migration-marked classmethods,
/// AssetMigration attributes and `migrations = [...]` entries. Per version,
/// the most-derived class that declares it wins; shadowing an inherited
/// migration with another attribute is an error.
pub(super) fn collect_migrations<'py>(
    cls: &Bound<'py, PyType>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let py = cls.py();
    let mro = user_mro(cls)?;
    // (attr, version, index in `mro` of the class that defines attr, entry)
    let mut entries: Vec<(String, i64, usize, MigrationEntry<'py>)> = Vec::new();
    for (depth, klass) in mro.iter().enumerate().rev() {
        for item in klass
            .getattr("__dict__")?
            .call_method0("items")?
            .try_iter()?
        {
            let (attr, val): (String, Bound<PyAny>) = item?.extract()?;
            let (to, entry) = if let Some(meta) = marker_meta(&val, MIGRATION_META_ATTR)? {
                if !is_classmethod_or_static(&val)? {
                    return Err(definition_error(format!(
                        "{}.{attr}: @rs.migration bodies must be defined with @classmethod",
                        type_name(cls)?
                    )));
                }
                (
                    meta.get_item("to")?.extract()?,
                    MigrationEntry::Marker(meta),
                )
            } else if let Some(to) = migration_to(&val) {
                (to, MigrationEntry::Ready(val))
            } else {
                if entries.iter().any(|(k, ..)| k == &attr) && !attr.starts_with("__") {
                    return Err(definition_error(format!(
                        "{}.{attr} shadows the inherited migration '{attr}' with another \
                         attribute; override it with @rs.migration or rename the attribute",
                        type_name(cls)?
                    )));
                }
                continue;
            };
            match entries.iter_mut().find(|(k, ..)| k == &attr) {
                Some(slot) => *slot = (attr, to, depth, entry),
                None => entries.push((attr, to, depth, entry)),
            }
        }
    }

    // `migrations = [...]` mirrors the decorator's `migrations=` argument; the
    // most-derived class that declares it wins, like any other config attr.
    let mut list_depth = 0;
    let mut list_val = None;
    for (depth, klass) in mro.iter().enumerate() {
        let ns = klass.getattr("__dict__")?;
        if ns.contains("migrations")? {
            list_depth = depth;
            list_val = Some(ns.get_item("migrations")?);
            break;
        }
    }
    let mut listed: Vec<(i64, Bound<'py, PyAny>)> = Vec::new();
    if let Some(val) = list_val
        && !val.is_none()
    {
        for item in val.try_iter().map_err(|_| {
            definition_error(format!(
                "{}.migrations must be a list of AssetMigration objects",
                type_name(cls).unwrap_or_default()
            ))
        })? {
            let item = item?;
            let Some(to) = migration_to(&item) else {
                return Err(definition_error(format!(
                    "{}.migrations must contain AssetMigration objects, got {}",
                    type_name(cls)?,
                    item.repr()?
                )));
            };
            listed.push((to, item));
        }
    }

    // Per version, the most-derived class that declares it wins, in its
    // `migrations` list or as an attribute.
    let nearest = |to: i64| {
        let in_list = listed.iter().filter(|(v, _)| *v == to).map(|_| list_depth);
        let as_attr = entries
            .iter()
            .filter(|(_, v, ..)| *v == to)
            .map(|(_, _, d, _)| *d);
        in_list.chain(as_attr).min()
    };
    let mut built = Vec::with_capacity(entries.len() + listed.len());
    for (to, item) in &listed {
        if nearest(*to) == Some(list_depth) {
            built.push(item.clone());
        }
    }
    for (attr, to, depth, entry) in &entries {
        if nearest(*to) != Some(*depth) {
            continue;
        }
        if *depth == list_depth
            && let Some((_, item)) = listed.iter().find(|(v, _)| v == to)
        {
            if matches!(entry, MigrationEntry::Ready(v) if v.is(item)) {
                continue;
            }
            return Err(definition_error(format!(
                "{0}.{attr} and {0}.migrations both declare the migration to version {to}; \
                 keep one",
                type_name(&mro[*depth])?
            )));
        }
        match entry {
            MigrationEntry::Ready(v) => built.push(v.clone()),
            MigrationEntry::Marker(meta) => {
                let kwargs = PyDict::new(py);
                for key in ["breaking", "retry", "description", "start_at"] {
                    kwargs.set_item(key, meta.get_item(key)?)?;
                }
                let m = py
                    .get_type::<PyAssetMigration>()
                    .call((to,), Some(&kwargs))?;
                built.push(m.call1((cls.getattr(attr.as_str())?,))?);
            }
        }
    }
    Ok(built)
}
