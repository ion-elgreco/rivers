//! Class-form asset definitions — registration-time desugar.
//!
//! An asset class subclasses one of the four bases (`rs.Asset`, `rs.MultiAsset`,
//! `rs.GraphAsset`, `rs.ExternalAsset`) and lists its verbs in the class body.
//! `desugar` translates that body into the exact decorator-form calls — one
//! resolution path, one executor. Mixins are plain classes never listed in
//! `assets`. This module must never grow execution knowledge.

use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyType};

use crate::errors::AssetDefinitionError;

use super::action::PyAssetAction;
use super::decorator::{AssetDef, PyAsset};
use super::external_asset::PyExternalAsset;
use super::graph_asset::PyGraphAsset;
use super::multi_asset::PyMultiAsset;
use super::single_asset::PySingleAsset;

const ACTION_META_ATTR: &str = "__rivers_action_meta__";

// Computed by desugar; every other constructor keyword is forwarded 1:1 from a
// class attribute of the same name.
const NON_CONFIG_PARAMS: &[&str] = &["cls", "wraps", "name", "output_defs", "actions"];

/// Forwardable config keys, derived from the constructor's own signature so
/// new decorator parameters reach the class form without a list to maintain.
fn config_keys(ctor: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let py = ctor.py();
    let sig = py.import("inspect")?.call_method1("signature", (ctor,))?;
    let mut keys = Vec::new();
    for name in sig.getattr("parameters")?.try_iter()? {
        let name: String = name?.extract()?;
        if !NON_CONFIG_PARAMS.contains(&name.as_str()) {
            keys.push(name);
        }
    }
    Ok(keys)
}

fn definition_error(msg: String) -> PyErr {
    AssetDefinitionError::new_err(msg)
}

fn snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let boundary = prev.is_ascii_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_ascii_uppercase()
                    && chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase()));
            if boundary {
                out.push('_');
            }
        }
        out.extend(c.to_lowercase());
    }
    out
}

fn type_name(cls: &Bound<'_, PyType>) -> PyResult<String> {
    cls.getattr("__name__")?.extract()
}

fn is_rs_base(t: &Bound<'_, PyType>) -> bool {
    let py = t.py();
    t.is(&py.get_type::<PyAsset>())
        || t.is(&py.get_type::<PySingleAsset>())
        || t.is(&py.get_type::<PyMultiAsset>())
        || t.is(&py.get_type::<PyGraphAsset>())
        || t.is(&py.get_type::<PyExternalAsset>())
}

fn is_classmethod_or_static(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let builtins = obj.py().import("builtins")?;
    Ok(obj.is_instance(&builtins.getattr("classmethod")?)?
        || obj.is_instance(&builtins.getattr("staticmethod")?)?)
}

/// MRO classes the user wrote, most-derived first.
///
/// The rivers bases expose getset descriptors named like config attributes
/// (`name`, `io_handler`, ...), so collection must read `__dict__` of the
/// user's classes only — never `getattr` on the class.
fn user_mro<'py>(cls: &Bound<'py, PyType>) -> PyResult<Vec<Bound<'py, PyType>>> {
    let py = cls.py();
    let object_ty = py.import("builtins")?.getattr("object")?;
    let mut out = Vec::new();
    for k in cls.getattr("__mro__")?.try_iter()? {
        let k = k?;
        if k.is(&object_ty) {
            continue;
        }
        let kt = k.cast_into::<PyType>()?;
        if is_rs_base(&kt) {
            continue;
        }
        out.push(kt);
    }
    Ok(out)
}

/// Every key some asset base can carry, derived from the four constructors —
/// what a class body may legally declare. An attribute in this set that the
/// *target* base can't take is a definition error rather than a silent drop.
fn declarable_keys(py: Python<'_>) -> PyResult<Vec<String>> {
    let asset = py.get_type::<PyAsset>();
    let mut keys = config_keys(asset.as_any())?;
    for name in ["from_multi", "from_graph", "external"] {
        for key in config_keys(&asset.getattr(name)?)? {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    Ok(keys)
}

/// Near-misses of a real declaration. A class body accepts arbitrary user
/// attributes, so an unrecognized name can't be rejected on sight — but these
/// read as configuration and would otherwise be silently dropped, leaving the
/// asset built differently than the class body says. The decorator form raises
/// TypeError for the same typo.
///
/// Only plain data values are checked; see [`is_config_shaped`].
const MISSPELLED_KEYS: [(&str, &str); 8] = [
    ("automation", "automation_condition"),
    ("backfill", "backfill_strategy"),
    ("kind", "kinds"),
    ("partition_def", "partitions_def"),
    ("partitions", "partitions_def"),
    ("pools", "pool"),
    ("retries", "retry"),
    ("tag", "tags"),
];

/// Whether a near-miss attribute could plausibly have been meant as config.
/// A `MultiAsset` output, an action verb and a method are all legitimate uses
/// of these names — only a plain data value reads as a mistyped declaration.
fn is_config_shaped(val: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(!val.is_instance_of::<AssetDef>()
        && action_meta(val)?.is_none()
        && !is_classmethod_or_static(val)?
        && !val.is_callable())
}

fn collect_config<'py>(
    cls: &Bound<'py, PyType>,
    ctor: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let py = cls.py();
    let keys = config_keys(ctor)?;
    let unsupported: Vec<String> = declarable_keys(py)?
        .into_iter()
        .filter(|k| !keys.contains(k))
        .collect();
    let cfg = PyDict::new(py);
    for klass in user_mro(cls)? {
        let ns = klass.getattr("__dict__")?;
        for key in &unsupported {
            if ns.contains(key)? && !ns.get_item(key)?.is_none() {
                return Err(definition_error(format!(
                    "{}.{key}: this asset kind takes no '{key}' — remove it or \
                     move the output to a kind that supports it",
                    type_name(cls)?
                )));
            }
        }
        for (wrong, right) in MISSPELLED_KEYS {
            if ns.contains(wrong)?
                && !ns.get_item(wrong)?.is_none()
                && is_config_shaped(&ns.get_item(wrong)?)?
            {
                return Err(definition_error(format!(
                    "{}.{wrong}: did you mean '{right}'? An unrecognized name \
                     is kept as a plain attribute, so this would be dropped",
                    type_name(cls)?
                )));
            }
        }
        for key in &keys {
            if !cfg.contains(key)? && ns.contains(key)? {
                cfg.set_item(key, ns.get_item(key)?)?;
            }
        }
    }
    let out = PyDict::new(py);
    for (k, v) in cfg.iter() {
        if !v.is_none() {
            out.set_item(k, v)?;
        }
    }
    Ok(out)
}

fn class_attr<'py>(cls: &Bound<'py, PyType>, key: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    for klass in user_mro(cls)? {
        let ns = klass.getattr("__dict__")?;
        if ns.contains(key)? {
            return Ok(Some(ns.get_item(key)?));
        }
    }
    Ok(None)
}

/// The user class that defines `verb`, or None.
fn find_verb<'py>(cls: &Bound<'py, PyType>, verb: &str) -> PyResult<Option<Bound<'py, PyType>>> {
    for klass in user_mro(cls)? {
        if klass.getattr("__dict__")?.contains(verb)? {
            return Ok(Some(klass));
        }
    }
    Ok(None)
}

fn bound_verb<'py>(
    cls: &Bound<'py, PyType>,
    verb: &str,
    defined_on: &Bound<'py, PyType>,
) -> PyResult<Bound<'py, PyAny>> {
    let raw = defined_on.getattr("__dict__")?.get_item(verb)?;
    if !is_classmethod_or_static(&raw)? {
        return Err(definition_error(format!(
            "{}.{verb} must be defined with @classmethod \
             (a plain method's first parameter would be read as a dependency)",
            type_name(cls)?
        )));
    }
    cls.getattr(verb)
}

fn asset_name(cls: &Bound<'_, PyType>) -> PyResult<String> {
    if let Some(v) = class_attr(cls, "name")?
        && v.is_truthy()?
    {
        return v.extract();
    }
    Ok(snake(&type_name(cls)?))
}

/// Mark a classmethod in an asset class body as an action.
///
/// The action name defaults to the method name::
///
///     @rs.action(outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive)
///     @classmethod
///     def optimize(cls, ctx) -> None: ...
#[pyfunction]
#[pyo3(signature = (*, outcome, concurrency=None, ordering=None, retry=None, description=None, name=None))]
pub(crate) fn action(
    py: Python<'_>,
    outcome: Py<PyAny>,
    concurrency: Option<Py<PyAny>>,
    ordering: Option<Py<PyAny>>,
    retry: Option<Py<PyAny>>,
    description: Option<Py<PyAny>>,
    name: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let meta = PyDict::new(py);
    meta.set_item("name", name)?;
    meta.set_item("outcome", outcome)?;
    meta.set_item("concurrency", concurrency)?;
    meta.set_item("ordering", ordering)?;
    meta.set_item("retry", retry)?;
    meta.set_item("description", description)?;
    let meta: Py<PyDict> = meta.unbind();

    let deco = PyCFunction::new_closure(py, None, None, move |args, _kwargs| {
        let py = args.py();
        let f = args.get_item(0)?;
        let target = if is_classmethod_or_static(&f)? {
            f.getattr("__func__")?
        } else {
            f.clone()
        };
        target.setattr(ACTION_META_ATTR, meta.bind(py))?;
        Ok::<Py<PyAny>, PyErr>(f.unbind())
    })?;
    Ok(deco.into_any().unbind())
}

/// The @rs.action metadata on a class-body attribute, or None.
fn action_meta<'py>(val: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    let f = val.getattr("__func__").unwrap_or_else(|_| val.clone());
    match f.getattr(ACTION_META_ATTR) {
        Ok(meta) => Ok(Some(meta)),
        Err(_) => Ok(None),
    }
}

enum ActionEntry<'py> {
    Marker(Bound<'py, PyAny>),
    Ready(Bound<'py, PyAny>),
}

/// Actions across the MRO: @rs.action-marked classmethods and AssetAction
/// attributes; a subclass override replaces the inherited entry, and
/// shadowing an inherited action with a non-action attribute is an error.
fn collect_actions<'py>(cls: &Bound<'py, PyType>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let py = cls.py();
    let mut entries: Vec<(String, ActionEntry<'py>)> = Vec::new();
    let upsert = |entries: &mut Vec<(String, ActionEntry<'py>)>, attr: &str, e| match entries
        .iter_mut()
        .find(|(k, _)| k == attr)
    {
        Some(slot) => slot.1 = e,
        None => entries.push((attr.to_string(), e)),
    };
    for klass in user_mro(cls)?.iter().rev() {
        for item in klass
            .getattr("__dict__")?
            .call_method0("items")?
            .try_iter()?
        {
            let (attr, val): (String, Bound<PyAny>) = item?.extract()?;
            if let Some(meta) = action_meta(&val)? {
                if !is_classmethod_or_static(&val)? {
                    return Err(definition_error(format!(
                        "{}.{attr}: @rs.action bodies must be defined with @classmethod",
                        type_name(cls)?
                    )));
                }
                upsert(&mut entries, &attr, ActionEntry::Marker(meta));
            } else if val.is_instance_of::<PyAssetAction>() {
                upsert(&mut entries, &attr, ActionEntry::Ready(val));
            } else if entries.iter().any(|(k, _)| k == &attr) && !attr.starts_with("__") {
                return Err(definition_error(format!(
                    "{}.{attr} shadows the inherited action '{attr}' with a non-action \
                     attribute; override it with @rs.action or rename the attribute",
                    type_name(cls)?
                )));
            }
        }
    }

    // `actions = [...]` mirrors the decorator's `actions=` argument; the
    // most-derived class that declares it wins, like any other config attr.
    let mut listed: Vec<Bound<'py, PyAny>> = Vec::new();
    if let Some(val) = class_attr(cls, "actions")?
        && !val.is_none()
    {
        for item in val.try_iter().map_err(|_| {
            definition_error(format!(
                "{}.actions must be a list of AssetAction objects",
                type_name(cls).unwrap_or_default()
            ))
        })? {
            let item = item?;
            if !item.is_instance_of::<PyAssetAction>() {
                return Err(definition_error(format!(
                    "{}.actions must contain AssetAction objects, got {}",
                    type_name(cls)?,
                    item.repr()?
                )));
            }
            listed.push(item);
        }
    }

    let mut built = Vec::with_capacity(entries.len() + listed.len());
    built.extend(listed);
    for (attr, entry) in entries {
        match entry {
            ActionEntry::Ready(v) => built.push(v),
            ActionEntry::Marker(meta) => {
                let kwargs = PyDict::new(py);
                let name_v = meta.get_item("name")?;
                if name_v.is_truthy()? {
                    kwargs.set_item("name", name_v)?;
                } else {
                    kwargs.set_item("name", &attr)?;
                }
                for key in ["outcome", "concurrency", "ordering", "retry", "description"] {
                    kwargs.set_item(key, meta.get_item(key)?)?;
                }
                let act = py.get_type::<PyAssetAction>().call((), Some(&kwargs))?;
                built.push(act.call1((cls.getattr(attr.as_str())?,))?);
            }
        }
    }
    Ok(built)
}

/// True for a user-defined subclass of one of the four asset bases.
#[pyfunction]
pub(crate) fn is_asset_class(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let Ok(t) = obj.cast::<PyType>() else {
        return Ok(false);
    };
    Ok(t.is_subclass_of::<PyAsset>()? && !is_rs_base(t))
}

/// Graph node names a class-form asset registers under (Job selection).
#[pyfunction]
pub(crate) fn node_names(cls: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if !is_asset_class(cls)? {
        return Err(definition_error(format!(
            "expected an Asset subclass, got {}",
            cls.repr()?
        )));
    }
    let t = cls.cast::<PyType>()?;
    if t.is_subclass_of::<PyMultiAsset>()? {
        let defs = asset_def_attrs(t)?;
        if defs.is_empty() {
            return Err(definition_error(format!(
                "{} (MultiAsset subclass) defines no AssetDef attributes",
                type_name(t)?
            )));
        }
        return Ok(defs
            .iter()
            .map(|(attr, ad)| ad.borrow().name.clone().unwrap_or_else(|| attr.clone()))
            .collect());
    }
    Ok(vec![asset_name(t)?])
}

/// Translate a class-form asset into the equivalent decorator-form object.
///
/// Called by `CodeRepository` for each type in `assets=[...]`.
#[pyfunction]
pub(crate) fn desugar(cls: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let not_a_subclass = || {
        definition_error(format!(
            "desugar expects an Asset subclass, got {}",
            cls.repr().map(|r| r.to_string()).unwrap_or_default()
        ))
    };
    let t = cls.cast::<PyType>().map_err(|_| not_a_subclass())?;
    if !t.is_subclass_of::<PyAsset>()? {
        return Err(not_a_subclass());
    }
    if is_rs_base(t) {
        return Err(definition_error(format!(
            "cannot register the rivers base class {} itself; \
             subclass it and define its verbs",
            type_name(t)?
        )));
    }
    if t.is_subclass_of::<PyMultiAsset>()? {
        desugar_multi(t)
    } else if t.is_subclass_of::<PyGraphAsset>()? {
        desugar_graph(t)
    } else if t.is_subclass_of::<PyExternalAsset>()? {
        desugar_external(t)
    } else {
        desugar_single(t)
    }
}

fn require_verb<'py>(
    cls: &Bound<'py, PyType>,
    verb: &str,
    base_name: &str,
) -> PyResult<Bound<'py, PyType>> {
    match find_verb(cls, verb)? {
        Some(defined_on) => Ok(defined_on),
        None => {
            let hint = match verb {
                "materialize" => {
                    "a class with no executable verb is a mixin — leave it out of assets=[...]"
                }
                "compose" => {
                    "GraphAsset subclasses compose other assets in a compose() classmethod"
                }
                _ => "ExternalAsset subclasses may define observe() to track freshness",
            };
            Err(definition_error(format!(
                "{} (subclass of {base_name}) defines no {verb}(); {hint}",
                type_name(cls)?
            )))
        }
    }
}

fn reject_verb(cls: &Bound<'_, PyType>, verb: &str, why: &str) -> PyResult<()> {
    if find_verb(cls, verb)?.is_some() {
        return Err(definition_error(format!(
            "{} defines {verb}(), but {why}",
            type_name(cls)?
        )));
    }
    Ok(())
}

/// attr name → AssetDef, base classes first, subclass overrides in place.
fn asset_def_attrs<'py>(cls: &Bound<'py, PyType>) -> PyResult<Vec<(String, Bound<'py, AssetDef>)>> {
    let mut defs: Vec<(String, Bound<AssetDef>)> = Vec::new();
    for klass in user_mro(cls)?.iter().rev() {
        for item in klass
            .getattr("__dict__")?
            .call_method0("items")?
            .try_iter()?
        {
            let (attr, val): (String, Bound<PyAny>) = item?.extract()?;
            if let Ok(ad) = val.cast_into::<AssetDef>() {
                match defs.iter_mut().find(|(k, _)| k == &attr) {
                    Some(slot) => slot.1 = ad,
                    None => defs.push((attr, ad)),
                }
            }
        }
    }
    let mut seen: Vec<(usize, &str)> = Vec::new();
    for (attr, ad) in &defs {
        let ptr = ad.as_ptr() as usize;
        if let Some((_, prev)) = seen.iter().find(|(p, _)| *p == ptr) {
            return Err(definition_error(format!(
                "{}: attributes '{prev}' and '{attr}' share one AssetDef instance; \
                 each output needs its own",
                type_name(cls)?
            )));
        }
        seen.push((ptr, attr));
    }
    Ok(defs)
}

fn desugar_single(cls: &Bound<'_, PyType>) -> PyResult<Py<PyAny>> {
    let py = cls.py();
    let defined_on = require_verb(cls, "materialize", "Asset")?;
    reject_verb(cls, "observe", "only ExternalAsset subclasses are observed")?;
    reject_verb(
        cls,
        "compose",
        "composition belongs on a GraphAsset subclass",
    )?;
    if !asset_def_attrs(cls)?.is_empty() {
        return Err(definition_error(format!(
            "{} declares AssetDef attributes; multiple outputs need a MultiAsset subclass",
            type_name(cls)?
        )));
    }
    let f = bound_verb(cls, "materialize", &defined_on)?;
    let ctor = py.get_type::<PyAsset>();
    let cfg = collect_config(cls, ctor.as_any())?;
    cfg.set_item("name", asset_name(cls)?)?;
    cfg.set_item("actions", collect_actions(cls)?)?;
    let factory = ctor.call((), Some(&cfg))?;
    Ok(factory.call1((f,))?.unbind())
}

fn desugar_multi(cls: &Bound<'_, PyType>) -> PyResult<Py<PyAny>> {
    let py = cls.py();
    let defined_on = require_verb(cls, "materialize", "MultiAsset")?;
    let defs = asset_def_attrs(cls)?;
    if defs.is_empty() {
        return Err(definition_error(format!(
            "{} (MultiAsset subclass) defines no AssetDef attributes; \
             each output is an attribute: `customers = rs.AssetDef(...)`",
            type_name(cls)?
        )));
    }
    for (attr, ad) in &defs {
        if ad.borrow().name.is_none() {
            ad.borrow_mut().name = Some(attr.clone());
        }
    }
    let f = bound_verb(cls, "materialize", &defined_on)?;
    let ctor = py.get_type::<PyAsset>().getattr("from_multi")?;
    let cfg = collect_config(cls, &ctor)?;
    cfg.set_item(
        "output_defs",
        defs.iter().map(|(_, ad)| ad).collect::<Vec<_>>(),
    )?;
    cfg.set_item("name", asset_name(cls)?)?;
    cfg.set_item("actions", collect_actions(cls)?)?;
    let factory = ctor.call((), Some(&cfg))?;
    Ok(factory.call1((f,))?.unbind())
}

fn desugar_graph(cls: &Bound<'_, PyType>) -> PyResult<Py<PyAny>> {
    let py = cls.py();
    let defined_on = require_verb(cls, "compose", "GraphAsset")?;
    reject_verb(
        cls,
        "materialize",
        "a graph asset materializes through its composition",
    )?;
    let f = bound_verb(cls, "compose", &defined_on)?;
    let ctor = py.get_type::<PyAsset>().getattr("from_graph")?;
    let cfg = collect_config(cls, &ctor)?;
    cfg.set_item("name", asset_name(cls)?)?;
    cfg.set_item("actions", collect_actions(cls)?)?;
    let factory = ctor.call((), Some(&cfg))?;
    Ok(factory.call1((f,))?.unbind())
}

fn desugar_external(cls: &Bound<'_, PyType>) -> PyResult<Py<PyAny>> {
    let py = cls.py();
    // observe is optional, as in `Asset.external` — a verb-less external is a
    // deliberate reference to data produced elsewhere, not a mixin.
    reject_verb(
        cls,
        "materialize",
        "external assets are observed, not produced — subclass rs.Asset",
    )?;
    if !collect_actions(cls)?.is_empty() {
        return Err(definition_error(format!(
            "{}: external assets support no user-defined actions — \
             rivers only observes this data; model data you operate on as a \
             regular asset",
            type_name(cls)?
        )));
    }
    let ctor = py.get_type::<PyAsset>().getattr("external")?;
    let cfg = collect_config(cls, &ctor)?;
    cfg.set_item("name", asset_name(cls)?)?;
    let factory = ctor.call((), Some(&cfg))?;
    if let Some(defined_on) = find_verb(cls, "observe")? {
        return Ok(factory
            .call1((bound_verb(cls, "observe", &defined_on)?,))?
            .unbind());
    }
    Ok(factory.unbind())
}
