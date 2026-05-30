//! PartitionsDefinition — Static, TimeWindow, Multi, and Dynamic partition schemes.
//!
//! `PartitionsDefinition` enum with four variants. `get_partition_keys()` enumerates all valid
//! keys for a given definition. Time window partitions are generated from cron expressions
//! via the `croner` crate, bounded by `start_date` / `end_date` / `end_offset`.
use std::collections::HashMap;

use chrono::{Local, NaiveDateTime, TimeZone};
use croner::Cron;
use ordermap::OrderSet;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::errors::PartitionDefinitionError;

const MAX_PARTITION_KEYS: usize = 1_000_000;
use rivers_core::util::parse_key_datetime;

use super::key::PyPartitionKey;

/// Insertion-ordered set of static partition keys.
#[derive(Clone, PartialEq)]
pub struct OrderedKeySet(OrderSet<String>);

impl OrderedKeySet {
    /// Build from a key list; duplicate keys collapse (set semantics).
    pub fn new(keys: Vec<String>) -> Self {
        Self(keys.into_iter().collect())
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }

    /// O(1) membership.
    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }

    /// O(1) position of `key` in definition order, if present.
    pub fn get_index_of(&self, key: &str) -> Option<usize> {
        self.0.get_index_of(key)
    }
}

impl std::fmt::Debug for OrderedKeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Render as a list literal so the PartitionsDefinition repr stays
        // `static_(["a", "b"])`.
        f.debug_list().entries(self.0.iter()).finish()
    }
}

impl<'py> IntoPyObject<'py> for &OrderedKeySet {
    type Target = PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyList::new(py, &self.0)
    }
}

impl<'py> IntoPyObject<'py> for OrderedKeySet {
    type Target = PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyList::new(py, &self.0)
    }
}

impl FromPyObject<'_, '_> for OrderedKeySet {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        Ok(OrderedKeySet::new(ob.extract::<Vec<String>>()?))
    }
}

#[pyclass(
    name = "PartitionsDefinition",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionsDefinition {
    Static {
        keys: OrderedKeySet,
    },
    TimeWindow {
        cron_schedule: Option<String>,
        interval_seconds: Option<f64>,
        start: NaiveDateTime,
        end: Option<NaiveDateTime>,
        fmt: String,
    },
    Multi {
        dimensions: Vec<(String, PartitionsDefinition)>,
    },
    Dynamic {
        name: String,
    },
}

impl PartitionsDefinition {
    /// Return the variant name as a string (e.g. "Static", "TimeWindow", "Multi", "Dynamic").
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Static { .. } => "Static",
            Self::TimeWindow { .. } => "TimeWindow",
            Self::Multi { .. } => "Multi",
            Self::Dynamic { .. } => "Dynamic",
        }
    }

    /// Check if two definitions are the same variant.
    pub fn same_variant(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    /// For Static definitions, return the key set. Returns None for other variants.
    pub fn static_keys(&self) -> Option<&OrderedKeySet> {
        match self {
            Self::Static { keys } => Some(keys),
            _ => None,
        }
    }

    /// Enumerate keys for a single-dimension definition (Static or TimeWindow),
    /// returning them as plain strings. Errors if `self` is Multi or Dynamic,
    /// since both yield non-Single PyPartitionKey values.
    pub fn enumerate_single_dim_keys(&self) -> PyResult<Vec<String>> {
        self.get_partition_keys()?
            .into_iter()
            .map(|pk| match pk {
                PyPartitionKey::Single { mut key } => Ok(key.remove(0)),
                PyPartitionKey::Multi { .. } => Err(PartitionDefinitionError::new_err(
                    "enumerate_single_dim_keys called on a Multi partition definition",
                )),
            })
            .collect()
    }

    /// Enumerate all valid partition keys.
    pub fn get_partition_keys(&self) -> PyResult<Vec<PyPartitionKey>> {
        match self {
            Self::Static { keys } => Ok(keys
                .iter()
                .map(|k| PyPartitionKey::Single {
                    key: vec![k.clone()],
                })
                .collect()),
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let keys =
                    enumerate_time_windows(cron_schedule, interval_seconds, start, end, fmt)?;
                Ok(keys
                    .into_iter()
                    .map(|k| PyPartitionKey::Single { key: vec![k] })
                    .collect())
            }
            Self::Multi { dimensions } => {
                let mut dim_keys: Vec<(String, Vec<String>)> = Vec::new();
                for (name, def) in dimensions {
                    dim_keys.push((name.clone(), def.enumerate_single_dim_keys()?));
                }
                let total: usize = dim_keys.iter().map(|(_, k)| k.len()).product();
                if total > MAX_PARTITION_KEYS {
                    return Err(PartitionDefinitionError::new_err(format!(
                        "Multi partition cartesian product ({total}) exceeds limit of {MAX_PARTITION_KEYS}"
                    )));
                }
                let combos = cartesian_product(&dim_keys);
                Ok(combos
                    .into_iter()
                    .map(|keys| PyPartitionKey::Multi {
                        keys: keys.into_iter().map(|(k, v)| (k, vec![v])).collect(),
                    })
                    .collect())
            }
            Self::Dynamic { .. } => Err(PyNotImplementedError::new_err(
                "Cannot enumerate dynamic partition keys",
            )),
        }
    }

    /// Compute the time window (start, end) for a given partition key string.
    /// Returns None for non-TimeWindow definitions or if neither cron nor interval is set.
    pub fn compute_time_window(
        &self,
        key: &str,
    ) -> PyResult<Option<(NaiveDateTime, NaiveDateTime)>> {
        let (cron_schedule, interval_seconds, fmt) = match self {
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                fmt,
                ..
            } => (cron_schedule, interval_seconds, fmt.as_str()),
            _ => return Ok(None),
        };

        let window_start = parse_key_datetime(key, fmt).map_err(|e| {
            PartitionDefinitionError::new_err(format!(
                "Cannot parse partition key '{key}' with format '{fmt}': {e}"
            ))
        })?;

        if let Some(interval) = interval_seconds {
            let duration = chrono::Duration::nanoseconds((*interval * 1_000_000_000.0) as i64);
            let window_end = window_start + duration;
            Ok(Some((window_start, window_end)))
        } else if let Some(cron_expr) = cron_schedule {
            let cron = parse_cron(cron_expr)?;
            let start_local = naive_to_local(&window_start)?;
            let next = cron
                .find_next_occurrence(&start_local, false)
                .map_err(|e| {
                    PartitionDefinitionError::new_err(format!("Failed to find next cron tick: {e}"))
                })?;
            Ok(Some((window_start, next.naive_local())))
        } else {
            Ok(None)
        }
    }

    /// Check if a partition key is valid for this definition.
    pub fn validate_partition_key(&self, key: &PyPartitionKey) -> PyResult<bool> {
        match (self, key) {
            (Self::Static { keys: valid_keys }, PyPartitionKey::Single { key }) => {
                Ok(key.iter().all(|k| valid_keys.contains(k)))
            }
            (
                Self::TimeWindow {
                    cron_schedule,
                    interval_seconds,
                    start,
                    end,
                    fmt,
                },
                PyPartitionKey::Single { key },
            ) => validate_time_window_key(key, cron_schedule, interval_seconds, start, end, fmt),
            (Self::Multi { dimensions }, PyPartitionKey::Multi { keys }) => {
                for (dim_name, dim_def) in dimensions {
                    match keys.get(dim_name) {
                        Some(vals) => {
                            for val in vals {
                                let single_key = PyPartitionKey::Single {
                                    key: vec![val.clone()],
                                };
                                if !dim_def.validate_partition_key(&single_key)? {
                                    return Ok(false);
                                }
                            }
                        }
                        None => return Ok(false),
                    }
                }
                Ok(true)
            }
            // Dynamic partitions accept any single key (keys are storage-managed, not statically validated).
            (Self::Dynamic { .. }, PyPartitionKey::Single { .. }) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Compute the structural intersection of two partition definitions —
    /// the def whose keys are valid for both inputs. `None` (well, `Err`)
    /// when no key can satisfy both: different kinds, mismatched cadence
    /// or format on TimeWindow, mismatched dimensions on Multi, mismatched
    /// namespace on Dynamic, or empty key overlap on Static.
    ///
    /// Used at `repo.resolve()` to reject jobs whose partitioned assets
    /// can never share a single partition_key. The `Err` payload is a
    /// human-readable reason so callers can render a precise error.
    pub fn intersect(&self, other: &Self) -> Result<Self, String> {
        match (self, other) {
            (Self::Static { keys: a }, Self::Static { keys: b }) => {
                let a_set: std::collections::HashSet<&str> = a.iter().map(String::as_str).collect();
                // Preserve `b`'s order on the intersection — same convention
                // as set intersection elsewhere in the codebase.
                let common: Vec<String> = b
                    .iter()
                    .filter(|k| a_set.contains(k.as_str()))
                    .cloned()
                    .collect();
                if common.is_empty() {
                    Err(format!("disjoint Static keys ({:?} vs {:?})", a, b))
                } else {
                    Ok(Self::Static {
                        keys: OrderedKeySet::new(common),
                    })
                }
            }
            (
                Self::TimeWindow {
                    cron_schedule: ca,
                    interval_seconds: ia,
                    start: sa,
                    end: ea,
                    fmt: fa,
                },
                Self::TimeWindow {
                    cron_schedule: cb,
                    interval_seconds: ib,
                    start: sb,
                    end: eb,
                    fmt: fb,
                },
            ) => {
                if ca != cb || ia != ib {
                    let cadence = |c: &Option<String>, i: &Option<f64>| match (c, i) {
                        (Some(c), _) => format!("cron='{}'", c),
                        (_, Some(i)) => format!("interval_seconds={}", i),
                        _ => "<unspecified>".to_string(),
                    };
                    return Err(format!(
                        "TimeWindow cadence mismatch ({} vs {})",
                        cadence(ca, ia),
                        cadence(cb, ib)
                    ));
                }
                if fa != fb {
                    return Err(format!("TimeWindow fmt mismatch ('{}' vs '{}')", fa, fb));
                }
                let start = (*sa).max(*sb);
                let end = match (ea, eb) {
                    (Some(a), Some(b)) => Some((*a).min(*b)),
                    (Some(a), None) | (None, Some(a)) => Some(*a),
                    (None, None) => None,
                };
                if let Some(e) = end
                    && e <= start
                {
                    return Err(format!(
                        "TimeWindow date ranges don't overlap ([{}, {}) vs [{}, {}))",
                        sa,
                        ea.map(|d| d.to_string()).unwrap_or_else(|| "∞".to_string()),
                        sb,
                        eb.map(|d| d.to_string()).unwrap_or_else(|| "∞".to_string()),
                    ));
                }
                Ok(Self::TimeWindow {
                    cron_schedule: ca.clone(),
                    interval_seconds: *ia,
                    start,
                    end,
                    fmt: fa.clone(),
                })
            }
            (Self::Multi { dimensions: a }, Self::Multi { dimensions: b }) => {
                let a_names: std::collections::BTreeSet<&str> =
                    a.iter().map(|(n, _)| n.as_str()).collect();
                let b_names: std::collections::BTreeSet<&str> =
                    b.iter().map(|(n, _)| n.as_str()).collect();
                if a_names != b_names {
                    let av: Vec<String> = a.iter().map(|(n, _)| n.clone()).collect();
                    let bv: Vec<String> = b.iter().map(|(n, _)| n.clone()).collect();
                    return Err(format!(
                        "Multi dimension name mismatch ({:?} vs {:?})",
                        av, bv
                    ));
                }
                let b_map: HashMap<&str, &PartitionsDefinition> =
                    b.iter().map(|(n, d)| (n.as_str(), d)).collect();
                let mut dims: Vec<(String, PartitionsDefinition)> = Vec::with_capacity(a.len());
                for (name, def_a) in a {
                    let def_b = b_map
                        .get(name.as_str())
                        .expect("dim name set checked above");
                    let intersected = def_a
                        .intersect(def_b)
                        .map_err(|inner| format!("Multi dimension '{}': {}", name, inner))?;
                    dims.push((name.clone(), intersected));
                }
                Ok(Self::Multi { dimensions: dims })
            }
            (Self::Dynamic { name: a }, Self::Dynamic { name: b }) => {
                if a == b {
                    Ok(Self::Dynamic { name: a.clone() })
                } else {
                    Err(format!("Dynamic namespace mismatch ('{}' vs '{}')", a, b))
                }
            }
            (a, b) => Err(format!(
                "different partition kinds ({} vs {})",
                a.variant_name(),
                b.variant_name()
            )),
        }
    }
}

#[pymethods]
impl PartitionsDefinition {
    #[staticmethod]
    fn static_(keys: Vec<String>) -> PyResult<Self> {
        if keys.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "Static partitions must have at least one key",
            ));
        }
        Ok(Self::Static {
            keys: OrderedKeySet::new(keys),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (start, end=None, fmt=None))]
    fn daily(start: NaiveDateTime, end: Option<NaiveDateTime>, fmt: Option<String>) -> Self {
        Self::TimeWindow {
            cron_schedule: Some("0 0 * * *".to_string()),
            interval_seconds: None,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%d".to_string()),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (start, end=None, fmt=None))]
    fn hourly(start: NaiveDateTime, end: Option<NaiveDateTime>, fmt: Option<String>) -> Self {
        Self::TimeWindow {
            cron_schedule: Some("0 * * * *".to_string()),
            interval_seconds: None,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%dT%H:00".to_string()),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (start, cron_schedule=None, interval_seconds=None, end=None, fmt=None))]
    fn time_window(
        start: NaiveDateTime,
        cron_schedule: Option<String>,
        interval_seconds: Option<f64>,
        end: Option<NaiveDateTime>,
        fmt: Option<String>,
    ) -> PyResult<Self> {
        if cron_schedule.is_none() && interval_seconds.is_none() {
            return Err(PartitionDefinitionError::new_err(
                "time_window requires either cron_schedule or interval_seconds",
            ));
        }
        if cron_schedule.is_some() && interval_seconds.is_some() {
            return Err(PartitionDefinitionError::new_err(
                "time_window accepts cron_schedule or interval_seconds, not both",
            ));
        }
        Ok(Self::TimeWindow {
            cron_schedule,
            interval_seconds,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%dT%H:%M:%S".to_string()),
        })
    }

    #[staticmethod]
    fn multi(dimensions: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut dims = Vec::new();
        for (k, v) in dimensions.iter() {
            let name: String = k.extract()?;
            let def: PartitionsDefinition = v.extract()?;
            if matches!(def, PartitionsDefinition::Multi { .. }) {
                return Err(PartitionDefinitionError::new_err(
                    "Multi partitions cannot contain nested Multi dimensions",
                ));
            }
            dims.push((name, def));
        }
        if dims.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "Multi partitions must have at least one dimension",
            ));
        }
        Ok(Self::Multi { dimensions: dims })
    }

    #[staticmethod]
    fn dynamic(name: String) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PartitionDefinitionError::new_err(
                "Dynamic partition definition name cannot be empty",
            ));
        }
        Ok(Self::Dynamic { name })
    }

    #[pyo3(name = "get_partition_keys")]
    fn py_get_partition_keys(&self) -> PyResult<Vec<PyPartitionKey>> {
        self.get_partition_keys()
    }

    #[pyo3(name = "validate_partition_key")]
    fn py_validate_partition_key(&self, key: &PyPartitionKey) -> PyResult<bool> {
        self.validate_partition_key(key)
    }

    pub fn __repr__(&self) -> String {
        match self {
            Self::Static { keys } => {
                format!("PartitionsDefinition.static_({keys:?})")
            }
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let schedule = if let Some(cron) = cron_schedule {
                    format!("cron={cron:?}")
                } else if let Some(interval) = interval_seconds {
                    format!("interval={interval}s")
                } else {
                    "".to_string()
                };
                let end_str = match end {
                    Some(e) => format!(", end={e}"),
                    None => String::new(),
                };
                format!(
                    "PartitionsDefinition.time_window({schedule}, start={start}, fmt={fmt:?}{end_str})"
                )
            }
            Self::Multi { dimensions } => {
                let dims: Vec<String> = dimensions
                    .iter()
                    .map(|(name, def)| format!("{name:?}: {}", def.__repr__()))
                    .collect();
                format!("PartitionsDefinition.multi({{{}}})", dims.join(", "))
            }
            Self::Dynamic { name } => {
                format!("PartitionsDefinition.dynamic({name:?})")
            }
        }
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_partitions_definition")?
            .unbind();
        let data = PyDict::new(py);
        match self {
            Self::Static { keys } => {
                data.set_item("variant", "Static")?;
                data.set_item("keys", keys)?;
            }
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                data.set_item("variant", "TimeWindow")?;
                data.set_item("cron_schedule", cron_schedule.clone())?;
                data.set_item("interval_seconds", *interval_seconds)?;
                data.set_item("start", start.into_pyobject(py)?)?;
                if let Some(e) = end {
                    data.set_item("end", e.into_pyobject(py)?)?;
                } else {
                    data.set_item("end", py.None())?;
                }
                data.set_item("fmt", fmt.clone())?;
            }
            Self::Multi { dimensions } => {
                data.set_item("variant", "Multi")?;
                let dims = PyList::empty(py);
                for (name, def) in dimensions {
                    let def_py = Py::new(py, def.clone())?;
                    let tuple = PyTuple::new(
                        py,
                        [
                            name.clone().into_pyobject(py)?.into_any(),
                            def_py.into_any().into_bound(py).into_any(),
                        ],
                    )?;
                    dims.append(tuple)?;
                }
                data.set_item("dimensions", dims)?;
            }
            Self::Dynamic { name } => {
                data.set_item("variant", "Dynamic")?;
                data.set_item("name", name.clone())?;
            }
        }
        let args = PyTuple::new(py, [data.into_any()])?;
        Ok((reconstruct, args.unbind().into_any()))
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

fn parse_cron(expr: &str) -> PyResult<Cron> {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Optional)
        .build()
        .parse(expr)
        .map_err(|e| {
            PartitionDefinitionError::new_err(format!("Invalid cron expression '{expr}': {e}"))
        })
}

fn naive_to_local(dt: &NaiveDateTime) -> PyResult<chrono::DateTime<Local>> {
    Local
        .from_local_datetime(dt)
        .single()
        .ok_or_else(|| PartitionDefinitionError::new_err(format!("Ambiguous local time: {dt}")))
}

/// Check if all key strings fall on valid time window boundaries within [start, end).
fn validate_time_window_key(
    key: &[String],
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &NaiveDateTime,
    end: &Option<NaiveDateTime>,
    fmt: &str,
) -> PyResult<bool> {
    let now = Local::now().naive_local();
    let end_dt = end.unwrap_or(now);
    let cron = cron_schedule.as_deref().map(parse_cron).transpose()?;
    for k in key {
        let dt = match parse_key_datetime(k, fmt) {
            Ok(dt) => dt,
            Err(_) => return Ok(false),
        };
        if dt < *start || dt >= end_dt {
            return Ok(false);
        }
        if let Some(secs) = interval_seconds {
            let from_start = dt.signed_duration_since(*start);
            let interval_ns = (*secs * 1_000_000_000.0) as i64;
            if from_start
                .num_nanoseconds()
                .is_none_or(|n| n % interval_ns != 0)
            {
                return Ok(false);
            }
        } else if let Some(ref cron) = cron {
            let dt_local = naive_to_local(&dt)?;
            let matches = cron
                .is_time_matching(&dt_local)
                .map_err(|e| PartitionDefinitionError::new_err(format!("Cron match error: {e}")))?;
            if !matches {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Enumerate time window partition keys between start and end (or now).
fn enumerate_time_windows(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &NaiveDateTime,
    end: &Option<NaiveDateTime>,
    fmt: &str,
) -> PyResult<Vec<String>> {
    let now = Local::now().naive_local();
    let end_dt = end.unwrap_or(now);

    if let Some(secs) = interval_seconds {
        let interval = chrono::Duration::nanoseconds((*secs * 1_000_000_000.0) as i64);
        let mut keys = Vec::new();
        let mut current = *start;
        while current < end_dt {
            if keys.len() >= MAX_PARTITION_KEYS {
                return Err(PartitionDefinitionError::new_err(format!(
                    "TimeWindow partition count exceeds limit of {MAX_PARTITION_KEYS}"
                )));
            }
            keys.push(current.format(fmt).to_string());
            current += interval;
        }
        Ok(keys)
    } else if let Some(cron_expr) = cron_schedule {
        let cron = parse_cron(cron_expr)?;
        let start_local = naive_to_local(start)?;
        let mut keys = Vec::new();
        for tick in cron.iter_from(start_local, croner::Direction::Forward) {
            let naive = tick.naive_local();
            if naive >= end_dt {
                break;
            }
            if keys.len() >= MAX_PARTITION_KEYS {
                return Err(PartitionDefinitionError::new_err(format!(
                    "TimeWindow partition count exceeds limit of {MAX_PARTITION_KEYS}"
                )));
            }
            keys.push(naive.format(fmt).to_string());
        }
        Ok(keys)
    } else {
        Err(PartitionDefinitionError::new_err(
            "TimeWindow requires either cron_schedule or interval_seconds",
        ))
    }
}

/// Compute the cartesian product of dimension keys.
pub fn cartesian_product(dim_keys: &[(String, Vec<String>)]) -> Vec<HashMap<String, String>> {
    if dim_keys.is_empty() {
        return vec![HashMap::new()];
    }
    let (name, keys) = &dim_keys[0];
    let rest = cartesian_product(&dim_keys[1..]);
    let mut result = Vec::new();
    for key in keys {
        for r in &rest {
            let mut m = r.clone();
            m.insert(name.clone(), key.clone());
            result.push(m);
        }
    }
    result
}
