//! PartitionsDefinition — Static, TimeWindow, Multi, and Dynamic partition schemes.
use std::collections::HashMap;

use chrono::{Local, NaiveDateTime, TimeZone, Utc};
use croner::Cron;
use ordermap::OrderSet;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::errors::PartitionDefinitionError;
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

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Membership test.
    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }

    /// Position of `key` in definition order, if present.
    pub fn get_index_of(&self, key: &str) -> Option<usize> {
        self.0.get_index_of(key)
    }

    /// The key at position `idx` in definition order, if in range.
    pub fn get_at(&self, idx: usize) -> Option<&String> {
        self.0.get_index(idx)
    }
}

impl std::fmt::Debug for OrderedKeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    /// The single-dim sub-definition of `Multi` dimension `name`.
    pub fn dimension_def(&self, name: &str) -> Option<&PartitionsDefinition> {
        match self {
            Self::Multi { dimensions } => {
                dimensions.iter().find(|(n, _)| n == name).map(|(_, d)| d)
            }
            _ => None,
        }
    }

    /// The `idx`-th key (in definition order) of a single-dimension definition.
    fn nth_single_dim_key(&self, idx: usize) -> PyResult<Option<String>> {
        match self {
            Self::Static { keys } => Ok(keys.get_at(idx).cloned()),
            Self::TimeWindow { .. } => Ok(self
                .get_partition_keys_window(idx, 1)?
                .into_iter()
                .next()
                .and_then(|pk| match pk {
                    PyPartitionKey::Single { key } => key.into_iter().next(),
                    _ => None,
                })),
            _ => Ok(None),
        }
    }

    /// Enumerate keys for a single-dimension definition (Static or TimeWindow) as plain strings.
    pub fn enumerate_single_dim_keys(&self) -> PyResult<Vec<String>> {
        self.get_partition_keys()?
            .into_iter()
            .map(|pk| match pk {
                PyPartitionKey::Single { mut key } => Ok(key.remove(0)),
                PyPartitionKey::Multi { .. } | PyPartitionKey::Set { .. } => {
                    Err(PartitionDefinitionError::new_err(
                        "enumerate_single_dim_keys called on a non-single-dimension definition",
                    ))
                }
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

    /// Like [`get_partition_keys`](Self::get_partition_keys) but clamps every TimeWindow grid's effective end to `cap`.
    pub fn get_partition_keys_capped(
        &self,
        cap: NaiveDateTime,
    ) -> PyResult<Vec<PyPartitionKey>> {
        match self {
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let capped_end = Some(match end {
                    Some(e) => (*e).min(cap),
                    None => cap,
                });
                let keys =
                    enumerate_time_windows(cron_schedule, interval_seconds, start, &capped_end, fmt)?;
                Ok(keys
                    .into_iter()
                    .map(|k| PyPartitionKey::Single { key: vec![k] })
                    .collect())
            }
            Self::Multi { dimensions } => {
                let mut dim_keys: Vec<(String, Vec<String>)> = Vec::new();
                for (name, def) in dimensions {
                    dim_keys.push((name.clone(), def.enumerate_single_dim_keys_capped(cap)?));
                }
                let combos = cartesian_product(&dim_keys);
                Ok(combos
                    .into_iter()
                    .map(|keys| PyPartitionKey::Multi {
                        keys: keys.into_iter().map(|(k, v)| (k, vec![v])).collect(),
                    })
                    .collect())
            }
            _ => self.get_partition_keys(),
        }
    }

    /// Single-dimension variant of [`get_partition_keys_capped`].
    pub fn enumerate_single_dim_keys_capped(&self, cap: NaiveDateTime) -> PyResult<Vec<String>> {
        self.get_partition_keys_capped(cap)?
            .into_iter()
            .map(|pk| match pk {
                PyPartitionKey::Single { mut key } => Ok(key.remove(0)),
                PyPartitionKey::Multi { .. } | PyPartitionKey::Set { .. } => {
                    Err(PartitionDefinitionError::new_err(
                        "enumerate_single_dim_keys called on a non-single-dimension definition",
                    ))
                }
            })
            .collect()
    }

    /// A window `[offset, offset+limit)` of keys without materializing the full set.
    pub fn get_partition_keys_window(
        &self,
        offset: usize,
        limit: usize,
    ) -> PyResult<Vec<PyPartitionKey>> {
        let single = |k: String| PyPartitionKey::Single { key: vec![k] };
        match self {
            Self::Static { keys } => Ok(keys
                .iter()
                .skip(offset)
                .take(limit)
                .map(|k| single(k.clone()))
                .collect()),
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let end_dt = time_window_end(end);
                let keys = if let Some(secs) = interval_seconds {
                    interval_window(*secs, start, end_dt, fmt, offset, limit)
                } else if let Some(expr) = cron_schedule {
                    let mut out = Vec::new();
                    let mut idx = 0usize;
                    for_each_cron_tick(expr, start, end_dt, |naive| {
                        if idx >= offset && out.len() < limit {
                            out.push(naive.format(fmt).to_string());
                        }
                        idx += 1;
                        out.len() < limit
                    })?;
                    out
                } else {
                    Vec::new()
                };
                Ok(keys.into_iter().map(single).collect())
            }
            Self::Multi { dimensions } => {
                let n = dimensions.len();
                let sizes: Vec<usize> = dimensions
                    .iter()
                    .map(|(_, d)| d.partition_count())
                    .collect();
                let total = sizes.iter().fold(1usize, |acc, &s| acc.saturating_mul(s));
                let end = offset.saturating_add(limit).min(total);
                if offset >= end {
                    return Ok(Vec::new());
                }
                let mut strides = vec![1usize; n];
                for i in (0..n.saturating_sub(1)).rev() {
                    strides[i] = strides[i + 1].saturating_mul(sizes[i + 1]);
                }
                let mut out = Vec::with_capacity(end - offset);
                for k in offset..end {
                    let mut rem = k;
                    let mut combo: HashMap<String, Vec<String>> = HashMap::new();
                    for (i, (name, def)) in dimensions.iter().enumerate() {
                        let idx_i = rem / strides[i];
                        rem %= strides[i];
                        let key = def.nth_single_dim_key(idx_i)?.ok_or_else(|| {
                            PartitionDefinitionError::new_err(format!(
                                "dimension '{name}' has no key at index {idx_i}"
                            ))
                        })?;
                        combo.insert(name.clone(), vec![key]);
                    }
                    out.push(PyPartitionKey::Multi { keys: combo });
                }
                Ok(out)
            }
            Self::Dynamic { .. } => Ok(self
                .get_partition_keys()?
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect()),
        }
    }

    /// `(page, match_count)` for single-dim keys containing `query`, in one pass.
    pub fn get_partition_keys_filtered(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> PyResult<(Vec<String>, usize)> {
        let mut total = 0usize;
        let mut page: Vec<String> = Vec::new();
        let mut consume = |k: String| {
            if k.contains(query) {
                if total >= offset && page.len() < limit {
                    page.push(k);
                }
                total += 1;
            }
        };
        match self {
            Self::Static { keys } => {
                for k in keys.iter() {
                    consume(k.clone());
                }
            }
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let end_dt = time_window_end(end);
                if let Some(secs) = interval_seconds {
                    for_each_interval_tick(*secs, start, end_dt, |dt| {
                        consume(dt.format(fmt).to_string());
                        true
                    });
                } else if let Some(expr) = cron_schedule {
                    for_each_cron_tick(expr, start, end_dt, |naive| {
                        consume(naive.format(fmt).to_string());
                        true
                    })?;
                }
            }
            _ => {}
        }
        Ok((page, total))
    }

    /// Index of `key` in single-dim order — the "jump to key" target.
    pub fn single_dim_key_index(&self, key: &str) -> PyResult<Option<usize>> {
        match self {
            Self::Static { keys } => Ok(keys.get_index_of(key)),
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
                let end_dt = time_window_end(end);
                if let Some(secs) = interval_seconds {
                    Ok(interval_index(*secs, start, end_dt, fmt, key))
                } else if let Some(expr) = cron_schedule {
                    let mut idx = 0usize;
                    let mut found = None;
                    for_each_cron_tick(expr, start, end_dt, |naive| {
                        if naive.format(fmt).to_string() == key {
                            found = Some(idx);
                            return false;
                        }
                        idx += 1;
                        true
                    })?;
                    Ok(found)
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Total partitions, as cheaply as the kind allows.
    pub fn partition_count(&self) -> usize {
        match self {
            Self::Static { keys } => keys.len(),
            Self::Multi { dimensions } => dimensions
                .iter()
                .map(|(_, d)| d.partition_count())
                .fold(1usize, |acc, n| acc.saturating_mul(n)),
            Self::Dynamic { .. } => 0,
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                ..
            } => {
                let end_dt = time_window_end(end);
                if let Some(secs) = interval_seconds {
                    interval_window_count(*secs, start, end_dt)
                } else if let Some(expr) = cron_schedule {
                    let mut n = 0usize;
                    let _ = for_each_cron_tick(expr, start, end_dt, |_| {
                        n += 1;
                        true
                    });
                    n
                } else {
                    0
                }
            }
        }
    }

    /// Compute the time window (start, end) for a given partition key string.
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
            let start_utc = Utc.from_utc_datetime(&window_start);
            let next = cron.find_next_occurrence(&start_utc, false).map_err(|e| {
                PartitionDefinitionError::new_err(format!("Failed to find next cron tick: {e}"))
            })?;
            Ok(Some((window_start, next.naive_utc())))
        } else {
            Ok(None)
        }
    }

    /// The definition's wall-clock grid, for shifting/aligning keys.
    pub fn time_grid(&self) -> Option<rivers_core::timegrid::TimeGrid> {
        match self {
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => Some(rivers_core::timegrid::TimeGrid {
                cron_schedule: cron_schedule.clone(),
                interval_seconds: *interval_seconds,
                start: *start,
                end: *end,
                fmt: fmt.clone(),
            }),
            _ => None,
        }
    }

    /// Shift a TimeWindow key by `offset` windows (negative = earlier).
    pub fn shift_time_key(&self, key: &str, offset: i64) -> PyResult<String> {
        let grid = self.time_grid().ok_or_else(|| {
            PartitionDefinitionError::new_err(
                "time_window mapping requires a TimeWindow partition definition",
            )
        })?;
        grid.shift_key(key, offset)
            .map_err(|e| PartitionDefinitionError::new_err(e.to_string()))
    }

    /// Check if a partition key is valid for this definition.
    pub fn validate_partition_key(&self, key: &PyPartitionKey) -> PyResult<bool> {
        match (self, key) {
            (Self::Static { keys: valid_keys }, PyPartitionKey::Single { key }) => {
                Ok(!key.is_empty() && key.iter().all(|k| valid_keys.contains(k)))
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
                if keys.len() != dimensions.len() {
                    return Ok(false);
                }
                for (dim_name, dim_def) in dimensions {
                    match keys.get(dim_name) {
                        Some(vals) => {
                            if vals.is_empty() {
                                return Ok(false);
                            }
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
            (Self::Dynamic { .. }, PyPartitionKey::Single { key }) => Ok(!key.is_empty()),
            (_, PyPartitionKey::Set { keys }) => {
                if keys.is_empty() {
                    return Ok(false);
                }
                for member in keys {
                    if !self.validate_partition_key(member)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Compute the structural intersection of two partition definitions.
    pub fn intersect(&self, other: &Self) -> Result<Self, String> {
        match (self, other) {
            (Self::Static { keys: a }, Self::Static { keys: b }) => {
                let a_set: std::collections::HashSet<&str> = a.iter().map(String::as_str).collect();
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

impl PartitionsDefinition {
    /// Full structural validation — every rule the factory staticmethods enforce.
    pub(crate) fn validate_definition(&self) -> PyResult<()> {
        match self {
            Self::Static { keys } => {
                if keys.is_empty() {
                    return Err(PartitionDefinitionError::new_err(
                        "Static partitions must have at least one key",
                    ));
                }
                for key in keys.iter() {
                    if key.is_empty() {
                        return Err(PartitionDefinitionError::new_err(
                            "static partition keys must not be empty",
                        ));
                    }
                    if let Some(ch) = rivers_core::storage::PartitionKey::reserved_display_char(key)
                    {
                        return Err(PartitionDefinitionError::new_err(format!(
                            "partition key '{key}' contains reserved character '{ch}' \
                             (used by the canonical display form)"
                        )));
                    }
                }
                Ok(())
            }
            Self::TimeWindow {
                cron_schedule,
                interval_seconds,
                start,
                end,
                fmt,
            } => {
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
                if let Some(secs) = interval_seconds {
                    if !secs.is_finite() || *secs <= 0.0 || (secs * 1_000_000_000.0) as i64 == 0 {
                        return Err(PartitionDefinitionError::new_err(format!(
                            "interval_seconds must be positive and at least 1 nanosecond, \
                             got {secs}"
                        )));
                    }
                }
                validate_time_window_fmt(cron_schedule, interval_seconds, start, end, fmt)
            }
            Self::Multi { dimensions } => {
                if dimensions.is_empty() {
                    return Err(PartitionDefinitionError::new_err(
                        "Multi partitions must have at least one dimension",
                    ));
                }
                let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
                for (name, _) in dimensions {
                    if !seen.insert(name.as_str()) {
                        return Err(PartitionDefinitionError::new_err(format!(
                            "Multi partitions: duplicate dimension name '{name}'"
                        )));
                    }
                }
                for (name, def) in dimensions {
                    if matches!(def, Self::Multi { .. }) {
                        return Err(PartitionDefinitionError::new_err(
                            "Multi partitions cannot contain nested Multi dimensions",
                        ));
                    }
                    if name.is_empty() {
                        return Err(PartitionDefinitionError::new_err(
                            "Multi dimension names cannot be empty",
                        ));
                    }
                    if let Some(ch) = ['|', ',', '='].into_iter().find(|&c| name.contains(c)) {
                        return Err(PartitionDefinitionError::new_err(format!(
                            "dimension name '{name}' contains reserved character '{ch}' \
                             (used by the canonical display form)"
                        )));
                    }
                    def.validate_definition()?;
                }
                Ok(())
            }
            Self::Dynamic { name } => {
                if name.is_empty() {
                    return Err(PartitionDefinitionError::new_err(
                        "Dynamic partition definition name cannot be empty",
                    ));
                }
                Ok(())
            }
        }
    }
}

#[pymethods]
impl PartitionsDefinition {
    #[staticmethod]
    fn static_(keys: Vec<String>) -> PyResult<Self> {
        let def = Self::Static {
            keys: OrderedKeySet::new(keys),
        };
        def.validate_definition()?;
        Ok(def)
    }

    #[staticmethod]
    #[pyo3(signature = (start, end=None, fmt=None))]
    fn daily(
        start: NaiveDateTime,
        end: Option<NaiveDateTime>,
        fmt: Option<String>,
    ) -> PyResult<Self> {
        let def = Self::TimeWindow {
            cron_schedule: Some("0 0 * * *".to_string()),
            interval_seconds: None,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%d".to_string()),
        };
        def.validate_definition()?;
        Ok(def)
    }

    #[staticmethod]
    #[pyo3(signature = (start, end=None, fmt=None))]
    fn hourly(
        start: NaiveDateTime,
        end: Option<NaiveDateTime>,
        fmt: Option<String>,
    ) -> PyResult<Self> {
        let def = Self::TimeWindow {
            cron_schedule: Some("0 * * * *".to_string()),
            interval_seconds: None,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%dT%H:00".to_string()),
        };
        def.validate_definition()?;
        Ok(def)
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
        let def = Self::TimeWindow {
            cron_schedule,
            interval_seconds,
            start,
            end,
            fmt: fmt.unwrap_or_else(|| "%Y-%m-%dT%H:%M:%S".to_string()),
        };
        def.validate_definition()?;
        Ok(def)
    }

    #[staticmethod]
    fn multi(dimensions: &Bound<'_, PyDict>) -> PyResult<Self> {
        let mut dims = Vec::new();
        for (k, v) in dimensions.iter() {
            let name: String = k.extract()?;
            let def: PartitionsDefinition = v.extract()?;
            dims.push((name, def));
        }
        let def = Self::Multi { dimensions: dims };
        def.validate_definition()?;
        Ok(def)
    }

    #[staticmethod]
    fn dynamic(name: String) -> PyResult<Self> {
        let def = Self::Dynamic { name };
        def.validate_definition()?;
        Ok(def)
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

/// A TimeWindow fmt must round-trip the grid's window starts.
fn validate_time_window_fmt(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &NaiveDateTime,
    end: &Option<NaiveDateTime>,
    fmt: &str,
) -> PyResult<()> {
    if cron_schedule.is_some() {
        use chrono::Timelike;
        if start.nanosecond() != 0 {
            return Err(PartitionDefinitionError::new_err(format!(
                "cron-gridded time windows require a start on a whole second, got {start}"
            )));
        }
    }
    const MAX_TICKS: usize = 1024;
    let horizon = start
        .checked_add_signed(chrono::Duration::days(1461))
        .unwrap_or(NaiveDateTime::MAX);
    let bound = match end {
        Some(e) => (*e).min(horizon),
        None => horizon,
    };
    fn round_trip_tick(t: NaiveDateTime, fmt: &str, failure: &mut Option<PyErr>) -> bool {
        let key = t.format(fmt).to_string();
        if let Some(ch) = rivers_core::storage::PartitionKey::reserved_display_char(&key) {
            *failure = Some(PartitionDefinitionError::new_err(format!(
                "fmt '{fmt}' produces keys containing reserved character '{ch}' \
                 (used by the canonical display form): '{key}'"
            )));
            return false;
        }
        match parse_key_datetime(&key, fmt) {
            Ok(parsed) if parsed == t => true,
            Ok(parsed) => {
                *failure = Some(PartitionDefinitionError::new_err(format!(
                    "fmt '{fmt}' cannot represent the partition grid: window start {t} \
                     formats to '{key}', which parses back to {parsed}; \
                     use a format at least as fine as the grid"
                )));
                false
            }
            Err(e) => {
                *failure = Some(PartitionDefinitionError::new_err(format!(
                    "fmt '{fmt}' cannot represent the partition grid: window start {t} \
                     formats to '{key}', which does not parse back: {e}"
                )));
                false
            }
        }
    }
    let mut checked = 0usize;
    let mut failure: Option<PyErr> = None;
    if let Some(secs) = interval_seconds {
        for_each_interval_tick(*secs, start, bound, &mut |t| {
            checked += 1;
            round_trip_tick(t, fmt, &mut failure) && checked < MAX_TICKS
        });
    } else if let Some(expr) = cron_schedule {
        for_each_cron_tick(expr, start, bound, &mut |t| {
            checked += 1;
            round_trip_tick(t, fmt, &mut failure) && checked < MAX_TICKS
        })?;
    }
    if failure.is_none() && checked < 2 {
        let true_end = match end {
            Some(e) => *e,
            None => NaiveDateTime::MAX,
        };
        let mut taken = 0usize;
        if let Some(secs) = interval_seconds {
            for_each_interval_tick(*secs, start, true_end, &mut |t| {
                taken += 1;
                round_trip_tick(t, fmt, &mut failure) && taken < 2
            });
        } else if let Some(expr) = cron_schedule {
            for_each_cron_tick(expr, start, true_end, &mut |t| {
                taken += 1;
                round_trip_tick(t, fmt, &mut failure) && taken < 2
            })?;
        }
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
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
    if key.is_empty() {
        return Ok(false);
    }
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
            if interval_ns <= 0 {
                return Ok(false);
            }
            if from_start
                .num_nanoseconds()
                .is_none_or(|n| n % interval_ns != 0)
            {
                return Ok(false);
            }
        } else if let Some(expr) = cron_schedule {
            let window_start = (dt - chrono::Duration::hours(26)).max(*start);
            let window_end = (dt + chrono::Duration::hours(26)).min(end_dt);
            let mut found = false;
            for_each_cron_tick(expr, &window_start, window_end, |naive| {
                if naive.format(fmt).to_string() == *k {
                    found = true;
                    return false;
                }
                true
            })?;
            if !found {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Enumerate all time window partition keys in `[start, end)` (or now).
fn enumerate_time_windows(
    cron_schedule: &Option<String>,
    interval_seconds: &Option<f64>,
    start: &NaiveDateTime,
    end: &Option<NaiveDateTime>,
    fmt: &str,
) -> PyResult<Vec<String>> {
    let end_dt = time_window_end(end);
    let mut keys = Vec::new();
    if let Some(secs) = interval_seconds {
        for_each_interval_tick(*secs, start, end_dt, |dt| {
            keys.push(dt.format(fmt).to_string());
            true
        });
    } else if let Some(expr) = cron_schedule {
        for_each_cron_tick(expr, start, end_dt, |naive| {
            keys.push(naive.format(fmt).to_string());
            true
        })?;
    } else {
        return Err(PartitionDefinitionError::new_err(
            "TimeWindow requires either cron_schedule or interval_seconds",
        ));
    }
    Ok(keys)
}

/// Effective end bound for a TimeWindow: the explicit `end`, else now.
fn time_window_end(end: &Option<NaiveDateTime>) -> NaiveDateTime {
    end.unwrap_or_else(|| Local::now().naive_local())
}

/// Count of interval windows in `[start, end)`.
fn interval_window_count(secs: f64, start: &NaiveDateTime, end_dt: NaiveDateTime) -> usize {
    if end_dt <= *start {
        return 0;
    }
    let interval_ns = (secs * 1_000_000_000.0) as i128;
    if interval_ns <= 0 {
        return 0;
    }
    let delta = end_dt - *start;
    let span_ns =
        i128::from(delta.num_seconds()) * 1_000_000_000 + i128::from(delta.subsec_nanos());
    let count = ((span_ns - 1) / interval_ns) + 1;
    count.clamp(0, usize::MAX as i128) as usize
}

/// Up to `limit` interval keys from index `offset`, via arithmetic seek.
fn interval_window(
    secs: f64,
    start: &NaiveDateTime,
    end_dt: NaiveDateTime,
    fmt: &str,
    offset: usize,
    limit: usize,
) -> Vec<String> {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 || limit == 0 {
        return Vec::new();
    }
    let step = chrono::Duration::nanoseconds(interval_ns);
    let Some(off_ns) = (offset as i64).checked_mul(interval_ns) else {
        return Vec::new();
    };
    let Some(mut current) = start.checked_add_signed(chrono::Duration::nanoseconds(off_ns)) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(limit.min(1024));
    while current < end_dt && out.len() < limit {
        out.push(current.format(fmt).to_string());
        current = match current.checked_add_signed(step) {
            Some(c) => c,
            None => break,
        };
    }
    out
}

/// Index of `key` if it's an aligned interval window in `[start, end)`, else `None`.
fn interval_index(
    secs: f64,
    start: &NaiveDateTime,
    end_dt: NaiveDateTime,
    fmt: &str,
    key: &str,
) -> Option<usize> {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 {
        return None;
    }
    let dt = parse_key_datetime(key, fmt).ok()?;
    if dt < *start || dt >= end_dt {
        return None;
    }
    let delta = (dt - *start).num_nanoseconds()?;
    if delta % interval_ns != 0 {
        return None;
    }
    Some((delta / interval_ns) as usize)
}

/// Walk interval windows in `[start, end)` lazily; `f` returns false to stop.
pub(crate) fn for_each_interval_tick(
    secs: f64,
    start: &NaiveDateTime,
    end_dt: NaiveDateTime,
    mut f: impl FnMut(NaiveDateTime) -> bool,
) {
    let interval_ns = (secs * 1_000_000_000.0) as i64;
    if interval_ns <= 0 {
        return;
    }
    let step = chrono::Duration::nanoseconds(interval_ns);
    let mut current = *start;
    while current < end_dt {
        if !f(current) {
            break;
        }
        current = match current.checked_add_signed(step) {
            Some(c) => c,
            None => break,
        };
    }
}

/// Walk cron occurrences in `[start, end)` lazily; `f` returns false to stop.
pub(crate) fn for_each_cron_tick(
    cron_expr: &str,
    start: &NaiveDateTime,
    end_dt: NaiveDateTime,
    mut f: impl FnMut(NaiveDateTime) -> bool,
) -> PyResult<()> {
    let cron = parse_cron(cron_expr)?;
    let start_utc = Utc.from_utc_datetime(start);
    for tick in cron.iter_from(start_utc, croner::Direction::Forward) {
        let naive = tick.naive_utc();
        if naive >= end_dt {
            break;
        }
        if !f(naive) {
            break;
        }
    }
    Ok(())
}

/// Whether `t` falls exactly on the cron grid (cron is second-granular).
pub(crate) fn cron_grid_contains(cron_expr: &str, t: NaiveDateTime) -> PyResult<bool> {
    use chrono::Timelike;
    if t.nanosecond() != 0 {
        return Ok(false);
    }
    let probe_end = t
        .checked_add_signed(chrono::Duration::seconds(1))
        .unwrap_or(NaiveDateTime::MAX);
    let mut hit = false;
    for_each_cron_tick(cron_expr, &t, probe_end, |tick| {
        hit = tick == t;
        false
    })?;
    Ok(hit)
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
