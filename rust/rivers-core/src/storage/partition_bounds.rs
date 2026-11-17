use std::cmp::Ordering;

use super::PartitionKey;

/// The stored keys of an asset from `lower` to `upper`, both included.
///
/// The store keeps the keys of an asset in order: by the value of each
/// dimension, the dimensions in the order of their names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionBounds {
    pub lower: PartitionKey,
    pub upper: PartitionKey,
}

impl PartitionBounds {
    /// The keys of one dimension from `first` to `last`.
    pub fn of_keys(first: &str, last: &str) -> Self {
        Self {
            lower: PartitionKey::Single {
                keys: vec![first.to_string()],
            },
            upper: PartitionKey::Single {
                keys: vec![last.to_string()],
            },
        }
    }

    /// The keys of several dimensions with the values `fixed` for their
    /// first dimensions, and a value from `first` to `last` for `dimension`,
    /// the next one.
    pub fn of_dimension(
        fixed: &[(String, String)],
        dimension: &str,
        first: &str,
        last: &str,
    ) -> Self {
        let dims = |values: Vec<String>| {
            let mut dims: Vec<(String, Vec<String>)> = fixed
                .iter()
                .map(|(name, value)| (name.clone(), vec![value.clone()]))
                .collect();
            dims.push((dimension.to_string(), values));
            dims.sort_by(|a, b| a.0.cmp(&b.0));
            dims
        };

        // A key with the value `first` has more dimensions than the lower
        // bound, or as many: it sorts at or after it. A key with the value
        // `last` has one value where the upper bound has two: it sorts before.
        Self {
            lower: PartitionKey::Multi {
                dims: dims(vec![first.to_string()]),
            },
            upper: PartitionKey::Multi {
                dims: dims(vec![last.to_string(), String::new()]),
            },
        }
    }

    /// Whether the store keeps `key` in these bounds.
    pub fn contains(&self, key: &PartitionKey) -> bool {
        stored_order(&self.lower, key).is_le() && stored_order(key, &self.upper).is_le()
    }
}

/// The order in which the store keeps two keys of one asset.
pub fn stored_order(a: &PartitionKey, b: &PartitionKey) -> Ordering {
    Stored::of(a).cmp(&Stored::of(b))
}

/// A key as the store compares it: a key with dimensions sorts before a key
/// with none, and its dimensions are in the order of their names.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Stored<'a> {
    Multi(Vec<(&'a str, &'a [String])>),
    Single(&'a [String]),
    Other,
}

impl<'a> Stored<'a> {
    fn of(key: &'a PartitionKey) -> Self {
        match key {
            PartitionKey::Multi { dims } => {
                let mut dims: Vec<(&str, &[String])> = dims
                    .iter()
                    .map(|(name, values)| (name.as_str(), values.as_slice()))
                    .collect();
                dims.sort_by(|a, b| a.0.cmp(b.0));
                Self::Multi(dims)
            }
            PartitionKey::Single { keys } => Self::Single(keys),
            _ => Self::Other,
        }
    }
}
