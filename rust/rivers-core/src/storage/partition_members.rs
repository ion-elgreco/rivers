//! The partitions that keys name, without a key object for each partition.

use std::collections::HashSet;

use super::key_item::SpanWindows;
use super::{KeyItem, PartitionKey};

/// The partitions that a list of keys names. A key can name many partitions;
/// this answers "is this partition one of them" without building them.
pub struct NamedPartitions<'a> {
    /// Keys that name one partition.
    single: HashSet<&'a PartitionKey>,
    /// Keys that name several.
    batched: Vec<Batched<'a>>,
}

enum Batched<'a> {
    Single(ValueSet<'a>),
    Multi(Vec<(&'a str, ValueSet<'a>)>),
}

/// The values of one dimension of a key. A span is asked, not written out.
struct ValueSet<'a> {
    values: HashSet<&'a str>,
    spans: Vec<SpanWindows<'a>>,
}

impl<'a> ValueSet<'a> {
    fn of_values(values: &'a [String]) -> Self {
        Self {
            values: values.iter().map(String::as_str).collect(),
            spans: Vec::new(),
        }
    }

    fn of_items(items: &'a [KeyItem]) -> Self {
        let mut set = Self {
            values: HashSet::new(),
            spans: Vec::new(),
        };
        for item in items {
            match item {
                KeyItem::Value(value) => {
                    set.values.insert(value.as_str());
                }
                KeyItem::Span(span) => set.spans.extend(span.prepared()),
            }
        }
        set
    }

    fn contains(&self, value: &str) -> bool {
        self.values.contains(value) || self.spans.iter().any(|span| span.contains(value))
    }
}

impl<'a> NamedPartitions<'a> {
    pub fn new(keys: &'a [PartitionKey]) -> Self {
        let mut named = Self {
            single: HashSet::new(),
            batched: Vec::new(),
        };
        for key in keys {
            named.add(key);
        }
        named
    }

    fn add(&mut self, key: &'a PartitionKey) {
        match key {
            PartitionKey::Set { keys } => keys.iter().for_each(|member| self.add(member)),
            // A key with items is not equal to the partition that it names,
            // also when it names only one.
            PartitionKey::SingleItems { items } => self
                .batched
                .push(Batched::Single(ValueSet::of_items(items))),
            PartitionKey::MultiItems { dims } => self.batched.push(Batched::Multi(
                dims.iter()
                    .map(|(name, items)| (name.as_str(), ValueSet::of_items(items)))
                    .collect(),
            )),
            _ if key.member_count() == 1 => {
                self.single.insert(key);
            }
            PartitionKey::Single { keys } => self
                .batched
                .push(Batched::Single(ValueSet::of_values(keys))),
            PartitionKey::Multi { dims } => self.batched.push(Batched::Multi(
                dims.iter()
                    .map(|(name, values)| (name.as_str(), ValueSet::of_values(values)))
                    .collect(),
            )),
        }
    }

    /// Whether `partition`, a key that names one partition, is one of them.
    pub fn contains(&self, partition: &PartitionKey) -> bool {
        self.single.contains(partition) || self.batched.iter().any(|key| key.contains(partition))
    }
}

impl Batched<'_> {
    fn contains(&self, partition: &PartitionKey) -> bool {
        match (self, partition) {
            (Self::Single(values), PartitionKey::Single { keys }) => {
                matches!(keys.as_slice(), [value] if values.contains(value))
            }
            (
                Self::Multi(dims),
                PartitionKey::Multi {
                    dims: partition_dims,
                },
            ) => {
                dims.len() == partition_dims.len()
                    && partition_dims.iter().all(|(name, values)| {
                        let [value] = values.as_slice() else {
                            return false;
                        };
                        dims.iter()
                            .any(|(dim, allowed)| dim == name && allowed.contains(value))
                    })
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(values: &[&str]) -> PartitionKey {
        PartitionKey::Single {
            keys: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn multi(dims: &[(&str, &[&str])]) -> PartitionKey {
        PartitionKey::Multi {
            dims: dims
                .iter()
                .map(|(name, values)| {
                    (
                        name.to_string(),
                        values.iter().map(|v| v.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn a_key_with_several_values_names_each_of_them() {
        let keys = [single(&["a", "b"]), single(&["c"])];
        let named = NamedPartitions::new(&keys);

        for value in ["a", "b", "c"] {
            assert!(named.contains(&single(&[value])), "{value}");
        }
        assert!(!named.contains(&single(&["d"])));
    }

    #[test]
    fn a_multi_key_names_each_combination_of_its_values() {
        let keys = [multi(&[("date", &["d1", "d2"]), ("region", &["eu", "us"])])];
        let named = NamedPartitions::new(&keys);

        assert!(named.contains(&multi(&[("date", &["d2"]), ("region", &["eu"])])));
        assert!(named.contains(&multi(&[("region", &["us"]), ("date", &["d1"])])));
        assert!(!named.contains(&multi(&[("date", &["d3"]), ("region", &["eu"])])));
        assert!(!named.contains(&multi(&[("date", &["d1"])])));
        assert!(!named.contains(&single(&["d1"])));
    }

    #[test]
    fn a_set_names_the_partitions_of_its_keys() {
        let keys = [PartitionKey::Set {
            keys: vec![
                single(&["a"]),
                multi(&[("date", &["d1", "d2"]), ("region", &["eu"])]),
            ],
        }];
        let named = NamedPartitions::new(&keys);

        assert!(named.contains(&single(&["a"])));
        assert!(named.contains(&multi(&[("date", &["d2"]), ("region", &["eu"])])));
        assert!(!named.contains(&single(&["b"])));
    }

    fn hours(first: &str, last: &str) -> KeyItem {
        let step = crate::storage::SpanStep::Cron("0 * * * *".into());
        KeyItem::Span(crate::storage::KeySpan::new(first, last, step, "%Y-%m-%d-%H:%M").unwrap())
    }

    #[test]
    fn a_span_names_its_windows_without_writing_them_out() {
        let keys = [
            PartitionKey::MultiItems {
                dims: vec![
                    (
                        "hour".into(),
                        vec![
                            hours("2024-01-01-00:00", "2024-06-30-23:00"),
                            KeyItem::Value("2024-08-01-00:00".into()),
                        ],
                    ),
                    ("zone".into(), vec![KeyItem::Value("132".into())]),
                ],
            },
            PartitionKey::SingleItems {
                items: vec![hours("2025-01-01-00:00", "2025-01-01-02:00")],
            },
        ];
        let named = NamedPartitions::new(&keys);
        let at = |hour: &str, zone: &str| multi(&[("hour", &[hour]), ("zone", &[zone])]);

        assert!(named.contains(&at("2024-01-01-00:00", "132")));
        assert!(named.contains(&at("2024-03-10-07:00", "132")));
        assert!(named.contains(&at("2024-06-30-23:00", "132")));
        assert!(named.contains(&at("2024-08-01-00:00", "132")));
        assert!(named.contains(&single(&["2025-01-01-01:00"])));

        assert!(!named.contains(&at("2024-07-01-00:00", "132")));
        assert!(!named.contains(&at("2024-03-10-07:30", "132")));
        assert!(!named.contains(&at("2024-03-10-07:00", "7")));
        assert!(!named.contains(&single(&["2024-03-10-07:00"])));
        assert!(!named.contains(&single(&["2025-01-01-03:00"])));
    }

    #[test]
    fn a_key_with_items_that_names_one_partition_names_that_partition() {
        let keys = [PartitionKey::SingleItems {
            items: vec![KeyItem::Value("a".into())],
        }];
        let named = NamedPartitions::new(&keys);

        assert!(named.contains(&single(&["a"])));
        assert!(!named.contains(&single(&["b"])));
    }

    #[test]
    fn no_keys_name_no_partition() {
        assert!(!NamedPartitions::new(&[]).contains(&single(&["a"])));
    }
}
