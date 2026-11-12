//! The partitions that keys name, without a key object for each partition.

use std::collections::HashSet;

use super::PartitionKey;

/// The partitions that a list of keys names. A key can name many partitions;
/// this answers "is this partition one of them" without building them.
pub struct NamedPartitions<'a> {
    /// Keys that name one partition.
    single: HashSet<&'a PartitionKey>,
    /// Keys that name several.
    batched: Vec<Batched<'a>>,
}

enum Batched<'a> {
    Single(HashSet<&'a str>),
    Multi(Vec<(&'a str, HashSet<&'a str>)>),
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
            _ if key.member_count() == 1 => {
                self.single.insert(key);
            }
            PartitionKey::Single { keys } => self
                .batched
                .push(Batched::Single(keys.iter().map(String::as_str).collect())),
            PartitionKey::Multi { dims } => self.batched.push(Batched::Multi(
                dims.iter()
                    .map(|(name, values)| {
                        (name.as_str(), values.iter().map(String::as_str).collect())
                    })
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
                matches!(keys.as_slice(), [value] if values.contains(value.as_str()))
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
                            .any(|(dim, allowed)| dim == name && allowed.contains(value.as_str()))
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

    #[test]
    fn no_keys_name_no_partition() {
        assert!(!NamedPartitions::new(&[]).contains(&single(&["a"])));
    }
}
