//! Partition keys as PostgreSQL compares them.
//!
//! `jsonb` does not sort keys as [`stored_order`] does, so each row with a
//! partition key also has `partition_ord`: bytes that sort as the key sorts.
//! Two keys are equal exactly when their bytes are equal, so the column is
//! also the identity of a key in a unique index.

use crate::storage::{PartitionBounds, PartitionKey};

const MULTI: u8 = 0x10;
const SINGLE: u8 = 0x20;
const OTHER: u8 = 0x30;
/// Before each member of a list.
const MEMBER: u8 = 0x01;
/// The end of a list or of a string. A zero byte in a string becomes
/// `0x00 0xFF`, and no byte after an end is `0xFF`, so a string sorts before
/// each string it starts.
const END: u8 = 0x00;

fn push_str(out: &mut Vec<u8>, text: &str) {
    for &byte in text.as_bytes() {
        out.push(byte);
        if byte == END {
            out.push(0xFF);
        }
    }
    out.push(END);
}

fn push_strs(out: &mut Vec<u8>, values: &[String]) {
    for value in values {
        out.push(MEMBER);
        push_str(out, value);
    }
    out.push(END);
}

/// The bytes of `key` for the `partition_ord` column: they sort as
/// [`stored_order`](crate::storage::stored_order) sorts the keys with one or
/// more dimensions. The other keys sort after those, in no order of meaning.
pub(crate) fn partition_ord(key: &PartitionKey) -> Vec<u8> {
    let mut out = Vec::new();
    match key {
        PartitionKey::Multi { dims } => {
            let mut dims: Vec<&(String, Vec<String>)> = dims.iter().collect();
            dims.sort_by(|a, b| a.0.cmp(&b.0));
            out.push(MULTI);
            for (name, values) in dims {
                out.push(MEMBER);
                push_str(&mut out, name);
                push_strs(&mut out, values);
            }
            out.push(END);
        }
        PartitionKey::Single { keys } => {
            out.push(SINGLE);
            push_strs(&mut out, keys);
        }
        other => {
            out.push(OTHER);
            let canonical = serde_json::to_vec(&canonical(other));
            out.extend(canonical.expect("a partition key serializes to JSON"));
        }
    }
    out
}

/// `key` with its dimensions in the order of their names. The SurrealDB
/// backend stores a key in this form, so a key reads back in it.
fn canonical(key: &PartitionKey) -> PartitionKey {
    match key {
        PartitionKey::Multi { dims } => {
            let mut dims = dims.clone();
            dims.sort_by(|a, b| a.0.cmp(&b.0));
            PartitionKey::Multi { dims }
        }
        PartitionKey::MultiItems { dims } => {
            let mut dims = dims.clone();
            dims.sort_by(|a, b| a.0.cmp(&b.0));
            PartitionKey::MultiItems { dims }
        }
        PartitionKey::Set { keys } => PartitionKey::Set {
            keys: keys.iter().map(canonical).collect(),
        },
        other => other.clone(),
    }
}

/// A partition key as a `partition_key` column holds it.
pub(crate) fn key_json(key: &PartitionKey) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::to_value(canonical(key))?)
}

/// The bytes of the lower and the upper bound, both included.
pub(crate) fn bounds_ord(bounds: &PartitionBounds) -> (Vec<u8>, Vec<u8>) {
    (partition_ord(&bounds.lower), partition_ord(&bounds.upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::stored_order;

    fn single(keys: &[&str]) -> PartitionKey {
        PartitionKey::Single {
            keys: keys.iter().map(|k| k.to_string()).collect(),
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
    fn a_key_is_stored_with_its_dimensions_in_the_order_of_their_names() {
        let given = multi(&[("region", &["us"]), ("date", &["2024-01-01"])]);
        let sorted = multi(&[("date", &["2024-01-01"]), ("region", &["us"])]);
        assert_eq!(key_json(&given).unwrap(), key_json(&sorted).unwrap());

        let set = |member: &PartitionKey| PartitionKey::Set {
            keys: vec![member.clone()],
        };
        assert_eq!(
            key_json(&set(&given)).unwrap(),
            serde_json::to_value(set(&sorted)).unwrap()
        );
        assert_eq!(partition_ord(&set(&given)), partition_ord(&set(&sorted)));
    }

    #[test]
    fn bytes_sort_as_the_store_sorts_keys() {
        let keys = vec![
            single(&[]),
            single(&[""]),
            single(&["a"]),
            single(&["a", ""]),
            single(&["a", "b"]),
            single(&["a\0"]),
            single(&["a\0b"]),
            single(&["a\u{1}"]),
            single(&["ab"]),
            single(&["b"]),
            single(&["é"]),
            multi(&[]),
            multi(&[("date", &["2024-01-01"])]),
            multi(&[("date", &["2024-01-01"]), ("region", &["eu"])]),
            multi(&[("region", &["eu"]), ("date", &["2024-01-01"])]),
            multi(&[("date", &["2024-01-01"]), ("region", &["us"])]),
            multi(&[("date", &["2024-01-01", ""])]),
            multi(&[("date", &["2024-01-02"])]),
            multi(&[("dat", &["z"])]),
            multi(&[("date", &[])]),
            multi(&[("zone", &["001"]), ("hour", &["h00001"])]),
        ];
        for a in &keys {
            for b in &keys {
                assert_eq!(
                    partition_ord(a).cmp(&partition_ord(b)),
                    stored_order(a, b),
                    "{a:?} against {b:?}"
                );
                assert_eq!(
                    partition_ord(a) == partition_ord(b),
                    a == b,
                    "{a:?} against {b:?}"
                );
            }
        }
    }

    #[test]
    fn bounds_of_a_dimension_hold_the_keys_with_its_values() {
        let fixed = vec![("date".to_string(), "2024-01-01".to_string())];
        let bounds = PartitionBounds::of_dimension(&fixed, "region", "eu", "us");
        let (lower, upper) = bounds_ord(&bounds);
        let inside = |key: &PartitionKey| {
            let ord = partition_ord(key);
            lower <= ord && ord <= upper
        };
        for key in [
            multi(&[("date", &["2024-01-01"]), ("region", &["eu"])]),
            multi(&[("date", &["2024-01-01"]), ("region", &["us"])]),
            multi(&[
                ("date", &["2024-01-01"]),
                ("region", &["us"]),
                ("zone", &["1"]),
            ]),
            multi(&[("date", &["2024-01-01"]), ("region", &["dk"])]),
            multi(&[("date", &["2024-01-02"]), ("region", &["eu"])]),
        ] {
            assert_eq!(inside(&key), bounds.contains(&key), "{key:?}");
        }
    }

    #[test]
    fn keys_with_spans_keep_their_identity() {
        let a = PartitionKey::Set {
            keys: vec![single(&["a"])],
        };
        let b = PartitionKey::Set {
            keys: vec![single(&["b"])],
        };
        assert_ne!(partition_ord(&a), partition_ord(&b));
        assert!(partition_ord(&single(&["zzz"])) < partition_ord(&a));
    }
}
