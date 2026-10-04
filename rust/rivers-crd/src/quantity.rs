//! Kubernetes quantities (`10Gi`, `500M`, `1e9`) as the API server reads
//! them, for the sizes the operator copies into PVCs and `emptyDir`s.
//! Not `kube_quantity` (which rivers-k8s uses for arithmetic): its parser
//! takes `1e3Ki` and `++1`, which the API server refuses, and refuses `1E`
//! and `1Ei`, which it takes — and a CRD `pattern` needs the regex anyway.

/// Schema `pattern` of [`is_positive`] for CRD fields, except that it takes
/// an exponent of any size.
pub const POSITIVE_PATTERN: &str = concat!(
    r"^\+?([0-9]*[1-9][0-9]*(\.[0-9]*)?|[0-9]*\.[0-9]*[1-9][0-9]*)",
    r"([numkMGTPE]|[KMGTPE]i|[eE][+-]?[0-9]+)?$",
);

const SUFFIXES: [&str; 15] = [
    "n", "u", "m", "k", "M", "G", "T", "P", "E", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei",
];

/// `s` is a quantity more than zero by the grammar of the API server's
/// `resource.ParseQuantity`: an optional sign, digits with an optional `.`,
/// and a suffix (`n` to `E`, `Ki` to `Ei`, or `e` / `E` and a whole number
/// that fits in 64 bits) or none. Spaces around it, which the API server's
/// JSON decoder drops, are refused.
pub fn is_positive(s: &str) -> bool {
    let (negative, rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let digits_after =
        |start: usize| start + rest[start..].bytes().take_while(u8::is_ascii_digit).count();
    let whole = digits_after(0);
    let end = if rest[whole..].starts_with('.') {
        digits_after(whole + 1)
    } else {
        whole
    };
    let more_than_zero = rest[..end].bytes().any(|b| matches!(b, b'1'..=b'9'));
    let suffix = &rest[end..];
    let exponent =
        suffix.len() > 1 && suffix.starts_with(['e', 'E']) && suffix[1..].parse::<i64>().is_ok();
    !negative && more_than_zero && (suffix.is_empty() || SUFFIXES.contains(&suffix) || exponent)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Strings a kube-apiserver (v1.33) takes as a PVC's storage request:
    /// quantities more than zero.
    const MORE_THAN_ZERO: &[&str] = &[
        "1",
        "10Gi",
        "500M",
        "1e9",
        "1E9",
        "1e+9",
        "1e-3",
        "1.5Gi",
        "0.5Ki",
        ".5",
        "5.",
        "+1",
        "007",
        "0.001",
        "100m",
        "1n",
        "1u",
        "1k",
        "1M",
        "1G",
        "1T",
        "1P",
        "1E",
        "1Ki",
        "1Mi",
        "1Ti",
        "1Pi",
        "1Ei",
        "1e0",
        "1e05",
        "1.e5",
        "5000000000",
        "1e-9223372036854775808",
        "1e9223372036854775807",
    ];

    /// Strings it refuses as that request, but for `"1 "` and `" 1"`, whose
    /// spaces its JSON decoder drops.
    const OTHERS: &[&str] = &[
        "",
        "0",
        "-1",
        "-1Gi",
        "0Gi",
        "0.0",
        ".",
        "+",
        "-",
        "Gi",
        "k",
        "e5",
        "5GB",
        "abc",
        "1e",
        "1Ee5",
        "1KI",
        "1K",
        "1mi",
        "1gi",
        "1.5.5",
        "1e3Ki",
        "1e1.5",
        "1 ",
        " 1",
        "1 Gi",
        "1_000",
        "0x10",
        "++1",
        "+-1",
        "1e9223372036854775808",
        "1e-9223372036854775809",
        "1E+",
        "1e-",
        "١",
    ];

    #[test]
    fn reads_quantities_like_the_api_server() {
        for s in MORE_THAN_ZERO {
            assert!(is_positive(s), "{s:?} is more than zero");
        }
        for s in OTHERS {
            assert!(!is_positive(s), "{s:?} is not");
        }
    }

    #[test]
    fn the_schema_pattern_reads_quantities_like_is_positive() {
        let pattern = regex::Regex::new(POSITIVE_PATTERN).unwrap();
        let unbounded_exponents = ["1e9223372036854775808", "1e-9223372036854775809"];
        for s in MORE_THAN_ZERO.iter().chain(OTHERS) {
            let expected = is_positive(s) || unbounded_exponents.contains(s);
            assert_eq!(pattern.is_match(s), expected, "{s:?}");
        }
    }
}
