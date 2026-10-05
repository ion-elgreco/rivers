use super::*;

#[test]
fn test_partition_mapping_identity() {
    let m = PartitionMappingKind::Identity;
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_all_partitions() {
    let m = PartitionMappingKind::AllPartitions;
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    // map_to_downstream: any upstream change affects all downstream
    assert_eq!(m.map_to_downstream(&sel), PartitionSelection::All);
    // Empty input → Empty
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
}

#[test]
fn test_partition_mapping_static() {
    let m = PartitionMappingKind::Static {
        mapping: HashMap::from([("d1".into(), "u1".into()), ("d2".into(), "u2".into())]),
    };
    // Upstream u2 maps to downstream d2 plus its identity image u2 (a downstream key named
    // u2 forward-reads upstream u2); phantoms filtered against the universe.
    let sel2 = PartitionSelection::Keys(HashSet::from([spk("u2")]));
    assert_eq!(
        m.map_to_downstream(&sel2),
        PartitionSelection::Keys(HashSet::from([spk("d2"), spk("u2")]))
    );
}

#[test]
fn test_partition_mapping_static_identity_fallback() {
    // Partial Static map (d1 explicit, d2 unmapped): the runtime reads upstream d2 for
    // unmapped downstream d2 via identity, so an upstream d2 update must trigger downstream d2.
    let m = PartitionMappingKind::Static {
        mapping: HashMap::from([("d1".to_string(), "u1".to_string())]),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Keys(HashSet::from([spk("d2")]))),
        PartitionSelection::Keys(HashSet::from([spk("d2")])),
        "unmapped upstream d2 must identity-map to downstream d2"
    );
    // Both the explicit reverse mapping and the identity image fire: forward map_key applies
    // identity to any downstream key absent from the mapping keys, so a downstream named u1
    // reads upstream u1 too (spurious keys filtered against the universe).
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Keys(HashSet::from([spk("u1")]))),
        PartitionSelection::Keys(HashSet::from([spk("d1"), spk("u1")])),
        "explicit u1 -> d1 plus the identity image u1 -> u1"
    );
}

#[test]
fn test_partition_mapping_specific() {
    let m = PartitionMappingKind::SpecificPartitions {
        keys: vec!["latest".into()],
    };
    // Any upstream change affects all downstream
    let up = PartitionSelection::Keys(HashSet::from([spk("latest")]));
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_resolver_identity_passthrough() {
    let mappings = HashMap::from([(("b".into(), "a".into()), PartitionMappingKind::Identity)]);
    let upstream_keys =
        HashMap::from([("a".into(), HashSet::from([spk("p1"), spk("p2"), spk("p3")]))]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1"), spk("p2")]));
    assert_eq!(resolver.map_downstream("a", "b", &sel, None), sel);
}

#[test]
fn test_partition_resolver_no_mapping_is_identity() {
    // No mapping registered for edge (c, d) → identity passthrough
    let resolver = PartitionResolver::empty();
    let sel = PartitionSelection::Keys(HashSet::from([spk("p1")]));
    assert_eq!(resolver.map_downstream("d", "c", &sel, None), sel);
}

#[test]
fn test_unpartitioned_result_has_no_selection() {
    // When partitions is None, result.selection should be None
    let record = make_record("a");
    let records = HashMap::from([("a".into(), record.clone())]);
    let deps = HashMap::new();
    let ctx = make_ctx("a", &record, &records, &deps);
    let result = evaluate(&ConditionNode::Missing, &ctx);
    assert!(result.fired);
    assert!(result.selection.is_none());
    assert!(result.sub_selections.is_none());
}

#[test]
fn test_partition_mapping_multi_identity_dims() {
    // Multi mapping with identity per dimension: same-named dims
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let sel = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_multi_dimension_rename() {
    // Multi mapping with dimension rename: upstream "src_date" → downstream "date"
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "src_date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "src_region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("src_date", "2024-01-01"),
        ("src_region", "us"),
    ])]));
    let down = m.map_to_downstream(&upstream);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([mpk(&[
            ("date", "2024-01-01"),
            ("region", "us")
        ])]))
    );
}

#[test]
fn test_partition_mapping_multi_with_static_sub() {
    // Multi mapping with static per-dimension mapping on one dimension
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::Static {
                        mapping: HashMap::from([
                            ("north".into(), "us".into()),
                            ("europe".into(), "eu".into()),
                        ]),
                    }),
                ),
            ),
        ]),
    };
    // Upstream "eu" → downstream "europe" (explicit) plus the identity image "eu"
    // (a downstream region named "eu" reads upstream "eu"); spurious combos filtered against the universe.
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "eu"),
    ])]));
    let down = m.map_to_downstream(&up);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "europe")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ]))
    );
}

#[test]
fn test_partition_mapping_multi_many_to_one_sub_keeps_all_downstream_keys() {
    // A many-to-one per-dimension Static sub-mapping must reverse-map one upstream value
    // to both downstream keys (cartesian product), not just the first.
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::Static {
                        mapping: HashMap::from([
                            ("north".into(), "shared".into()),
                            ("south".into(), "shared".into()),
                        ]),
                    }),
                ),
            ),
        ]),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "shared"),
    ])]));
    let down = m.map_to_downstream(&up);
    assert_eq!(
        down,
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "north")]),
            mpk(&[("date", "2024-01-01"), ("region", "south")]),
            // Identity image: a downstream region named "shared" would forward-read upstream "shared" too.
            mpk(&[("date", "2024-01-01"), ("region", "shared")]),
        ]))
    );
}

#[test]
fn test_partition_mapping_multi_empty_and_all() {
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([(
            "d".into(),
            ("d".into(), Box::new(PartitionMappingKind::Identity)),
        )]),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::All),
        PartitionSelection::All
    );
}

#[test]
fn test_partition_mapping_multi_all_sub_expands_against_universe() {
    // A per-dimension AllPartitions sub-mapping fans that dimension out; with
    // the downstream universe available the mapping expands precisely instead
    // of escalating the whole selection to `All` (which over-materialized
    // every unrelated date).
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                (
                    "region".into(),
                    Box::new(PartitionMappingKind::AllPartitions),
                ),
            ),
        ]),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let universe = HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        mpk(&[("date", "2024-01-02"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]);
    assert_eq!(
        m.map_to_downstream_in(&up, Some(&universe)),
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "us")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ])),
        "the constrained date must limit the fan-out to that date's regions"
    );
    // Without a universe the over-approximation remains (never drop the key).
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_mapping_multi_to_single_all_sub_overapproximates_to_all() {
    // MultiToSingle whose inner fans the dimension in must over-approximate to `All`, not drop the key.
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "region".into(),
        inner: Box::new(PartitionMappingKind::AllPartitions),
    };
    let up = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(m.map_to_downstream(&up), PartitionSelection::All);
}

#[test]
fn test_partition_mapping_multi_multiple_keys() {
    let m = PartitionMappingKind::Multi {
        dimension_mappings: HashMap::from([
            (
                "date".into(),
                ("date".into(), Box::new(PartitionMappingKind::Identity)),
            ),
            (
                "region".into(),
                ("region".into(), Box::new(PartitionMappingKind::Identity)),
            ),
        ]),
    };
    let sel = PartitionSelection::Keys(HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]));
    // Identity per-dim → same keys
    assert_eq!(m.map_to_downstream(&sel), sel);
}

#[test]
fn test_partition_mapping_multi_to_single_extract_dimension() {
    // MultiToSingle: extract "date" from multi key
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };

    // Upstream is multi "date=2024-01-01|region=us" → downstream extracts "date" → "2024-01-01"
    let upstream = PartitionSelection::Keys(HashSet::from([
        mpk(&[("date", "2024-01-01"), ("region", "us")]),
        mpk(&[("date", "2024-01-02"), ("region", "eu")]),
    ]));
    let downstream = m.map_to_downstream(&upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("2024-01-01"), spk("2024-01-02")]))
    );
}

#[test]
fn test_partition_mapping_multi_to_single_with_static_inner() {
    // MultiToSingle with static inner mapping
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "region".into(),
        inner: Box::new(PartitionMappingKind::Static {
            mapping: HashMap::from([("north".into(), "us".into())]),
        }),
    };

    // Upstream region=us → downstream "north" (reverse of the explicit entry) plus the
    // identity image "us"; phantoms filtered against the universe.
    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let downstream = m.map_to_downstream(&upstream);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("north"), spk("us")]))
    );
}

#[test]
fn test_partition_mapping_multi_to_single_empty_and_all() {
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::Empty),
        PartitionSelection::Empty
    );
    assert_eq!(
        m.map_to_downstream(&PartitionSelection::All),
        PartitionSelection::All
    );
}

#[test]
fn test_partition_mapping_single_to_multi_expands_against_universe() {
    // MultiToSingle's other orientation: a Single-partitioned upstream feeding
    // a Multi-partitioned downstream. The upstream key constrains the named
    // dimension; the remaining dimensions expand against the downstream
    // universe.
    let m = PartitionMappingKind::MultiToSingle {
        dimension_name: "date".into(),
        inner: Box::new(PartitionMappingKind::Identity),
    };
    let universe = HashSet::from([
        mpk(&[("date", "2024-01-05"), ("region", "us")]),
        mpk(&[("date", "2024-01-05"), ("region", "eu")]),
        mpk(&[("date", "2024-01-06"), ("region", "us")]),
    ]);
    let upstream = PartitionSelection::Keys(HashSet::from([spk("2024-01-05")]));
    assert_eq!(
        m.map_to_downstream_in(&upstream, Some(&universe)),
        PartitionSelection::Keys(HashSet::from([
            mpk(&[("date", "2024-01-05"), ("region", "us")]),
            mpk(&[("date", "2024-01-05"), ("region", "eu")]),
        ])),
        "a Single upstream key must select every downstream Multi key matching the named dimension"
    );
    // Without a universe the fan-out must over-approximate, not drop the update.
    assert_eq!(m.map_to_downstream(&upstream), PartitionSelection::All);
    // An upstream key matching nothing downstream maps to Empty.
    assert_eq!(
        m.map_to_downstream_in(
            &PartitionSelection::Keys(HashSet::from([spk("2020-12-31")])),
            Some(&universe)
        ),
        PartitionSelection::Empty
    );
}

#[test]
fn test_resolver_multi_mapping() {
    let mappings = HashMap::from([(
        ("down".into(), "up".into()),
        PartitionMappingKind::Multi {
            dimension_mappings: HashMap::from([
                (
                    "date".into(),
                    ("date".into(), Box::new(PartitionMappingKind::Identity)),
                ),
                (
                    "region".into(),
                    ("region".into(), Box::new(PartitionMappingKind::Identity)),
                ),
            ]),
        },
    )]);
    let upstream_keys = HashMap::from([(
        "up".to_string(),
        HashSet::from([
            mpk(&[("date", "2024-01-01"), ("region", "us")]),
            mpk(&[("date", "2024-01-01"), ("region", "eu")]),
        ]),
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let sel = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    assert_eq!(resolver.map_downstream("up", "down", &sel, None), sel);
}

#[test]
fn test_resolver_multi_to_single_mapping() {
    let mappings = HashMap::from([(
        ("single_down".into(), "multi_up".into()),
        PartitionMappingKind::MultiToSingle {
            dimension_name: "date".into(),
            inner: Box::new(PartitionMappingKind::Identity),
        },
    )]);
    let upstream_keys = HashMap::from([(
        "multi_up".to_string(),
        HashSet::from([mpk(&[("date", "2024-01-01"), ("region", "us")])]),
    )]);
    let resolver = PartitionResolver::new(&mappings, &upstream_keys);

    let upstream = PartitionSelection::Keys(HashSet::from([mpk(&[
        ("date", "2024-01-01"),
        ("region", "us"),
    ])]));
    let downstream = resolver.map_downstream("multi_up", "single_down", &upstream, None);
    assert_eq!(
        downstream,
        PartitionSelection::Keys(HashSet::from([spk("2024-01-01")]))
    );
}
