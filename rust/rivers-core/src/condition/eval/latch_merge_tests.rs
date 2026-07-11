use super::*;

fn scope<'a>(
    prev: &'a HashMap<String, HashMap<u32, PartitionSelection>>,
    acc: &'a mut HashMap<String, HashMap<u32, PartitionSelection>>,
) -> DepScope<'a, PartitionSelection> {
    DepScope {
        prev,
        acc,
        cur_prev: None,
        bridged: HashMap::new(),
    }
}

fn keys(s: &str) -> PartitionSelection {
    PartitionSelection::Keys(std::collections::HashSet::from([
        crate::storage::PartitionKey::Single {
            keys: vec![s.to_string()],
        },
    ]))
}

#[test]
fn bridged_write_never_clobbers_precise_latch() {
    let prev = HashMap::new();
    let mut acc = HashMap::new();
    let mut sc = scope(&prev, &mut acc);

    collect_dep_latch(&mut sc, "d", HashMap::from([(2u32, keys("d1"))]));
    collect_bridged_latch(
        &mut sc,
        "d",
        HashMap::from([(2u32, PartitionSelection::All)]),
    );
    assert_eq!(
        sc.acc["d"][&2],
        keys("d1"),
        "a bridged All must not widen a precise Keys latch"
    );

    collect_bridged_latch(
        &mut sc,
        "d",
        HashMap::from([(3u32, PartitionSelection::All)]),
    );
    collect_dep_latch(&mut sc, "d", HashMap::from([(3u32, keys("d2"))]));
    assert_eq!(sc.acc["d"][&3], keys("d2"));
    collect_bridged_latch(
        &mut sc,
        "d",
        HashMap::from([(3u32, PartitionSelection::Empty)]),
    );
    assert_eq!(sc.acc["d"][&3], keys("d2"));
}

#[test]
fn bridged_writes_union_instead_of_clobbering() {
    let prev = HashMap::new();
    let mut acc = HashMap::new();
    let mut sc = scope(&prev, &mut acc);

    collect_bridged_latch(
        &mut sc,
        "d",
        HashMap::from([(1u32, PartitionSelection::All)]),
    );
    collect_bridged_latch(
        &mut sc,
        "d",
        HashMap::from([(1u32, PartitionSelection::Empty)]),
    );
    assert_eq!(
        sc.acc["d"][&1],
        PartitionSelection::All,
        "a sibling's false latch must not erase a latched true"
    );
}
