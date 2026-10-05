use std::collections::HashMap;

use crate::types::{AssetDefinitionInfo, PartitionDefinitionInfo, PartitionDimensionInfo};

pub(super) fn make_info(asset_key: &str, keys: Option<&[&str]>) -> AssetDefinitionInfo {
    AssetDefinitionInfo {
        asset_key: asset_key.to_string(),
        description: None,
        partition_def: keys.map(|ks| PartitionDefinitionInfo {
            kind: "Static".to_string(),
            keys: ks.iter().map(|s| s.to_string()).collect(),
            dimensions: vec![],
            total_count: ks.len() as u64,
            keys_truncated: false,
            dynamic_name: String::new(),
        }),
        hooks: vec![],
        io_handler: None,
        has_self_dependency: false,
        is_external: false,
        automation_condition: None,
        tags: vec![],
        kinds: vec![],
        group: None,
        code_version: None,
        asset_type: "asset".to_string(),
        actions: vec![],
        config_schema: None,
        metadata: Default::default(),
    }
}

pub(super) fn make_multi(asset_key: &str, dims: &[(&str, &[&str])]) -> AssetDefinitionInfo {
    let mut info = make_info(asset_key, None);
    info.partition_def = Some(PartitionDefinitionInfo {
        kind: "Multi".to_string(),
        keys: vec![],
        dimensions: dims
            .iter()
            .map(|(name, ks)| PartitionDimensionInfo {
                name: name.to_string(),
                keys: ks.iter().map(|s| s.to_string()).collect(),
                total_count: ks.len() as u64,
                keys_truncated: false,
            })
            .collect(),
        total_count: 0,
        keys_truncated: false,
        dynamic_name: String::new(),
    });
    info
}

pub(super) fn make_map(items: Vec<AssetDefinitionInfo>) -> HashMap<String, AssetDefinitionInfo> {
    items
        .into_iter()
        .map(|i| (i.asset_key.clone(), i))
        .collect()
}

pub(super) fn with_actions(asset_key: &str, verbs: &[(&str, &str)]) -> AssetDefinitionInfo {
    let mut info = make_info(asset_key, None);
    info.actions = verbs
        .iter()
        .map(|(name, outcome)| crate::types::AssetActionInfo {
            name: name.to_string(),
            outcome: outcome.to_string(),
            exclusive: false,
            partitioning: "required".to_string(),
            description: None,
            config_schema: None,
        })
        .collect();
    info
}

pub(super) fn partitioned_with(
    asset_key: &str,
    verb: &str,
    outcome: &str,
    partitioning: &str,
) -> AssetDefinitionInfo {
    let mut info = make_info(asset_key, Some(&["p1", "p2"]));
    info.actions = vec![crate::types::AssetActionInfo {
        name: verb.to_string(),
        outcome: outcome.to_string(),
        exclusive: true,
        partitioning: partitioning.to_string(),
        description: None,
        config_schema: None,
    }];
    info
}
