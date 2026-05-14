//! Code repository — the top-level container for asset definitions.

use std::collections::{BTreeMap, HashMap};

use crate::assets::graph::{AssetGraph, NodeRef};

pub struct CodeRepository {
    pub(crate) assets: BTreeMap<String, Vec<NodeRef>>,
    pub graph: Option<AssetGraph>,
}

impl CodeRepository {
    pub fn new(assets: BTreeMap<String, Vec<NodeRef>>) -> Self {
        Self {
            assets,
            graph: None,
        }
    }

    pub fn assets(&self) -> &BTreeMap<String, Vec<NodeRef>> {
        &self.assets
    }

    pub fn add_assets(&mut self, assets: BTreeMap<String, Vec<NodeRef>>) {
        self.assets.extend(assets);
    }

    /// Sort items by the resolved graph's topological order (deps first).
    /// Items not present in the graph are appended at the end; if the graph is
    /// unresolved, items come out in arbitrary order.
    pub fn sort_topologically<T>(&self, mut items: HashMap<String, T>) -> Vec<T> {
        let mut result = Vec::with_capacity(items.len());
        if let Some(graph) = self.graph.as_ref() {
            // Edges point downstream→upstream; reverse toposort to put deps first.
            if let Ok(topo) = petgraph::algo::toposort(graph, None) {
                for idx in topo.into_iter().rev() {
                    let name = &graph[idx].name;
                    if let Some(item) = items.remove(name) {
                        result.push(item);
                    }
                }
            }
        }
        result.extend(items.into_values());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_repo(assets: Vec<(&str, Vec<&str>)>) -> CodeRepository {
        let mut map = BTreeMap::new();
        for (name, deps) in assets {
            map.insert(
                name.to_string(),
                deps.into_iter()
                    .map(|d| NodeRef::ByName(d.to_string()))
                    .collect(),
            );
        }
        let mut repo = CodeRepository::new(map);
        repo.resolve_asset_graph().unwrap();
        repo
    }

    #[test]
    fn test_sort_topologically_linear_chain() {
        let repo = make_repo(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let items: HashMap<String, &str> =
            HashMap::from([("c".into(), "C"), ("a".into(), "A"), ("b".into(), "B")]);
        let sorted = repo.sort_topologically(items);
        assert_eq!(sorted, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_sort_topologically_subset() {
        // a → b → c, only a and c have items: order is [a, c]
        let repo = make_repo(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let items: HashMap<String, &str> = HashMap::from([("c".into(), "C"), ("a".into(), "A")]);
        let sorted = repo.sort_topologically(items);
        assert_eq!(sorted, vec!["A", "C"]);
    }

    #[test]
    fn test_sort_topologically_diamond() {
        let repo = make_repo(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let items: HashMap<String, u32> = HashMap::from([
            ("d".into(), 4),
            ("a".into(), 1),
            ("c".into(), 3),
            ("b".into(), 2),
        ]);
        let sorted = repo.sort_topologically(items);
        assert_eq!(sorted[0], 1);
        assert_eq!(sorted[3], 4);
        assert!(sorted[1..3].contains(&2));
        assert!(sorted[1..3].contains(&3));
    }

    #[test]
    fn test_sort_topologically_no_graph() {
        let repo = CodeRepository::new(BTreeMap::new());
        let items: HashMap<String, &str> = HashMap::from([("x".into(), "X")]);
        let sorted = repo.sort_topologically(items);
        assert_eq!(sorted, vec!["X"]);
    }
}
