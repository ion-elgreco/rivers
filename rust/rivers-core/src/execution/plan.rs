//! Execution plan and step types.

use std::collections::{HashMap, HashSet};

use crate::assets::graph::{AssetGraph, ancestors, descendants};
use crate::composition::InvocationKind;

#[derive(Debug, Clone, Default, PartialEq)]
pub enum StepKind {
    #[default]
    Normal,
    /// Fan-out: run once per element of the source's output. N determined at runtime.
    Mapped {
        fan_out_source: String,
        max_concurrency: Option<usize>,
    },
    /// Barrier: downstream task receives `list[T]`.
    Collect { mapped_step: String },
    /// Streaming: downstream task receives `Generator[T]`.
    CollectStream { mapped_step: String, ordered: bool },
}

impl From<&InvocationKind> for StepKind {
    fn from(kind: &InvocationKind) -> Self {
        match kind {
            InvocationKind::Normal => StepKind::Normal,
            InvocationKind::Map {
                source_node,
                max_concurrency,
                ..
            } => StepKind::Mapped {
                fan_out_source: source_node.clone(),
                max_concurrency: *max_concurrency,
            },
            InvocationKind::Collect { mapped_node } => StepKind::Collect {
                mapped_step: mapped_node.clone(),
            },
            InvocationKind::CollectStream {
                mapped_node,
                ordered,
            } => StepKind::CollectStream {
                mapped_step: mapped_node.clone(),
                ordered: *ordered,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionStep {
    pub name: String,
    pub kind: StepKind,
    /// All output names from a multi-asset step; empty for single-output steps.
    pub outputs: Vec<String>,
    /// Dependencies within the execution plan (used for ordering and skip-on-failure).
    pub plan_dependencies: Vec<String>,
    /// Superset of `plan_dependencies` — used for provenance (`input_data_versions`).
    pub graph_dependencies: Vec<String>,
}

impl ExecutionStep {
    /// Names to emit events for: all outputs for multi-asset steps, just the step name otherwise.
    pub fn event_names(&self) -> &[String] {
        if self.outputs.is_empty() {
            std::slice::from_ref(&self.name)
        } else {
            &self.outputs
        }
    }
}

/// Asset order within an action run. Ordering bounds partial failure — it
/// decides which half-completed states are reachable — it cannot make a
/// multi-asset action atomic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActionOrdering {
    /// Targets are independent (optimize, merge).
    #[default]
    Unordered,
    /// Upstream targets first.
    UpstreamFirst,
    /// Downstream targets first (delete) — an asset exists only while its
    /// upstream does, the LIFO rule of FK teardown.
    DownstreamFirst,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub steps: Vec<ExecutionStep>,
    /// The verb every step executes. `None` means materialize — read it
    /// through [`ExecutionPlan::verb`] / [`ExecutionPlan::is_action`] rather
    /// than matching the field, so the default reads as a decision. Every
    /// executor backend must make that decision: a backend that ignores the
    /// verb silently materializes whatever it was handed.
    pub action: Option<String>,
}

impl ExecutionPlan {
    /// The verb this plan runs, or `None` for materialize.
    pub fn verb(&self) -> Option<&str> {
        self.action.as_deref()
    }

    /// True when this plan runs a named action rather than materializing.
    pub fn is_action(&self) -> bool {
        self.action.is_some()
    }

    /// True when this plan materializes (no named verb).
    pub fn is_materialize(&self) -> bool {
        self.action.is_none()
    }
    /// Build an execution plan from a resolved asset graph (deps before dependents).
    #[tracing::instrument(skip_all, target = "rivers::execution")]
    pub fn from_graph(graph: &AssetGraph) -> Self {
        let order =
            petgraph::algo::toposort(graph, None).expect("Graph has cycles (invalid dependencies)");

        let steps: Vec<ExecutionStep> = order
            .into_iter()
            .rev()
            .map(|idx| {
                let node = &graph[idx];
                let deps: Vec<String> = graph
                    .neighbors_directed(idx, petgraph::Direction::Outgoing)
                    .map(|dep_idx| graph[dep_idx].name.clone())
                    .collect();
                ExecutionStep {
                    name: node.name.clone(),
                    kind: StepKind::Normal,
                    outputs: Vec::new(),
                    plan_dependencies: deps.clone(),
                    graph_dependencies: deps,
                }
            })
            .collect();

        Self {
            steps,
            action: None,
        }
    }

    /// Build a plan that runs `action` once per target, without pulling in
    /// upstream nodes — an action operates on existing data and never invokes
    /// the producing function. `ordering` sequences targets that are related
    /// in the asset graph; unrelated targets stay parallel.
    #[tracing::instrument(skip_all, target = "rivers::execution", fields(targets = targets.len()))]
    pub fn for_action(
        graph: &AssetGraph,
        action: &str,
        targets: &[String],
        ordering: ActionOrdering,
    ) -> Self {
        // Collapse duplicates (first occurrence wins): one step per unique
        // target, or a repeated name runs the verb once per occurrence. The
        // materialize path dedups by construction in `from_subgraph`.
        let mut seen = HashSet::new();
        let targets: Vec<String> = targets
            .iter()
            .filter(|t| seen.insert(t.as_str()))
            .cloned()
            .collect();

        // One transitive-closure walk per target, O(T·(V+E)) overall — pairwise
        // reachability probes made DownstreamFirst T·(T−1)² whole-graph
        // walks: minutes of pure planning at a few hundred targets, re-run per
        // backfill child under the repository's state read lock.
        let related_targets = |closure: HashSet<String>| -> Vec<String> {
            targets
                .iter()
                .filter(|t| closure.contains(t.as_str()))
                .cloned()
                .collect()
        };

        let steps = targets
            .iter()
            .map(|target| {
                let plan_dependencies = match ordering {
                    ActionOrdering::Unordered => Vec::new(),
                    // Wait for every related target upstream of this one.
                    ActionOrdering::UpstreamFirst => related_targets(ancestors(graph, target)),
                    // Wait for every related target this one is upstream of
                    // (its dependents).
                    ActionOrdering::DownstreamFirst => {
                        related_targets(descendants(graph, target))
                    }
                };
                ExecutionStep {
                    name: target.clone(),
                    kind: StepKind::Normal,
                    outputs: Vec::new(),
                    plan_dependencies,
                    graph_dependencies: Vec::new(),
                }
            })
            .collect();

        Self {
            steps,
            action: Some(action.to_string()),
        }
    }

    /// Build an execution plan from a subset of nodes, grouping multi-asset outputs
    /// into single steps during construction.
    ///
    /// `groups` maps each output node name to a group key (the multi-asset name).
    /// Outputs sharing the same group collapse into one step: the first output
    /// encountered (in topo order) becomes the step name, and all output names
    /// land in `step.outputs`. Downstream `plan_dependencies` pointing to grouped
    /// outputs are rewritten to the group's step name; `graph_dependencies`
    /// preserve original output names for provenance.
    #[tracing::instrument(skip_all, target = "rivers::execution", fields(selection_size = asset_names.len()))]
    pub fn from_subgraph(
        graph: &AssetGraph,
        asset_names: &HashSet<String>,
        groups: &HashMap<String, String>,
        composition_order: &HashMap<String, usize>,
    ) -> Self {
        let order =
            petgraph::algo::toposort(graph, None).expect("Graph has cycles (invalid dependencies)");

        let mut group_step_idx: HashMap<&str, usize> = HashMap::new();
        // Rewrite map: grouped output_name -> the group's step name.
        let mut output_to_step: HashMap<String, String> = HashMap::new();
        let mut steps: Vec<ExecutionStep> = Vec::new();

        for idx in order.into_iter().rev() {
            let node_name = &graph[idx].name;
            if !asset_names.contains(node_name) {
                continue;
            }

            let neighbors: Vec<String> = graph
                .neighbors_directed(idx, petgraph::Direction::Outgoing)
                .map(|dep_idx| graph[dep_idx].name.clone())
                .collect();

            let plan_dependencies: Vec<String> = neighbors
                .iter()
                .filter(|n| asset_names.contains(n.as_str()))
                .map(|n| output_to_step.get(n).cloned().unwrap_or_else(|| n.clone()))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            if let Some(group_key) = groups.get(node_name) {
                if let Some(&existing_idx) = group_step_idx.get(group_key.as_str()) {
                    let step = &mut steps[existing_idx];
                    step.outputs.push(node_name.clone());
                    // Union deps, dropping intra-group references.
                    for dep in &plan_dependencies {
                        if dep != &step.name
                            && !step.outputs.contains(dep)
                            && !step.plan_dependencies.contains(dep)
                        {
                            step.plan_dependencies.push(dep.clone());
                        }
                    }
                    for dep in &neighbors {
                        if !step.outputs.contains(dep)
                            && dep != &step.name
                            && !step.graph_dependencies.contains(dep)
                        {
                            step.graph_dependencies.push(dep.clone());
                        }
                    }
                    output_to_step.insert(node_name.clone(), step.name.clone());
                } else {
                    let step_name = node_name.clone();
                    let step_idx = steps.len();
                    output_to_step.insert(node_name.clone(), step_name.clone());
                    group_step_idx.insert(group_key.as_str(), step_idx);
                    steps.push(ExecutionStep {
                        name: step_name,
                        kind: StepKind::Normal,
                        outputs: vec![node_name.clone()],
                        plan_dependencies,
                        graph_dependencies: neighbors,
                    });
                }
            } else {
                steps.push(ExecutionStep {
                    name: node_name.clone(),
                    kind: StepKind::Normal,
                    outputs: Vec::new(),
                    plan_dependencies,
                    graph_dependencies: neighbors,
                });
            }
        }

        // Preserve the order tasks were written in graph asset bodies within each
        // dependency level. Steps without a composition hint sort to the end.
        if !composition_order.is_empty() {
            let mut step_levels: HashMap<String, usize> = HashMap::new();
            for step in &steps {
                let level = if step.plan_dependencies.is_empty() {
                    0
                } else {
                    step.plan_dependencies
                        .iter()
                        .map(|dep| step_levels.get(dep).copied().unwrap_or(0) + 1)
                        .max()
                        .unwrap_or(0)
                };
                step_levels.insert(step.name.clone(), level);
                for output in &step.outputs {
                    step_levels.insert(output.clone(), level);
                }
            }

            steps.sort_by(|a, b| {
                let level_a = step_levels.get(&a.name).copied().unwrap_or(0);
                let level_b = step_levels.get(&b.name).copied().unwrap_or(0);
                level_a.cmp(&level_b).then_with(|| {
                    // Only apply composition order between steps in the same graph
                    // (matching prefix before '/').
                    let prefix_a = a.name.split('/').next();
                    let prefix_b = b.name.split('/').next();
                    let same_graph =
                        a.name.contains('/') && b.name.contains('/') && prefix_a == prefix_b;
                    if same_graph {
                        let order_a = composition_order
                            .get(&a.name)
                            .copied()
                            .unwrap_or(usize::MAX);
                        let order_b = composition_order
                            .get(&b.name)
                            .copied()
                            .unwrap_or(usize::MAX);
                        order_a.cmp(&order_b)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            });
        }

        Self {
            steps,
            action: None,
        }
    }

    /// Apply step kinds from a map; steps not in the map remain Normal.
    pub fn apply_fan_out_kinds(&mut self, kinds: &HashMap<String, StepKind>) {
        for step in &mut self.steps {
            if let Some(kind) = kinds.get(&step.name) {
                step.kind = kind.clone();
            }
        }
    }

    /// Effective asset names — all outputs for multi-asset steps, the step name otherwise.
    pub fn all_asset_names(&self) -> Vec<String> {
        self.steps
            .iter()
            .flat_map(|s| s.event_names())
            .cloned()
            .collect()
    }

    /// Group steps into levels for parallel execution; level-N steps depend only on 0..N-1.
    ///
    /// Each step's level is resolved by descending into its dependencies first,
    /// so the result does not depend on the order `steps` happens to be in.
    /// `from_graph`/`from_subgraph` emit topologically; `for_action` emits in
    /// target order, which the caller derives from a HashMap.
    pub fn group_steps_by_level(&self) -> Vec<Vec<usize>> {
        // Multi-asset outputs resolve to their owning step.
        let mut by_name: HashMap<&str, usize> = HashMap::new();
        for (idx, step) in self.steps.iter().enumerate() {
            by_name.insert(step.name.as_str(), idx);
            for output in &step.outputs {
                by_name.insert(output.as_str(), idx);
            }
        }

        // Post-order DFS. A dep that is unknown, self-referential, or a
        // back-edge counts as level 0, which bounds a malformed plan to
        // finite work rather than a hang.
        let mut level_of: Vec<Option<usize>> = vec![None; self.steps.len()];
        let mut on_stack = vec![false; self.steps.len()];
        for root in 0..self.steps.len() {
            if level_of[root].is_some() {
                continue;
            }
            let mut stack = vec![(root, false)];
            while let Some((idx, settle)) = stack.pop() {
                if settle {
                    level_of[idx] = Some(
                        self.steps[idx]
                            .plan_dependencies
                            .iter()
                            .map(|dep| {
                                by_name
                                    .get(dep.as_str())
                                    .and_then(|&d| level_of[d])
                                    .unwrap_or(0)
                                    + 1
                            })
                            .max()
                            .unwrap_or(0),
                    );
                    on_stack[idx] = false;
                    continue;
                }
                if level_of[idx].is_some() || on_stack[idx] {
                    continue;
                }
                on_stack[idx] = true;
                stack.push((idx, true));
                for dep in &self.steps[idx].plan_dependencies {
                    if let Some(&d) = by_name.get(dep.as_str())
                        && level_of[d].is_none()
                        && !on_stack[d]
                    {
                        stack.push((d, false));
                    }
                }
            }
        }

        let mut levels: Vec<Vec<usize>> = Vec::new();
        for (idx, level) in level_of.iter().enumerate() {
            let level = level.unwrap_or(0);
            while levels.len() <= level {
                levels.push(Vec::new());
            }
            levels[level].push(idx);
        }

        levels
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::graph::NodeRef;
    use crate::repo::CodeRepository;
    use std::collections::BTreeMap;

    fn make_graph(assets: Vec<(&str, Vec<&str>)>) -> AssetGraph {
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
        repo.graph.unwrap()
    }

    #[test]
    fn test_step_kind_from_invocation_normal() {
        let kind = InvocationKind::Normal;
        assert_eq!(StepKind::from(&kind), StepKind::Normal);
    }

    #[test]
    fn test_step_kind_from_invocation_map() {
        let kind = InvocationKind::Map {
            source_node: "src".to_string(),
            source_output: "result".to_string(),
            max_concurrency: Some(4),
        };
        assert_eq!(
            StepKind::from(&kind),
            StepKind::Mapped {
                fan_out_source: "src".to_string(),
                max_concurrency: Some(4),
            }
        );
    }

    #[test]
    fn test_step_kind_from_invocation_collect() {
        let kind = InvocationKind::Collect {
            mapped_node: "mapped".to_string(),
        };
        assert_eq!(
            StepKind::from(&kind),
            StepKind::Collect {
                mapped_step: "mapped".to_string(),
            }
        );
    }

    #[test]
    fn test_step_kind_from_invocation_collect_stream() {
        let kind = InvocationKind::CollectStream {
            mapped_node: "mapped".to_string(),
            ordered: true,
        };
        assert_eq!(
            StepKind::from(&kind),
            StepKind::CollectStream {
                mapped_step: "mapped".to_string(),
                ordered: true,
            }
        );
    }

    #[test]
    fn test_event_names_single_output() {
        let step = ExecutionStep {
            name: "my_step".to_string(),
            kind: StepKind::Normal,
            outputs: vec![],
            plan_dependencies: vec![],
            graph_dependencies: vec![],
        };
        assert_eq!(step.event_names(), &["my_step".to_string()]);
    }

    #[test]
    fn test_event_names_multi_output() {
        let step = ExecutionStep {
            name: "my_step".to_string(),
            kind: StepKind::Normal,
            outputs: vec!["out_a".to_string(), "out_b".to_string()],
            plan_dependencies: vec![],
            graph_dependencies: vec![],
        };
        assert_eq!(
            step.event_names(),
            &["out_a".to_string(), "out_b".to_string()]
        );
    }

    #[test]
    fn test_from_graph_linear_chain() {
        // a -> b -> c (a is root, c depends on b, b depends on a)
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let plan = ExecutionPlan::from_graph(&graph);

        assert_eq!(plan.steps.len(), 3);
        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        let a_pos = names.iter().position(|&n| n == "a").unwrap();
        let b_pos = names.iter().position(|&n| n == "b").unwrap();
        let c_pos = names.iter().position(|&n| n == "c").unwrap();
        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);

        let step_a = plan.steps.iter().find(|s| s.name == "a").unwrap();
        assert!(step_a.plan_dependencies.is_empty());
        assert_eq!(step_a.kind, StepKind::Normal);
        assert!(step_a.outputs.is_empty());

        let step_b = plan.steps.iter().find(|s| s.name == "b").unwrap();
        assert_eq!(step_b.plan_dependencies, vec!["a".to_string()]);
        assert_eq!(step_b.graph_dependencies, vec!["a".to_string()]);

        let step_c = plan.steps.iter().find(|s| s.name == "c").unwrap();
        assert_eq!(step_c.plan_dependencies, vec!["b".to_string()]);
    }

    #[test]
    fn test_from_graph_diamond() {
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let plan = ExecutionPlan::from_graph(&graph);
        assert_eq!(plan.steps.len(), 4);

        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        let a_pos = names.iter().position(|&n| n == "a").unwrap();
        let d_pos = names.iter().position(|&n| n == "d").unwrap();
        assert!(a_pos < d_pos);

        let step_d = plan.steps.iter().find(|s| s.name == "d").unwrap();
        let mut d_deps = step_d.plan_dependencies.clone();
        d_deps.sort();
        assert_eq!(d_deps, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_from_graph_single_node() {
        let graph = make_graph(vec![("a", vec![])]);
        let plan = ExecutionPlan::from_graph(&graph);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].name, "a");
        assert!(plan.steps[0].plan_dependencies.is_empty());
        assert!(plan.steps[0].graph_dependencies.is_empty());
        assert_eq!(plan.steps[0].kind, StepKind::Normal);
    }

    #[test]
    fn test_from_subgraph_filters_to_selection() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let selection: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let plan =
            ExecutionPlan::from_subgraph(&graph, &selection, &HashMap::new(), &HashMap::new());

        assert_eq!(plan.steps.len(), 2);
        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(!names.contains(&"c"));
    }

    #[test]
    fn test_from_subgraph_groups_multi_asset() {
        // a is root, b and c are grouped as "multi_bc"
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["a"])]);
        let selection: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let groups: HashMap<String, String> = [
            ("b".to_string(), "multi_bc".to_string()),
            ("c".to_string(), "multi_bc".to_string()),
        ]
        .into();

        let plan = ExecutionPlan::from_subgraph(&graph, &selection, &groups, &HashMap::new());

        assert_eq!(plan.steps.len(), 2); // a + grouped(b,c)
        let grouped_step = plan.steps.iter().find(|s| s.outputs.len() > 1).unwrap();
        assert_eq!(grouped_step.outputs.len(), 2);
        let mut outputs = grouped_step.outputs.clone();
        outputs.sort();
        assert_eq!(outputs, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_from_subgraph_rewrites_grouped_deps() {
        // a -> b (grouped), d depends on b
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b"]),
        ]);
        let selection: HashSet<String> =
            ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();
        let groups: HashMap<String, String> = [
            ("b".to_string(), "multi_bc".to_string()),
            ("c".to_string(), "multi_bc".to_string()),
        ]
        .into();

        let plan = ExecutionPlan::from_subgraph(&graph, &selection, &groups, &HashMap::new());

        // d's plan_dependency points to the grouped step (named for whichever of
        // b/c was first in topo order).
        let step_d = plan.steps.iter().find(|s| s.name == "d").unwrap();
        let grouped_step = plan.steps.iter().find(|s| !s.outputs.is_empty()).unwrap();
        assert!(step_d.plan_dependencies.contains(&grouped_step.name));
    }

    #[test]
    fn test_apply_fan_out_kinds() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let mut plan = ExecutionPlan::from_graph(&graph);

        let kinds: HashMap<String, StepKind> = [(
            "b".to_string(),
            StepKind::Mapped {
                fan_out_source: "a".to_string(),
                max_concurrency: Some(2),
            },
        )]
        .into();
        plan.apply_fan_out_kinds(&kinds);

        let step_a = plan.steps.iter().find(|s| s.name == "a").unwrap();
        assert_eq!(step_a.kind, StepKind::Normal);

        let step_b = plan.steps.iter().find(|s| s.name == "b").unwrap();
        assert_eq!(
            step_b.kind,
            StepKind::Mapped {
                fan_out_source: "a".to_string(),
                max_concurrency: Some(2),
            }
        );

        let step_c = plan.steps.iter().find(|s| s.name == "c").unwrap();
        assert_eq!(step_c.kind, StepKind::Normal);
    }

    #[test]
    fn test_all_asset_names_simple() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"])]);
        let plan = ExecutionPlan::from_graph(&graph);
        let mut names = plan.all_asset_names();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_all_asset_names_with_multi_output() {
        let plan = ExecutionPlan {
            action: None,
            steps: vec![
                ExecutionStep {
                    name: "step_1".to_string(),
                    kind: StepKind::Normal,
                    outputs: vec!["out_a".to_string(), "out_b".to_string()],
                    plan_dependencies: vec![],
                    graph_dependencies: vec![],
                },
                ExecutionStep {
                    name: "step_2".to_string(),
                    kind: StepKind::Normal,
                    outputs: vec![],
                    plan_dependencies: vec!["step_1".to_string()],
                    graph_dependencies: vec![],
                },
            ],
        };
        let mut names = plan.all_asset_names();
        names.sort();
        assert_eq!(
            names,
            vec![
                "out_a".to_string(),
                "out_b".to_string(),
                "step_2".to_string(),
            ]
        );
    }

    #[test]
    fn test_group_steps_by_level_linear() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let plan = ExecutionPlan::from_graph(&graph);
        let levels = plan.group_steps_by_level();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(plan.steps[levels[0][0]].name, "a");
        assert_eq!(levels[1].len(), 1);
        assert_eq!(plan.steps[levels[1][0]].name, "b");
        assert_eq!(levels[2].len(), 1);
        assert_eq!(plan.steps[levels[2][0]].name, "c");
    }

    #[test]
    fn test_group_steps_by_level_diamond() {
        let graph = make_graph(vec![
            ("a", vec![]),
            ("b", vec!["a"]),
            ("c", vec!["a"]),
            ("d", vec!["b", "c"]),
        ]);
        let plan = ExecutionPlan::from_graph(&graph);
        let levels = plan.group_steps_by_level();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(plan.steps[levels[0][0]].name, "a");
        assert_eq!(levels[1].len(), 2);
        let mut level1_names: Vec<&str> = levels[1]
            .iter()
            .map(|&i| plan.steps[i].name.as_str())
            .collect();
        level1_names.sort();
        assert_eq!(level1_names, vec!["b", "c"]);
        assert_eq!(levels[2].len(), 1);
        assert_eq!(plan.steps[levels[2][0]].name, "d");
    }

    #[test]
    fn test_group_steps_by_level_parallel_roots() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec![]), ("c", vec!["a", "b"])]);
        let plan = ExecutionPlan::from_graph(&graph);
        let levels = plan.group_steps_by_level();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 2);
        let mut level0_names: Vec<&str> = levels[0]
            .iter()
            .map(|&i| plan.steps[i].name.as_str())
            .collect();
        level0_names.sort();
        assert_eq!(level0_names, vec!["a", "b"]);
        assert_eq!(levels[1].len(), 1);
        assert_eq!(plan.steps[levels[1][0]].name, "c");
    }

    #[test]
    fn test_for_action_unordered_is_flat() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let plan = ExecutionPlan::for_action(
            &graph,
            "optimize",
            &["a".into(), "b".into(), "c".into()],
            ActionOrdering::Unordered,
        );

        assert_eq!(plan.action.as_deref(), Some("optimize"));
        assert_eq!(plan.steps.len(), 3);
        for step in &plan.steps {
            assert!(step.plan_dependencies.is_empty());
            assert!(step.graph_dependencies.is_empty());
            assert!(step.outputs.is_empty());
        }
        assert_eq!(plan.group_steps_by_level().len(), 1);
    }

    #[test]
    fn test_for_action_never_pulls_in_upstream() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"])]);
        let plan =
            ExecutionPlan::for_action(&graph, "optimize", &["b".into()], ActionOrdering::Unordered);
        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["b"]);
    }

    #[test]
    fn test_for_action_downstream_first_orders_teardown() {
        // events -> rollups (rollups depends on events); delete must run
        // rollups before events. Unrelated "other" stays parallel.
        let graph = make_graph(vec![
            ("events", vec![]),
            ("rollups", vec!["events"]),
            ("other", vec![]),
        ]);
        let plan = ExecutionPlan::for_action(
            &graph,
            "delete",
            &["events".into(), "rollups".into(), "other".into()],
            ActionOrdering::DownstreamFirst,
        );

        let events = plan.steps.iter().find(|s| s.name == "events").unwrap();
        assert_eq!(events.plan_dependencies, vec!["rollups".to_string()]);
        let rollups = plan.steps.iter().find(|s| s.name == "rollups").unwrap();
        assert!(rollups.plan_dependencies.is_empty());
        let other = plan.steps.iter().find(|s| s.name == "other").unwrap();
        assert!(other.plan_dependencies.is_empty());
    }

    #[test]
    fn test_for_action_upstream_first_orders_buildup() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);
        let plan = ExecutionPlan::for_action(
            &graph,
            "compact",
            &["a".into(), "c".into()],
            ActionOrdering::UpstreamFirst,
        );

        let c = plan.steps.iter().find(|s| s.name == "c").unwrap();
        // b is not a target, but a is transitively upstream of c.
        assert_eq!(c.plan_dependencies, vec!["a".to_string()]);
        let a = plan.steps.iter().find(|s| s.name == "a").unwrap();
        assert!(a.plan_dependencies.is_empty());
    }

    /// Ordered action planning must stay linear in the graph: it re-runs per
    /// backfill child, under the repository's state read lock. The old
    /// T·(T−1)² whole-graph reachability walks took over a minute of pure
    /// planning at 400 targets.
    #[test]
    fn test_for_action_ordering_scales_linearly() {
        let n = 200;
        let names: Vec<String> = (0..n).map(|i| format!("a{i}")).collect();
        let defs: Vec<(&str, Vec<&str>)> = (0..n)
            .map(|i| {
                let deps = if i == 0 {
                    vec![]
                } else {
                    vec![names[i - 1].as_str()]
                };
                (names[i].as_str(), deps)
            })
            .collect();
        let graph = make_graph(defs);

        let start = std::time::Instant::now();
        let plan =
            ExecutionPlan::for_action(&graph, "delete", &names, ActionOrdering::DownstreamFirst);
        let elapsed = start.elapsed();

        assert_eq!(plan.steps.len(), n);
        // The root of the chain waits on every downstream target.
        let root = plan.steps.iter().find(|s| s.name == "a0").unwrap();
        assert_eq!(root.plan_dependencies.len(), n - 1);
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "downstream-first planning took {elapsed:?} for {n} chained targets"
        );
    }

    /// Duplicate names in an action selection must collapse to one step — a
    /// destructive verb must not execute once per occurrence. The materialize
    /// path dedups by construction; this is its action-side counterpart.
    #[test]
    fn test_for_action_dedups_duplicate_targets() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"])]);
        let plan = ExecutionPlan::for_action(
            &graph,
            "delete",
            &["b".into(), "a".into(), "b".into()],
            ActionOrdering::DownstreamFirst,
        );

        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["b", "a"],
            "duplicates collapse, first-occurrence order kept"
        );
        let a = plan.steps.iter().find(|s| s.name == "a").unwrap();
        assert_eq!(
            a.plan_dependencies,
            vec!["b".to_string()],
            "ordering still holds on the deduped set"
        );
    }

    /// Levels must not depend on the order targets arrive in — the caller
    /// derives the target list from a HashMap, so every permutation is
    /// reachable, and a level that puts `a` alongside its downstream `b` is
    /// exactly the concurrent teardown `DownstreamFirst` exists to prevent.
    #[test]
    fn test_for_action_levels_independent_of_target_order() {
        let graph = make_graph(vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])]);

        let level_names = |plan: &ExecutionPlan| -> Vec<Vec<String>> {
            plan.group_steps_by_level()
                .into_iter()
                .map(|level| {
                    let mut names: Vec<String> =
                        level.iter().map(|&i| plan.steps[i].name.clone()).collect();
                    names.sort();
                    names
                })
                .collect()
        };

        for perm in [
            ["a", "b", "c"],
            ["a", "c", "b"],
            ["b", "a", "c"],
            ["b", "c", "a"],
            ["c", "a", "b"],
            ["c", "b", "a"],
        ] {
            let targets: Vec<String> = perm.iter().map(|s| s.to_string()).collect();

            let plan = ExecutionPlan::for_action(
                &graph,
                "purge",
                &targets,
                ActionOrdering::DownstreamFirst,
            );
            assert_eq!(
                level_names(&plan),
                vec![vec!["c"], vec!["b"], vec!["a"]],
                "downstream-first levels for targets {perm:?}"
            );

            let plan =
                ExecutionPlan::for_action(&graph, "compact", &targets, ActionOrdering::UpstreamFirst);
            assert_eq!(
                level_names(&plan),
                vec![vec!["a"], vec!["b"], vec!["c"]],
                "upstream-first levels for targets {perm:?}"
            );
        }
    }

    #[test]
    fn test_from_subgraph_composition_order() {
        // Graph asset "g" with tasks g/t1, g/t2, g/t3 all depending on root "a";
        // composition order should sort them.
        let graph = make_graph(vec![
            ("a", vec![]),
            ("g/t1", vec!["a"]),
            ("g/t2", vec!["a"]),
            ("g/t3", vec!["a"]),
        ]);
        let selection: HashSet<String> = ["a", "g/t1", "g/t2", "g/t3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let composition_order: HashMap<String, usize> = [
            ("g/t1".to_string(), 0),
            ("g/t2".to_string(), 1),
            ("g/t3".to_string(), 2),
        ]
        .into();

        let plan =
            ExecutionPlan::from_subgraph(&graph, &selection, &HashMap::new(), &composition_order);

        // a at level 0, g/t1..t3 at level 1 sorted by composition order.
        let names: Vec<&str> = plan.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names[0], "a");
        let g_tasks: Vec<&str> = names
            .iter()
            .filter(|n| n.starts_with("g/"))
            .copied()
            .collect();
        assert_eq!(g_tasks, vec!["g/t1", "g/t2", "g/t3"]);
    }
}
