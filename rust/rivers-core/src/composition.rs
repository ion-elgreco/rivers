//! Composition context for `@Asset.from_graph` definitions.
//!
//! The Python layer pushes a [`CompositionContext`] before executing the graph
//! body and pops it afterward to capture the internal DAG.

use std::cell::RefCell;
use std::collections::HashMap;

pub const DEFAULT_OUTPUT_NAME: &str = "result";

#[derive(Debug, Clone)]
pub enum InvokedNodeType {
    Task,
    Asset,
}

#[derive(Debug, Clone)]
pub struct InputBinding {
    pub upstream_node_name: String,
    pub output_name: String,
    /// Parameter name for kwargs; `None` for positional args.
    pub param_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub enum InvocationKind {
    #[default]
    Normal,
    /// Fan-out: run a task once per element of an upstream iterable output.
    Map {
        source_node: String,
        source_output: String,
        /// `None` = unlimited.
        max_concurrency: Option<usize>,
    },
    /// Barrier collect: gather all mapped outputs into a list.
    Collect { mapped_node: String },
    /// Streaming collect: downstream receives a generator of results.
    CollectStream {
        mapped_node: String,
        /// Yield in mapping-key order (buffering out-of-order completions).
        ordered: bool,
    },
}

#[derive(Debug, Clone)]
pub struct InvokedNode {
    pub name: String,
    pub node_type: InvokedNodeType,
    pub input_bindings: Vec<InputBinding>,
    pub invocation_kind: InvocationKind,
}

/// Placeholder returned from Task/Asset invocations inside a composition context;
/// carries node + output name so downstream invocations can reference it.
#[derive(Debug, Clone)]
pub struct InvokedNodeOutput {
    pub node_name: String,
    pub output_name: String,
}

impl InvokedNodeOutput {
    pub fn new(node_name: String, output_name: String) -> Self {
        Self {
            node_name,
            output_name,
        }
    }
}

#[derive(Debug)]
pub struct CompositionContext {
    pub name: String,
    pub invocations: HashMap<String, InvokedNode>,
    invocation_order: Vec<String>,
}

impl CompositionContext {
    fn new(name: String) -> Self {
        Self {
            name,
            invocations: HashMap::new(),
            invocation_order: Vec::new(),
        }
    }

    /// Tasks are namespaced as `{graph_name}/{task_name}`; assets keep their bare
    /// name (they're external dependencies). Returns the registered name.
    pub fn observe_invocation(
        &mut self,
        name: &str,
        node_type: InvokedNodeType,
        input_bindings: Vec<InputBinding>,
    ) -> String {
        let registered_name = match node_type {
            InvokedNodeType::Task => format!("{}/{}", self.name, name),
            InvokedNodeType::Asset => name.to_string(),
        };
        let node = InvokedNode {
            name: registered_name.clone(),
            node_type,
            input_bindings,
            invocation_kind: InvocationKind::Normal,
        };
        self.invocations.insert(registered_name.clone(), node);
        self.invocation_order.push(registered_name.clone());
        registered_name
    }

    /// Fan-out a task over an upstream iterable; returns the namespaced map step name.
    pub fn observe_map_invocation(
        &mut self,
        task_name: &str,
        source_node: &str,
        source_output: &str,
        max_concurrency: Option<usize>,
    ) -> String {
        let registered_name = format!("{}/{}", self.name, task_name);
        let node = InvokedNode {
            name: registered_name.clone(),
            node_type: InvokedNodeType::Task,
            input_bindings: vec![InputBinding {
                upstream_node_name: source_node.to_string(),
                output_name: source_output.to_string(),
                param_name: None,
            }],
            invocation_kind: InvocationKind::Map {
                source_node: source_node.to_string(),
                source_output: source_output.to_string(),
                max_concurrency,
            },
        };
        self.invocations.insert(registered_name.clone(), node);
        self.invocation_order.push(registered_name.clone());
        registered_name
    }

    /// Gather mapped outputs; returns a synthetic collect step name.
    pub fn observe_collect_invocation(
        &mut self,
        mapped_node: &str,
        streaming: bool,
        ordered: bool,
    ) -> String {
        let collect_name = format!("{}__collect", mapped_node);
        let kind = if streaming {
            InvocationKind::CollectStream {
                mapped_node: mapped_node.to_string(),
                ordered,
            }
        } else {
            InvocationKind::Collect {
                mapped_node: mapped_node.to_string(),
            }
        };
        let node = InvokedNode {
            name: collect_name.clone(),
            node_type: InvokedNodeType::Task,
            input_bindings: vec![InputBinding {
                upstream_node_name: mapped_node.to_string(),
                output_name: DEFAULT_OUTPUT_NAME.to_string(),
                param_name: None,
            }],
            invocation_kind: kind,
        };
        self.invocations.insert(collect_name.clone(), node);
        self.invocation_order.push(collect_name.clone());
        collect_name
    }

    pub fn invocation_order(&self) -> &[String] {
        &self.invocation_order
    }
}

thread_local! {
    static COMPOSITION_STACK: RefCell<Vec<CompositionContext>> = const { RefCell::new(Vec::new()) };
}

pub fn is_in_composition() -> bool {
    COMPOSITION_STACK.with(|stack| !stack.borrow().is_empty())
}

pub fn enter_composition(name: &str) {
    COMPOSITION_STACK.with(|stack| {
        stack
            .borrow_mut()
            .push(CompositionContext::new(name.to_string()));
    });
}

pub fn exit_composition() -> CompositionContext {
    COMPOSITION_STACK.with(|stack| {
        stack
            .borrow_mut()
            .pop()
            .expect("exit_composition called without matching enter_composition")
    })
}

/// Record an invocation in the current composition context.
pub fn observe_invocation(
    name: &str,
    node_type: InvokedNodeType,
    input_bindings: Vec<InputBinding>,
) -> String {
    COMPOSITION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let ctx = stack
            .last_mut()
            .expect("observe_invocation called outside composition context");
        ctx.observe_invocation(name, node_type, input_bindings)
    })
}

pub fn observe_map_invocation(
    task_name: &str,
    source_node: &str,
    source_output: &str,
    max_concurrency: Option<usize>,
) -> String {
    COMPOSITION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let ctx = stack
            .last_mut()
            .expect("observe_map_invocation called outside composition context");
        ctx.observe_map_invocation(task_name, source_node, source_output, max_concurrency)
    })
}

pub fn observe_collect_invocation(mapped_node: &str, streaming: bool, ordered: bool) -> String {
    COMPOSITION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let ctx = stack
            .last_mut()
            .expect("observe_collect_invocation called outside composition context");
        ctx.observe_collect_invocation(mapped_node, streaming, ordered)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observe_task_namespaced() {
        let mut ctx = CompositionContext::new("my_graph".to_string());
        let name = ctx.observe_invocation("do_work", InvokedNodeType::Task, vec![]);
        assert_eq!(name, "my_graph/do_work");
        assert_eq!(ctx.invocations.len(), 1);
        assert!(ctx.invocations.contains_key("my_graph/do_work"));
    }

    #[test]
    fn test_observe_asset_bare_name() {
        let mut ctx = CompositionContext::new("my_graph".to_string());
        let name = ctx.observe_invocation("upstream", InvokedNodeType::Asset, vec![]);
        assert_eq!(name, "upstream");
        assert!(ctx.invocations.contains_key("upstream"));
    }

    #[test]
    fn test_observe_invocation_with_bindings() {
        let mut ctx = CompositionContext::new("g".to_string());
        let bindings = vec![InputBinding {
            upstream_node_name: "g/step_a".to_string(),
            output_name: "result".to_string(),
            param_name: Some("data".to_string()),
        }];
        let name = ctx.observe_invocation("step_b", InvokedNodeType::Task, bindings);
        assert_eq!(name, "g/step_b");
        let node = &ctx.invocations["g/step_b"];
        assert_eq!(node.input_bindings.len(), 1);
        assert_eq!(node.input_bindings[0].upstream_node_name, "g/step_a");
        assert_eq!(node.input_bindings[0].param_name, Some("data".to_string()));
        assert!(matches!(node.invocation_kind, InvocationKind::Normal));
    }

    #[test]
    fn test_invocation_order_preserved() {
        let mut ctx = CompositionContext::new("g".to_string());
        ctx.observe_invocation("first", InvokedNodeType::Task, vec![]);
        ctx.observe_invocation("second", InvokedNodeType::Task, vec![]);
        ctx.observe_invocation("third", InvokedNodeType::Task, vec![]);
        assert_eq!(ctx.invocation_order(), &["g/first", "g/second", "g/third"]);
    }

    #[test]
    fn test_observe_map_invocation() {
        let mut ctx = CompositionContext::new("g".to_string());
        let name = ctx.observe_map_invocation("mapper", "g/source", "result", Some(4));
        assert_eq!(name, "g/mapper");
        let node = &ctx.invocations["g/mapper"];
        assert!(matches!(
            node.invocation_kind,
            InvocationKind::Map {
                ref source_node,
                ref source_output,
                max_concurrency: Some(4),
            } if source_node == "g/source" && source_output == "result"
        ));
        assert_eq!(node.input_bindings.len(), 1);
        assert_eq!(node.input_bindings[0].upstream_node_name, "g/source");
    }

    #[test]
    fn test_observe_map_no_concurrency_limit() {
        let mut ctx = CompositionContext::new("g".to_string());
        ctx.observe_map_invocation("mapper", "src", "result", None);
        let node = &ctx.invocations["g/mapper"];
        assert!(matches!(
            node.invocation_kind,
            InvocationKind::Map {
                max_concurrency: None,
                ..
            }
        ));
    }

    #[test]
    fn test_observe_barrier_collect() {
        let mut ctx = CompositionContext::new("g".to_string());
        let name = ctx.observe_collect_invocation("g/mapper", false, false);
        assert_eq!(name, "g/mapper__collect");
        let node = &ctx.invocations["g/mapper__collect"];
        assert!(matches!(
            node.invocation_kind,
            InvocationKind::Collect { ref mapped_node } if mapped_node == "g/mapper"
        ));
        assert_eq!(node.input_bindings[0].upstream_node_name, "g/mapper");
        assert_eq!(node.input_bindings[0].output_name, DEFAULT_OUTPUT_NAME);
    }

    #[test]
    fn test_observe_streaming_collect_ordered() {
        let mut ctx = CompositionContext::new("g".to_string());
        let name = ctx.observe_collect_invocation("g/mapper", true, true);
        assert_eq!(name, "g/mapper__collect");
        let node = &ctx.invocations["g/mapper__collect"];
        assert!(matches!(
            node.invocation_kind,
            InvocationKind::CollectStream { ref mapped_node, ordered: true }
            if mapped_node == "g/mapper"
        ));
    }

    #[test]
    fn test_observe_streaming_collect_unordered() {
        let mut ctx = CompositionContext::new("g".to_string());
        ctx.observe_collect_invocation("g/mapper", true, false);
        let node = &ctx.invocations["g/mapper__collect"];
        assert!(matches!(
            node.invocation_kind,
            InvocationKind::CollectStream { ordered: false, .. }
        ));
    }

    #[test]
    fn test_enter_exit_composition() {
        assert!(!is_in_composition());
        enter_composition("test_graph");
        assert!(is_in_composition());
        let ctx = exit_composition();
        assert!(!is_in_composition());
        assert_eq!(ctx.name, "test_graph");
        assert!(ctx.invocations.is_empty());
    }

    #[test]
    fn test_nested_composition() {
        enter_composition("outer");
        observe_invocation("task_a", InvokedNodeType::Task, vec![]);

        enter_composition("inner");
        observe_invocation("task_b", InvokedNodeType::Task, vec![]);

        let inner = exit_composition();
        assert_eq!(inner.name, "inner");
        assert!(inner.invocations.contains_key("inner/task_b"));
        assert!(!inner.invocations.contains_key("outer/task_a"));

        let outer = exit_composition();
        assert_eq!(outer.name, "outer");
        assert!(outer.invocations.contains_key("outer/task_a"));
        assert!(!outer.invocations.contains_key("inner/task_b"));
    }

    #[test]
    fn test_module_observe_invocation() {
        enter_composition("g");
        let name = observe_invocation("step", InvokedNodeType::Task, vec![]);
        assert_eq!(name, "g/step");
        let ctx = exit_composition();
        assert_eq!(ctx.invocations.len(), 1);
    }

    #[test]
    fn test_module_observe_map_invocation() {
        enter_composition("g");
        let name = observe_map_invocation("mapper", "g/src", "result", Some(2));
        assert_eq!(name, "g/mapper");
        let ctx = exit_composition();
        assert!(matches!(
            ctx.invocations["g/mapper"].invocation_kind,
            InvocationKind::Map {
                max_concurrency: Some(2),
                ..
            }
        ));
    }

    #[test]
    fn test_module_observe_collect_invocation() {
        enter_composition("g");
        let name = observe_collect_invocation("g/mapper", false, false);
        assert_eq!(name, "g/mapper__collect");
        let ctx = exit_composition();
        assert!(matches!(
            ctx.invocations["g/mapper__collect"].invocation_kind,
            InvocationKind::Collect { .. }
        ));
    }

    #[test]
    fn test_full_map_collect_pipeline() {
        enter_composition("pipeline");

        let src = observe_invocation("source", InvokedNodeType::Task, vec![]);
        assert_eq!(src, "pipeline/source");

        let mapped = observe_map_invocation("transform", &src, "result", None);
        assert_eq!(mapped, "pipeline/transform");

        let collected = observe_collect_invocation(&mapped, false, false);
        assert_eq!(collected, "pipeline/transform__collect");

        let _sink = observe_invocation(
            "sink",
            InvokedNodeType::Task,
            vec![InputBinding {
                upstream_node_name: collected.clone(),
                output_name: "result".to_string(),
                param_name: None,
            }],
        );

        let ctx = exit_composition();
        assert_eq!(ctx.invocations.len(), 4);
        assert_eq!(
            ctx.invocation_order(),
            &[
                "pipeline/source",
                "pipeline/transform",
                "pipeline/transform__collect",
                "pipeline/sink",
            ]
        );
    }

    #[test]
    fn test_invoked_node_output_new() {
        let out = InvokedNodeOutput::new("my_node".to_string(), "result".to_string());
        assert_eq!(out.node_name, "my_node");
        assert_eq!(out.output_name, "result");
    }

    #[test]
    fn test_default_output_name() {
        assert_eq!(DEFAULT_OUTPUT_NAME, "result");
    }

    #[test]
    #[should_panic(expected = "exit_composition called without matching enter_composition")]
    fn test_exit_without_enter_panics() {
        exit_composition();
    }

    #[test]
    #[should_panic(expected = "observe_invocation called outside composition context")]
    fn test_observe_outside_context_panics() {
        observe_invocation("x", InvokedNodeType::Task, vec![]);
    }
}
