//! Automation condition evaluation engine.
//!
//! Assets declare conditions (e.g., `AutomationCondition.eager()`) that the
//! daemon evaluates periodically. When a condition fires, the daemon triggers
//! a materialization. Pure-Rust, no PyO3, fully testable.

pub mod cache;
pub mod eval;
pub mod node;
pub mod partition;
pub mod pass;
pub mod state;

#[cfg(test)]
mod tests;

pub use cache::*;
pub use eval::*;
pub use node::*;
pub use partition::*;
pub use pass::*;
pub use state::*;
