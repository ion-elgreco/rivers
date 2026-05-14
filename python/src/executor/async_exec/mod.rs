//! Async execution: bridge + concurrent executor backend.
//!
//! - `bridge` — `AsyncBridge` managing a Python event loop on a background thread
//! - `backend` — `AsyncBackend` implementing `ExecutorBackend` via tokio JoinSet

mod backend;
pub(crate) mod bridge;

pub(crate) use backend::AsyncBackend;
pub(crate) use bridge::AsyncBridge;
