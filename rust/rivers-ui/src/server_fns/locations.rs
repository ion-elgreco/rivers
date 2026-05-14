//! Server function exposing the operator's `CodeLocationRegistry` contents to
//! the UI. Polling-based; streaming `Watch` is a follow-up.

use leptos::prelude::*;

use crate::types::CodeLocationEntry;

#[server]
pub async fn list_code_locations() -> Result<Vec<CodeLocationEntry>, ServerFnError> {
    let state = expect_context::<crate::state::AppState>();
    state
        .registry
        .list()
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))
}
