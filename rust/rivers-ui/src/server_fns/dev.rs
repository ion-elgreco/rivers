//! Reloading the code location under `rivers dev`.

use leptos::prelude::*;

use crate::types::DevReloadState;

#[server(endpoint = "dev/reload-state")]
pub async fn get_dev_reload_state() -> Result<DevReloadState, ServerFnError> {
    Ok(crate::dev_reload::state())
}

/// Ask the host to restart the code location and wait for the verdict: `Ok`
/// once the new one serves, the host's reason when it did not come back.
#[server(endpoint = "dev/reload")]
pub async fn reload_code_location() -> Result<(), ServerFnError> {
    let Some(before) = crate::dev_reload::request() else {
        return Err(ServerFnError::new(
            "the code location can only be reloaded under `rivers dev`",
        ));
    };
    let verdict = crate::dev_reload::settled(before).await;
    match verdict.error {
        Some(error) => Err(ServerFnError::new(error)),
        None if verdict.enabled => Ok(()),
        None => Err(ServerFnError::new("rivers dev is shutting down")),
    }
}
