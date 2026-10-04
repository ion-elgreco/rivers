pub const MODULE: &str = "definitions";
pub const SURREAL_ENDPOINT: &str = "ws://surrealdb.rivers.svc:8000";
pub const NAMESPACE: &str = "rivers";
pub const SERVICE_ACCOUNT: &str = "rivers-executor";
pub const RUN_CPU: &str = "500m";
pub const RUN_MEMORY: &str = "512Mi";
pub const WORKER_CPU: &str = RUN_CPU;
pub const WORKER_MEMORY: &str = RUN_MEMORY;
pub const MAX_RESTARTS: u32 = 3;
pub const CANCEL_GRACE_PERIOD: u64 = 300;
pub const MAX_CONCURRENT_STEPS: u32 = 10;
/// Runtime image for git-sourced CodeLocations when `RIVERS_RUNTIME_IMAGE`
/// is unset: the image this release publishes for the chart's default
/// `codeLocation.runtime.pythonVersion`.
pub const RUNTIME_IMAGE: &str = concat!(
    "ghcr.io/ion-elgreco/rivers-runtime:",
    env!("CARGO_PKG_VERSION"),
    "-py3.12"
);

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_image_is_the_release_tag_for_the_chart_default_python() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/helm/rivers/values.yaml"
        );
        let values: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let python = values["codeLocation"]["runtime"]["pythonVersion"]
            .as_str()
            .unwrap();
        assert_eq!(
            super::RUNTIME_IMAGE,
            format!(
                "ghcr.io/ion-elgreco/rivers-runtime:{}-py{python}",
                env!("CARGO_PKG_VERSION")
            )
        );
    }
}
