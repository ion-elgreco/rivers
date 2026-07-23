#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunQueueConfig {
    pub max_concurrent_runs: i32,
    pub tag_concurrency_limits: Vec<TagConcurrencyLimit>,
    pub dequeue_interval: std::time::Duration,
    /// How long a dequeued run may sit in `NotStarted` with no live executor
    /// before the coordinator marks it `Failure`.
    pub start_timeout: std::time::Duration,
}

impl Default for RunQueueConfig {
    fn default() -> Self {
        Self {
            max_concurrent_runs: 10,
            tag_concurrency_limits: Vec::new(),
            dequeue_interval: std::time::Duration::from_millis(250),
            start_timeout: std::time::Duration::from_secs(180),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TagConcurrencyLimit {
    pub key: String,
    pub value: Option<String>,
    pub per_unique_value: bool,
    pub limit: u32,
}
