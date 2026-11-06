//! A scratch store does not sync each commit to disk: a new store's migration
//! makes about 25 commits, and with an fsync each, two test stores took more
//! than 55 s to open on a busy Windows runner. An embedded store does.

use std::io::Write;
use std::sync::{Arc, Mutex};

use rivers_core::storage::surrealdb_backend::SurrealStorage;

/// The log SurrealKV writes as it opens a store, which names its sync mode.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Log {
    fn take(&self) -> String {
        String::from_utf8(std::mem::take(&mut *self.0.lock().unwrap())).unwrap()
    }
}

// A module: `test_temp_dir!` names the directory after `<module>::<test>`.
mod sync_mode {
    use super::*;

    #[test]
    fn only_scratch_stores_skip_the_sync_of_each_commit() {
        let log = Log::default();
        let writer = log.clone();
        tracing_subscriber::fmt()
            .with_env_filter("surrealdb::core::kvs::surrealkv=info")
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .init();
        let dir = test_temp_dir::test_temp_dir!();

        let scratch = dir.as_path_untracked().join("scratch");
        drop(SurrealStorage::new_scratch_blocking(scratch.to_str().unwrap()).unwrap());
        assert!(log.take().contains("Sync mode: never"));

        let embedded = dir.as_path_untracked().join("embedded");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        drop(
            runtime
                .block_on(SurrealStorage::new_embedded(embedded.to_str().unwrap()))
                .unwrap(),
        );
        assert!(log.take().contains("Sync mode: every transaction commit"));
    }
}
