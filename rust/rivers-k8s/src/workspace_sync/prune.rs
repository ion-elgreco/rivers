//! Step 5: delete the trees beside this one that nothing keeps.

use std::collections::HashSet;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::log;

/// The floors as the operator wrote them; whole numbers only.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Floors {
    pub keep_revisions: String,
    pub min_age_seconds: String,
}

struct Parsed {
    keep_revisions: u64,
    min_age: Duration,
}

impl Floors {
    /// Digits only: a floor test that errors would let the tree go.
    fn parse(&self) -> Option<Parsed> {
        Some(Parsed {
            keep_revisions: whole(&self.keep_revisions)?,
            min_age: Duration::from_secs(whole(&self.min_age_seconds)?),
        })
    }
}

fn whole(text: &str) -> Option<u64> {
    (!text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// Walks `root` (the PVC root, mounted only on the code-location pod) and
/// removes every tree that is not this pod's own, not in the keep-set, not
/// among the `keep_revisions` newest, and older than `min_age`. At most one
/// pruner at a time (`flock -n`); the rest skip — a prune that finds nothing
/// eligible is a directory listing, so the redundancy is free.
pub(crate) fn run(
    root: &Path,
    own: Option<&str>,
    keep: &HashSet<String>,
    floors: &Floors,
) -> io::Result<()> {
    // emptyDir mode has no /workspaces mount — nothing to reclaim.
    if !root.is_dir() {
        return Ok(());
    }
    let Some(parsed) = floors.parse() else {
        log(format_args!(
            "prune: skipped — RIVERS_WORKSPACE_KEEP_REVISIONS='{}' and RIVERS_WORKSPACE_MIN_AGE_SECONDS='{}' must both be whole numbers",
            floors.keep_revisions, floors.min_age_seconds
        ));
        return Ok(());
    };
    let lock = File::options()
        .create(true)
        .write(true)
        .open(root.join(".prune.lock"))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            log("prune: another pod holds the prune lock — skipping");
            return Ok(());
        }
        Err(TryLockError::Error(e)) => return Err(e),
    }
    let entries: Vec<fs::DirEntry> = fs::read_dir(root)?.collect::<io::Result<_>>()?;
    for entry in &entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(".deleting-") {
            continue;
        }
        log(format_args!(
            "prune: removing {name}, left by an earlier prune"
        ));
        if fs::remove_dir_all(entry.path()).is_err() {
            log(format_args!("prune: failed to remove {name} (non-fatal)"));
        }
    }
    let now = SystemTime::now();
    // Newest first by mtime; `cache` (the shared UV_CACHE_DIR) and dot
    // entries are not trees.
    let mut trees: Vec<(String, SystemTime)> = entries
        .iter()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "cache" || name.starts_with('.') || !entry.path().is_dir() {
                return None;
            }
            let modified = fs::metadata(entry.path())
                .and_then(|m| m.modified())
                .unwrap_or(now);
            Some((name, modified))
        })
        .collect();
    trees.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (rank, (tree, modified)) in (1u64..).zip(&trees) {
        if Some(tree.as_str()) == own || keep.contains(tree) || rank <= parsed.keep_revisions {
            continue;
        }
        // The age floor closes the admission-vs-commit race.
        let age = now.duration_since(*modified).unwrap_or_default();
        if age < parsed.min_age {
            continue;
        }
        log(format_args!(
            "prune: removing {tree} (rank {rank}, age {}s)",
            age.as_secs()
        ));
        // One rename takes the whole tree off its key before any file goes:
        // a delete cut short must not leave a partial tree with .ready.
        let stamp = now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let aside = root.join(format!(
            ".deleting-{tree}-{}-{}",
            stamp.as_secs(),
            stamp.subsec_nanos()
        ));
        if fs::rename(root.join(tree), &aside)
            .and_then(|()| fs::remove_dir_all(&aside))
            .is_err()
        {
            log(format_args!("prune: failed to remove {tree} (non-fatal)"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const DAY: u64 = 24 * 3600;

    fn floors(keep_revisions: &str, min_age_seconds: &str) -> Floors {
        Floors {
            keep_revisions: keep_revisions.to_string(),
            min_age_seconds: min_age_seconds.to_string(),
        }
    }

    fn keep(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|key| key.to_string()).collect()
    }

    /// A built tree on the volume, last modified `age` seconds ago.
    fn tree(root: &Path, name: &str, age: u64) {
        let dir = root.join(name);
        fs::create_dir_all(dir.join("venv/bin")).unwrap();
        fs::write(dir.join("venv/bin/rivers"), "").unwrap();
        fs::write(dir.join(".ready"), "").unwrap();
        let modified = SystemTime::now() - Duration::from_secs(age);
        File::open(&dir).unwrap().set_modified(modified).unwrap();
    }

    fn trees(root: &Path, ages: &[(&str, u64)]) {
        for (name, age) in ages {
            tree(root, name, *age);
        }
    }

    /// Every entry at the volume root, dot entries included.
    fn volume(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(root)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    const OLD: &[(&str, u64)] = &[("old-1", 2 * DAY), ("old-2", 3 * DAY), ("old-3", 4 * DAY)];

    #[test]
    fn floors_accept_whole_numbers_only() {
        for (keep_revisions, min_age_seconds) in [("0", "0"), ("3", "3600"), ("007", "1")] {
            assert!(floors(keep_revisions, min_age_seconds).parse().is_some());
        }
        for bad in [
            "three",
            "2.5",
            "+1",
            "-1",
            "1d",
            "1h30m",
            "",
            " 3",
            "99999999999999999999",
        ] {
            assert!(floors(bad, "3600").parse().is_none(), "{bad:?}");
            assert!(floors("3", bad).parse().is_none(), "{bad:?}");
        }
    }

    #[test]
    fn unreadable_floors_skip_without_touching_the_volume() {
        let dir = tempdir().unwrap();
        trees(dir.path(), OLD);
        for (keep_revisions, min_age_seconds) in [("0", "1d"), ("three", "3600"), ("2.5", "0")] {
            run(
                dir.path(),
                None,
                &keep(&[]),
                &floors(keep_revisions, min_age_seconds),
            )
            .unwrap();
        }
        assert_eq!(volume(dir.path()), ["old-1", "old-2", "old-3"]);
    }

    #[test]
    fn keeps_only_whole_key_matches_and_the_cache() {
        let dir = tempdir().unwrap();
        let kept = "9f3c1ab8d2e4-1a2b3c4d-37c771fd";
        let stale = "9f3c1ab8d2e4-1a2b3c4d-03a30844";
        for name in [kept, stale, "cache"] {
            fs::create_dir(dir.path().join(name)).unwrap();
        }
        run(dir.path(), None, &keep(&["tree", kept]), &floors("0", "0")).unwrap();
        assert_eq!(volume(dir.path()), [".prune.lock", kept, "cache"]);
    }

    #[test]
    fn recency_and_age_floors() {
        for (keep_revisions, left) in [
            ("1", vec![".prune.lock", "young"]),
            ("3", vec![".prune.lock", "old-1", "old-2", "young"]),
        ] {
            let dir = tempdir().unwrap();
            tree(dir.path(), "young", 60);
            trees(dir.path(), OLD);
            run(
                dir.path(),
                None,
                &keep(&[]),
                &floors(keep_revisions, "3600"),
            )
            .unwrap();
            assert_eq!(volume(dir.path()), left);
        }
    }

    #[test]
    fn never_removes_the_own_tree() {
        for (built, min_age_seconds) in [(true, "3600"), (false, "0")] {
            let dir = tempdir().unwrap();
            tree(dir.path(), "in-use", DAY);
            trees(dir.path(), OLD);
            if built {
                tree(dir.path(), "tree", 5 * DAY);
            } else {
                fs::create_dir(dir.path().join("tree")).unwrap();
            }
            run(
                dir.path(),
                Some("tree"),
                &keep(&["in-use"]),
                &floors("0", min_age_seconds),
            )
            .unwrap();
            assert_eq!(volume(dir.path()), [".prune.lock", "in-use", "tree"]);
        }
    }

    #[test]
    fn removes_what_an_earlier_prune_left() {
        let dir = tempdir().unwrap();
        let leftover = dir
            .path()
            .join(".deleting-4b1d2c3e4f5a-1a2b3c4d-37c771fd-1-1");
        fs::create_dir_all(&leftover).unwrap();
        fs::write(leftover.join(".ready"), "").unwrap();
        tree(dir.path(), "tree", 60);
        run(dir.path(), Some("tree"), &keep(&[]), &floors("3", "3600")).unwrap();
        assert_eq!(volume(dir.path()), [".prune.lock", "tree"]);
    }

    #[cfg(unix)]
    #[test]
    fn renames_before_deleting() {
        use std::os::unix::fs::PermissionsExt as _;
        // SAFETY: a plain syscall without preconditions.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipped: root deletes files whatever their mode");
            return;
        }
        let dir = tempdir().unwrap();
        tree(dir.path(), "old", 2 * DAY);
        let locked = dir.path().join("old/venv/sub");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("f"), "").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o500)).unwrap();

        run(dir.path(), None, &keep(&[]), &floors("0", "0")).unwrap();

        assert!(!dir.path().join("old").exists());
        let aside: Vec<String> = volume(dir.path())
            .into_iter()
            .filter(|name| name.starts_with(".deleting-old-"))
            .collect();
        assert_eq!(aside.len(), 1, "{:?}", volume(dir.path()));
        let sub = dir.path().join(&aside[0]).join("venv/sub");
        fs::set_permissions(&sub, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn skips_when_another_pod_holds_the_prune_lock() {
        let dir = tempdir().unwrap();
        trees(dir.path(), OLD);
        let holder = File::options()
            .create(true)
            .write(true)
            .open(dir.path().join(".prune.lock"))
            .unwrap();
        holder.lock().unwrap();
        run(dir.path(), None, &keep(&[]), &floors("0", "0")).unwrap();
        assert_eq!(
            volume(dir.path()),
            [".prune.lock", "old-1", "old-2", "old-3"]
        );
    }

    #[test]
    fn fallback_root_absent_is_a_no_op() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("unmounted");
        run(&root, None, &keep(&[]), &floors("0", "0")).unwrap();
        assert!(!root.exists());
    }
}
