//! The reconcile tests against a mock API server.

pub(super) mod support;

mod failures;
mod outages;
mod registry;
mod rollout;
mod switch;

use std::time::Duration;

use rivers_k8s::crd::code_location::GitRef;

use self::support::{jittered, requeue_after};
use super::{git_requeue, jitter};

#[test]
fn jitter_within_bounds() {
    let base = Duration::from_secs(60);
    for _ in 0..100 {
        let j = jitter(base);
        assert!(j >= base);
        assert!(j <= base + Duration::from_secs(15));
    }
}

#[test]
fn ready_git_code_location_requeues_at_the_sooner_of_ref_poll_and_image_refresh() {
    let minutes = |m: u64| Duration::from_secs(60 * m);
    let (two, five, ten, hour) = (minutes(2), minutes(5), minutes(10), minutes(60));
    let git_ref = |field: &str, value: &str| -> GitRef {
        serde_json::from_value(serde_json::json!({ field: value })).unwrap()
    };
    let pinned = git_ref("commit", &"a".repeat(40));
    let semver_tag = git_ref("tag", "v1.2.3");
    let other_tag = git_ref("tag", "nightly");
    let branch = git_ref("branch", "main");
    // The runtime image as resolve_image_ref answers it, (refresh_after,
    // immutable), with digestRefreshInterval 5m.
    let images = [
        ("digest", (hour, true)),
        ("immutable tag", (five, true)),
        ("mutable tag", (five, false)),
    ];
    // The requeue with each of `images`, by ref and pollInterval.
    let table = [
        (&pinned, two, [None, None, Some(five)]),
        (&semver_tag, two, [Some(hour), Some(hour), Some(five)]),
        (&branch, two, [Some(two), Some(two), Some(two)]),
        (&branch, ten, [Some(ten), Some(ten), Some(five)]),
        (&other_tag, ten, [Some(ten), Some(ten), Some(five)]),
    ];

    let mut wrong = Vec::new();
    for (git_ref, poll, wants) in table {
        for ((image, (refresh_after, immutable)), want) in images.into_iter().zip(wants) {
            let got = requeue_after(&git_requeue(git_ref, poll, refresh_after, immutable));
            let right = match want {
                None => got.is_none(),
                Some(want) => got.is_some_and(|got| jittered(want).contains(&got)),
            };
            if !right {
                let git_ref = serde_json::to_string(git_ref).unwrap();
                wrong.push(format!(
                    "{git_ref} polled every {poll:?}, {image}: requeue after {got:?}, \
                     want {want:?} + jitter"
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
}
