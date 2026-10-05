//! The registry of the runtime image down or refusing.

use std::time::Duration;

use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::*;
use wiremock::ResponseTemplate;

use super::support::*;
use crate::codelocation::reconcile::TERMINAL_RETRY;
use crate::leader::LeaderGate;

#[tokio::test]
async fn pinned_commit_on_a_mutable_runtime_tag_requeues_after_digest_refresh_interval() {
    let registry = registry(vec![manifest('b')]).await;
    let state = api_with(latest_runtime_cl(&registry), rolled_out());

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    assert_eq!(status["phase"], "Ready", "{status}");
    assert_eq!(status["resolvedCommit"], commit('a'));
    assert_eq!(
        status["resolvedImage"],
        format!("{}/acme/runtime@{}", registry.address(), digest('b'))
    );
    assert_eq!(registry.received_requests().await.unwrap().len(), 1);
    let refresh = Duration::from_secs(300);
    let after = requeue_after(&requeue);
    assert!(
        after.is_some_and(|after| jittered(refresh).contains(&after)),
        "requeue after {after:?}, want {refresh:?} plus up to a quarter"
    );
}

#[tokio::test]
async fn transient_registry_error_keeps_the_serving_tree_ready() {
    let cases = [
        (
            ResponseTemplate::new(503),
            REASON_REGISTRY_ERROR,
            "status 503 Service Unavailable",
            Duration::from_secs(60),
        ),
        (
            ResponseTemplate::new(429).insert_header("retry-after", "900"),
            REASON_RATE_LIMITED,
            "registry rate-limited; retrying in 900s",
            Duration::from_secs(900),
        ),
    ];
    for (answer, reason, error, retry) in cases {
        let outage = registry_outage(vec![answer]).await;
        let status = patched_status(&outage.requests).expect("status patch");

        assert_eq!(status["phase"], "Ready", "{status}");
        for field in [
            "grpcEndpoint",
            "readyReplicas",
            "runSource",
            "resolvedCommit",
            "resolvedImage",
            "source",
        ] {
            assert_eq!(status[field], outage.serving[field], "{reason}: {field}");
        }
        assert_eq!(
            says(&status, CONDITION_IMAGE_RESOLVED),
            ("False", reason, error)
        );
        assert_eq!(status["message"], error);
        assert_eq!(
            condition(&status, CONDITION_SOURCE_RESOLVED),
            condition(&outage.serving, CONDITION_SOURCE_RESOLVED)
        );
        assert_eq!(
            condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["reason"],
            REASON_MIN_REPLICAS
        );
        // The Deployment keeps the tree it runs.
        let applies: Vec<_> = outage
            .requests
            .iter()
            .filter(|r| r.method == "PATCH" && r.path.contains("/deployments/"))
            .collect();
        assert!(applies.is_empty(), "{reason}: {applies:?}");
        assert_eq!(outage.requeue, Action::requeue(retry), "{reason}");
        assert_eq!(
            pass(&outage.state, LeaderGate::new()).await,
            None,
            "{reason}: the follower disagrees"
        );
    }
}

#[tokio::test]
async fn missing_tag_or_failed_registry_login_still_fails_a_serving_code_location() {
    for (code, reason) in [(404, REASON_TAG_NOT_FOUND), (401, REASON_AUTH_FAILED)] {
        let outage = registry_outage(vec![ResponseTemplate::new(code)]).await;
        let failed = patched_status(&outage.requests).expect("status patch");

        assert_eq!(failed["phase"], "Failed", "{failed}");
        assert_eq!(failed.get("grpcEndpoint"), None, "{failed}");
        assert_eq!(
            says(&failed, CONDITION_IMAGE_RESOLVED).1,
            reason,
            "{failed}"
        );
        assert_eq!(
            outage.requeue,
            Action::requeue(TERMINAL_RETRY),
            "HTTP {code}"
        );
        assert_eq!(
            pass(&outage.state, LeaderGate::new()).await,
            None,
            "HTTP {code}"
        );
    }
}

#[tokio::test]
async fn registry_outage_fails_a_git_code_location_with_nothing_to_serve() {
    let registry = registry(vec![ResponseTemplate::new(503)]).await;
    let state = ApiState::default();
    state
        .lock()
        .unwrap()
        .code_locations
        .insert("x".into(), latest_runtime_cl(&registry));

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(
        says(&status, CONDITION_IMAGE_RESOLVED),
        (
            "False",
            REASON_REGISTRY_ERROR,
            "status 503 Service Unavailable"
        )
    );
    assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
}
