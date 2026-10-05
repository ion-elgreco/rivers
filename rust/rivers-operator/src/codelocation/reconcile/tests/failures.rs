//! Failed builds and objects the API server refused.

use std::time::Duration;

use k8s_openapi::api::core::v1::ContainerStateTerminated;
use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::*;
use serde_json::json;

use super::support::*;
use crate::codelocation::reconcile::rollout::{build_failure_message, newest_build_failure};
use crate::codelocation::reconcile::{TERMINAL_RETRY, WorkspaceConfig};
use crate::leader::LeaderGate;

#[tokio::test]
async fn a_failed_build_of_the_tree_rolling_out_shows_in_status() {
    let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());
    // The old pod serves a; the new pod cannot build b.
    add_pods(
        &state,
        [
            code_location_pod("x-a", &tree('a'), completed_sync()),
            code_location_pod(
                "x-b",
                &tree('b'),
                failed_sync(&format!("{STALE_LOCK}\n"), "2026-10-03T00:00:00Z"),
            ),
        ],
    );

    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    let failure =
        format!("workspace build of bbbbbbb (pinned) failed (Error, exit code 1): {STALE_LOCK}");
    assert_eq!(status["message"], failure, "{status}");
    let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
    assert_eq!(available["reason"], REASON_ROLLING_OUT, "{available}");
    assert_eq!(
        available["message"],
        format!(
            "rolling out bbbbbbb (pinned) on {}; runs use aaaaaaa (pinned) on {}; {failure}",
            runtime('b'),
            runtime('a')
        )
    );
    assert_eq!(status["phase"], "Ready");
    assert_eq!(status["runSource"], run_source_json('a'));
    // The same failure in the next passes: nothing new to write, from
    // either replica.
    assert_eq!(pass(&state, LeaderGate::leading()).await, None);
    assert_eq!(pass(&state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn a_refused_deployment_shows_in_status_while_runs_keep_the_serving_tree() {
    let state = api_with(sized_5gb(git_cl_at('b', serving_status('a'))), rolled_out());
    let answer = unreadable_size(422);
    refuse(&state, "deployments/x", &answer);

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    let failure = format!("applying Deployment 'x' failed: {}", answer.message);
    let available = format!("{failure}; runs use aaaaaaa (pinned)");
    assert_eq!(status["message"], failure, "{status}");
    assert_eq!(
        says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
        ("True", REASON_APPLY_FAILED, available.as_str())
    );
    assert_eq!(status["phase"], "Ready");
    assert_eq!(status["runSource"], run_source_json('a'));
    assert_eq!(status["resolvedCommit"], commit('a'));
    assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));
    // The same refusal in the next passes: nothing new to write, from
    // either replica.
    assert_eq!(pass(&state, LeaderGate::leading()).await, None);
    assert_eq!(pass(&state, LeaderGate::new()).await, None);

    // Once the API server takes the Deployment, the next pass says so.
    state.lock().unwrap().refused.clear();
    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");
    assert_eq!(status.get("message"), None, "{status}");
    assert_eq!(
        says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
        ("True", REASON_MIN_REPLICAS, "")
    );
    assert_eq!(status["runSource"], run_source_json('b'));
}

#[tokio::test]
async fn a_refused_first_deployment_fails_the_code_location() {
    let mut cl = sized_5gb(git_cl_at('a', json!({})));
    cl.status = None;
    let state = ApiState::default();
    state.lock().unwrap().code_locations.insert("x".into(), cl);
    let answer = unreadable_size(500);
    refuse(&state, "deployments/x", &answer);

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    let failure = format!("applying Deployment 'x' failed: {}", answer.message);
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["message"], failure);
    assert_eq!(
        says(&status, CONDITION_DEPLOYMENT_AVAILABLE),
        ("False", REASON_APPLY_FAILED, failure.as_str())
    );
    for field in [
        "grpcEndpoint",
        "readyReplicas",
        "runSource",
        "resolvedCommit",
    ] {
        assert_eq!(status.get(field), None, "{field}: {status}");
    }
    // A server error may pass: the leader tries again sooner.
    assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
    assert_eq!(pass(&state, LeaderGate::leading()).await, None);
    assert_eq!(pass(&state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn each_refused_object_of_a_git_code_location_shows_in_status() {
    for (workspace, object, action) in [
        (
            shared(),
            "persistentvolumeclaims/x-workspace",
            "creating PersistentVolumeClaim 'x-workspace'",
        ),
        (
            shared(),
            "configmaps/x-workspace-keep",
            "applying ConfigMap 'x-workspace-keep'",
        ),
        (
            WorkspaceConfig::default(),
            "deployments/x",
            "applying Deployment 'x'",
        ),
        (
            WorkspaceConfig::default(),
            "services/x-grpc",
            "applying Service 'x-grpc'",
        ),
    ] {
        let state = api_with(git_cl_at('b', serving_status('a')), rolled_out());
        let answer = kube_core::Status::failure(
            &format!("{object} is forbidden: denied by the cluster's policy"),
            "Forbidden",
        )
        .with_code(403);
        refuse(&state, object, &answer);

        let (requeue, requests) = reconcile_in(
            &state,
            LeaderGate::leading(),
            &resolver(),
            workspace,
            run_store(Vec::new()),
        )
        .await;

        let status = patched_status(&requests).expect("status patch");
        assert_eq!(
            status["message"],
            format!("{action} failed: {}", answer.message),
            "{object}"
        );
        assert_eq!(
            says(&status, CONDITION_DEPLOYMENT_AVAILABLE).1,
            REASON_APPLY_FAILED,
            "{object}"
        );
        assert_eq!(requeue, Action::requeue(TERMINAL_RETRY), "{object}");
    }
}

#[test]
fn build_failure_is_the_newest_failed_sync_of_a_pod_that_runs_the_template() {
    let failed_now = json!({ "state": { "terminated": {
        "exitCode": 1,
        "reason": "Error",
        "message": "second",
        "finishedAt": "2026-10-03T00:02:00Z",
    } } });
    let recovered = json!({
        "state": { "terminated": {
            "exitCode": 0,
            "reason": "Completed",
            "finishedAt": "2026-10-03T00:09:00Z",
        } },
        "lastState": { "terminated": {
            "exitCode": 1,
            "reason": "Error",
            "message": "index down",
            "finishedAt": "2026-10-03T00:08:00Z",
        } },
    });
    let pods = [
        code_location_pod(
            "x-a",
            &tree('a'),
            failed_sync("other tree", "2026-10-03T00:10:00Z"),
        ),
        code_location_pod(
            "x-b-1",
            &tree('b'),
            failed_sync("first", "2026-10-03T00:01:00Z"),
        ),
        code_location_pod("x-b-2", &tree('b'), failed_now),
        code_location_pod("x-b-3", &tree('b'), recovered),
    ];

    let newest = newest_build_failure(&pods, &tree('b')).expect("a failed build");
    assert_eq!(newest.message.as_deref(), Some("second"));
    assert_eq!(newest_build_failure(&pods[..1], &tree('b')), None);
    assert_eq!(newest_build_failure(&pods[3..], &tree('b')), None);
}

#[test]
fn build_failure_message_shapes() {
    let failed =
        |reason: Option<&str>, exit_code, message: Option<&str>| ContainerStateTerminated {
            exit_code,
            reason: reason.map(str::to_string),
            message: message.map(str::to_string),
            ..Default::default()
        };
    for (failed, expected) in [
        (
            failed(Some("Error"), 1, Some("error: no lock\n")),
            "workspace build of bbbbbbb (pinned) failed (Error, exit code 1): \
             error: no lock",
        ),
        (
            failed(Some("OOMKilled"), 137, None),
            "workspace build of bbbbbbb (pinned) failed (OOMKilled, exit code 137)",
        ),
        (
            failed(None, 2, Some(" \n")),
            "workspace build of bbbbbbb (pinned) failed (exit code 2)",
        ),
    ] {
        assert_eq!(build_failure_message(&tree('b'), &failed), expected);
    }
}

#[tokio::test]
async fn a_complete_rollout_reads_no_pods() {
    let state = api_with(git_cl_at('b', serving_status('a')), rolled_out());

    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let pod_reads: Vec<_> = requests
        .iter()
        .filter(|r| r.path.contains("/pods"))
        .collect();
    assert!(pod_reads.is_empty(), "{pod_reads:?}");
}
