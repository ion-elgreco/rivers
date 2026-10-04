//! Rollouts of a git code location: which tree runs get, the keep-set,
//! the pods' env.

use k8s_openapi::api::apps::v1::{Deployment, DeploymentStatus};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_client::api::ObjectMeta;
use rivers_k8s::crd::code_location::*;
use rivers_k8s::crd::run::{Run, RunPhase, RunSource};
use rivers_k8s::workspace::{self, WorkspaceSpec, WorkspaceVolume};
use serde_json::json;

use super::support::*;
use crate::codelocation::reconcile::WorkspaceConfig;
use crate::codelocation::reconcile::rollout::{
    GitRollout, git_rollout, rollout_complete, template_source,
};
use crate::codelocation::reconcile::workspace::workspace_keep_csv;
use crate::codelocation::resources::{MAIN_CONTAINER, build_deployment, keep_config_map_name};
use crate::leader::LeaderGate;

#[test]
fn mid_rollout_keeps_runs_on_the_rolled_out_tree() {
    // Both pods still run a; the first pod of b is not ready.
    let mut cl = git_cl_at('b', serving_status('a'));
    cl.spec.replicas = 2;

    let status = leader_status(&cl, 'b', rollout('b', false, 2));

    assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
    assert_eq!(status.resolved_commit, Some(commit('a')));
    assert_eq!(status.resolved_ref, None);
    assert_eq!(status.resolved_image, Some(runtime('a')));
    assert_eq!(status.run_source, Some(tree('a')));
    assert_eq!(status.source.as_deref(), Some("aaaaaaa (pinned)"));
    let message = format!(
        "rolling out bbbbbbb (pinned) on {}; runs use aaaaaaa (pinned) on {}",
        runtime('b'),
        runtime('a')
    );
    assert_eq!(
        available(&status),
        ("True", Some(REASON_ROLLING_OUT), Some(message.as_str()))
    );
}

#[test]
fn completed_rollout_moves_every_serving_field_to_the_template() {
    let mut cl = git_cl_at('b', serving_status('a'));
    cl.spec.replicas = 2;

    let status = leader_status(&cl, 'b', rollout('b', true, 2));

    assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
    assert_eq!(status.resolved_commit, Some(commit('b')));
    assert_eq!(status.resolved_ref, None);
    assert_eq!(status.resolved_image, Some(runtime('b')));
    assert_eq!(status.run_source, Some(tree('b')));
    assert_eq!(status.source.as_deref(), Some("bbbbbbb (pinned)"));
    assert_eq!(
        available(&status),
        ("True", Some(REASON_MIN_REPLICAS), None)
    );
}

#[test]
fn first_rollout_stays_deploying_until_it_completes() {
    // The one pod is ready, but the controller has not confirmed the
    // rollout and no tree has rolled out before.
    let mut cl = git_cl_at('b', json!({}));
    cl.status = None;

    let status = leader_status(&cl, 'b', rollout('b', false, 1));

    assert_eq!(status.phase, Some(CodeLocationPhase::Deploying));
    assert_eq!(status.resolved_commit, None);
    assert_eq!(status.resolved_image, None);
    assert_eq!(status.run_source, None);
    assert_eq!(status.source, None);
    assert_eq!(
        available(&status),
        (
            "False",
            Some(REASON_ROLLING_OUT),
            Some("rolling out bbbbbbb (pinned)")
        )
    );
}

#[test]
fn failed_build_keeps_runs_on_the_rolled_out_tree_and_says_so() {
    // b fails to build: its pod never gets ready, the old pod serves.
    let cl = git_cl_at('b', serving_status('a'));
    let rollout = GitRollout {
        template: Some(RunSource {
            runtime_image: runtime('a'),
            ..tree('b')
        }),
        deadline_exceeded: true,
        ..rollout('b', false, 1)
    };

    let status = leader_status(&cl, 'b', rollout);

    assert_eq!(status.phase, Some(CodeLocationPhase::Ready));
    assert_eq!(status.resolved_commit, Some(commit('a')));
    assert_eq!(status.run_source, Some(tree('a')));
    assert_eq!(
        available(&status),
        (
            "True",
            Some(REASON_PROGRESS_DEADLINE),
            Some(
                "rollout of bbbbbbb (pinned) exceeded its progress deadline; \
                 runs use aaaaaaa (pinned)"
            )
        )
    );
}

#[test]
fn rollout_completes_only_when_every_pod_runs_the_current_template() {
    let deployment =
        |replicas, generation, [observed, total, updated, ready, available]: [i32; 5]| Deployment {
            metadata: ObjectMeta {
                generation: Some(generation),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
                replicas: Some(replicas),
                ..Default::default()
            }),
            status: Some(DeploymentStatus {
                observed_generation: Some(observed.into()),
                replicas: Some(total),
                updated_replicas: Some(updated),
                ready_replicas: Some(ready),
                available_replicas: Some(available),
                ..Default::default()
            }),
        };
    let cases = [
        (
            "every pod updated and ready",
            deployment(2, 3, [3, 2, 2, 2, 2]),
            true,
        ),
        (
            "template not yet seen by the controller",
            deployment(2, 3, [2, 2, 2, 2, 2]),
            false,
        ),
        ("old pod left", deployment(2, 3, [3, 3, 2, 2, 2]), false),
        (
            "new pod not created",
            deployment(2, 3, [3, 2, 1, 2, 2]),
            false,
        ),
        (
            "new pod not ready",
            deployment(2, 3, [3, 2, 2, 1, 1]),
            false,
        ),
        (
            "new pod not available",
            deployment(2, 3, [3, 2, 2, 2, 1]),
            false,
        ),
        ("scaled to zero", deployment(0, 3, [3, 0, 0, 0, 0]), false),
    ];
    for (case, d, complete) in cases {
        assert_eq!(rollout_complete(&d), complete, "{case}");
    }
}

#[test]
fn rollout_reads_the_tree_from_the_pod_template() {
    let cl = git_cl_at('b', serving_status('a'));
    let pieces = workspace::builder_pod_pieces(
        &WorkspaceSpec {
            source: tree('b'),
            volume: WorkspaceVolume::EmptyDir { size_limit: None },
            extra_env: Vec::new(),
        },
        &WorkspaceConfig::default().prune("x"),
    );
    let mut d = build_deployment(
        &cl,
        &runtime('b'),
        "rivers-code-location",
        &Default::default(),
        &Default::default(),
        Some(&pieces),
    );
    d.metadata.generation = Some(2);
    d.status = Some(DeploymentStatus {
        observed_generation: Some(2),
        ready_replicas: Some(1),
        conditions: Some(vec![k8s_openapi::api::apps::v1::DeploymentCondition {
            type_: "Progressing".into(),
            status: "False".into(),
            reason: Some("ProgressDeadlineExceeded".into()),
            ..Default::default()
        }]),
        ..Default::default()
    });

    let rollout = git_rollout(Some(&d));
    assert_eq!(rollout.template, Some(tree('b')));
    assert!(rollout.deadline_exceeded);

    // Until the controller sees this template, the deadline is the
    // previous rollout's.
    d.status.as_mut().unwrap().observed_generation = Some(1);
    assert!(!git_rollout(Some(&d)).deadline_exceeded);
}

#[tokio::test]
async fn code_location_pods_hand_their_runtime_image_with_their_source() {
    let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());

    pass(&state, LeaderGate::leading()).await;

    let template = state.lock().unwrap().deployments["x"]
        .spec
        .clone()
        .and_then(|d| d.template.spec)
        .unwrap();
    let main = template
        .containers
        .iter()
        .find(|c| c.name == MAIN_CONTAINER)
        .unwrap();
    let source: serde_json::Value = main
        .env
        .iter()
        .flatten()
        .find(|e| e.name == rivers_k8s::env::ENV_RUN_SOURCE)
        .and_then(|e| e.value.as_deref())
        .map(|json| serde_json::from_str(json).unwrap())
        .unwrap();
    assert_eq!(main.image, Some(runtime('b')));
    assert_eq!(source["runtimeImage"], runtime('b'), "{source}");
}

#[test]
fn keep_set_holds_the_serving_tree_beside_the_target() {
    let keep = workspace_keep_csv(
        &tree('b').workspace_key(),
        Some(&tree('a').workspace_key()),
        "y",
        "x",
        &[],
    );

    assert_eq!(
        keep,
        format!(
            "{},{}",
            tree('a').workspace_key(),
            tree('b').workspace_key()
        )
    );
}

#[tokio::test]
async fn new_dependencies_at_the_same_commit_get_and_keep_their_own_tree() {
    // Tree a with the gpu extra serves; the user adds a group, with
    // no new commit or runtime image.
    let mut serving = serving_status('a');
    serving["runSource"]["dependencies"]["extras"] = json!(["gpu"]);
    let mut cl = git_cl_at('a', serving.clone());
    let deps = &mut cl.spec.git.as_mut().unwrap().dependencies;
    deps.extras = vec!["gpu".into()];
    deps.groups = vec!["test".into()];
    let state = api_with(cl, mid_rollout());
    // Admitted before the gpu extra: it still runs on plain tree a.
    let mut run = Run::new(
        "r",
        serde_json::from_value(json!({
            "codeLocationRef": { "name": "x" },
            "image": runtime('a'),
            "target": "job",
            "source": run_source_json('a'),
        }))
        .unwrap(),
    );
    run.metadata.namespace = Some("y".into());

    reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(vec![run]),
    )
    .await;

    let s = state.lock().unwrap();
    let target = template_source(&s.deployments["x"]).unwrap();
    assert_eq!(target.dependencies.groups, ["test"]);
    let pod = s.deployments["x"]
        .spec
        .clone()
        .unwrap()
        .template
        .spec
        .unwrap();
    let sub_paths: Vec<_> = pod
        .init_containers
        .iter()
        .flatten()
        .chain(&pod.containers)
        .flat_map(|c| c.volume_mounts.iter().flatten())
        .filter(|m| m.mount_path == workspace::WORKSPACE_MOUNT)
        .map(|m| m.sub_path.clone())
        .collect();
    let target_key = target.workspace_key();
    assert_eq!(
        sub_paths,
        [Some(target_key.clone()), Some(target_key.clone())]
    );
    let serving_tree: RunSource = serde_json::from_value(serving["runSource"].clone()).unwrap();
    let mut trees = [
        tree('a').workspace_key(),
        serving_tree.workspace_key(),
        target_key,
    ];
    trees.sort();
    assert_eq!(
        s.config_maps[&keep_config_map_name("x")]
            .data
            .as_ref()
            .unwrap()["keep"],
        trees.join(","),
        "one tree per dependency setting"
    );
}

#[tokio::test]
async fn keep_set_takes_the_runs_of_this_code_location_from_the_run_store() {
    let state = rolling_out_beside_a_long_run();
    let mut other_namespace = run_for("x", 'f', Some(RunPhase::Running));
    other_namespace.metadata.namespace = Some("z".into());
    let runs = run_store(vec![
        long_run(),
        run_for("x", 'd', Some(RunPhase::Succeeded)),
        run_for("other", 'e', Some(RunPhase::Running)),
        other_namespace,
    ]);

    let (_, requests) =
        reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

    let lists = runs_lists(&requests);
    assert!(lists.is_empty(), "{lists:?}");
    assert_eq!(
        keep_in(&state),
        keep_value(vec![
            tree('a').workspace_key(),
            tree('b').workspace_key(),
            source_at('c').workspace_key(),
        ])
    );
}

#[tokio::test]
async fn keep_set_in_force_stays_until_the_run_store_has_synced() {
    let state = rolling_out_beside_a_long_run();
    let in_force = keep_in(&state);
    // The first LIST has not reached the long run yet.
    let (runs, _listing) = listing_run_store(Vec::new());

    let (_, requests) =
        reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

    let lists = runs_lists(&requests);
    assert!(lists.is_empty(), "{lists:?}");
    let keep_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.path.contains("/configmaps/"))
        .collect();
    assert!(keep_requests.is_empty(), "{keep_requests:?}");
    assert_eq!(keep_in(&state), in_force);
    // The rest of the pass goes on: b rolls out, runs stay on a.
    assert_eq!(
        template_source(&state.lock().unwrap().deployments["x"]),
        Some(tree('b'))
    );
    assert_eq!(
        patched_status(&requests).expect("status patch")["runSource"],
        run_source_json('a')
    );
}

#[tokio::test]
async fn sync_container_gets_the_chart_floors_as_whole_numbers() {
    for (chart, keep, age) in [
        (vec![], "3", "3600"),
        (
            vec![
                ("RIVERS_WORKSPACE_KEEP_REVISIONS", "5"),
                ("RIVERS_WORKSPACE_MIN_AGE", "90m"),
            ],
            "5",
            "5400",
        ),
        (
            vec![
                ("RIVERS_WORKSPACE_KEEP_REVISIONS", "0"),
                ("RIVERS_WORKSPACE_MIN_AGE", "0s"),
            ],
            "0",
            "0",
        ),
        (vec![("RIVERS_WORKSPACE_MIN_AGE", "24h")], "3", "86400"),
        (vec![("RIVERS_WORKSPACE_MIN_AGE", "45s")], "3", "45"),
        (vec![("RIVERS_WORKSPACE_MIN_AGE", "600")], "3", "600"),
    ] {
        let mut vars = vec![("RIVERS_WORKSPACE_SHARED_ENABLED", "true")];
        vars.extend(chart.iter().copied());
        let workspace = workspace_config(&vars).unwrap();
        let state = api_with(git_cl_at('a', serving_status('a')), rolled_out());

        reconcile_in(
            &state,
            LeaderGate::leading(),
            &resolver(),
            workspace,
            run_store(Vec::new()),
        )
        .await;

        let pod = state.lock().unwrap().deployments["x"]
            .spec
            .clone()
            .unwrap()
            .template
            .spec
            .unwrap();
        let floors: Vec<(&str, &str)> = pod.init_containers.as_ref().unwrap()[0]
            .env
            .iter()
            .flatten()
            .filter(|e| {
                matches!(
                    e.name.as_str(),
                    "RIVERS_WORKSPACE_KEEP_REVISIONS" | "RIVERS_WORKSPACE_MIN_AGE_SECONDS"
                )
            })
            .filter_map(|e| Some((e.name.as_str(), e.value.as_deref()?)))
            .collect();
        assert_eq!(
            floors,
            [
                ("RIVERS_WORKSPACE_KEEP_REVISIONS", keep),
                ("RIVERS_WORKSPACE_MIN_AGE_SECONDS", age),
            ],
            "{chart:?}"
        );
    }
}

#[tokio::test]
async fn fallback_pods_ignore_the_chart_floors() {
    let mut prune_env = Vec::new();
    let mut templates = Vec::new();
    for chart in [
        vec![],
        vec![
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "5"),
            ("RIVERS_WORKSPACE_MIN_AGE", "90m"),
        ],
    ] {
        let state = api_with(git_cl_at('a', serving_status('a')), rolled_out());

        reconcile_in(
            &state,
            LeaderGate::leading(),
            &resolver(),
            workspace_config(&chart).unwrap(),
            run_store(Vec::new()),
        )
        .await;

        let template = state.lock().unwrap().deployments["x"]
            .spec
            .clone()
            .unwrap()
            .template;
        let sync = &template
            .spec
            .as_ref()
            .unwrap()
            .init_containers
            .as_ref()
            .unwrap()[0];
        prune_env.push(
            sync.env
                .iter()
                .flatten()
                .filter(|e| e.name.starts_with("RIVERS_WORKSPACE_"))
                .map(|e| (e.name.clone(), e.value.clone()))
                .collect::<Vec<_>>(),
        );
        templates.push(template);
    }

    // An emptyDir holds no other tree to prune.
    assert_eq!(prune_env, [vec![], vec![]]);
    // So a change of the floors rolls no fallback Deployment.
    assert_eq!(templates[0], templates[1]);
}

#[tokio::test]
async fn leader_keeps_runs_on_the_rolled_out_tree_while_a_commit_rolls_out() {
    let status = leader_pass(git_cl_at('b', serving_status('a')), mid_rollout()).await;

    assert_eq!(status["phase"], "Ready");
    assert_eq!(status["resolvedCommit"], commit('a'));
    assert_eq!(status["resolvedImage"], runtime('a'));
    assert_eq!(status["runSource"], run_source_json('a'));
    let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
    assert_eq!(available["reason"], REASON_ROLLING_OUT, "{available}");
    assert!(
        available["message"]
            .as_str()
            .is_some_and(|m| m.contains(&commit('b')[..7])),
        "{available}"
    );
}

#[tokio::test]
async fn leader_moves_runs_to_the_new_tree_once_every_pod_runs_it() {
    let status = leader_pass(git_cl_at('b', serving_status('a')), rolled_out()).await;

    assert_eq!(status["phase"], "Ready");
    assert_eq!(status["resolvedCommit"], commit('b'));
    assert_eq!(status["resolvedImage"], runtime('b'));
    assert_eq!(status["runSource"], run_source_json('b'));
    assert_eq!(status["source"], format!("{} (pinned)", &commit('b')[..7]));
    let available = condition(&status, CONDITION_DEPLOYMENT_AVAILABLE);
    assert_eq!(available["reason"], REASON_MIN_REPLICAS, "{available}");
}

#[tokio::test]
async fn follower_publishes_nothing_new_after_the_leader_mid_rollout() {
    let state = api_with(git_cl_at('b', serving_status('a')), mid_rollout());

    let leader = pass(&state, LeaderGate::leading()).await;
    assert_eq!(leader.unwrap()["resolvedCommit"], commit('a'));
    // Same Deployment, the leader's status: a follower that disagreed
    // would patch here, and the two would overwrite each other.
    assert_eq!(pass(&state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn run_pods_size_their_workspace_like_the_code_location_pods() {
    use crate::run::test_helpers::emptydir_limits;
    use k8s_openapi::api::core::v1::Pod;
    use rivers_k8s::crd::run::Run;

    for workspace_size in [Some("10Gi"), None] {
        let mut cl = git_cl_at('a', serving_status('a'));
        cl.spec.git.as_mut().unwrap().workspace_size =
            workspace_size.map(|s| Quantity(s.to_string()));
        let state = api_with(cl.clone(), rolled_out());
        pass(&state, LeaderGate::leading()).await;
        let cl_pod = Pod {
            spec: state.lock().unwrap().deployments["x"]
                .spec
                .clone()
                .and_then(|d| d.template.spec),
            ..Default::default()
        };

        let run = Run::new(
            "r",
            serde_json::from_value(json!({
                "codeLocationRef": { "name": "x" },
                "image": runtime('a'),
                "target": "job",
                "source": run_source_json('a'),
            }))
            .unwrap(),
        );
        let run_pod = crate::run::pod_builder::build_executor_pod(
            &run,
            "r-executor",
            "run-1",
            false,
            &cl.spec,
            &Default::default(),
            &Default::default(),
            &WorkspaceConfig::default(),
        );

        let limit = workspace_size.unwrap_or("2Gi").to_string();
        assert_eq!(
            emptydir_limits(&cl_pod),
            (Some(limit.clone()), Some(limit)),
            "workspaceSize {workspace_size:?}"
        );
        assert_eq!(
            emptydir_limits(&run_pod),
            emptydir_limits(&cl_pod),
            "workspaceSize {workspace_size:?}"
        );
    }
}
