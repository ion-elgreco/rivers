//! Switches between image and git mode, and the git workspace left behind.

use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::*;
use rivers_k8s::crd::run::RunPhase;
use serde_json::json;
use wiremock::ResponseTemplate;

use super::support::*;
use crate::codelocation::reconcile::{DEPLOYMENT_ROLLOUT_POLL, FOLLOWER_WAIT, WORKSPACE_RECHECK};
use crate::codelocation::resources::{keep_config_map_name, workspace_pvc_name};
use crate::leader::LeaderGate;

#[tokio::test]
async fn image_mode_publishes_the_new_digest_mid_rollout_as_before() {
    let image = |c: char| format!("ghcr.io/acme/pipeline@{}", digest(c));
    let mut cl = make_cl(json!({ "image": "ghcr.io/acme/pipeline", "digest": digest('b') }));
    cl.status = Some(
        serde_json::from_value(json!({ "phase": "Ready", "resolvedImage": image('a') })).unwrap(),
    );

    let status = leader_pass(cl, mid_rollout()).await;

    assert_eq!(status["phase"], "Ready");
    assert_eq!(status["resolvedImage"], image('b'));
    assert_eq!(status["source"], image('b'));
    assert_eq!(status.get("runSource"), None, "{status}");
    assert_eq!(
        condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["reason"],
        REASON_MIN_REPLICAS
    );
}

#[tokio::test]
async fn image_mode_publishes_no_git_fields_after_a_switch_from_git() {
    let state = api_with(image_cl(git_era_status()), rolled_out());

    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    assert_eq!(status["phase"], "Ready", "{status}");
    assert_eq!(
        status["resolvedImage"],
        format!("{PIPELINE}@{}", digest('i'))
    );
    for field in GIT_FIELDS {
        assert_eq!(status.get(field), None, "{field} in {status}");
    }
}

#[tokio::test]
async fn a_follower_leaves_the_switch_from_git_to_the_leader() {
    let registry = registry(vec![manifest('i')]).await;
    // The git status's resolvedImage is the runtime image, on a tag
    // or with spec.digest pinned.
    for cl in [
        image_cl_on(&registry, git_era_status()),
        image_cl(git_era_status()),
    ] {
        let spec = serde_json::to_string(&cl.spec).unwrap();
        let state = api_with(cl, rolled_out());

        let (requeue, requests) = reconcile_with(&state, LeaderGate::new(), &resolver()).await;

        assert_eq!(writes(&requests), [], "{spec}");
        assert_eq!(main_image(&state), None, "{spec}");
        assert_eq!(requeue, Action::requeue(FOLLOWER_WAIT), "{spec}");
    }

    let state = api_with(image_cl_on(&registry, git_era_status()), rolled_out());
    let image = format!("{}/acme/runtime@{}", registry.address(), digest('i'));
    let leader = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");
    assert_eq!(leader["resolvedImage"], image);

    // Then the follower refreshes as usual: it applies the leader's
    // image and has nothing new to publish.
    let (_, requests) = reconcile_with(&state, LeaderGate::new(), &resolver()).await;

    assert!(
        writes(&requests).contains(&("PATCH", "/apis/apps/v1/namespaces/y/deployments/x")),
        "{requests:?}"
    );
    assert_eq!(main_image(&state), Some(image));
    assert_eq!(patched_status(&requests), None);
}

#[tokio::test]
async fn a_switch_to_image_mode_that_cannot_resolve_its_image_keeps_the_git_status() {
    let registry = registry(vec![ResponseTemplate::new(404)]).await;
    let state = api_with(image_cl_on(&registry, git_era_status()), rolled_out());

    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let failed = patched_status(&requests).expect("status patch");
    assert_eq!(failed["phase"], "Failed", "{failed}");
    assert_eq!(
        says(&failed, CONDITION_IMAGE_RESOLVED).1,
        REASON_TAG_NOT_FOUND
    );
    // The pods still run the git source: its status stays until an
    // image pass replaces it.
    let git = git_era_status();
    for field in GIT_FIELDS.into_iter().chain(["resolvedImage"]) {
        assert_eq!(failed[field], git[field], "{field}");
    }
    assert_eq!(
        writes(&requests),
        [(
            "PATCH",
            "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
        )]
    );
    assert_eq!(pass(&state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn a_registry_error_keeps_the_kept_workspace_in_status() {
    let registry = registry(vec![ResponseTemplate::new(503), manifest('i')]).await;
    let cl = image_cl_on(&registry, image_status_keeping_the_workspace());
    let state = api_with(cl.clone(), rolled_out());
    add_git_workspace(&state, &cl);

    let failed = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    assert_eq!(failed["phase"], "Failed", "{failed}");
    assert_eq!(
        workspace_kept(&failed),
        workspace_kept(&image_status_keeping_the_workspace())
    );
    assert_eq!(git_workspace_in(&state), (true, true));

    // The registry answers again: the cleanup goes on.
    let (_, requests) = reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(Vec::new()),
    )
    .await;

    assert_eq!(
        deleted(&requests),
        [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
    );
    assert_eq!(workspace_kept(&stored_status(&state)), None);
}

#[tokio::test]
async fn the_leader_looks_for_a_git_workspace_only_after_a_git_source() {
    let looks = vec![("GET", KEEP_PATH), ("GET", PVC_PATH)];
    let cases = [
        ("never reconciled", None, vec![]),
        ("an image status", Some(image_status()), vec![]),
        ("a git status", Some(git_era_status()), looks.clone()),
        // Also after an operator restart: the condition stays until
        // the workspace is gone.
        (
            "a kept workspace",
            Some(image_status_keeping_the_workspace()),
            looks,
        ),
    ];
    for (case, prior, expected) in cases {
        let mut cl = make_cl(json!({ "image": PIPELINE, "digest": digest('i') }));
        cl.status = prior.map(|prior| serde_json::from_value(prior).unwrap());
        let state = api_with(cl, rolled_out());

        let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

        let mut seen: Vec<_> = requests
            .iter()
            .filter(|r| {
                r.path.contains("/persistentvolumeclaims") || r.path.contains("/configmaps")
            })
            .map(|r| (r.method.as_str(), r.path.as_str()))
            .collect();
        seen.sort();
        assert_eq!(seen, expected, "{case}");
        // Nothing of the git workspace is left: nothing to keep.
        assert_eq!(workspace_kept(&stored_status(&state)), None, "{case}");
    }
}

#[tokio::test]
async fn leaving_git_deletes_the_workspace_once_no_pod_can_mount_it() {
    let cl = image_cl(git_era_status());
    let state = api_with(cl.clone(), rolled_out());
    add_git_workspace(&state, &cl);
    let mut other_namespace = run_for("x", 'f', Some(RunPhase::Running));
    other_namespace.metadata.namespace = Some("z".into());
    // None of these runs mounts x's workspace.
    let runs = || {
        run_store(vec![
            image_run("image-run", RunPhase::Running),
            run_for("other", 'e', Some(RunPhase::Running)),
            other_namespace.clone(),
        ])
    };

    // The pass that leaves git: until this status is out, the
    // webhook still gives new runs the git source.
    let (_, leaving) =
        reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs()).await;
    assert_eq!(deleted(&leaving), []);
    let status = patched_status(&leaving).expect("status patch");
    assert_eq!(
        workspace_kept(&status),
        Some((
            "True",
            REASON_ROLLING_OUT,
            "PVC 'x-workspace' of the git source stays until every code-location \
             pod runs the image"
        ))
    );

    let (requeue, requests) =
        reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs()).await;

    assert_eq!(
        deleted(&requests),
        [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
    );
    assert_eq!(git_workspace_in(&state), (false, false));
    let status = patched_status(&requests).expect("status patch");
    assert_eq!(workspace_kept(&status), None, "{status}");
    assert_eq!(status["phase"], "Ready");
    // Ready on a pinned digest, with nothing left to delete.
    assert_eq!(requeue, Action::await_change());
}

#[tokio::test]
async fn the_workspace_stays_while_a_pod_may_mount_it_and_the_status_says_why() {
    let git_run = |c: char, phase: Option<RunPhase>| run_for("x", c, phase);
    let runs_use_it = |names: &str| {
        format!(
            "PVC 'x-workspace' of the git source stays while the pods of these runs \
             mount it: {names}"
        )
    };
    let (listing, _writer) = listing_run_store(Vec::new());
    let cases = [
        (
            "a pod of the git source runs",
            mid_rollout(),
            run_store(Vec::new()),
            REASON_ROLLING_OUT,
            "PVC 'x-workspace' of the git source stays until every code-location pod \
             runs the image"
                .to_string(),
        ),
        (
            "the run store has not synced",
            rolled_out(),
            listing,
            REASON_RUNS_NOT_LISTED,
            "PVC 'x-workspace' of the git source stays until the operator has listed \
             the runs"
                .to_string(),
        ),
        (
            "a queued run",
            rolled_out(),
            run_store(vec![git_run('b', None)]),
            REASON_RUNS_USE_WORKSPACE,
            runs_use_it("run-b"),
        ),
        (
            "a pending and a running run",
            rolled_out(),
            run_store(vec![
                git_run('c', Some(RunPhase::Running)),
                git_run('b', Some(RunPhase::Pending)),
                image_run("image-run", RunPhase::Running),
            ]),
            REASON_RUNS_USE_WORKSPACE,
            runs_use_it("run-b, run-c"),
        ),
        (
            "a finished run, whose pod stays until the run is deleted",
            rolled_out(),
            run_store(vec![git_run('d', Some(RunPhase::Succeeded))]),
            REASON_RUNS_USE_WORKSPACE,
            runs_use_it("run-d"),
        ),
        (
            "many runs",
            rolled_out(),
            run_store(
                "abcdefg"
                    .chars()
                    .map(|c| git_run(c, Some(RunPhase::Failed)))
                    .collect(),
            ),
            REASON_RUNS_USE_WORKSPACE,
            runs_use_it("run-a, run-b, run-c, run-d, run-e and 2 more"),
        ),
    ];

    for (case, observed, runs, reason, message) in cases {
        let cl = image_cl(image_status_keeping_the_workspace());
        let state = api_with(cl.clone(), observed);
        add_git_workspace(&state, &cl);

        let (requeue, requests) =
            reconcile_in(&state, LeaderGate::leading(), &resolver(), shared(), runs).await;

        assert_eq!(deleted(&requests), [], "{case}");
        assert_eq!(git_workspace_in(&state), (true, true), "{case}");
        assert_eq!(
            workspace_kept(&stored_status(&state)),
            Some(("True", reason, message.as_str())),
            "{case}"
        );
        assert_eq!(requeue, Action::requeue(WORKSPACE_RECHECK), "{case}");
        // The follower shows what the leader found.
        assert_eq!(pass(&state, LeaderGate::new()).await, None, "{case}");
    }
}

#[tokio::test]
async fn the_workspace_goes_once_the_last_run_of_the_git_source_is_deleted() {
    let cl = image_cl(image_status_keeping_the_workspace());
    let state = api_with(cl.clone(), rolled_out());
    add_git_workspace(&state, &cl);
    let finished = run_for("x", 'd', Some(RunPhase::Succeeded));
    reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(vec![finished]),
    )
    .await;
    assert_eq!(git_workspace_in(&state), (true, true));

    let (requeue, requests) = reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(Vec::new()),
    )
    .await;

    assert_eq!(
        deleted(&requests),
        [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))]
    );
    assert_eq!(git_workspace_in(&state), (false, false));
    let status = patched_status(&requests).expect("status patch");
    assert_eq!(workspace_kept(&status), None, "{status}");
    assert_eq!(requeue, Action::await_change());
}

#[tokio::test]
async fn only_the_code_locations_own_workspace_is_deleted() {
    // (owner uid of the PVC and ConfigMap named like x's, deleted)
    let cases = [
        (Some("u"), true),
        (Some("uid-of-an-earlier-x"), false),
        (None, false),
    ];
    for (owner_uid, deletes) in cases {
        let cl = image_cl(image_status_keeping_the_workspace());
        assert_eq!(cl.metadata.uid.as_deref(), Some("u"));
        let state = api_with(cl.clone(), rolled_out());
        let mut owner = cl.clone();
        owner.metadata.uid = owner_uid.map(str::to_string);
        let (mut pvc, mut keep) = git_workspace_of(&owner);
        if owner_uid.is_none() {
            pvc.metadata.owner_references = None;
            keep.metadata.owner_references = None;
        }
        {
            let mut s = state.lock().unwrap();
            s.pvcs.insert(workspace_pvc_name("x"), pvc.clone());
            s.config_maps
                .insert(keep_config_map_name("x"), keep.clone());
        }

        let (requeue, requests) = reconcile_in(
            &state,
            LeaderGate::leading(),
            &resolver(),
            shared(),
            run_store(Vec::new()),
        )
        .await;

        let case = format!("owner {owner_uid:?}");
        // Nothing of the code location's own is left to keep.
        assert_eq!(workspace_kept(&stored_status(&state)), None, "{case}");
        let s = state.lock().unwrap();
        if deletes {
            assert_eq!(
                deleted(&requests),
                [(KEEP_PATH, Some(KEEP_UID)), (PVC_PATH, Some(PVC_UID))],
                "{case}"
            );
            assert!(s.pvcs.is_empty(), "{case}");
            assert!(s.config_maps.is_empty(), "{case}");
        } else {
            assert_eq!(deleted(&requests), [], "{case}");
            assert_eq!(s.pvcs.get(&workspace_pvc_name("x")), Some(&pvc), "{case}");
            assert_eq!(
                s.config_maps.get(&keep_config_map_name("x")),
                Some(&keep),
                "{case}"
            );
        }
        assert_eq!(requeue, Action::await_change(), "{case}");
    }
}

#[tokio::test]
async fn a_git_rollout_waits_until_the_old_workspace_pvc_is_gone() {
    // Back to git while the PVC of the earlier git source is being
    // deleted: a pod cannot mount it.
    let cl = git_cl_at('a', image_status());
    let state = api_with(cl.clone(), rolled_out());
    let (mut pvc, _) = git_workspace_of(&cl);
    pvc.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-10-03T00:00:00Z".parse().unwrap(),
    ));
    pvc.metadata.finalizers = Some(vec!["kubernetes.io/pvc-protection".into()]);
    state
        .lock()
        .unwrap()
        .pvcs
        .insert(workspace_pvc_name("x"), pvc);

    let (requeue, requests) = reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(Vec::new()),
    )
    .await;

    let writes: Vec<_> = requests
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| (r.method.as_str(), r.path.as_str()))
        .collect();
    assert_eq!(writes, []);
    assert_eq!(state.lock().unwrap().deployments["x"].spec, None);
    assert_eq!(requeue, Action::requeue(DEPLOYMENT_ROLLOUT_POLL));

    // Kubernetes deletes it once no pod mounts it.
    state.lock().unwrap().pvcs.clear();
    let (_, requests) = reconcile_in(
        &state,
        LeaderGate::leading(),
        &resolver(),
        shared(),
        run_store(Vec::new()),
    )
    .await;

    let s = state.lock().unwrap();
    let created = requests
        .iter()
        .find(|r| r.method == "POST" && r.path.ends_with("/persistentvolumeclaims"))
        .and_then(|r| r.body.clone())
        .expect("a new workspace PVC");
    assert_eq!(created["metadata"]["name"], "x-workspace");
    assert_eq!(created["metadata"].get("deletionTimestamp"), None);
    let claims: Vec<_> = s.deployments["x"]
        .spec
        .clone()
        .and_then(|d| d.template.spec)
        .unwrap()
        .volumes
        .into_iter()
        .flatten()
        .filter_map(|v| v.persistent_volume_claim.map(|c| c.claim_name))
        .collect();
    assert_eq!(claims, ["x-workspace"]);
}
