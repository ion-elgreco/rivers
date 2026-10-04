//! The git host down, rate-limiting, or refusing the url or ref; fetch
//! times; credentials on the wire.

use std::sync::Arc;
use std::time::Duration;

use kube_runtime::controller::Action;
use rivers_k8s::crd::code_location::*;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::support::*;
use crate::codelocation::git;
use crate::codelocation::reconcile::TERMINAL_RETRY;
use crate::codelocation::reconcile::rollout::template_source;
use crate::leader::LeaderGate;

#[tokio::test]
async fn unreachable_git_host_keeps_the_serving_tree_ready() {
    let outage = outage(vec![ResponseTemplate::new(503)]).await;
    let status = patched_status(&outage.requests).expect("status patch");

    assert_eq!(status["phase"], "Ready", "{status}");
    assert_eq!(status["grpcEndpoint"], outage.serving["grpcEndpoint"]);
    assert_eq!(status["readyReplicas"], 1);
    for field in [
        "runSource",
        "resolvedCommit",
        "resolvedRef",
        "resolvedImage",
        "source",
    ] {
        assert_eq!(status[field], outage.serving[field], "{field}");
    }
    let error = unreachable_error(&outage.forge, "503 Service Unavailable");
    assert_eq!(
        source_resolved(&status),
        ("False", REASON_GIT_UNREACHABLE, error.as_str())
    );
    assert_eq!(status["message"], error);
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
    assert!(applies.is_empty(), "{applies:?}");
    assert_eq!(outage.requeue, Action::requeue(Duration::from_secs(60)));
}

#[tokio::test]
async fn a_git_resolution_error_keeps_status_message_over_a_failed_build() {
    let forge = forge(vec![advertisement(), ResponseTemplate::new(503)]).await;
    let state = api_with(branch_cl(&forge), rolled_out());
    pass(&state, LeaderGate::leading())
        .await
        .expect("rollout of main");
    // The pod restarts and cannot fetch main either: the git host is
    // down for the pods too.
    let fetch_error = format!(
        "fatal: unable to access '{}{REPO}/': The requested URL returned error: 503",
        forge.uri()
    );
    {
        let mut s = state.lock().unwrap();
        let deployment = s.deployments.get_mut("x").unwrap();
        let rollout = deployment.status.as_mut().unwrap();
        rollout.ready_replicas = Some(0);
        rollout.available_replicas = Some(0);
        let main = template_source(deployment).unwrap();
        let pod = code_location_pod(
            "x-1",
            &main,
            failed_sync(&fetch_error, "2026-10-03T00:00:00Z"),
        );
        s.pods.insert("x-1".into(), pod);
    }

    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    let error = unreachable_error(&forge, "503 Service Unavailable");
    assert_eq!(
        source_resolved(&status),
        ("False", REASON_GIT_UNREACHABLE, error.as_str())
    );
    assert_eq!(status["message"], error);
    assert_eq!(
        condition(&status, CONDITION_DEPLOYMENT_AVAILABLE)["message"],
        format!("workspace build of main@aaaaaaa failed (Error, exit code 1): {fetch_error}")
    );
}

#[tokio::test]
async fn rate_limited_git_host_keeps_the_serving_tree_ready_and_says_when_it_asks_again() {
    let before = jiff::Timestamp::now();
    let outage = outage(vec![
        ResponseTemplate::new(429).insert_header("retry-after", "900"),
    ])
    .await;
    let after = jiff::Timestamp::now();
    let status = patched_status(&outage.requests).expect("status patch");

    assert_eq!(status["phase"], "Ready", "{status}");
    for field in [
        "grpcEndpoint",
        "runSource",
        "resolvedCommit",
        "resolvedRef",
        "resolvedImage",
        "source",
    ] {
        assert_eq!(status[field], outage.serving[field], "{field}");
    }
    let (condition_status, reason, message) = source_resolved(&status);
    assert_eq!(
        (condition_status, reason),
        ("False", REASON_GIT_RATE_LIMITED),
        "{status}"
    );
    assert_eq!(status["message"], message);
    let (error, asks_again_at) = message
        .split_once("; the operator asks again at ")
        .unwrap_or_else(|| panic!("no retry time: {message}"));
    assert_eq!(
        error,
        format!(
            "rate-limited by the git host: HTTP 429 Too Many Requests from {}",
            outage.forge.address()
        )
    );
    let asks_again_at: jiff::Timestamp = asks_again_at.parse().unwrap();
    let wait = jiff::SignedDuration::from_secs(900);
    let second = jiff::SignedDuration::from_secs(1);
    assert!(
        before + wait - second <= asks_again_at && asks_again_at <= after + wait + second,
        "{asks_again_at} is not 900s after {before}..{after}"
    );
    assert_eq!(outage.requeue, Action::requeue(Duration::from_secs(900)));
}

#[tokio::test]
async fn status_stays_unresolved_while_the_ref_backs_off() {
    // The host answers again right after the failed poll; the leader
    // must not ask it before the backoff ends.
    let outage = outage(vec![ResponseTemplate::new(503), advertisement()]).await;
    let stale = patched_status(&outage.requests).expect("status patch");

    let (_, requests) = reconcile_with(&outage.state, LeaderGate::leading(), &outage.git).await;

    assert_eq!(patched_status(&requests), None);
    assert_eq!(outage.forge.received_requests().await.unwrap().len(), 2);
    let stored =
        serde_json::to_value(&outage.state.lock().unwrap().code_locations["x"]).unwrap()["status"]
            .clone();
    assert_eq!(source_resolved(&stored), source_resolved(&stale));
    assert_eq!(source_resolved(&stored).0, "False");
}

#[tokio::test]
async fn a_new_fetch_of_the_ref_shows_in_status_and_a_cached_answer_writes_nothing() {
    let forge = forge(vec![advertisement()]).await;
    let state = api_with(branch_cl(&forge), rolled_out());
    let first = pass(&state, LeaderGate::leading())
        .await
        .expect("rollout of main");
    assert_eq!(first["phase"], "Ready", "{first}");

    // The cached commit has expired: the leader fetches main again,
    // and it is still commit a.
    let git = resolver();
    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &git).await;

    let refetched = patched_status(&requests).expect("status patch");
    assert_eq!(forge.received_requests().await.unwrap().len(), 2);
    assert!(fetched_at(&refetched) > fetched_at(&first), "{refetched}");
    let mut rest = refetched.clone();
    for field in ["lastFetchedAt", "lastReconciled"] {
        rest[field] = first[field].clone();
    }
    assert_eq!(rest, first);

    // The pass that this status write starts: the cache answers, with
    // the same fetch.
    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &git).await;

    assert_eq!(patched_status(&requests), None);
    assert_eq!(forge.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_replica_with_an_older_fetch_does_not_move_last_fetched_at_back() {
    let forge = forge(vec![advertisement()]).await;
    let state = api_with(branch_cl(&forge), rolled_out());
    let older = resolver();
    reconcile_with(&state, LeaderGate::leading(), &older).await;
    let first = stored_status(&state);
    let newer = resolver();
    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &newer).await;
    let second = patched_status(&requests).expect("status patch");
    assert!(fetched_at(&second) > fetched_at(&first), "{second}");

    // Another replica that also acts as leader: its cache still holds
    // the first fetch.
    let (_, requests) = reconcile_with(&state, LeaderGate::leading(), &older).await;

    assert_eq!(patched_status(&requests), None);
    assert_eq!(
        stored_status(&state)["lastFetchedAt"],
        second["lastFetchedAt"]
    );
    assert_eq!(forge.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_pinned_commit_has_no_fetch_time() {
    // Never reconciled, or pinned after main (commit a) was fetched.
    for prior in [None, Some(git_era_status())] {
        let mut cl = git_cl_at('a', json!({}));
        cl.status = prior.map(|prior| serde_json::from_value(prior).unwrap());
        let case = format!("{:?}", cl.status);
        let state = api_with(cl, rolled_out());

        let status = pass(&state, LeaderGate::leading())
            .await
            .expect("status patch");

        assert_eq!(status["resolvedCommit"], commit('a'), "{case}");
        assert_eq!(status.get("lastFetchedAt"), None, "{case}: {status}");
        assert_eq!(pass(&state, LeaderGate::leading()).await, None, "{case}");
    }
}

#[tokio::test]
async fn follower_publishes_nothing_new_while_the_git_host_is_unreachable() {
    let outage = outage(vec![ResponseTemplate::new(503)]).await;
    assert!(patched_status(&outage.requests).is_some());

    assert_eq!(pass(&outage.state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn follower_publishes_nothing_new_after_a_terminal_git_error() {
    for (code, reason) in [(401, REASON_GIT_AUTH_FAILED), (404, REASON_REF_NOT_FOUND)] {
        let outage = outage(vec![ResponseTemplate::new(code)]).await;
        let failed = patched_status(&outage.requests).expect("status patch");
        assert_eq!(failed["phase"], "Failed", "{failed}");
        assert_eq!(failed.get("grpcEndpoint"), None, "{failed}");
        assert_eq!(source_resolved(&failed).1, reason, "{failed}");

        assert_eq!(
            pass(&outage.state, LeaderGate::new()).await,
            None,
            "HTTP {code}"
        );
    }
}

#[tokio::test]
async fn unreachable_git_host_fails_a_code_location_with_nothing_to_serve() {
    let forge = forge(vec![ResponseTemplate::new(503)]).await;
    let state = ApiState::default();
    state
        .lock()
        .unwrap()
        .code_locations
        .insert("x".into(), branch_cl(&forge));

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    let error = unreachable_error(&forge, "503 Service Unavailable");
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["message"], error);
    for field in [
        "grpcEndpoint",
        "readyReplicas",
        "runSource",
        "resolvedCommit",
    ] {
        assert_eq!(status.get(field), None, "{field}: {status}");
    }
    assert_eq!(
        source_resolved(&status),
        ("False", REASON_GIT_UNREACHABLE, error.as_str())
    );
    assert_eq!(requeue, Action::requeue(Duration::from_secs(60)));
    // The follower leaves the leader's verdict alone.
    assert_eq!(pass(&state, LeaderGate::new()).await, None);
}

#[tokio::test]
async fn a_pinned_uppercase_commit_fails_before_any_deployment() {
    // Created before the webhook and the CRD schema refused one.
    let upper = commit('a').to_ascii_uppercase();
    let state = ApiState::default();
    state.lock().unwrap().code_locations.insert(
        "x".into(),
        make_cl(json!({
            "git": { "url": URL, "ref": { "commit": upper } },
            "digest": digest('a'),
        })),
    );

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &resolver()).await;

    let status = patched_status(&requests).expect("status patch");
    let error = format!(
        "invalid git ref: git.ref.commit '{upper}' has uppercase letters — use the \
         lowercase SHA '{}'",
        commit('a')
    );
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["message"], error);
    assert_eq!(
        source_resolved(&status),
        ("False", REASON_INVALID_REF, error.as_str())
    );
    let writes: Vec<_> = requests
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| (r.method.as_str(), r.path.as_str()))
        .collect();
    assert_eq!(
        writes,
        [(
            "PATCH",
            "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
        )]
    );
    assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));
}

#[tokio::test]
async fn an_http_code_location_fails_without_a_request_unless_the_operator_allows_it() {
    // Created before the webhook refused http://, or with no webhook.
    let forge = forge(vec![advertisement()]).await;
    let state = api_with(branch_cl(&forge), rolled_out());
    let refusing = Arc::new(git::GitResolver::new(Duration::from_secs(5), false));

    let (requeue, requests) = reconcile_with(&state, LeaderGate::leading(), &refusing).await;

    let status = patched_status(&requests).expect("status patch");
    let error = "invalid git url: http:// sends the code and the git Secret's \
                 credentials unencrypted — use https://, or set \
                 operator.git.allowInsecure to true for a git host on a trusted network";
    assert_eq!(status["phase"], "Failed", "{status}");
    assert_eq!(status["message"], error);
    assert_eq!(
        source_resolved(&status),
        ("False", REASON_INVALID_URL, error)
    );
    assert_eq!(forge.received_requests().await.unwrap().len(), 0);
    let writes: Vec<_> = requests
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| (r.method.as_str(), r.path.as_str()))
        .collect();
    assert_eq!(
        writes,
        [(
            "PATCH",
            "/apis/rivers.io/v1alpha1/namespaces/y/codelocations/x/status"
        )]
    );
    assert_eq!(requeue, Action::requeue(TERMINAL_RETRY));

    // With operator.git.allowInsecure, the same code location rolls out.
    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");
    assert_eq!(status["resolvedCommit"], commit('a'), "{status}");
    assert_eq!(forge.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn https_code_location_fetches_with_the_password_of_a_secret_shared_with_ssh() {
    use super::support::{IDENTITY, KNOWN_HOSTS, PASSWORD, USERNAME, secret};
    use base64::Engine as _;
    use wiremock::matchers::header;

    let forge = MockServer::start().await;
    let basic =
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", USERNAME.1, PASSWORD.1));
    Mock::given(method("GET"))
        .and(path(format!("{REPO}/info/refs")))
        .and(header("authorization", format!("Basic {basic}")))
        .respond_with(advertisement())
        .expect(1)
        .mount(&forge)
        .await;
    let cl = make_cl(json!({
        "git": {
            "url": format!("{}{REPO}", forge.uri()),
            "ref": { "branch": "main" },
            "secretRef": { "name": "git-creds" },
        },
        "digest": digest('a'),
    }));
    let state = api_with(cl, rolled_out());
    state.lock().unwrap().secrets.insert(
        "git-creds".into(),
        Ok(secret(&[IDENTITY, KNOWN_HOSTS, USERNAME, PASSWORD])),
    );

    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    assert_eq!(status["phase"], "Ready", "{status}");
    assert_eq!(status["resolvedCommit"], commit('a'));
    assert_eq!(status["resolvedRef"], "refs/heads/main");
    assert_eq!(
        source_resolved(&status),
        ("True", REASON_COMMIT_RESOLVED, commit('a').as_str())
    );
    // The tree of the applied Deployment's pod template.
    assert_eq!(status["runSource"]["git"]["commit"], commit('a'));
    assert_eq!(status["runSource"]["git"]["secretName"], "git-creds");
}

#[tokio::test]
async fn https_code_location_fetches_with_a_password_file_that_ends_in_a_newline() {
    use super::support::secret;
    use base64::Engine as _;
    use wiremock::matchers::header;

    let forge = MockServer::start().await;
    let basic = base64::engine::general_purpose::STANDARD.encode("bot:ghp_TOKEN");
    Mock::given(method("GET"))
        .and(path(format!("{REPO}/info/refs")))
        .and(header("authorization", format!("Basic {basic}")))
        .respond_with(advertisement())
        .expect(1)
        .mount(&forge)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{REPO}/info/refs")))
        .respond_with(ResponseTemplate::new(401))
        .mount(&forge)
        .await;
    let cl = make_cl(json!({
        "git": {
            "url": format!("{}{REPO}", forge.uri()),
            "ref": { "branch": "main" },
            "secretRef": { "name": "git-creds" },
        },
        "digest": digest('a'),
    }));
    let state = api_with(cl, rolled_out());
    // echo ghp_TOKEN > token.txt; kubectl create secret generic git-creds \
    //   --from-literal=username=bot --from-file=password=token.txt
    state.lock().unwrap().secrets.insert(
        "git-creds".into(),
        Ok(secret(&[("username", "bot"), ("password", "ghp_TOKEN\n")])),
    );

    let status = pass(&state, LeaderGate::leading())
        .await
        .expect("status patch");

    assert_eq!(status["phase"], "Ready", "{status}");
    assert_eq!(status["resolvedCommit"], commit('a'));
    assert_eq!(
        source_resolved(&status),
        ("True", REASON_COMMIT_RESOLVED, commit('a').as_str())
    );
    let deployment = state.lock().unwrap().deployments["x"].clone();
    assert_eq!(
        template_source(&deployment).map(|source| source.git.commit),
        Some(commit('a'))
    );
}
