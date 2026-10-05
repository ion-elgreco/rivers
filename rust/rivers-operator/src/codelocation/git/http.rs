//! Smart-HTTP transport arm: fetch the ref advertisement over the
//! resolver's rustls `reqwest` client. One GET, the body read as it arrives,
//! no negotiation:
//!
//! ```text
//! GET <repo-url>/info/refs?service=git-upload-pack
//! ```
//!
//! Status mapping: 401/403 are terminal auth failures, 404 means the
//! repository itself is absent (terminal until the CR or the remote
//! changes), 429 — and 503 with a `Retry-After` — is a rate limit that
//! carries the host's `Retry-After`, everything else transport-level is
//! transient. Dumb-HTTP servers (which ignore the `service` parameter and
//! return a plain refs file) are rejected on content-type rather than
//! producing a confusing parse error.

use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use rivers_k8s::crd::code_location::GitRef;

use super::{Advertisement, GitError, RefMatcher};
use crate::codelocation::registry::parse_retry_after;

/// The most advertisement bytes a resolve reads. Protocol v0 lists every
/// ref, so a repo with very many refs answers with a huge body; the resolver
/// keeps only the lines it needs, but still reads them all. Past this size
/// the CR is a v2 `ls-refs` candidate (deferred).
pub const DEFAULT_MAX_ADVERTISEMENT_BYTES: usize = 32 * 1024 * 1024;

const ADVERTISEMENT_CONTENT_TYPE: &str = "application/x-git-upload-pack-advertisement";

/// Credentials resolved from the CR's Secret (`username`/`password` keys —
/// a forge token is just a password with any username).
#[derive(Clone, Debug, Default)]
pub enum GitAuth {
    #[default]
    Anonymous,
    Basic {
        username: String,
        password: String,
    },
}

/// GET the smart-HTTP ref advertisement and keep the refs that answer
/// `wanted`, reading the body as it arrives.
pub async fn fetch_advertisement(
    client: &reqwest::Client,
    repo_url: &str,
    auth: &GitAuth,
    wanted: &GitRef,
    timeout: Duration,
) -> Result<Advertisement, GitError> {
    fetch_advertisement_with_cap(
        client,
        repo_url,
        auth,
        wanted,
        timeout,
        DEFAULT_MAX_ADVERTISEMENT_BYTES,
    )
    .await
}

pub(crate) async fn fetch_advertisement_with_cap(
    client: &reqwest::Client,
    repo_url: &str,
    auth: &GitAuth,
    wanted: &GitRef,
    timeout: Duration,
    cap: usize,
) -> Result<Advertisement, GitError> {
    // Credentials come from the git Secret only, as in the pods; the url's
    // user would go out as a second `Authorization` header.
    let mut repo = super::parse_url(repo_url)?;
    let _ = repo.set_username("");
    let _ = repo.set_password(None);
    let url = format!(
        "{}/info/refs?service=git-upload-pack",
        repo.as_str().trim_end_matches('/')
    );
    let mut request = client.get(&url).timeout(timeout);
    if let GitAuth::Basic { username, password } = auth {
        request = request.basic_auth(username, Some(password));
    }

    let response = request
        .send()
        .await
        .map_err(|e| GitError::Unreachable(e.to_string()))?;

    match response.status() {
        s if s.is_success() => {}
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return Err(GitError::AuthFailed(format!(
                "HTTP {} from {url}",
                response.status()
            )));
        }
        StatusCode::NOT_FOUND => {
            return Err(GitError::RefNotFound(format!(
                "repository not found at {url} (HTTP 404)"
            )));
        }
        s => {
            let retry_after = parse_retry_after(response.headers());
            if s == StatusCode::TOO_MANY_REQUESTS
                || (s == StatusCode::SERVICE_UNAVAILABLE && retry_after.is_some())
            {
                return Err(GitError::RateLimited {
                    message: format!(
                        "HTTP {s} from {}",
                        super::host_of(repo_url).unwrap_or_default()
                    ),
                    retry_after,
                });
            }
            return Err(GitError::Unreachable(format!("HTTP {s} from {url}")));
        }
    }

    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !content_type.starts_with(ADVERTISEMENT_CONTENT_TYPE) {
        return Err(GitError::Malformed(format!(
            "server did not answer smart HTTP (content-type '{content_type}'); \
             dumb HTTP is not supported"
        )));
    }

    let mut response = response;
    let mut refs = RefMatcher::new(wanted);
    let mut read = 0;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| GitError::Unreachable(e.to_string()))?
    {
        read += chunk.len();
        if read > cap {
            return Err(GitError::Malformed(format!(
                "advertisement exceeds {cap} bytes"
            )));
        }
        if refs.feed(&chunk)? {
            break;
        }
    }
    refs.finish()
}

#[cfg(test)]
mod tests {
    use super::super::fixtures;
    use super::super::{GitError, commit_for_ref};
    use super::*;
    use base64::Engine as _;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    fn advertisement_response() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("content-type", ADVERTISEMENT_CONTENT_TYPE)
            .set_body_bytes(fixtures::http_adv())
    }

    /// Fetch the refs of `repo` that answer `main`, anonymously.
    async fn fetch_main(repo: &str) -> Result<Advertisement, GitError> {
        let main = fixtures::branch("main");
        fetch_advertisement(&client(), repo, &GitAuth::Anonymous, &main, TIMEOUT).await
    }

    /// The commit `adv` resolves `main` to.
    fn main_commit(adv: &Advertisement) -> String {
        commit_for_ref(adv, &fixtures::branch("main"))
            .unwrap()
            .commit
    }

    #[tokio::test]
    async fn fetches_and_resolves_a_branch_end_to_end() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/acme/pipelines.git/info/refs"))
            .and(query_param("service", "git-upload-pack"))
            .respond_with(advertisement_response())
            .mount(&server)
            .await;

        let repo = format!("{}/acme/pipelines.git", server.uri());
        let adv = fetch_main(&repo).await.unwrap();
        let resolved = commit_for_ref(&adv, &fixtures::branch("main")).unwrap();
        assert_eq!(resolved.commit, fixtures::oid('a'));
        assert_eq!(resolved.ref_name.as_deref(), Some("refs/heads/main"));
    }

    #[tokio::test]
    async fn sends_basic_auth_and_normalizes_trailing_slash() {
        let server = MockServer::start().await;
        let expected = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("bot:s3cret")
        );
        // The mock only matches when the Authorization header is exactly
        // right and the path is not doubled by the trailing slash — a wrong
        // request falls through to wiremock's 404 and fails the test.
        Mock::given(method("GET"))
            .and(path("/repo.git/info/refs"))
            .and(header("authorization", expected.as_str()))
            .respond_with(advertisement_response())
            .mount(&server)
            .await;

        let repo = format!("{}/repo.git/", server.uri());
        let auth = GitAuth::Basic {
            username: "bot".to_string(),
            password: "s3cret".to_string(),
        };
        let main = fixtures::branch("main");
        let adv = fetch_advertisement(&client(), &repo, &auth, &main, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(main_commit(&adv), fixtures::oid('a'));
    }

    #[tokio::test]
    async fn unauthorized_and_forbidden_are_auth_failed() {
        for status in [401u16, 403] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let err = fetch_main(&format!("{}/r.git", server.uri()))
                .await
                .unwrap_err();
            assert!(
                matches!(err, GitError::AuthFailed(_)),
                "HTTP {status}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn repository_not_found_is_ref_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let err = fetch_main(&format!("{}/gone.git", server.uri()))
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::RefNotFound(ref m) if m.contains("404")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn server_error_is_unreachable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let err = fetch_main(&format!("{}/r.git", server.uri()))
            .await
            .unwrap_err();
        assert!(matches!(err, GitError::Unreachable(_)), "{err}");
    }

    #[tokio::test]
    async fn error_messages_leave_out_the_urls_user_and_password() {
        for (status, error) in [
            (
                403,
                "git authentication failed: HTTP 403 Forbidden from {url}",
            ),
            (
                404,
                "ref not found: repository not found at {url} (HTTP 404)",
            ),
            (
                503,
                "git host unreachable: HTTP 503 Service Unavailable from {url}",
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            for userinfo in ["bot@", "bot:ghp_S3cr3t@"] {
                let repo = format!("http://{userinfo}{}/r.git", server.address());
                let err = fetch_main(&repo).await.unwrap_err();

                let url = format!(
                    "http://{}/r.git/info/refs?service=git-upload-pack",
                    server.address()
                );
                assert_eq!(err.to_string(), error.replace("{url}", &url), "{repo}");
            }
        }
    }

    #[tokio::test]
    async fn only_the_secrets_credentials_are_sent_never_the_urls_user() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(advertisement_response())
            .mount(&server)
            .await;
        let repo = format!("http://bot@{}/r.git", server.address());
        let secret = GitAuth::Basic {
            username: "ci-bot".to_string(),
            password: "ghp_S3cr3t".to_string(),
        };
        let from_secret = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("ci-bot:ghp_S3cr3t")
        );

        let main = fixtures::branch("main");
        for (case, auth, sent) in [
            ("no Secret", GitAuth::Anonymous, vec![]),
            ("a Secret", secret, vec![from_secret]),
        ] {
            fetch_advertisement(&client(), &repo, &auth, &main, TIMEOUT)
                .await
                .unwrap();
            let requests = server.received_requests().await.unwrap();
            let authorization: Vec<&str> = requests
                .last()
                .unwrap()
                .headers
                .get_all("authorization")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect();
            assert_eq!(authorization, sent, "{case}");
        }
    }

    #[tokio::test]
    async fn connection_refused_is_unreachable() {
        // Port 1 needs root to bind, so nothing listens there — unlike
        // "drop a MockServer and reuse its port", which races against a
        // concurrent test's server grabbing the freed port.
        let err = fetch_main("http://127.0.0.1:1/r.git").await.unwrap_err();
        assert!(matches!(err, GitError::Unreachable(_)), "{err}");
    }

    #[tokio::test]
    async fn dumb_http_server_is_rejected_on_content_type() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/plain; charset=utf-8")
                    .set_body_bytes(fixtures::ssh_adv()),
            )
            .mount(&server)
            .await;
        let err = fetch_main(&format!("{}/r.git", server.uri()))
            .await
            .unwrap_err();
        assert!(
            matches!(err, GitError::Malformed(ref m) if m.contains("smart")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn oversized_advertisement_is_rejected() {
        // A well-formed advertisement: the cap counts the bytes read, not
        // the bytes the resolver keeps.
        let pulls = fixtures::lines_with_pulls(1_000);
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                advertisement_response()
                    .set_body_bytes(fixtures::over_http(fixtures::body(&pulls))),
            )
            .mount(&server)
            .await;
        let err = fetch_advertisement_with_cap(
            &client(),
            &format!("{}/r.git", server.uri()),
            &GitAuth::Anonymous,
            &fixtures::branch("main"),
            TIMEOUT,
            16 * 1024,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed advertisement: advertisement exceeds 16384 bytes"
        );
    }

    #[tokio::test]
    async fn an_advertisement_cut_before_its_flush_pkt_is_malformed() {
        let mut cut = fixtures::http_adv();
        cut.truncate(cut.len() - fixtures::flush().len());
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(advertisement_response().set_body_bytes(cut))
            .mount(&server)
            .await;
        let err = fetch_main(&format!("{}/r.git", server.uri()))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed advertisement: advertisement not terminated by a flush-pkt (truncated \
             response?)"
        );
    }
}
