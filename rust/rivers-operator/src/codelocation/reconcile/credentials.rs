//! The git Secret of a code location, as [`GitCredentials`].

use k8s_openapi::api::core::v1::Secret;
use kube_client::api::Api;
use rivers_k8s::crd::code_location::GitSource;

use crate::codelocation::git::{self, GitCredentials};

/// Build [`GitCredentials`] from the CR's Secret. The url's scheme picks the
/// keys, like the pod's `rivers-runtime workspace-sync`: `ssh://` needs `identity` +
/// `known_hosts`;
/// `https://` / `http://` use `username` + `password`, or none for an
/// anonymous fetch. These two lose their trailing newlines, as the pod's sync
/// drops them. The other scheme's keys are ignored, so ssh and
/// https code locations can share one Secret. A Secret that does not exist
/// or that the operator may not read is an auth failure, not an outage.
pub(super) async fn git_credentials(
    git_spec: &GitSource,
    secrets_api: &Api<Secret>,
) -> Result<GitCredentials, git::GitError> {
    let transport = git::Transport::of(&git_spec.url)?;
    let Some(secret_ref) = &git_spec.secret_ref else {
        return match transport {
            git::Transport::Http => Ok(GitCredentials::Anonymous),
            git::Transport::Ssh => Err(git::GitError::AuthFailed(
                "ssh:// urls need a git Secret (spec.git.secretRef) with `identity` and \
                 `known_hosts`"
                    .to_string(),
            )),
        };
    };
    let name = &secret_ref.name;
    let secret = secrets_api.get(name).await.map_err(|e| match e {
        kube_client::Error::Api(status) if status.code == 404 => {
            git::GitError::AuthFailed(format!("git Secret '{name}' does not exist"))
        }
        kube_client::Error::Api(status) if status.code == 403 => {
            git::GitError::AuthFailed(format!(
                "the operator may not read git Secret '{name}': {}",
                status.message
            ))
        }
        e => git::GitError::Unreachable(format!("reading git Secret '{name}': {e}")),
    })?;
    let data = secret.data.unwrap_or_default();
    let entry = |key: &str| {
        data.get(key)
            .map(|v| String::from_utf8_lossy(&v.0).into_owned())
    };
    let cat = |key: &str| entry(key).map(|v| v.trim_end_matches('\n').to_owned());

    match transport {
        git::Transport::Ssh => match (entry("identity"), entry("known_hosts")) {
            (Some(private_key_openssh), Some(known_hosts)) => Ok(GitCredentials::Ssh {
                private_key_openssh,
                known_hosts,
            }),
            (Some(_), None) => Err(git::GitError::KnownHostsUnavailable(format!(
                "git Secret '{name}' has `identity` but no `known_hosts` — refusing SSH \
                 without host-key pinning"
            ))),
            (None, known_hosts) => {
                let missing = match known_hosts {
                    Some(_) => "`identity`",
                    None => "`identity` or `known_hosts`",
                };
                Err(git::GitError::AuthFailed(format!(
                    "git Secret '{name}' has no {missing} — ssh:// urls need `identity` and \
                     `known_hosts`"
                )))
            }
        },
        git::Transport::Http => match (cat("username"), cat("password")) {
            (Some(username), Some(password)) => Ok(GitCredentials::Basic { username, password }),
            (None, None) => Ok(GitCredentials::Anonymous),
            (Some(_), None) => Err(git::GitError::AuthFailed(format!(
                "git Secret '{name}' has `username` but no `password`"
            ))),
            (None, Some(_)) => Err(git::GitError::AuthFailed(format!(
                "git Secret '{name}' has `password` but no `username`"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codelocation::reconcile::tests::support::{
        IDENTITY, KNOWN_HOSTS, PASSWORD, USERNAME, secret,
    };
    use crate::run::test_helpers::{MockApiState, mock_client};
    use std::sync::Arc;

    const SECRET: &str = "git-creds";
    const HTTPS: &str = "https://forge.example/acme/pipelines.git";
    const SSH: &str = "ssh://git@forge.example/acme/pipelines.git";

    /// The credentials of a code location on `url`, whose secretRef (if
    /// `secret_ref`) names [`SECRET`]. A GET of that Secret answers
    /// `stored`; `None` is a Secret that does not exist.
    async fn credentials(
        url: &str,
        secret_ref: bool,
        stored: Option<Result<Secret, kube_core::Status>>,
    ) -> Result<GitCredentials, git::GitError> {
        let mut state = MockApiState::default();
        state
            .secrets
            .extend(stored.map(|answer| (SECRET.to_string(), answer)));
        let api = Api::namespaced(mock_client(Arc::new(std::sync::Mutex::new(state))), "y");
        let source = serde_json::from_value(serde_json::json!({
            "url": url,
            "ref": { "branch": "main" },
            "secretRef": secret_ref.then(|| serde_json::json!({ "name": SECRET })),
        }))
        .unwrap();
        git_credentials(&source, &api).await
    }

    #[tokio::test]
    async fn the_url_scheme_picks_the_secret_keys() {
        let basic = GitCredentials::Basic {
            username: USERNAME.1.into(),
            password: PASSWORD.1.into(),
        };
        let ssh = GitCredentials::Ssh {
            private_key_openssh: IDENTITY.1.into(),
            known_hosts: KNOWN_HOSTS.1.into(),
        };
        let shared = [IDENTITY, KNOWN_HOSTS, USERNAME, PASSWORD];
        let refused = |message: &str| Err(format!("git authentication failed: {message}"));
        let cases: [(&str, &[(&str, &str)], Result<GitCredentials, String>); 10] = [
            (HTTPS, &shared, Ok(basic)),
            (SSH, &shared, Ok(ssh)),
            (
                HTTPS,
                &[IDENTITY, KNOWN_HOSTS],
                Ok(GitCredentials::Anonymous),
            ),
            (HTTPS, &[IDENTITY], Ok(GitCredentials::Anonymous)),
            (HTTPS, &[], Ok(GitCredentials::Anonymous)),
            (
                HTTPS,
                &[IDENTITY, KNOWN_HOSTS, USERNAME],
                refused("git Secret 'git-creds' has `username` but no `password`"),
            ),
            (
                HTTPS,
                &[PASSWORD],
                refused("git Secret 'git-creds' has `password` but no `username`"),
            ),
            (
                SSH,
                &[USERNAME, PASSWORD],
                refused(
                    "git Secret 'git-creds' has no `identity` or `known_hosts` — ssh:// urls \
                     need `identity` and `known_hosts`",
                ),
            ),
            (
                SSH,
                &[KNOWN_HOSTS, USERNAME, PASSWORD],
                refused(
                    "git Secret 'git-creds' has no `identity` — ssh:// urls need `identity` \
                     and `known_hosts`",
                ),
            ),
            (
                SSH,
                &[IDENTITY, USERNAME, PASSWORD],
                Err(
                    "known_hosts unavailable: git Secret 'git-creds' has `identity` but no \
                     `known_hosts` — refusing SSH without host-key pinning"
                        .into(),
                ),
            ),
        ];
        for (url, keys, want) in cases {
            let got = credentials(url, true, Some(Ok(secret(keys)))).await;
            let keys: Vec<_> = keys.iter().map(|(key, _)| *key).collect();
            assert_eq!(got.map_err(|e| e.to_string()), want, "{url} with {keys:?}");
        }
    }

    #[tokio::test]
    async fn without_a_secret_https_is_anonymous_and_ssh_is_refused() {
        assert_eq!(
            credentials(HTTPS, false, None)
                .await
                .map_err(|e| e.to_string()),
            Ok(GitCredentials::Anonymous)
        );
        assert_eq!(
            credentials(SSH, false, None)
                .await
                .map_err(|e| e.to_string()),
            Err("git authentication failed: ssh:// urls need a git Secret \
                 (spec.git.secretRef) with `identity` and `known_hosts`"
                .to_string())
        );
    }

    #[tokio::test]
    async fn a_git_secret_that_is_missing_or_forbidden_is_terminal() {
        let forbidden = "secrets \"git-creds\" is forbidden: User \
                         \"system:serviceaccount:rivers:rivers-operator\" cannot get \
                         resource \"secrets\" in API group \"\" in the namespace \"y\"";
        let cases = [
            (
                None,
                "git authentication failed: git Secret 'git-creds' does not exist".to_string(),
            ),
            (
                Some(Err(
                    kube_core::Status::failure(forbidden, "Forbidden").with_code(403)
                )),
                format!(
                    "git authentication failed: the operator may not read git Secret \
                     'git-creds': {forbidden}"
                ),
            ),
        ];
        for (stored, message) in cases {
            let error = credentials(HTTPS, true, stored).await.unwrap_err();
            assert!(matches!(error, git::GitError::AuthFailed(_)), "{error:?}");
            assert!(!error.is_transient(), "{error}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[tokio::test]
    async fn an_api_server_error_reading_the_git_secret_is_transient() {
        let status = kube_core::Status::failure("etcdserver: request timed out", "InternalError")
            .with_code(500);

        let error = credentials(HTTPS, true, Some(Err(status)))
            .await
            .unwrap_err();

        assert!(matches!(error, git::GitError::Unreachable(_)), "{error:?}");
        assert!(error.is_transient());
        assert!(
            error
                .to_string()
                .starts_with("git host unreachable: reading git Secret 'git-creds': "),
            "{error}"
        );
    }

    /// `[username, password]` of Basic credentials (their `Debug` is
    /// redacted).
    fn basic(credentials: &GitCredentials) -> [&str; 2] {
        match credentials {
            GitCredentials::Basic { username, password } => [username, password],
            other => panic!("not Basic: {other:?}"),
        }
    }

    #[tokio::test]
    async fn username_and_password_drop_trailing_newlines_like_the_pod() {
        let stored = secret(&[
            ("username", "bot\n"),
            ("password", "ghp_TOKEN\n\n"),
            ("identity", "PRIVATE KEY\n"),
            ("known_hosts", "forge.example ssh-ed25519 AAAA\n"),
        ]);

        let https = credentials(HTTPS, true, Some(Ok(stored.clone())))
            .await
            .unwrap();
        assert_eq!(basic(&https), ["bot", "ghp_TOKEN"]);

        let ssh = credentials(SSH, true, Some(Ok(stored))).await.unwrap();
        let GitCredentials::Ssh {
            private_key_openssh,
            known_hosts,
        } = &ssh
        else {
            panic!("not Ssh: {ssh:?}");
        };
        assert_eq!(
            [private_key_openssh.as_str(), known_hosts.as_str()],
            ["PRIVATE KEY\n", "forge.example ssh-ed25519 AAAA\n"]
        );
    }

    #[tokio::test]
    async fn username_and_password_keep_what_the_pods_cat_keeps() {
        // (stored value, what the pod's sync reads)
        let cases = [
            ("ghp\nTOKEN\n", "ghp\nTOKEN"),
            ("ghp_TOKEN\r\n", "ghp_TOKEN\r"),
        ];
        for (stored, read) in cases {
            let got = credentials(
                HTTPS,
                true,
                Some(Ok(secret(&[("username", stored), ("password", stored)]))),
            )
            .await
            .unwrap();

            assert_eq!(basic(&got), [read, read], "stored {stored:?}");
        }
    }
}
