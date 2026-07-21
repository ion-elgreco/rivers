//! The normalized identity both auth modes produce.

use serde::{Deserialize, Serialize};

use super::config::Allowlists;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    /// OIDC `sub` / forward user header — stable, unique per issuer.
    pub subject: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub groups: Vec<String>,
    /// Unix seconds. Unused in forward mode (the proxy re-asserts identity
    /// per request).
    pub expires_at: i64,
}

impl Identity {
    pub fn display(&self) -> &str {
        crate::types::display_name(self.name.as_deref(), self.email.as_deref(), &self.subject)
    }
}

impl Allowlists {
    /// The subset of `groups` that can affect an allow decision — those
    /// present in the configured group allowlist. `permits` only ever tests
    /// group membership against this list, so reducing to it is
    /// decision-preserving; it bounds what the OIDC session cookie carries so
    /// an unbounded IdP groups claim can't overflow the 4 KB cookie limit.
    pub fn relevant_groups(&self, groups: Vec<String>) -> Vec<String> {
        groups
            .into_iter()
            .filter(|g| self.groups.iter().any(|allowed| allowed == g))
            .collect()
    }

    /// Empty lists admit any authenticated identity; otherwise a match in
    /// any list admits.
    pub fn permits(&self, id: &Identity) -> bool {
        if self.is_empty() {
            return true;
        }
        let email = id.email.as_deref().map(str::to_ascii_lowercase);
        if let Some(email) = &email {
            if let Some(domain) = email
                .rsplit_once('@')
                .filter(|(local, _)| !local.is_empty())
                .map(|(_, d)| d)
            {
                if self.domains.iter().any(|d| d == domain) {
                    return true;
                }
            }
        }
        if self
            .groups
            .iter()
            .any(|g| id.groups.iter().any(|have| have == g))
        {
            return true;
        }
        self.users.iter().any(|u| {
            // Subject is an exact match; email is case-insensitive without
            // allocating a lowercased copy of each configured user per request.
            u == &id.subject || email.as_deref().is_some_and(|e| u.eq_ignore_ascii_case(e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(email: Option<&str>, groups: &[&str]) -> Identity {
        Identity {
            subject: "sub-1".into(),
            email: email.map(String::from),
            name: None,
            groups: groups.iter().map(|s| s.to_string()).collect(),
            expires_at: i64::MAX,
        }
    }

    #[test]
    fn empty_allowlists_admit_anyone() {
        assert!(Allowlists::default().permits(&id(None, &[])));
    }

    #[test]
    fn domain_match_is_case_insensitive() {
        let allow = Allowlists {
            domains: vec!["example.com".into()],
            ..Default::default()
        };
        assert!(allow.permits(&id(Some("John.Doe@Example.COM"), &[])));
        assert!(!allow.permits(&id(Some("john.doe@other.com"), &[])));
        assert!(!allow.permits(&id(None, &[])));
    }

    /// An empty local part (`@example.com`) is not a real account in the
    /// domain and must not satisfy a domain allowlist.
    #[test]
    fn domain_match_requires_a_non_empty_local_part() {
        let allow = Allowlists {
            domains: vec!["example.com".into()],
            ..Default::default()
        };
        assert!(!allow.permits(&id(Some("@example.com"), &[])));
    }

    #[test]
    fn relevant_groups_keeps_only_allowlisted_and_preserves_permits() {
        let allow = Allowlists {
            groups: vec!["data-eng".into()],
            ..Default::default()
        };
        let many: Vec<String> = (0..200)
            .map(|i| format!("g{i}"))
            .chain(["data-eng".into()])
            .collect();
        let reduced = allow.relevant_groups(many.clone());
        assert_eq!(reduced, vec!["data-eng".to_string()]);
        // Reducing the stored groups doesn't change the admit decision.
        assert_eq!(
            allow.permits(&id(None, &many.iter().map(String::as_str).collect::<Vec<_>>())),
            allow.permits(&id(None, &["data-eng"])),
        );
        // No configured group allowlist ⇒ groups are irrelevant ⇒ none kept.
        let none = Allowlists {
            users: vec!["sub-1".into()],
            ..Default::default()
        };
        assert!(none.relevant_groups(many).is_empty());
    }

    #[test]
    fn group_match_admits() {
        let allow = Allowlists {
            groups: vec!["data-eng".into()],
            ..Default::default()
        };
        assert!(allow.permits(&id(None, &["data-eng", "other"])));
        assert!(!allow.permits(&id(None, &["other"])));
    }

    #[test]
    fn user_match_on_subject_or_email() {
        let allow = Allowlists {
            users: vec!["sub-1".into()],
            ..Default::default()
        };
        assert!(allow.permits(&id(None, &[])));
        let allow = Allowlists {
            users: vec!["john.doe@example.com".into()],
            ..Default::default()
        };
        assert!(allow.permits(&id(Some("JOHN.DOE@example.com"), &[])));
        assert!(!allow.permits(&id(Some("bob@example.com"), &[])));
    }

    /// Email matching is case-insensitive on both sides — a mixed-case
    /// configured user still admits a differently-cased email — while the
    /// subject compare stays exact.
    #[test]
    fn user_email_match_is_case_insensitive_both_sides() {
        let allow = Allowlists {
            users: vec!["Ops@Example.COM".into()],
            ..Default::default()
        };
        assert!(allow.permits(&id(Some("ops@example.com"), &[])));
        assert!(allow.permits(&id(Some("OPS@EXAMPLE.COM"), &[])));
        assert!(!allow.permits(&id(Some("other@example.com"), &[])));
        // A case-variant of the configured value is NOT a subject match
        // (subjects are exact), so an unrelated email is still denied.
        assert!(!allow.permits(&id(None, &[])));
    }
}
