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
        self.name
            .as_deref()
            .or(self.email.as_deref())
            .unwrap_or(&self.subject)
    }
}

impl Allowlists {
    /// Empty lists admit any authenticated identity; otherwise a match in
    /// any list admits.
    pub fn permits(&self, id: &Identity) -> bool {
        if self.is_empty() {
            return true;
        }
        let email = id.email.as_deref().map(str::to_ascii_lowercase);
        if let Some(email) = &email {
            if let Some(domain) = email.rsplit_once('@').map(|(_, d)| d) {
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
            u == &id.subject || email.as_deref() == Some(u.to_ascii_lowercase().as_str())
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
}
