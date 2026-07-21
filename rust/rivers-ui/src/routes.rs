//! Auth route paths — one source of truth for the router, the OIDC
//! redirect-URI builder, the request gate, and the in-app sign-in/out links.
//! Not feature-gated: the client chip also builds the login redirect from
//! [`LOGIN`].

pub const LOGIN: &str = "/auth/login";
pub const CALLBACK: &str = "/auth/callback";
pub const LOGOUT: &str = "/auth/logout";
