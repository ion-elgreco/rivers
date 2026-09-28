//! 12px button icons. Each action uses the same icon everywhere.

use leptos::prelude::*;

/// Start a run: Materialize, Execute, Observe.
#[component]
pub fn IconPlay() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor" aria-hidden="true">
            <path d="M3 2l7 4-7 4V2z"/>
        </svg>
    }
}

/// Cancel a run or backfill.
#[component]
pub fn IconStop() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor" aria-hidden="true">
            <rect x="2.5" y="2.5" width="7" height="7" rx="1"/>
        </svg>
    }
}

#[component]
pub fn IconTrash() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
            <path
                d="M2 3h8M4.5 3V1.8h3V3M3 3l.6 7.2h4.8L9 3M4.9 5v3.5M7.1 5v3.5"
                stroke="currentColor"
                stroke-width="1.1"
                stroke-linecap="round"
            />
        </svg>
    }
}

/// Re-execute a run or backfill.
#[component]
pub fn IconRetry() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 14 14" fill="none" aria-hidden="true">
            <path
                d="M12 7a5 5 0 11-1.5-3.5M12 1.5V4H9.5"
                stroke="currentColor"
                stroke-width="1.3"
                stroke-linecap="round"
                stroke-linejoin="round"
            />
        </svg>
    }
}

/// Reload the page data.
#[component]
pub fn IconRefresh() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 14 14" fill="none" aria-hidden="true">
            <path
                d="M11.5 5.5A4.6 4.6 0 0 0 3 4.2M2.5 8.5A4.6 4.6 0 0 0 11 9.8M2.8 1.8v2.6h2.6M11.2 12.2V9.6H8.6"
                stroke="currentColor"
                stroke-width="1.2"
                stroke-linecap="round"
                stroke-linejoin="round"
            />
        </svg>
    }
}

#[component]
pub fn IconCopy() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
            <rect x="4" y="4" width="8" height="8" rx="1"/>
            <path d="M10 4V3a1 1 0 00-1-1H3a1 1 0 00-1 1v6a1 1 0 001 1h1"/>
        </svg>
    }
}

/// Disclosure chevron pointing right; `.chev-btn--open` turns it down.
#[component]
pub fn IconChevronRight() -> impl IntoView {
    view! {
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none" aria-hidden="true">
            <path
                d="M3.75 2.5L6.25 5L3.75 7.5"
                stroke="currentColor"
                stroke-width="1.3"
                stroke-linecap="round"
                stroke-linejoin="round"
            />
        </svg>
    }
}

#[component]
pub fn IconPlus() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
            <path d="M6 2v8M2 6h8" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
        </svg>
    }
}

#[component]
pub fn IconMinus() -> impl IntoView {
    view! {
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
            <path d="M2 6h8" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
        </svg>
    }
}
