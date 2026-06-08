//! Shared pagination component for list pages backed by server-side paginated
//! Resources (total row count + slice).

use leptos::prelude::*;

use crate::types::{Page, StoredEvent};

/// Compute total-page count from a row total and a page size. Treats
/// `page_size == 0` as "show all rows on one page" — guards against a
/// panic in `u64::div_ceil(0)` that would otherwise crash the whole WASM
/// bundle if a future "All" option, query-param state, or programmatic
/// caller ever set `page_size` to zero.
fn total_pages(total: u64, page_size: u64) -> u64 {
    if page_size == 0 {
        1
    } else {
        total.div_ceil(page_size).max(1)
    }
}

#[component]
pub fn Pagination(
    total: u64,
    page: ReadSignal<u64>,
    set_page: WriteSignal<u64>,
    page_size: ReadSignal<u64>,
    set_page_size: WriteSignal<u64>,
) -> impl IntoView {
    let info = move || {
        let ps = page_size.get();
        if ps == 0 {
            // Single-page view: show the whole range.
            return format!("1 - {total} of {total}");
        }
        let pages = total_pages(total, ps);
        let current = page.get().min(pages.saturating_sub(1));
        let start = current * ps;
        let end = (start + ps).min(total);
        format!("{} - {} of {}", start + 1, end, total)
    };
    let at_first = move || page.get() == 0;
    let at_last = move || page.get() + 1 >= total_pages(total, page_size.get());

    view! {
        <div class="pagination">
            <span class="pagination-info">{info}</span>
            <select
                class="pagination-select"
                on:change=move |ev| {
                    let val: u64 = leptos::prelude::event_target_value(&ev).parse().unwrap_or(25);
                    set_page_size.set(val);
                    set_page.set(0);
                }
            >
                <option value="25" selected=move || page_size.get() == 25>"25"</option>
                <option value="50" selected=move || page_size.get() == 50>"50"</option>
                <option value="100" selected=move || page_size.get() == 100>"100"</option>
                <option value="1000" selected=move || page_size.get() == 1000>"1000"</option>
            </select>
            <div class="pagination-btns">
                <button
                    class="btn btn-small"
                    disabled=at_first
                    on:click=move |_| set_page.update(|p| *p = p.saturating_sub(1))
                >"Prev"</button>
                <button
                    class="btn btn-small"
                    disabled=at_last
                    on:click=move |_| set_page.update(|p| *p += 1)
                >"Next"</button>
            </div>
        </div>
    }
}

/// Renders a server-paginated list (fallback / error / empty / rows +
/// `Pagination`) and snaps the page back to 0 when it slips past the last page.
/// Shared by the runs / asset-event / backfill-partition lists.
#[component]
pub fn PaginatedView<T, R>(
    /// Server-paginated data, keyed by the caller on its own filters + page.
    data: Resource<Result<Page<T>, ServerFnError>>,
    page: ReadSignal<u64>,
    set_page: WriteSignal<u64>,
    page_size: ReadSignal<u64>,
    set_page_size: WriteSignal<u64>,
    /// Renders the current page's rows (table, cards, heatmap, …).
    render: R,
    /// Shown while the first page resolves.
    #[prop(optional, into)]
    fallback: ViewFn,
    /// Shown when `total == 0` (nothing matches).
    #[prop(optional, into)]
    empty: ViewFn,
) -> impl IntoView
where
    T: Clone + Send + Sync + 'static,
    R: Fn(Vec<T>) -> AnyView + Send + Sync + 'static,
{
    // Snap a past-the-end page back to 0 (filter shrank `total`). Effect-only —
    // resource reads resolve outside SSR.
    Effect::new(move |_| {
        if let Some(Ok(p)) = data.get()
            && p.rows.is_empty()
            && p.total > 0
            && page.get_untracked() > 0
        {
            set_page.set(0);
        }
    });
    view! {
        <Transition fallback=move || fallback.run()>
            {move || {
                data.get().map(|result| match result {
                    Ok(p) if p.total == 0 => empty.run(),
                    Ok(p) => {
                        let total = p.total;
                        view! {
                            {render(p.rows)}
                            <Pagination
                                total=total
                                page=page
                                set_page=set_page
                                page_size=page_size
                                set_page_size=set_page_size
                            />
                        }
                        .into_any()
                    }
                    Err(e) => {
                        view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any()
                    }
                })
            }}
        </Transition>
    }
}

/// Infinite-scroll event list: accumulates rows and bumps `set_page` when
/// scrolled near the bottom. A keyed `<For>` keeps scroll position on append;
/// resetting `page` to 0 replaces the buffer.
#[component]
pub fn InfiniteEventList<R>(
    data: Resource<Result<Page<StoredEvent>, ServerFnError>>,
    page: ReadSignal<u64>,
    set_page: WriteSignal<u64>,
    /// Renders one event row.
    row: R,
    /// Shown when there are zero events.
    #[prop(optional, into)]
    empty: ViewFn,
    /// CSS max-height of the scroll viewport.
    #[prop(default = "320px".to_string(), into)]
    max_height: String,
) -> impl IntoView
where
    R: Fn(StoredEvent) -> AnyView + Clone + Send + Sync + 'static,
{
    let acc = RwSignal::new(Vec::<StoredEvent>::new());
    let total = RwSignal::new(0u64);
    let loaded_page = RwSignal::new(-1i64);

    // Page 0 replaces the buffer (initial / filter reset); later pages append.
    Effect::new(move |_| {
        if let Some(Ok(pg)) = data.get() {
            let p = page.get_untracked();
            total.set(pg.total);
            if p == 0 {
                acc.set(pg.rows);
                loaded_page.set(0);
            } else if loaded_page.get_untracked() < p as i64 {
                acc.update(|v| v.extend(pg.rows));
                loaded_page.set(p as i64);
            }
        }
    });

    // web-sys (DOM scroll metrics) is client-only, so this is a server no-op.
    let on_scroll = move |_ev: leptos::ev::Event| {
        #[cfg(any(feature = "hydrate", feature = "csr"))]
        {
            let el = event_target::<web_sys::HtmlElement>(&_ev);
            let near =
                (el.scroll_top() + el.client_height()) as f64 >= el.scroll_height() as f64 - 200.0;
            if near
                && loaded_page.get_untracked() == page.get_untracked() as i64
                && (acc.with_untracked(Vec::len) as u64) < total.get_untracked()
            {
                set_page.update(|p| *p += 1);
            }
        }
        #[cfg(not(any(feature = "hydrate", feature = "csr")))]
        let _ = set_page;
    };

    view! {
        <div
            class="infinite-scroll"
            style=format!("max-height:{max_height};overflow-y:auto")
            on:scroll=on_scroll
        >
            <For each=move || acc.get() key=|e: &StoredEvent| e.id.clone() children=row/>
            {move || {
                if acc.with(Vec::is_empty) {
                    if loaded_page.get() >= 0 && total.get() == 0 {
                        empty.run()
                    } else {
                        view! { <div class="infinite-scroll-status">"Loading…"</div> }.into_any()
                    }
                } else if loaded_page.get() < page.get() as i64 {
                    view! { <div class="infinite-scroll-status">"Loading more…"</div> }.into_any()
                } else {
                    ().into_any()
                }
            }}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_pages_handles_zero_page_size() {
        // The regression: `u64::div_ceil(0)` panics. Collapsing to "1 page
        // total" is the safe interpretation of `page_size = 0` = "show all".
        assert_eq!(total_pages(0, 0), 1);
        assert_eq!(total_pages(100, 0), 1);
        assert_eq!(total_pages(1_000_000, 0), 1);
    }

    #[test]
    fn total_pages_normal_cases() {
        assert_eq!(total_pages(0, 25), 1); // empty → at least one page
        assert_eq!(total_pages(1, 25), 1);
        assert_eq!(total_pages(25, 25), 1);
        assert_eq!(total_pages(26, 25), 2);
        assert_eq!(total_pages(100, 25), 4);
        assert_eq!(total_pages(101, 25), 5);
    }
}
