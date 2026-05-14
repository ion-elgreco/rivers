//! Shared pagination component for list pages backed by server-side paginated
//! Resources (total row count + slice).

use leptos::prelude::*;

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
