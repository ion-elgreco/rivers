//! Loading placeholder skeleton components for tables, cards, and detail views.

use leptos::prelude::*;

#[component]
pub fn TableSkeleton(
    #[prop(default = 5)] rows: usize,
    #[prop(default = 4)] cols: usize,
) -> impl IntoView {
    view! {
        <div class="table-container skeleton-container">
            <table>
                <thead>
                    <tr>
                        {(0..cols).map(|_| view! {
                            <th><div class="skeleton skeleton-text" style="width: 80px"></div></th>
                        }).collect::<Vec<_>>()}
                    </tr>
                </thead>
                <tbody>
                    {(0..rows).map(|_| view! {
                        <tr>
                            {(0..cols).map(|_| view! {
                                <td><div class="skeleton skeleton-text"></div></td>
                            }).collect::<Vec<_>>()}
                        </tr>
                    }).collect::<Vec<_>>()}
                </tbody>
            </table>
        </div>
    }
}

#[component]
pub fn CardSkeleton() -> impl IntoView {
    view! {
        <div class="card skeleton-container">
            <div class="skeleton skeleton-title"></div>
            <div class="skeleton skeleton-text" style="width: 60%"></div>
            <div class="skeleton skeleton-text" style="width: 80%"></div>
        </div>
    }
}

#[component]
pub fn StatsSkeleton(#[prop(default = 4)] count: usize) -> impl IntoView {
    view! {
        <div class="stats-grid">
            {(0..count).map(|_| view! {
                <div class="stat-card skeleton-container">
                    <div class="skeleton skeleton-stat-value"></div>
                    <div class="skeleton skeleton-text" style="width: 60%; margin: 0.5rem auto 0"></div>
                </div>
            }).collect::<Vec<_>>()}
        </div>
    }
}

/// Skeleton matching the Rivers grid-row layout used on list pages
/// (runs / backfills / jobs / assets). Renders `rows` placeholder rows with a
/// leading rail stripe and evenly-sized cells.
#[component]
pub fn GridRowSkeleton(
    #[prop(default = 8)] rows: usize,
    #[prop(default = 6)] cols: usize,
) -> impl IntoView {
    let grid_style = format!("grid-template-columns: 4px repeat({cols}, minmax(80px, 1fr))");
    view! {
        <div class="grid-table skeleton-container">
            {(0..rows).map(|_| {
                let gs = grid_style.clone();
                view! {
                    <div class="grid-row" style=gs>
                        <span class="grid-row-rail grid-row-rail--muted"></span>
                        {(0..cols).map(|_| view! {
                            <div class="skeleton skeleton-text" style="height:12px; width:70%"></div>
                        }).collect::<Vec<_>>()}
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}
