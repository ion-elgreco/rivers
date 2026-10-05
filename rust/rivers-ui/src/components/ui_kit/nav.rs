use leptos::prelude::*;
use leptos::web_sys;
use leptos_router::components::A;

#[derive(Clone)]
pub struct Crumb {
    pub label: String,
    pub href: Option<String>,
    pub mono: bool,
    pub copy_value: Option<String>,
}

impl Crumb {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: None,
            mono: false,
            copy_value: None,
        }
    }
    pub fn linked(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: Some(href.into()),
            mono: false,
            copy_value: None,
        }
    }
    pub fn mono(mut self) -> Self {
        self.mono = true;
        self
    }
    /// Render this crumb as a click-to-copy chip that copies the given value.
    pub fn copyable(mut self, value: impl Into<String>) -> Self {
        self.copy_value = Some(value.into());
        self
    }
}

/// A button with a chevron menu holding one variant. `children` is the main
/// button; the chevron takes its `variant` class (`btn-primary`,
/// `btn-danger` or none). Clicks stop at the split, so it can sit in a row
/// link. Pass `open` to keep the menu open across a re-render.
#[component]
pub fn SplitButton(
    #[prop(into)] variant: Signal<&'static str>,
    #[prop(into)] disabled: Signal<bool>,
    #[prop(into)] menu_label: String,
    on_menu: Callback<()>,
    #[prop(optional)] open: Option<RwSignal<bool>>,
    children: Children,
) -> impl IntoView {
    let open = open.unwrap_or_else(|| RwSignal::new(false));
    let stop = |ev: &web_sys::MouseEvent| {
        ev.prevent_default();
        ev.stop_propagation();
    };
    view! {
        <div class="btn-split">
            {children()}
            <button
                class=move || format!("btn {} btn-split-toggle", variant.get())
                on:click=move |ev| {
                    stop(&ev);
                    open.update(|o| *o = !*o);
                }
                disabled=disabled
                title="More options"
                aria-label="More options"
                aria-haspopup="menu"
                aria-expanded=move || open.get().to_string()
            >
                <crate::components::icons::IconChevronRight/>
            </button>
            <Show when=move || open.get()>
                <div
                    class="btn-split-backdrop"
                    on:click=move |ev| {
                        stop(&ev);
                        open.set(false);
                    }
                ></div>
                <div class="btn-split-menu" role="menu">
                    <button
                        class="btn-split-menu-item"
                        role="menuitem"
                        on:click=move |ev| {
                            stop(&ev);
                            open.set(false);
                            on_menu.run(());
                        }
                    >
                        {menu_label.clone()}
                    </button>
                </div>
            </Show>
        </div>
    }
}

/// Page header. List pages pass `title` (+ an optional `subtitle` line);
/// detail pages pass `crumbs`. Children are the page actions.
#[component]
pub fn Topbar(
    #[prop(optional, into)] crumbs: Vec<Crumb>,
    #[prop(optional, into)] title: Option<String>,
    #[prop(optional, into)] subtitle: Option<ViewFn>,
    #[prop(optional)] children: Option<Children>,
) -> impl IntoView {
    let last = crumbs.len().saturating_sub(1);
    let rendered: Vec<_> = crumbs
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            let sep = (i > 0).then(|| view! { <span class="topbar-crumb-sep">"/"</span> });
            let mut cls = String::from("topbar-crumb");
            if c.mono {
                cls.push_str(" topbar-crumb--mono");
            }
            if i == last {
                cls.push_str(" topbar-crumb--current");
            }
            if c.copy_value.is_some() {
                cls.push_str(" copyable");
            }
            let item = if let Some(href) = c.href.clone() {
                view! { <A href=href attr:class=cls>{c.label.clone()}</A> }.into_any()
            } else if let Some(cv) = c.copy_value.clone() {
                view! { <span class=cls data-copy=cv title="Click to copy">{c.label.clone()}</span> }.into_any()
            } else {
                view! { <span class=cls>{c.label.clone()}</span> }.into_any()
            };
            view! { <>{sep}{item}</> }
        })
        .collect();

    let head = match title {
        Some(t) => view! { <h1 class="topbar-title">{t}</h1> }.into_any(),
        None => view! { <div class="topbar-crumbs">{rendered}</div> }.into_any(),
    };
    let subtitled = subtitle.is_some();
    view! {
        <div class="topbar">
            <div class="topbar-row" class:topbar-row--subtitled=subtitled>
                {head}
                <div class="topbar-actions">
                    {children.map(|c| c())}
                </div>
            </div>
            {subtitle.map(|s| view! { <div class="topbar-subtitle">{s.run()}</div> })}
        </div>
    }
}

#[component]
pub fn RiversSearch(
    #[prop(into)] value: Signal<String>,
    #[prop(into)] on_input: Callback<String>,
    #[prop(optional, into, default = "Search…".to_string())] placeholder: String,
) -> impl IntoView {
    view! {
        <div class="rv-search">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="11" cy="11" r="8"/>
                <line x1="21" y1="21" x2="16.65" y2="16.65"/>
            </svg>
            <input
                type="text"
                placeholder=placeholder
                prop:value=move || value.get()
                on:input=move |ev| on_input.run(event_target_value(&ev))
            />
        </div>
    }
}

/// Segmented control. Items are `(id, label, optional count)`; `on_select`
/// receives the id.
#[component]
pub fn FilterPillGroup(
    #[prop(optional, into)] label: Option<String>,
    #[prop(into)] items: Vec<(String, String, Option<usize>)>,
    #[prop(into)] active: Signal<String>,
    #[prop(into)] on_select: Callback<String>,
) -> impl IntoView {
    view! {
        <div class="filter-pill-group">
            {label.map(|l| view! { <span class="filter-pill-group-label">{l}</span> })}
            {items.into_iter().map(|(id, text, count)| {
                let id_for_cls = id.clone();
                let id_for_aria = id.clone();
                let id_for_cb = id.clone();
                let cb = on_select;
                let cls = move || {
                    if active.get() == id_for_cls {
                        "filter-pill filter-pill--active"
                    } else {
                        "filter-pill"
                    }
                };
                view! {
                    <button
                        class=cls
                        aria-pressed=move || (active.get() == id_for_aria).to_string()
                        on:click=move |_| cb.run(id_for_cb.clone())
                    >
                        {text}
                        {count.map(|n| view! { <span class="count">{n}</span> })}
                    </button>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

/// Underline tab bar for switching page content. Items are
/// `(id, label, optional count)`.
#[component]
pub fn UnderlineTabs(
    #[prop(into)] tabs: Vec<(String, String, Option<usize>)>,
    #[prop(into)] active: Signal<String>,
    #[prop(into)] on_select: Callback<String>,
) -> impl IntoView {
    view! {
        <div class="tabs-underline" role="tablist">
            {tabs.into_iter().map(|(id, label, count)| {
                let id_for_cls = id.clone();
                let id_for_aria = id.clone();
                let id_for_cb = id.clone();
                let cb = on_select;
                let cls = move || {
                    if active.get() == id_for_cls {
                        "tab active"
                    } else {
                        "tab"
                    }
                };
                view! {
                    <button
                        class=cls
                        role="tab"
                        aria-selected=move || (active.get() == id_for_aria).to_string()
                        on:click=move |_| cb.run(id_for_cb.clone())
                    >
                        {label}
                        {count.map(|n| view! { <span class="tab-count">{n}</span> })}
                    </button>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}
