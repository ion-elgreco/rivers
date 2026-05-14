//! Multi-select dropdown component with search filtering and tag display.

use leptos::prelude::*;

#[derive(Clone, Debug)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
    pub enabled: bool,
}

#[component]
pub fn MultiSelect(
    options: Signal<Vec<SelectOption>>,
    selected: Signal<Vec<String>>,
    on_toggle: Callback<String>,
    placeholder: &'static str,
) -> impl IntoView {
    let open = RwSignal::new(false);

    let trigger_label = move || {
        let sel = selected.get();
        if sel.is_empty() {
            format!("All {placeholder}")
        } else if sel.len() == 1 {
            sel[0].clone()
        } else {
            format!("{} {}", sel.len(), placeholder.to_lowercase())
        }
    };

    let has_selection = move || !selected.get().is_empty();

    view! {
        <div class="multi-select">
            <button
                class="multi-select-trigger"
                class:multi-select-trigger--active=has_selection
                on:click=move |_| open.update(|o| *o = !*o)
            >
                <span class="multi-select-label">{trigger_label}</span>
                <span class="multi-select-arrow">{move || if open.get() { "\u{25B4}" } else { "\u{25BE}" }}</span>
            </button>
            <Show when=move || open.get()>
                <div class="multi-select-backdrop" on:click=move |_| open.set(false)></div>
                <div class="multi-select-dropdown">
                    {move || {
                        let sel = selected.get();
                        options.get().into_iter().map(|opt| {
                            let val = opt.value.clone();
                            let val2 = val.clone();
                            let is_selected = sel.contains(&val);
                            let is_enabled = opt.enabled || is_selected;
                            view! {
                                <label
                                    class="multi-select-option"
                                    class:multi-select-option--disabled=!is_enabled
                                >
                                    <input
                                        type="checkbox"
                                        prop:checked=is_selected
                                        disabled=!is_enabled
                                        on:change=move |_| { on_toggle.run(val2.clone()); }
                                    />
                                    <span>{opt.label}</span>
                                    {(!opt.enabled && !is_selected).then(|| view! {
                                        <span class="multi-select-count">"0"</span>
                                    })}
                                </label>
                            }
                        }).collect::<Vec<_>>()
                    }}
                </div>
            </Show>
        </div>
    }
}
