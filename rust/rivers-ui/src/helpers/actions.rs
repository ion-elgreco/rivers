use leptos::prelude::*;
use leptos_router::hooks::use_location;

use crate::types::AssetDefinitionInfo;

/// The runs list's verb input: empty is any run, `materialize` is runs with no
/// verb, anything else is runs of exactly that verb.
pub fn verb_filter_from_input(input: &str) -> crate::types::VerbFilter {
    use crate::types::VerbFilter;
    match input.trim() {
        "" => VerbFilter::Any,
        "materialize" => VerbFilter::MaterializeOnly,
        verb => VerbFilter::Verb(verb.to_string()),
    }
}

/// Prefix a run's summary label with its verb, so an action run never reads
/// as a plain materialization (a finished delete is not a green rebuild).
pub fn with_verb(label: String, action: Option<&str>) -> String {
    match action {
        Some(verb) => format!("{verb} · {label}"),
        None => label,
    }
}

/// Click on a replay button (re-run a run, re-execute a backfill). Replaying
/// an action re-applies its verb — a delete deletes again — so it takes a
/// second click; a materialize replays at once. Returns `(dispatch, armed)`.
pub fn replay_click(is_action: bool, armed: bool) -> (bool, bool) {
    match (is_action, armed) {
        (false, _) => (true, false),
        (true, false) => (false, true),
        (true, true) => (true, false),
    }
}

/// Armed state for a two-click confirm, disarmed whenever `track` reports a
/// change. Pages outlive their route params, so a confirm armed on one job
/// would otherwise fire the next job's verb on its first click.
pub fn use_confirm_armed(track: impl Fn() + Send + Sync + 'static) -> RwSignal<bool> {
    let armed = RwSignal::new(false);
    Effect::new(move |_| {
        track();
        armed.set(false);
    });
    armed
}

/// Close a dialog when the path changes under it. Pages outlive their route
/// params, so an open dialog would otherwise run its verb on another code
/// location, asset or job than the one it shows.
pub fn close_on_navigation(show: RwSignal<bool>) {
    let pathname = use_location().pathname;
    Effect::new(move |prev: Option<String>| {
        let path = pathname.get();
        if prev.is_some_and(|p| p != path) {
            show.set(false);
        }
        path
    });
}

/// Text for a Re-execute button: an action replay names the verb it
/// re-applies.
pub fn replay_button_text(action: Option<&str>, armed: bool, pending: bool) -> String {
    match action {
        _ if pending => "Re-executing…".to_string(),
        Some(verb) if armed => format!("Confirm re-execute {verb}?"),
        Some(verb) => format!("Re-execute {verb}"),
        None => "Re-execute".to_string(),
    }
}

/// Actions an asset offers as generic verb buttons — everything but
/// `observe`, which external assets surface through their own Observe button.
pub fn offered_actions(info: &AssetDefinitionInfo) -> Vec<crate::types::AssetActionInfo> {
    info.actions
        .iter()
        .filter(|a| a.name != "observe")
        .cloned()
        .collect()
}

/// Hover title for an action trigger button. `on_selection` switches to the
/// multi-select wording the bulk surfaces use.
pub fn action_title(act: &crate::types::AssetActionInfo, on_selection: bool) -> String {
    act.description
        .clone()
        .unwrap_or_else(|| match (act.is_destructive(), on_selection) {
            (true, true) => format!(
                "Run '{}' on the selection — clears materialization state",
                act.name
            ),
            (false, true) => format!("Run action '{}' on the selection", act.name),
            (true, false) => format!("Run '{}' — clears materialization state", act.name),
            (false, false) => format!("Run action '{}'", act.name),
        })
}

/// Verb buttons in row order: plain verbs first, destructive verbs last.
/// The sort is stable, so declaration order holds within each group.
pub fn sorted_verbs(
    mut verbs: Vec<crate::types::AssetActionInfo>,
) -> Vec<crate::types::AssetActionInfo> {
    verbs.sort_by_key(|v| v.is_destructive());
    verbs
}

/// Actions every asset in `assets` declares, in the first asset's declaration
/// order. An action run targets the whole selection, so a verb any member
/// lacks (or an asset with no info yet) offers nothing. `observe` is excluded
/// — external assets surface it through their own Observe button.
pub fn common_actions(
    assets: &[String],
    asset_info_by_key: &std::collections::HashMap<String, AssetDefinitionInfo>,
) -> Vec<crate::types::AssetActionInfo> {
    let Some(first) = assets.first() else {
        return Vec::new();
    };
    let Some(base) = asset_info_by_key.get(first) else {
        return Vec::new();
    };
    base.actions
        .iter()
        .filter(|a| a.name != "observe")
        .filter_map(|a| {
            // One pass over the selection per verb: bail if any member lacks
            // the verb, collecting each member's variant on the way.
            // Outcomes may diverge per asset for one verb name, and this row
            // only drives the confirmation styling (execution re-resolves per
            // target) — so a destructive variant on ANY member wins over the
            // first asset's.
            let variants: Option<Vec<&crate::types::AssetActionInfo>> = assets
                .iter()
                .map(|k| {
                    asset_info_by_key
                        .get(k)?
                        .actions
                        .iter()
                        .find(|b| b.name == a.name)
                })
                .collect();
            let variants = variants?;
            Some(
                (*variants
                    .iter()
                    .find(|b| b.is_destructive())
                    .unwrap_or(&variants[0]))
                .clone(),
            )
        })
        .collect()
}

/// The verb a job's runs execute, resolved against its assets' declarations
/// (a destructive variant on any asset wins, as in [`common_actions`]).
/// `None` is a materialize job. Declarations not loaded yet read as the worst
/// case — destructive, key required — so a confirmation is never skipped.
pub fn job_verb(
    action: Option<&str>,
    assets: &[String],
    asset_info_by_key: &std::collections::HashMap<String, AssetDefinitionInfo>,
) -> Option<crate::types::AssetActionInfo> {
    let verb = action?;
    let fallback = |outcome: &str, partitioning: &str| crate::types::AssetActionInfo {
        name: verb.to_string(),
        outcome: outcome.to_string(),
        exclusive: false,
        partitioning: partitioning.to_string(),
        description: None,
        config_schema: None,
    };
    if verb == "observe" {
        return Some(fallback("observe", "optional"));
    }
    Some(
        common_actions(assets, asset_info_by_key)
            .into_iter()
            .find(|a| a.name == verb)
            .unwrap_or_else(|| fallback("unmaterialize", "required")),
    )
}

/// `job name → verb` for the code location's action jobs; materialize jobs
/// are absent. Schedules and sensors name a job, and this says what it does.
pub fn job_actions_by_name(
    jobs: &[crate::types::JobRecord],
) -> std::collections::HashMap<String, String> {
    jobs.iter()
        .filter_map(|j| Some((j.name.clone(), j.action.clone()?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::fixtures::{make_map, partitioned_with, with_actions};

    #[test]
    fn verb_filter_reads_the_runs_list_input() {
        use crate::types::VerbFilter;
        assert_eq!(verb_filter_from_input(""), VerbFilter::Any);
        assert_eq!(verb_filter_from_input("  "), VerbFilter::Any);
        assert_eq!(
            verb_filter_from_input("materialize"),
            VerbFilter::MaterializeOnly
        );
        assert_eq!(
            verb_filter_from_input(" delete "),
            VerbFilter::Verb("delete".to_string())
        );
    }

    #[test]
    fn run_labels_name_the_verb() {
        assert_eq!(with_verb("orders".to_string(), None), "orders");
        assert_eq!(
            with_verb("orders".to_string(), Some("delete")),
            "delete · orders"
        );
    }

    #[test]
    fn replaying_an_action_takes_a_second_click() {
        // Materialize: dispatch at once, nothing armed.
        assert_eq!(replay_click(false, false), (true, false));
        // A verb: the first click arms, the second dispatches and disarms.
        assert_eq!(replay_click(true, false), (false, true));
        assert_eq!(replay_click(true, true), (true, false));
    }

    #[test]
    fn replay_button_names_the_verb_it_reapplies() {
        assert_eq!(replay_button_text(None, false, false), "Re-execute");
        assert_eq!(
            replay_button_text(Some("delete"), false, false),
            "Re-execute delete"
        );
        assert_eq!(
            replay_button_text(Some("delete"), true, false),
            "Confirm re-execute delete?"
        );
        assert_eq!(
            replay_button_text(Some("delete"), true, true),
            "Re-executing…"
        );
    }

    #[test]
    fn job_verb_resolves_the_declaration_behind_a_job() {
        let infos = make_map(vec![
            partitioned_with("events", "vacuum", "unchanged", "keyless"),
            partitioned_with("orders", "delete", "unmaterialize", "optional"),
        ]);
        let assets = |a: &str| vec![a.to_string()];
        assert_eq!(job_verb(None, &assets("events"), &infos), None);
        let vacuum = job_verb(Some("vacuum"), &assets("events"), &infos).unwrap();
        assert!(vacuum.is_keyless() && !vacuum.is_destructive());
        assert!(
            job_verb(Some("delete"), &assets("orders"), &infos)
                .unwrap()
                .is_destructive()
        );
        // Infos not loaded yet: assume the verb destroys data and needs a key.
        let unknown = job_verb(Some("delete"), &assets("ghost"), &infos).unwrap();
        assert!(unknown.is_destructive() && !unknown.is_keyless());
        // `observe` never destroys and takes a key or not.
        assert!(
            job_verb(Some("observe"), &assets("ghost"), &infos)
                .unwrap()
                .key_optional()
        );
    }

    #[test]
    fn job_actions_by_name_keeps_only_action_jobs() {
        let job = |name: &str, action: Option<&str>| crate::types::JobRecord {
            name: name.to_string(),
            asset_selection: vec![],
            executor_type: "in_process".to_string(),
            action: action.map(String::from),
        };
        let map = job_actions_by_name(&[job("etl", None), job("purge", Some("delete"))]);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("purge").map(String::as_str), Some("delete"));
    }

    #[test]
    fn common_actions_intersects_and_keeps_declaration_order() {
        let infos = make_map(vec![
            with_actions(
                "a",
                &[
                    ("compact", "unchanged"),
                    ("destroy", "unmaterialize"),
                    ("refresh", "may_materialize"),
                ],
            ),
            with_actions(
                "b",
                &[("refresh", "may_materialize"), ("compact", "unchanged")],
            ),
        ]);
        let verbs: Vec<String> = common_actions(&["a".into(), "b".into()], &infos)
            .into_iter()
            .map(|a| a.name)
            .collect();
        // "destroy" only on `a`; order follows `a`'s declaration.
        assert_eq!(verbs, vec!["compact", "refresh"]);
    }

    #[test]
    fn common_actions_excludes_observe_and_unknown_assets() {
        let infos = make_map(vec![
            with_actions("a", &[("observe", "observe"), ("compact", "unchanged")]),
            with_actions("b", &[("observe", "observe"), ("compact", "unchanged")]),
        ]);
        let both = common_actions(&["a".into(), "b".into()], &infos);
        assert_eq!(both.len(), 1);
        assert_eq!(both[0].name, "compact");
        assert!(!both[0].is_destructive());
        // A selected asset with no info yet must offer nothing rather than a
        // verb that may fail on it.
        assert!(common_actions(&["a".into(), "ghost".into()], &infos).is_empty());
        assert!(common_actions(&[], &infos).is_empty());
    }

    #[test]
    fn destructive_verbs_are_flagged() {
        let infos = make_map(vec![with_actions("a", &[("destroy", "unmaterialize")])]);
        let acts = common_actions(&["a".into()], &infos);
        assert!(acts[0].is_destructive());
    }

    /// Outcomes may legally diverge per asset for one verb name, and the
    /// merged entry drives the confirmation styling — so the destructive
    /// variant must win no matter which asset was shift-clicked first.
    #[test]
    fn destructive_variant_wins_regardless_of_selection_order() {
        let infos = make_map(vec![
            with_actions("benign", &[("purge", "unchanged")]),
            with_actions("dangerous", &[("purge", "unmaterialize")]),
        ]);
        for order in [["benign", "dangerous"], ["dangerous", "benign"]] {
            let sel: Vec<String> = order.iter().map(|s| s.to_string()).collect();
            let acts = common_actions(&sel, &infos);
            assert_eq!(acts.len(), 1);
            assert!(
                acts[0].is_destructive(),
                "a destructive variant on any member must flag the merged entry (order {order:?})"
            );
        }
    }
}
