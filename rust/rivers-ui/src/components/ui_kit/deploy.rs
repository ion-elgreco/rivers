use leptos::prelude::*;

#[derive(Clone, Debug)]
pub struct DeploymentNode {
    pub label: String,
    pub sub: String,
    /// 1-character glyph (e.g. emoji or unicode) drawn inside the circle.
    pub glyph: String,
    pub ok: bool,
}

#[component]
pub fn DeploymentDiagram(#[prop(into)] nodes: Vec<DeploymentNode>) -> impl IntoView {
    let n = nodes.len().max(1);
    let total_w = 820.0;
    let h = 160.0;
    let col_w = total_w / n as f64;

    let node_views = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let cx = col_w * (i as f64 + 0.5);
            let cy = 70.0;
            let ring_cls = if node.ok {
                "deployment-node-ring deployment-node-ring--ok"
            } else {
                "deployment-node-ring deployment-node-ring--warn"
            };
            let stroke = if node.ok { "var(--success)" } else { "var(--warning)" };
            view! {
                <g>
                    <circle cx=cx cy=cy r="34" fill="var(--bg-surface-low)" stroke=stroke stroke-width="1.5"/>
                    <circle cx=cx cy=cy r="38" class=ring_cls/>
                    <text x=cx y=cy + 6.0 text-anchor="middle" font-size="20" fill="var(--text)">
                        {node.glyph.clone()}
                    </text>
                    <text x=cx y=cy + 56.0 class="deployment-node-label">{node.label.clone()}</text>
                    <text x=cx y=cy + 74.0 class="deployment-node-sub">{node.sub.clone()}</text>
                </g>
            }
        })
        .collect::<Vec<_>>();

    let links = (0..n.saturating_sub(1))
        .map(|i| {
            let x1 = col_w * (i as f64 + 0.5) + 38.0;
            let x2 = col_w * (i as f64 + 1.5) - 38.0;
            let y = 70.0;
            view! {
                <>
                    <line x1=x1 y1=y x2=x2 y2=y class="deployment-link-base"/>
                    <line x1=x1 y1=y x2=x2 y2=y class="deployment-link-flow" stroke="var(--accent)"/>
                </>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="deployment-diagram">
            <svg width="100%" height=h viewBox=format!("0 0 {:.0} {:.0}", total_w, h) style="display:block">
                {links}
                {node_views}
            </svg>
        </div>
    }
}

#[component]
pub fn DeployRow(
    #[prop(into)] label: String,
    #[prop(into)] value: String,
    #[prop(optional)] mono: bool,
) -> impl IntoView {
    let val_cls = if mono {
        "deploy-row-value deploy-row-value--mono"
    } else {
        "deploy-row-value"
    };
    view! {
        <div class="deploy-row">
            <span class="deploy-row-label">{label}</span>
            <span class=val_cls>{value}</span>
        </div>
    }
}

#[component]
pub fn DeployCard(
    #[prop(into)] label: String,
    #[prop(optional, into)] status_label: Option<String>,
    #[prop(optional)] status_ok: bool,
    children: Children,
) -> impl IntoView {
    let status_chip = status_label.map(|text| {
        let dot_cls = if status_ok {
            "dot dot-success"
        } else {
            "dot dot-stale"
        };
        view! {
            <span class="chip">
                <span class=dot_cls></span>
                {text}
            </span>
        }
    });
    view! {
        <div class="deploy-card">
            <div class="deploy-card-head">
                <span class="deploy-card-label">{label}</span>
                {status_chip}
            </div>
            <div class="deploy-card-body">{children()}</div>
        </div>
    }
}
