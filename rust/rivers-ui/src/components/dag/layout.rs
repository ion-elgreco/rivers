//! Layered DAG layout algorithm.

use crate::types::GraphTopology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const NODE_WIDTH: f64 = 200.0;
const NODE_HEIGHT: f64 = 48.0;
const LAYER_GAP_X: f64 = 260.0;
const NODE_GAP_Y: f64 = 16.0;
const PADDING: f64 = 60.0;
const GROUP_PAD: f64 = 14.0;
const GROUP_LABEL_H: f64 = 30.0;
const SECTION_GAP: f64 = 24.0;
/// Width of the arrowhead marker in user-space units.
pub const ARROW_W: f64 = 10.0;

const CROSSING_SWEEPS: usize = 24;
const POSITION_SWEEPS: usize = 8;

/// One node placed on the canvas: id + classification metadata + the
/// computed top-left position and box size. Output of [`compute_layout`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutNode {
    pub id: String,
    pub kind: String,
    pub group: Option<String>,
    pub parent_graph: Option<String>,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

/// Group bounding box drawn behind the nodes that share `group` — supplies
/// the colored backdrop for the canvas's visual grouping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutGroup {
    pub name: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

/// Routed edge between two [`LayoutNode`]s. Long edges that cross multiple
/// layers carry intermediate `waypoints` produced by the dummy-node
/// insertion phase; [`LayoutEdge::path_d`] renders them as smooth curves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayoutEdge {
    pub source: String,
    pub target: String,
    /// Intermediate routing points for edges spanning multiple layers.
    #[serde(default)]
    pub waypoints: Vec<(f64, f64)>,
}

impl LayoutEdge {
    /// Generate SVG path data. Uses waypoints (from dummy nodes) to route
    /// multi-layer edges smoothly. The path ends ARROW_W before the target
    /// node so the arrowhead marker fills the gap.
    pub fn path_d(&self, nodes: &HashMap<String, &LayoutNode>) -> String {
        let src = nodes.get(&self.source);
        let tgt = nodes.get(&self.target);
        match (src, tgt) {
            (Some(s), Some(t)) => {
                let sx = s.x + s.width;
                let sy = s.y + s.height / 2.0;
                let tx = t.x - ARROW_W;
                let ty = t.y + t.height / 2.0;

                let mut pts = vec![(sx, sy)];
                pts.extend_from_slice(&self.waypoints);
                pts.push((tx, ty));

                let mut d = format!("M {} {}", pts[0].0, pts[0].1);
                for i in 0..pts.len() - 1 {
                    let (x0, y0) = pts[i];
                    let (x1, y1) = pts[i + 1];
                    let dx = x1 - x0;
                    let cpx1 = x0 + dx * 0.5;
                    let cpx2 = x1 - dx * 0.5;
                    d.push_str(&format!(" C {cpx1} {y0}, {cpx2} {y1}, {x1} {y1}"));
                }
                d
            }
            _ => String::new(),
        }
    }
}

/// Output of [`compute_layout`]: positioned nodes, routed edges, group
/// backdrops, and the overall canvas size used to set the SVG viewBox.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LayoutResult {
    pub nodes: Vec<LayoutNode>,
    pub edges: Vec<LayoutEdge>,
    pub groups: Vec<LayoutGroup>,
    pub width: f64,
    pub height: f64,
}

// ─── Compound Sugiyama layout ───────────────────────────────────────────────

/// Compute a layered DAG layout using the Compound Sugiyama algorithm:
///  1. Longest-path layer assignment
///  2. Dummy node insertion for long edges
///  3. Iterative barycenter crossing minimisation (group-aware)
///  4. Y-coordinate assignment with neighbour-averaging refinement
pub fn compute_layout(topology: &GraphTopology, center_layers: bool) -> LayoutResult {
    if topology.nodes.is_empty() {
        return LayoutResult::default();
    }

    let n = topology.nodes.len();

    let name_to_idx: HashMap<&str, usize> = topology
        .nodes
        .iter()
        .enumerate()
        .map(|(i, nd)| (nd.name.as_str(), i))
        .collect();

    let groups: Vec<Option<&str>> = topology
        .nodes
        .iter()
        .map(|nd| nd.group.as_deref())
        .collect();

    // Forward adjacency in layout direction: dependency → dependent (left → right)
    let mut fwd: Vec<Vec<usize>> = vec![vec![]; n];
    let mut bwd: Vec<Vec<usize>> = vec![vec![]; n];

    for (from, to) in &topology.edges {
        if let (Some(&fi), Some(&ti)) =
            (name_to_idx.get(from.as_str()), name_to_idx.get(to.as_str()))
        {
            if fi == ti {
                continue; // skip self-edges
            }
            // `from` depends on `to`  →  layout: to → from
            fwd[ti].push(fi);
            bwd[fi].push(ti);
        }
    }

    // ── Step 1: Longest-path layer assignment ──
    let mut layer = vec![0usize; n];
    let mut visited = vec![false; n];
    let mut queue: Vec<usize> = Vec::new();

    for i in 0..n {
        if bwd[i].is_empty() {
            queue.push(i);
            visited[i] = true;
        }
    }

    let mut idx = 0;
    while idx < queue.len() {
        let cur = queue[idx];
        let cur_l = layer[cur];
        for &dep in &fwd[cur] {
            let new_l = cur_l + 1;
            if new_l > layer[dep] {
                layer[dep] = new_l;
            }
            if !visited[dep] && bwd[dep].iter().all(|&d| visited[d]) {
                visited[dep] = true;
                queue.push(dep);
            }
        }
        idx += 1;
    }

    // ── Step 1b: Group layer compaction ──
    // For each group, compact its members' layers so there are no empty layer
    // gaps within the group's span. This keeps group members visually tight.
    {
        let mut group_members: Vec<(&str, Vec<usize>)> = Vec::new();
        for (i, group) in groups.iter().enumerate().take(n) {
            if let Some(g) = *group {
                if let Some(entry) = group_members.iter_mut().find(|e| e.0 == g) {
                    entry.1.push(i);
                } else {
                    group_members.push((g, vec![i]));
                }
            }
        }
        for (_g, members) in &group_members {
            if members.len() <= 1 {
                continue;
            }
            let mut used_layers: Vec<usize> = members.iter().map(|&i| layer[i]).collect();
            used_layers.sort_unstable();
            used_layers.dedup();
            if used_layers.len() <= 1 {
                continue;
            }
            let has_gaps = used_layers.windows(2).any(|w| w[1] > w[0] + 1);
            if !has_gaps {
                continue;
            }
            let base = used_layers[0];
            for &m in members {
                if let Ok(idx) = used_layers.binary_search(&layer[m]) {
                    layer[m] = base + idx;
                }
            }
        }
    }

    let max_layer = layer.iter().copied().max().unwrap_or(0);

    // ── Step 2: Dummy node insertion ──
    // Collect original edges in layout direction (dependency, dependent)
    let mut orig_edges: Vec<(usize, usize)> = Vec::new();
    for (from, to) in &topology.edges {
        if let (Some(&fi), Some(&ti)) =
            (name_to_idx.get(from.as_str()), name_to_idx.get(to.as_str()))
            && fi != ti
            && layer[ti] < layer[fi]
        {
            orig_edges.push((ti, fi));
        }
    }

    // Extend graph with virtual nodes so every edge spans exactly one layer.
    // Pre-size vectors to avoid reallocations during dummy insertion.
    let est_dummies: usize = orig_edges
        .iter()
        .map(|&(s, t)| {
            let tl = layer[t];
            let sl = layer[s];
            if tl > sl + 1 { tl - sl - 1 } else { 0 }
        })
        .sum();
    let est_total = n + est_dummies;
    let mut ext_layer: Vec<usize> = Vec::with_capacity(est_total);
    ext_layer.extend_from_slice(&layer);
    let mut ext_group: Vec<Option<&str>> = Vec::with_capacity(est_total);
    ext_group.extend_from_slice(&groups);
    let mut ext_is_dummy: Vec<bool> = Vec::with_capacity(est_total);
    ext_is_dummy.resize(n, false);
    let mut ext_fwd: Vec<Vec<usize>> = Vec::with_capacity(est_total);
    ext_fwd.resize_with(n, Vec::new);
    let mut ext_bwd: Vec<Vec<usize>> = Vec::with_capacity(est_total);
    ext_bwd.resize_with(n, Vec::new);

    let mut edge_chains: Vec<Vec<usize>> = Vec::new();

    for &(src, tgt) in &orig_edges {
        let src_l = ext_layer[src];
        let tgt_l = ext_layer[tgt];
        let span = tgt_l - src_l;

        if span <= 1 {
            ext_fwd[src].push(tgt);
            ext_bwd[tgt].push(src);
            edge_chains.push(vec![src, tgt]);
        } else {
            // Assign dummies to common group of source and target (if same)
            let src_g = groups[src];
            let tgt_g = groups[tgt];
            let dummy_g = if src_g == tgt_g { src_g } else { None };
            let mut chain = vec![src];
            let mut prev = src;
            for l in (src_l + 1)..tgt_l {
                let dummy = ext_layer.len();
                ext_layer.push(l);
                ext_group.push(dummy_g);
                ext_is_dummy.push(true);
                // Dummy gets exactly 1 backward edge (from prev); forward filled below
                ext_fwd.push(Vec::new());
                ext_bwd.push(vec![prev]);

                ext_fwd[prev].push(dummy);

                chain.push(dummy);
                prev = dummy;
            }
            ext_fwd[prev].push(tgt);
            ext_bwd[tgt].push(prev);
            chain.push(tgt);
            edge_chains.push(chain);
        }
    }

    let total = ext_layer.len();

    // ── Step 3: Build layer buckets ──
    let mut buckets: Vec<Vec<usize>> = vec![vec![]; max_layer + 1];
    for i in 0..total {
        buckets[ext_layer[i]].push(i);
    }

    // Initial ordering: grouped nodes first (sorted by group name), ungrouped last
    for bucket in &mut buckets {
        bucket.sort_unstable_by(|&a, &b| {
            let ga = ext_group.get(a).copied().flatten();
            let gb = ext_group.get(b).copied().flatten();
            match (ga, gb) {
                (Some(ga), Some(gb)) => ga.cmp(gb).then(a.cmp(&b)),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.cmp(&b),
            }
        });
    }

    // ── Step 4: Crossing minimisation ──
    let mut best_buckets = buckets.clone();
    let mut best_xings = count_total_crossings(&buckets, &ext_fwd, &ext_layer, max_layer);

    // Reusable buffers to avoid per-iteration allocation
    let mut pos_buf: Vec<usize> = vec![0; total];
    let mut bary_buf: Vec<f64> = vec![0.0; total];

    let mut no_improve_count = 0usize;
    for iter in 0..CROSSING_SWEEPS {
        if best_xings == 0 {
            break; // optimal — no crossings left
        }
        if iter % 2 == 0 {
            // Forward sweep
            for l in 1..=max_layer {
                fill_pos_map(&buckets[l - 1], &mut pos_buf);
                sort_compound(
                    &mut buckets[l],
                    &ext_bwd,
                    &pos_buf,
                    &ext_group,
                    &ext_is_dummy,
                    &mut bary_buf,
                );
            }
        } else {
            // Backward sweep
            for l in (0..max_layer).rev() {
                fill_pos_map(&buckets[l + 1], &mut pos_buf);
                sort_compound(
                    &mut buckets[l],
                    &ext_fwd,
                    &pos_buf,
                    &ext_group,
                    &ext_is_dummy,
                    &mut bary_buf,
                );
            }
        }
        let xings = count_total_crossings(&buckets, &ext_fwd, &ext_layer, max_layer);
        if xings < best_xings {
            best_xings = xings;
            best_buckets.clone_from(&buckets);
            no_improve_count = 0;
        } else {
            no_improve_count += 1;
            if no_improve_count >= 4 {
                break; // converged — no improvement in 4 consecutive sweeps
            }
        }
    }
    buckets = best_buckets;

    // ── Step 5: Y-coordinate assignment ──
    let mut y_pos = vec![0.0f64; total];

    // Precompute half-heights to avoid per-access branching in refinement
    let half_h: Vec<f64> = (0..total)
        .map(|i| {
            if ext_is_dummy[i] {
                0.0
            } else {
                NODE_HEIGHT / 2.0
            }
        })
        .collect();

    // Initial placement: stack in order with group-aware spacing
    for bucket in &buckets {
        let mut y = PADDING;
        let mut cur_grp: Option<Option<&str>> = None;
        for &node in bucket {
            let g = ext_group[node];
            let is_d = ext_is_dummy[node];

            if cur_grp != Some(g) {
                if let Some(prev_g) = cur_grp {
                    if prev_g.is_some() {
                        y += GROUP_PAD;
                    }
                    y += SECTION_GAP;
                }
                if g.is_some() && !is_d {
                    y += GROUP_LABEL_H + GROUP_PAD;
                }
                cur_grp = Some(g);
            }

            y_pos[node] = y;
            if is_d {
                y += NODE_GAP_Y;
            } else {
                y += NODE_HEIGHT + NODE_GAP_Y;
            }
        }
    }

    // Refinement: iteratively pull nodes toward the average of their neighbours.
    // Dummy nodes are excluded from overlap resolution so they can float freely
    // to the interpolated position between their source and target — this
    // prevents edges from taking unnecessarily long detours around groups.
    // Pre-compute real-only buckets (no dummies) for resolve_overlaps
    let real_buckets: Vec<Vec<usize>> = buckets
        .iter()
        .map(|b| {
            b.iter()
                .copied()
                .filter(|&node| !ext_is_dummy[node])
                .collect()
        })
        .collect();

    const DAMPING: f64 = 0.6; // blend: 40% current + 60% ideal

    for _ in 0..POSITION_SWEEPS {
        // Forward pass
        for l in 1..=max_layer {
            for &node in &buckets[l] {
                let nb = &ext_bwd[node];
                if !nb.is_empty() {
                    let avg: f64 =
                        nb.iter().map(|&n| y_pos[n] + half_h[n]).sum::<f64>() / nb.len() as f64;
                    let ideal = avg - half_h[node];
                    y_pos[node] = y_pos[node] * (1.0 - DAMPING) + ideal * DAMPING;
                }
            }
            resolve_overlaps(&mut y_pos, &real_buckets[l], &ext_group, &ext_is_dummy);
            resolve_overlaps_reverse(&mut y_pos, &real_buckets[l], &ext_group, &ext_is_dummy);
        }
        // Backward pass
        for l in (0..max_layer).rev() {
            for &node in &buckets[l] {
                let nb = &ext_fwd[node];
                if !nb.is_empty() {
                    let avg: f64 =
                        nb.iter().map(|&n| y_pos[n] + half_h[n]).sum::<f64>() / nb.len() as f64;
                    let ideal = avg - half_h[node];
                    y_pos[node] = y_pos[node] * (1.0 - DAMPING) + ideal * DAMPING;
                }
            }
            resolve_overlaps(&mut y_pos, &real_buckets[l], &ext_group, &ext_is_dummy);
            resolve_overlaps_reverse(&mut y_pos, &real_buckets[l], &ext_group, &ext_is_dummy);
        }
    }

    // ── Step 5b: Normalize ──
    // Re-anchor so the topmost *connected* node starts near PADDING.
    {
        let topmost = (0..n)
            .filter(|&i| !ext_fwd[i].is_empty() || !ext_bwd[i].is_empty())
            .min_by(|&a, &b| {
                y_pos[a]
                    .partial_cmp(&y_pos[b])
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .or_else(|| {
                (0..n).min_by(|&a, &b| {
                    y_pos[a]
                        .partial_cmp(&y_pos[b])
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
            });
        if let Some(top) = topmost {
            let target = PADDING
                + if groups[top].is_some() {
                    GROUP_LABEL_H + GROUP_PAD
                } else {
                    0.0
                };
            let shift = target - y_pos[top];
            if shift.abs() > 0.001 {
                for y in y_pos.iter_mut() {
                    *y += shift;
                }
            }
        }
    }

    // ── Step 5c: Compact same-section gaps ──
    // Refinement can spread apart nodes that belong to the same section
    // (same group or consecutive ungrouped nodes). Close unnecessary gaps
    // by pulling nodes up to the minimum required distance from their
    // predecessor within each bucket.
    for bucket in &buckets {
        if bucket.len() < 2 {
            continue;
        }
        for i in 1..bucket.len() {
            let prev = bucket[i - 1];
            let curr = bucket[i];
            let prev_g = ext_group[prev];
            let curr_g = ext_group[curr];
            // Only compact within the same section (same group or both ungrouped)
            if prev_g != curr_g {
                continue;
            }
            let gap = min_gap(prev, curr, &ext_group, &ext_is_dummy);
            let tight_y = y_pos[prev] + gap;
            if y_pos[curr] > tight_y + 0.5 {
                y_pos[curr] = tight_y;
            }
        }
    }

    // ── Step 5d: Compact isolated nodes ──
    // Nodes with no edges don't benefit from refinement and can end up far from
    // the rest of the graph.  Pull them toward their nearest non-isolated neighbour.
    for bucket in &buckets {
        if bucket.len() < 2 {
            continue;
        }
        // Forward pass: pull isolated nodes toward predecessor
        for i in 1..bucket.len() {
            let curr = bucket[i];
            let prev = bucket[i - 1];
            let is_isolated = ext_fwd[curr].is_empty() && ext_bwd[curr].is_empty();
            if is_isolated {
                let gap = min_gap(prev, curr, &ext_group, &ext_is_dummy);
                let compact_y = y_pos[prev] + gap;
                if compact_y < y_pos[curr] {
                    y_pos[curr] = compact_y;
                }
            }
        }
        // Backward pass: pull isolated nodes at the top toward successor
        for i in (0..bucket.len() - 1).rev() {
            let curr = bucket[i];
            let next = bucket[i + 1];
            let is_isolated = ext_fwd[curr].is_empty() && ext_bwd[curr].is_empty();
            if is_isolated {
                let gap = min_gap(curr, next, &ext_group, &ext_is_dummy);
                let compact_y = y_pos[next] - gap;
                if compact_y > y_pos[curr] {
                    y_pos[curr] = compact_y;
                }
            }
        }
    }

    // ── Step 5e: Sibling group overlap resolution ──
    // After refinement and compaction, group bounding boxes within the same
    // layer may overlap. Detect overlapping groups and push them apart.
    for bucket in &real_buckets {
        // Collect groups present in this bucket (small Vec — typically <5 groups)
        let mut sorted: Vec<(&str, f64, f64)> = Vec::new(); // (group, top, bot)
        for &node in bucket {
            if let Some(g) = ext_group[node] {
                let top = y_pos[node] - GROUP_PAD - GROUP_LABEL_H;
                let bot = y_pos[node] + NODE_HEIGHT + GROUP_PAD;
                if let Some(entry) = sorted.iter_mut().find(|e| e.0 == g) {
                    entry.1 = entry.1.min(top);
                    entry.2 = entry.2.max(bot);
                } else {
                    sorted.push((g, top, bot));
                }
            }
        }
        if sorted.len() < 2 {
            continue;
        }
        sorted.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        for i in 1..sorted.len() {
            let prev_bot = sorted[i - 1].2;
            let curr_top = sorted[i].1;
            let overlap = prev_bot + SECTION_GAP - curr_top;
            if overlap > 0.5 {
                let curr_group = sorted[i].0;
                // Push all nodes in the overlapping group down
                for &node in bucket {
                    let g = ext_group[node];
                    if g == Some(curr_group) {
                        y_pos[node] += overlap;
                    }
                }
                // Also push all ungrouped nodes below this group down
                for &node in bucket {
                    let g = ext_group[node];
                    if g.is_none() && y_pos[node] >= curr_top {
                        y_pos[node] += overlap;
                    }
                }
                // Update span for cascading
                sorted[i].1 += overlap;
                sorted[i].2 += overlap;
            }
        }
    }

    // ── Step 5f: Layer centering (optional) ──
    // Center narrower layers relative to the tallest layer for a balanced look.
    if center_layers {
        // Compute the height span (top to bottom of real nodes) per layer
        let mut layer_spans: Vec<(f64, f64)> = Vec::with_capacity(max_layer + 1);
        for bucket in &real_buckets {
            if bucket.is_empty() {
                layer_spans.push((0.0, 0.0));
                continue;
            }
            let min_y = bucket
                .iter()
                .map(|&n| y_pos[n])
                .fold(f64::INFINITY, f64::min);
            let max_y = bucket
                .iter()
                .map(|&n| y_pos[n] + NODE_HEIGHT)
                .fold(f64::NEG_INFINITY, f64::max);
            layer_spans.push((min_y, max_y));
        }
        let max_height = layer_spans
            .iter()
            .map(|(lo, hi)| hi - lo)
            .fold(0.0f64, f64::max);
        let widest_center = layer_spans
            .iter()
            .filter(|(lo, hi)| (hi - lo - max_height).abs() < 0.5)
            .map(|(lo, hi)| (lo + hi) / 2.0)
            .next()
            .unwrap_or(0.0);

        for (l, bucket) in buckets.iter().enumerate() {
            let (lo, hi) = layer_spans[l.min(layer_spans.len() - 1)];
            let span = hi - lo;
            if span < 0.5 || (span - max_height).abs() < 0.5 {
                continue; // skip empty or widest layer
            }
            let cur_center = (lo + hi) / 2.0;
            let shift = widest_center - cur_center;
            if shift.abs() < 0.5 {
                continue;
            }
            for &node in bucket {
                y_pos[node] += shift;
            }
        }
    }

    // ── Step 6: Build output ──
    let mut layout_nodes = Vec::with_capacity(n);
    for i in 0..n {
        layout_nodes.push(LayoutNode {
            id: topology.nodes[i].name.clone(),
            kind: topology.nodes[i].kind.clone(),
            group: topology.nodes[i].group.clone(),
            parent_graph: topology.nodes[i].parent_graph.clone(),
            x: PADDING + layer[i] as f64 * LAYER_GAP_X,
            y: y_pos[i],
            width: NODE_WIDTH,
            height: NODE_HEIGHT,
        });
    }

    let mut layout_edges = Vec::with_capacity(orig_edges.len());
    for (ei, &(src, tgt)) in orig_edges.iter().enumerate() {
        let chain = &edge_chains[ei];
        let waypoints: Vec<(f64, f64)> = chain[1..chain.len() - 1]
            .iter()
            .filter(|&&idx| ext_is_dummy[idx])
            .map(|&idx| {
                let x = PADDING + ext_layer[idx] as f64 * LAYER_GAP_X + NODE_WIDTH / 2.0;
                let y = y_pos[idx] + half_h[idx];
                (x, y)
            })
            .collect();

        layout_edges.push(LayoutEdge {
            source: topology.nodes[src].name.clone(),
            target: topology.nodes[tgt].name.clone(),
            waypoints,
        });
    }

    // Per-column group bounding boxes (keyed by (&str, layer) to avoid String cloning)
    let mut group_bounds: HashMap<(&str, usize), (f64, f64, f64, f64)> = HashMap::new();
    for i in 0..n {
        if let Some(g) = groups[i] {
            let entry = group_bounds.entry((g, layer[i])).or_insert((
                f64::MAX,
                f64::MAX,
                f64::MIN,
                f64::MIN,
            ));
            let x = PADDING + layer[i] as f64 * LAYER_GAP_X;
            entry.0 = entry.0.min(x);
            entry.1 = entry.1.min(y_pos[i]);
            entry.2 = entry.2.max(x + NODE_WIDTH);
            entry.3 = entry.3.max(y_pos[i] + NODE_HEIGHT);
        }
    }
    let layout_groups: Vec<LayoutGroup> = group_bounds
        .into_iter()
        .map(|((name, _), (min_x, min_y, max_x, max_y))| LayoutGroup {
            name: name.to_string(),
            x: min_x - GROUP_PAD,
            y: min_y - GROUP_PAD - GROUP_LABEL_H,
            width: (max_x - min_x) + GROUP_PAD * 2.0,
            height: (max_y - min_y) + GROUP_PAD * 2.0 + GROUP_LABEL_H,
        })
        .collect();

    let width = PADDING * 2.0 + (max_layer + 1) as f64 * LAYER_GAP_X;
    let max_y = layout_nodes
        .iter()
        .map(|n| n.y + n.height)
        .fold(0.0f64, f64::max);
    let max_group_y = layout_groups
        .iter()
        .map(|g| g.y + g.height)
        .fold(0.0f64, f64::max);
    let height = max_y.max(max_group_y) + PADDING;

    LayoutResult {
        nodes: layout_nodes,
        edges: layout_edges,
        groups: layout_groups,
        width,
        height,
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Build a position lookup: node_id → position in bucket.
/// Uses a flat Vec indexed by node id. Caller must ensure `pos` is large enough.
#[inline]
fn fill_pos_map(bucket: &[usize], pos: &mut [usize]) {
    for (i, &node) in bucket.iter().enumerate() {
        pos[node] = i;
    }
}

/// Compound-aware ordering: group members stay adjacent as a block,
/// positioned by the group's average barycenter.  Dummies and ungrouped
/// nodes float freely by individual barycenter.
fn sort_compound(
    bucket: &mut [usize],
    adj: &[Vec<usize>],
    ref_pos: &[usize],
    groups: &[Option<&str>],
    is_dummy: &[bool],
    bary_buf: &mut Vec<f64>,
) {
    if bucket.is_empty() {
        return;
    }

    // Compute per-node barycenter (inline sum, no allocation)
    for (cur_pos, &node) in bucket.iter().enumerate() {
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for &nb in &adj[node] {
            sum += ref_pos[nb] as f64;
            count += 1;
        }
        bary_buf[node] = if count == 0 {
            cur_pos as f64
        } else {
            sum / count as f64
        };
    }

    // Compute average barycenter per compound group (small Vec — typically <5 groups per layer)
    let mut grp_stats: Vec<(&str, f64, usize)> = Vec::new(); // (group, sum, count)
    for &node in bucket.iter() {
        if is_dummy[node] {
            continue;
        }
        if let Some(g) = groups[node] {
            if let Some(entry) = grp_stats.iter_mut().find(|e| e.0 == g) {
                entry.1 += bary_buf[node];
                entry.2 += 1;
            } else {
                grp_stats.push((g, bary_buf[node], 1));
            }
        }
    }

    // Write group avg into bary_buf for grouped non-dummy nodes
    // so the sort closure needs only bary_buf lookups (no HashMap).
    // We store the group-avg in a separate small array and use a helper
    // to look it up during sort.
    let mut grp_avg: Vec<(&str, f64)> = grp_stats
        .iter()
        .map(|&(g, s, c)| (g, s / c as f64))
        .collect();
    grp_avg.sort_unstable_by(|a, b| a.0.cmp(b.0)); // sort for binary search

    let bary = &*bary_buf;

    // Sort: compound groups move as blocks by group avg barycenter,
    // free nodes (dummies + ungrouped) sort by individual barycenter.
    bucket.sort_unstable_by(|&a, &b| {
        let a_g = if is_dummy[a] { None } else { groups[a] };
        let b_g = if is_dummy[b] { None } else { groups[b] };

        let a_key = match a_g {
            Some(g) => grp_avg
                .binary_search_by_key(&g, |e| e.0)
                .map(|i| grp_avg[i].1)
                .unwrap_or(0.0),
            None => bary[a],
        };
        let b_key = match b_g {
            Some(g) => grp_avg
                .binary_search_by_key(&g, |e| e.0)
                .map(|i| grp_avg[i].1)
                .unwrap_or(0.0),
            None => bary[b],
        };

        a_key
            .partial_cmp(&b_key)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| match (a_g, b_g) {
                (Some(ga), Some(gb)) if ga == gb => bary[a]
                    .partial_cmp(&bary[b])
                    .unwrap_or(std::cmp::Ordering::Equal),
                (Some(ga), Some(gb)) => ga.cmp(gb),
                _ => bary[a]
                    .partial_cmp(&bary[b])
                    .unwrap_or(std::cmp::Ordering::Equal),
            })
    });
}

/// Count edge crossings between all adjacent layer pairs.
/// Uses O(E log E) merge-sort inversion counting instead of O(E²) brute force.
fn count_total_crossings(
    buckets: &[Vec<usize>],
    fwd: &[Vec<usize>],
    layers: &[usize],
    max_layer: usize,
) -> usize {
    let mut total = 0;
    let n = layers.len();
    // Reusable buffers across layers
    let mut edges: Vec<(usize, usize)> = Vec::new();
    let mut pos_a: Vec<usize> = vec![0; n];
    let mut pos_b: Vec<usize> = vec![0; n];
    let mut sort_buf: Vec<usize> = Vec::new();
    let mut sort_tmp: Vec<usize> = Vec::new();

    for l in 0..max_layer {
        fill_pos_map(&buckets[l], &mut pos_a);
        fill_pos_map(&buckets[l + 1], &mut pos_b);

        edges.clear();
        for &node_a in &buckets[l] {
            for &node_b in &fwd[node_a] {
                if layers[node_b] == l + 1 {
                    edges.push((pos_a[node_a], pos_b[node_b]));
                }
            }
        }

        if edges.len() <= 1 {
            continue;
        }

        // Edges are already sorted by pos_a because we iterate buckets[l]
        // in order. Extract B-positions and count inversions — O(E log E).
        sort_buf.clear();
        sort_buf.extend(edges.iter().map(|&(_, b)| b));
        sort_tmp.resize(sort_buf.len(), 0);
        let len = sort_buf.len();
        total += merge_sort_inner(&mut sort_buf, &mut sort_tmp, 0, len);
    }
    total
}

fn merge_sort_inner(arr: &mut [usize], tmp: &mut [usize], lo: usize, hi: usize) -> usize {
    if hi - lo <= 1 {
        return 0;
    }
    let mid = lo + (hi - lo) / 2;
    let mut count = 0;
    count += merge_sort_inner(arr, tmp, lo, mid);
    count += merge_sort_inner(arr, tmp, mid, hi);

    // Merge and count
    let (mut i, mut j, mut k) = (lo, mid, lo);
    while i < mid && j < hi {
        if arr[i] <= arr[j] {
            tmp[k] = arr[i];
            i += 1;
        } else {
            tmp[k] = arr[j];
            count += mid - i; // all remaining in left half are inversions
            j += 1;
        }
        k += 1;
    }
    while i < mid {
        tmp[k] = arr[i];
        i += 1;
        k += 1;
    }
    while j < hi {
        tmp[k] = arr[j];
        j += 1;
        k += 1;
    }
    arr[lo..hi].copy_from_slice(&tmp[lo..hi]);
    count
}

/// Push nodes apart to maintain minimum spacing, preserving the ordering.
fn resolve_overlaps(
    y_pos: &mut [f64],
    bucket: &[usize],
    groups: &[Option<&str>],
    is_dummy: &[bool],
) {
    if bucket.is_empty() {
        return;
    }

    let first = bucket[0];
    let min_first = PADDING
        + if groups[first].is_some() && !is_dummy[first] {
            GROUP_LABEL_H + GROUP_PAD
        } else {
            0.0
        };
    if y_pos[first] < min_first {
        y_pos[first] = min_first;
    }

    for i in 1..bucket.len() {
        let prev = bucket[i - 1];
        let curr = bucket[i];
        let gap = min_gap(prev, curr, groups, is_dummy);
        let min_y = y_pos[prev] + gap;
        if y_pos[curr] < min_y {
            y_pos[curr] = min_y;
        }
    }
}

/// Push nodes apart bottom-to-top to balance spacing (reverse of resolve_overlaps).
fn resolve_overlaps_reverse(
    y_pos: &mut [f64],
    bucket: &[usize],
    groups: &[Option<&str>],
    is_dummy: &[bool],
) {
    if bucket.len() < 2 {
        return;
    }
    for i in (0..bucket.len() - 1).rev() {
        let curr = bucket[i];
        let next = bucket[i + 1];
        let gap = min_gap(curr, next, groups, is_dummy);
        let max_y = y_pos[next] - gap;
        if y_pos[curr] > max_y {
            y_pos[curr] = max_y;
        }
    }
}

/// Minimum vertical distance from top of `prev` to top of `curr`.
#[inline]
fn min_gap(prev: usize, curr: usize, groups: &[Option<&str>], is_dummy: &[bool]) -> f64 {
    let prev_d = is_dummy[prev];
    let curr_d = is_dummy[curr];

    if prev_d && curr_d {
        NODE_GAP_Y
    } else if prev_d {
        let extra = if groups[curr].is_some() && !curr_d {
            GROUP_LABEL_H + GROUP_PAD
        } else {
            0.0
        };
        NODE_GAP_Y + extra
    } else if curr_d {
        NODE_HEIGHT + NODE_GAP_Y
    } else {
        let prev_g = groups[prev];
        let curr_g = groups[curr];
        let mut gap = NODE_HEIGHT + NODE_GAP_Y;
        if prev_g != curr_g {
            if prev_g.is_some() {
                gap += GROUP_PAD;
            }
            gap += SECTION_GAP;
            if curr_g.is_some() {
                gap += GROUP_LABEL_H + GROUP_PAD;
            }
        }
        gap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{GraphTopology, TopologyNode};
    use std::collections::HashSet;

    fn node(name: &str, kind: &str, group: Option<&str>) -> TopologyNode {
        TopologyNode {
            name: name.to_string(),
            kind: kind.to_string(),
            group: group.map(|s| s.to_string()),
            parent_graph: None,
        }
    }

    /// Generate an SVG string from a LayoutResult for visual inspection.
    fn render_svg(result: &LayoutResult) -> String {
        let node_map: HashMap<String, &LayoutNode> =
            result.nodes.iter().map(|n| (n.id.clone(), n)).collect();

        let w = result.width;
        let h = result.height;
        let mut svg = String::new();
        svg.push_str(&format!(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{}\" height=\"{}\" viewBox=\"0 0 {} {}\">",
            w, h, w, h
        ));
        svg.push_str(&format!(
            "<rect width=\"{}\" height=\"{}\" fill=\"#12141c\"/>",
            w, h
        ));
        svg.push_str(concat!(
            "<defs><marker id=\"arrowhead\" markerWidth=\"10\" markerHeight=\"10\" ",
            "refX=\"0\" refY=\"5\" orient=\"auto\" markerUnits=\"userSpaceOnUse\">",
            "<polygon points=\"0 1, 10 5, 0 9\" fill=\"#4a4f5e\"/></marker></defs>"
        ));

        // Groups
        for g in &result.groups {
            let hdr = 28.0;
            svg.push_str(&format!(
                "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" rx=\"8\" fill=\"#282a36\" stroke=\"#3a3f4b\" stroke-width=\"1\"/>",
                g.x, g.y, g.width, hdr
            ));
            svg.push_str(&format!(
                "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" fill=\"#1c1e26\" fill-opacity=\"0.6\" stroke=\"#3a3f4b\" stroke-width=\"1\"/>",
                g.x, g.y + hdr, g.width, (g.height - hdr).max(0.0)
            ));
            svg.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" fill=\"#a0a4b0\" font-size=\"12\" font-weight=\"600\" font-family=\"monospace\">{}</text>",
                g.x + 12.0, g.y + 18.0, g.name
            ));
        }

        // Edges
        for e in &result.edges {
            let d = e.path_d(&node_map);
            if !d.is_empty() {
                svg.push_str(&format!(
                    "<path d=\"{}\" stroke=\"#4a4f5e\" stroke-width=\"2\" fill=\"none\" marker-end=\"url(#arrowhead)\"/>",
                    d
                ));
            }
        }

        // Nodes
        for n in &result.nodes {
            let accent = match n.kind.as_str() {
                "asset" => "#ff8f78",
                "task" => "#50e1f9",
                "graph_asset" => "#ff775d",
                _ => "#535559",
            };
            svg.push_str(&format!(
                "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" rx=\"4\" fill=\"#171a1d\" stroke=\"rgba(70,72,75,0.15)\" stroke-width=\"1\"/>",
                n.x, n.y, n.width, n.height
            ));
            svg.push_str(&format!(
                "<rect x=\"{}\" y=\"{}\" width=\"3\" height=\"{}\" fill=\"{}\"/>",
                n.x, n.y, n.height, accent
            ));
            svg.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" fill=\"#e4e6eb\" font-size=\"13\" font-weight=\"600\" font-family=\"JetBrains Mono, monospace\">{}</text>",
                n.x + 15.0, n.y + 19.0, n.id
            ));
        }

        svg.push_str("</svg>\n");
        svg
    }

    fn demo_topology() -> GraphTopology {
        GraphTopology {
            nodes: vec![
                node("raw_users", "asset", Some("data_ingestion")),
                node("raw_orders", "asset", Some("data_ingestion")),
                node("raw_products", "asset", Some("data_ingestion")),
                node("active_users", "asset", Some("data_processing")),
                node("enriched_orders", "asset", Some("data_processing")),
                node("user_order_summary", "asset", Some("analytics")),
                node("product_revenue", "asset", Some("analytics")),
                node("daily_stats", "asset", Some("analytics")),
                node("regional_users", "asset", Some("regional")),
                node("regional_revenue", "asset", Some("regional")),
                node("external_weather", "asset", Some("external_sources")),
                node("validate_data", "task", None),
                node("export_report", "task", None),
            ],
            edges: vec![
                ("active_users".into(), "raw_users".into()),
                ("enriched_orders".into(), "raw_orders".into()),
                ("enriched_orders".into(), "raw_products".into()),
                ("user_order_summary".into(), "active_users".into()),
                ("user_order_summary".into(), "enriched_orders".into()),
                ("product_revenue".into(), "enriched_orders".into()),
                ("daily_stats".into(), "enriched_orders".into()),
                ("regional_users".into(), "active_users".into()),
                ("regional_revenue".into(), "regional_users".into()),
                ("regional_revenue".into(), "enriched_orders".into()),
                ("validate_data".into(), "enriched_orders".into()),
            ],
        }
    }

    #[test]
    fn test_generate_svg() {
        let topo = demo_topology();
        let result = compute_layout(&topo, false);
        let svg = render_svg(&result);
        let path = "/tmp/dag_layout.svg";
        std::fs::write(path, &svg).expect("failed to write SVG");
        eprintln!("SVG written to {path}");
    }

    /// Build a larger synthetic topology for benchmarking.
    /// Creates `num_layers` layers with `nodes_per_layer` nodes each,
    /// grouped into `num_groups` groups, with cross-layer edges.
    fn large_topology(
        num_layers: usize,
        nodes_per_layer: usize,
        num_groups: usize,
    ) -> GraphTopology {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let groups: Vec<String> = (0..num_groups).map(|i| format!("group_{i}")).collect();

        for layer in 0..num_layers {
            for i in 0..nodes_per_layer {
                let idx = layer * nodes_per_layer + i;
                let g = &groups[i % num_groups];
                nodes.push(node(&format!("node_{idx}"), "asset", Some(g)));
                // Edge from prev layer same position
                if layer > 0 {
                    let prev_idx = (layer - 1) * nodes_per_layer + i;
                    edges.push((format!("node_{idx}"), format!("node_{prev_idx}")));
                }
                // Cross edge to prev layer neighbor
                if layer > 0 && i + 1 < nodes_per_layer {
                    let prev_idx = (layer - 1) * nodes_per_layer + i + 1;
                    edges.push((format!("node_{idx}"), format!("node_{prev_idx}")));
                }
                // Long edge spanning 2 layers (creates dummies)
                if layer >= 2 && i % 3 == 0 {
                    let far_idx = (layer - 2) * nodes_per_layer + i;
                    edges.push((format!("node_{idx}"), format!("node_{far_idx}")));
                }
            }
        }

        GraphTopology { nodes, edges }
    }

    #[test]
    fn bench_layout_optimized() {
        use std::time::Instant;

        let small = demo_topology();
        let medium = large_topology(6, 10, 4); // 60 nodes
        let large = large_topology(10, 20, 5); // 200 nodes
        let xl = large_topology(20, 50, 8); // 1000 nodes
        let xxl = large_topology(50, 200, 10); // 10000 nodes
        let xxxl = large_topology(100, 500, 12); // 50000 nodes

        for (name, topo, iters) in [
            ("demo (13 nodes)", &small, 5000),
            ("medium (60 nodes)", &medium, 1000),
            ("large (200 nodes)", &large, 200),
            ("xl (1000 nodes)", &xl, 20),
            ("xxl (10000 nodes)", &xxl, 3),
            ("xxxl (50000 nodes)", &xxxl, 1),
        ] {
            // Warmup
            for _ in 0..10 {
                std::hint::black_box(compute_layout(topo, false));
            }
            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(compute_layout(topo, false));
            }
            let elapsed = start.elapsed();
            let per_iter = elapsed / iters as u32;
            eprintln!("{name:25} {iters:5} iters in {elapsed:?}  ({per_iter:?}/iter)");
        }
    }

    #[test]
    fn bench_layout_naive_baseline() {
        use std::time::Instant;

        // Naive crossing count: O(E²) brute force
        fn naive_count_crossings(
            buckets: &[Vec<usize>],
            fwd: &[Vec<usize>],
            layers: &[usize],
            max_layer: usize,
        ) -> usize {
            let mut total = 0;
            for l in 0..max_layer {
                let pos_a: HashMap<usize, usize> = buckets[l]
                    .iter()
                    .enumerate()
                    .map(|(i, &n)| (n, i))
                    .collect();
                let pos_b: HashMap<usize, usize> = buckets[l + 1]
                    .iter()
                    .enumerate()
                    .map(|(i, &n)| (n, i))
                    .collect();

                let mut edges: Vec<(usize, usize)> = Vec::new();
                for &node_a in &buckets[l] {
                    if node_a < fwd.len() {
                        for &node_b in &fwd[node_a] {
                            if node_b < layers.len() && layers[node_b] == l + 1 {
                                edges.push((
                                    *pos_a.get(&node_a).unwrap(),
                                    *pos_b.get(&node_b).unwrap(),
                                ));
                            }
                        }
                    }
                }
                // O(E²) brute force
                for i in 0..edges.len() {
                    for j in (i + 1)..edges.len() {
                        if (edges[i].0 < edges[j].0 && edges[i].1 > edges[j].1)
                            || (edges[i].0 > edges[j].0 && edges[i].1 < edges[j].1)
                        {
                            total += 1;
                        }
                    }
                }
            }
            total
        }

        // ── Micro-benchmarks for the hot inner loops ──

        let topo = large_topology(10, 20, 5); // 200 nodes
        let n = topo.nodes.len();
        let name_to_idx: HashMap<&str, usize> = topo
            .nodes
            .iter()
            .enumerate()
            .map(|(i, nd)| (nd.name.as_str(), i))
            .collect();

        let _groups: Vec<Option<&str>> = topo.nodes.iter().map(|nd| nd.group.as_deref()).collect();
        let mut fwd: Vec<Vec<usize>> = vec![vec![]; n];
        let mut bwd: Vec<Vec<usize>> = vec![vec![]; n];
        for (from, to) in &topo.edges {
            if let (Some(&fi), Some(&ti)) =
                (name_to_idx.get(from.as_str()), name_to_idx.get(to.as_str()))
                && fi != ti
            {
                fwd[ti].push(fi);
                bwd[fi].push(ti);
            }
        }

        // Layer assignment
        let mut layer = vec![0usize; n];
        let mut visited = vec![false; n];
        let mut queue: Vec<usize> = Vec::new();
        for i in 0..n {
            if bwd[i].is_empty() {
                queue.push(i);
                visited[i] = true;
            }
        }
        let mut idx = 0;
        while idx < queue.len() {
            let cur = queue[idx];
            for &dep in &fwd[cur] {
                let new_l = layer[cur] + 1;
                if new_l > layer[dep] {
                    layer[dep] = new_l;
                }
                if !visited[dep] && bwd[dep].iter().all(|&d| visited[d]) {
                    visited[dep] = true;
                    queue.push(dep);
                }
            }
            idx += 1;
        }
        let max_layer = layer.iter().copied().max().unwrap_or(0);
        let mut buckets: Vec<Vec<usize>> = vec![vec![]; max_layer + 1];
        for i in 0..n {
            buckets[layer[i]].push(i);
        }

        let iters = 2000;

        // Benchmark: O(n²) naive crossing count
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(naive_count_crossings(&buckets, &fwd, &layer, max_layer));
        }
        let naive_elapsed = start.elapsed();

        // Benchmark: O(n log n) optimized crossing count
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(count_total_crossings(&buckets, &fwd, &layer, max_layer));
        }
        let opt_elapsed = start.elapsed();

        let naive_per = naive_elapsed / iters as u32;
        let opt_per = opt_elapsed / iters as u32;
        let speedup = naive_elapsed.as_nanos() as f64 / opt_elapsed.as_nanos() as f64;
        eprintln!("── Crossing count (200 nodes, {iters} iters) ──");
        eprintln!("  Naive O(E²):    {naive_per:?}/iter");
        eprintln!("  Optimized:      {opt_per:?}/iter");
        eprintln!("  Speedup:        {speedup:.1}x");

        // Benchmark: full layout - optimized
        let small = demo_topology();
        let medium = large_topology(6, 10, 4);
        let large = large_topology(10, 20, 5);

        eprintln!("\n── Full layout (optimized) ──");
        for (name, topo, iters) in [
            ("demo (13 nodes)", &small, 5000),
            ("medium (60 nodes)", &medium, 1000),
            ("large (200 nodes)", &large, 200),
        ] {
            for _ in 0..10 {
                std::hint::black_box(compute_layout(topo, false));
            }
            let start = Instant::now();
            for _ in 0..iters {
                std::hint::black_box(compute_layout(topo, false));
            }
            let elapsed = start.elapsed();
            let per_iter = elapsed / iters as u32;
            eprintln!("  {name:25} {per_iter:?}/iter  ({iters} iters in {elapsed:?})");
        }
    }

    #[test]
    fn test_layout_demo_topology() {
        let topo = demo_topology();
        let result = compute_layout(&topo, false);

        eprintln!("=== Layout Result ===");
        eprintln!("Dimensions: {}x{}", result.width, result.height);
        eprintln!("Nodes: {}", result.nodes.len());
        eprintln!("Edges: {}", result.edges.len());
        eprintln!("Groups: {}", result.groups.len());

        for n in &result.nodes {
            eprintln!(
                "  Node {:25} x={:6.0} y={:6.0} group={:?}",
                n.id, n.x, n.y, n.group
            );
        }
        for e in &result.edges {
            eprintln!(
                "  Edge {} -> {} (waypoints: {})",
                e.source,
                e.target,
                e.waypoints.len()
            );
        }
        for g in &result.groups {
            eprintln!(
                "  Group {:25} x={:6.0} y={:6.0} w={:6.0} h={:6.0}",
                g.name, g.x, g.y, g.width, g.height
            );
        }

        assert_eq!(result.nodes.len(), 13, "all nodes should be present");
        assert!(result.edges.len() >= 11, "all edges should be present");
        assert!(result.width > 0.0, "width should be positive");
        assert!(result.height > 0.0, "height should be positive");

        // All nodes should have distinct positions (not all at same point)
        let unique_positions: HashSet<(i64, i64)> = result
            .nodes
            .iter()
            .map(|n| (n.x as i64, n.y as i64))
            .collect();
        assert!(
            unique_positions.len() > 1,
            "nodes should have distinct positions, got: {:?}",
            unique_positions
        );
    }
}
