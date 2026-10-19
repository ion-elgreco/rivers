//! Selection over a paged key list as index spans, so a range of any size costs
//! two numbers instead of every key in it.

/// Disjoint, sorted, inclusive `(start, end)` index spans.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct SpanSet(Vec<(u64, u64)>);

impl SpanSet {
    pub(super) fn spans(&self) -> &[(u64, u64)] {
        &self.0
    }

    /// Number of selected indices.
    pub(super) fn count(&self) -> u64 {
        self.0.iter().map(|(s, e)| e - s + 1).sum()
    }

    pub(super) fn contains(&self, idx: u64) -> bool {
        self.position(idx).is_ok()
    }

    /// Select `idx` if unselected, else unselect it (splitting its span).
    pub(super) fn toggle(&mut self, idx: u64) {
        match self.position(idx) {
            Ok(pos) => {
                let (start, end) = self.0.remove(pos);
                if idx < end {
                    self.0.insert(pos, (idx + 1, end));
                }
                if start < idx {
                    self.0.insert(pos, (start, idx - 1));
                }
            }
            Err(_) => self.insert(idx, idx),
        }
    }

    /// Select every index in `[a, b]` (either order), keeping the rest.
    pub(super) fn insert(&mut self, a: u64, b: u64) {
        let (mut start, mut end) = (a.min(b), a.max(b));
        let mut merged = Vec::with_capacity(self.0.len() + 1);
        let mut placed = false;
        for &(s, e) in &self.0 {
            if e.saturating_add(1) < start {
                merged.push((s, e));
            } else if end.saturating_add(1) < s {
                if !placed {
                    merged.push((start, end));
                    placed = true;
                }
                merged.push((s, e));
            } else {
                start = start.min(s);
                end = end.max(e);
            }
        }
        if !placed {
            merged.push((start, end));
        }
        self.0 = merged;
    }

    /// Drop the span `(start, end)`.
    pub(super) fn remove(&mut self, start: u64, end: u64) {
        self.0.retain(|&span| span != (start, end));
    }

    /// `Ok(span position)` when `idx` is selected, else `Err(insertion point)`.
    fn position(&self, idx: u64) -> Result<usize, usize> {
        self.0.binary_search_by(|&(s, e)| {
            if e < idx {
                std::cmp::Ordering::Less
            } else if s > idx {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SpanSet;

    fn set(spans: &[(u64, u64)]) -> SpanSet {
        SpanSet(spans.to_vec())
    }

    #[test]
    fn toggle_adds_merges_and_splits() {
        let mut s = SpanSet::default();
        s.toggle(5);
        s.toggle(7);
        assert_eq!(s, set(&[(5, 5), (7, 7)]));

        s.toggle(6);
        assert_eq!(s, set(&[(5, 7)]));

        s.toggle(6);
        assert_eq!(s, set(&[(5, 5), (7, 7)]));

        s.toggle(5);
        assert_eq!(s, set(&[(7, 7)]));
    }

    #[test]
    fn insert_unions_overlapping_and_adjacent_spans() {
        let mut s = set(&[(0, 1), (10, 12), (20, 20)]);
        s.insert(11, 2);
        assert_eq!(s, set(&[(0, 12), (20, 20)]));

        s.insert(30, 25);
        assert_eq!(s, set(&[(0, 12), (20, 20), (25, 30)]));

        s.insert(14, 15);
        assert_eq!(s, set(&[(0, 12), (14, 15), (20, 20), (25, 30)]));
    }

    #[test]
    fn large_range_is_two_numbers() {
        let mut s = SpanSet::default();
        s.toggle(500_000);
        s.insert(500_000, 699_999);
        assert_eq!(s.spans(), &[(500_000, 699_999)]);
        assert_eq!(s.count(), 200_000);
        assert!(s.contains(600_000));
        assert!(!s.contains(700_000));

        s.toggle(600_000);
        assert_eq!(s.count(), 199_999);
        assert!(!s.contains(600_000));

        s.remove(500_000, 599_999);
        assert_eq!(s.spans(), &[(600_001, 699_999)]);
    }
}
