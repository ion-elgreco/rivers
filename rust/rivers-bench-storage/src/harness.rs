//! Timing, statistics, and the result table.

use std::time::{Duration, Instant};

/// One measured operation, repeated.
pub struct Measured {
    /// What the scenario counts per iteration — events written, rows read.
    /// Throughput is reported against this, not against iteration count.
    pub units_per_iter: u64,
    pub samples: Vec<Duration>,
}

impl Measured {
    pub fn p(&self, pct: f64) -> Duration {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        // Nearest-rank: with 20 samples, p95 is the 19th, not an interpolation
        // between two neighbours that no run actually produced.
        let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    }

    /// Units per second at the median, which is the number a reader should
    /// quote. The mean would let one slow sample carry the whole figure.
    pub fn throughput(&self) -> f64 {
        let median = self.p(50.0).as_secs_f64();
        if median <= 0.0 {
            return f64::INFINITY;
        }
        self.units_per_iter as f64 / median
    }
}

/// Run `f` `iters` times after `warmup` untimed runs.
///
/// The warmup matters more here than in a CPU benchmark: the first call on a
/// pooled connection pays for the handshake, and PostgreSQL's planner caches
/// nothing until a statement has been prepared once.
pub async fn measure<F, Fut>(
    units_per_iter: u64,
    warmup: usize,
    iters: usize,
    mut f: F,
) -> anyhow::Result<Measured>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    for i in 0..warmup {
        f(i).await?;
    }
    let mut samples = Vec::with_capacity(iters);
    for i in 0..iters {
        let start = Instant::now();
        f(warmup + i).await?;
        samples.push(start.elapsed());
    }
    Ok(Measured {
        units_per_iter,
        samples,
    })
}

/// Like [`measure`], but with an untimed `setup` before each timed call.
///
/// Destructive operations — `delete_run`, `cancel_backfill`, the `prune_*`
/// family — consume the state they act on. Without fresh state per iteration
/// the second call onward measures a no-op, which is not the operation anyone
/// cares about.
pub async fn measure_with_setup<S, SFut, F, Fut>(
    units_per_iter: u64,
    warmup: usize,
    iters: usize,
    mut setup: S,
    mut f: F,
) -> anyhow::Result<Measured>
where
    S: FnMut(usize) -> SFut,
    SFut: Future<Output = anyhow::Result<()>>,
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    for i in 0..warmup {
        setup(i).await?;
        f(i).await?;
    }
    let mut samples = Vec::with_capacity(iters);
    for i in 0..iters {
        let n = warmup + i;
        setup(n).await?;
        let start = Instant::now();
        f(n).await?;
        samples.push(start.elapsed());
    }
    Ok(Measured {
        units_per_iter,
        samples,
    })
}

/// One row of the report: a scenario measured on one backend.
pub struct Row {
    pub scenario: String,
    pub backend: String,
    pub unit: String,
    pub measured: Measured,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Print the results grouped by scenario, with each backend compared against
/// the first one measured.
///
/// The ratio is the point of the whole exercise, so it gets a column rather
/// than leaving the reader to divide two numbers in their head.
pub fn report(rows: &[Row]) {
    let mut scenarios: Vec<&str> = Vec::new();
    for row in rows {
        if !scenarios.contains(&row.scenario.as_str()) {
            scenarios.push(&row.scenario);
        }
    }

    for scenario in scenarios {
        let in_scenario: Vec<&Row> = rows.iter().filter(|r| r.scenario == scenario).collect();
        let baseline = in_scenario[0];
        let base_median = ms(baseline.measured.p(50.0));

        println!("\n## {scenario}  ({})", baseline.unit);
        println!(
            "{:<20} {:>10} {:>10} {:>14} {:>10}",
            "backend", "p50 (ms)", "p95 (ms)", "per second", "vs first"
        );
        println!("{}", "-".repeat(68));
        for row in &in_scenario {
            let median = ms(row.measured.p(50.0));
            let ratio = if median > 0.0 {
                base_median / median
            } else {
                f64::INFINITY
            };
            println!(
                "{:<20} {:>10.2} {:>10.2} {:>14.0} {:>9.2}x",
                row.backend,
                median,
                ms(row.measured.p(95.0)),
                row.measured.throughput(),
                ratio
            );
        }
    }
}
