//! Leader election backed by a `coordination.k8s.io/v1` Lease.
//!
//! Gates registry polling so exactly one replica hits image registries at a
//! time (rate limits). The lease loop runs continuously;
//! [`LeaderGate::is_leader`] is a cheap atomic read the reconciler consults
//! on every pass.
//!
//! The algorithm is deliberately small:
//!   * Try to acquire or renew the lease every [`LEASE_RENEW_INTERVAL`].
//!   * On acquire, claim ownership and set `holderIdentity = identity`.
//!   * On renewal, bump `acquireTime`/`renewTime` while keeping identity.
//!   * If another replica holds a non-expired lease, back off and try again.
//!
//! Trade-offs accepted at current scale (1–2 replicas, single namespace): no
//! clock-skew compensation, polling rather than watching, TTL math that prefers
//! freshness over continuity.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff;
use kube_client::api::{Api, Patch, PatchParams, PostParams};

/// How often the leader loop attempts to acquire or renew the lease. Drives
/// the maximum drift between actual leader-state and what `is_leader()` returns.
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
/// How long a held lease stays valid without renewal. A follower that observes
/// `now > renewTime + LEASE_TTL` may take over. Set to 3× the renew interval
/// so a single missed tick doesn't trigger failover.
pub const LEASE_TTL: Duration = Duration::from_secs(30);

/// Cheap atomic reader of "am I currently the leader?". Cloned freely into
/// reconcilers and gRPC handlers; mutation happens only inside [`run_lease_loop`].
#[derive(Clone)]
pub struct LeaderGate {
    is_leader: Arc<AtomicBool>,
}

impl LeaderGate {
    pub fn new() -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `true` if this process currently holds the lease. Stale by at most
    /// [`LEASE_RENEW_INTERVAL`] in the worst case.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn set(&self, value: bool) {
        let previous = self.is_leader.swap(value, Ordering::SeqCst);
        if previous != value {
            crate::metrics::set_leader(value);
            if value {
                tracing::info!("acquired leader lease");
            } else {
                tracing::info!("lost leader lease");
            }
        }
    }
}

impl Default for LeaderGate {
    fn default() -> Self {
        Self::new()
    }
}

pub fn spawn(
    client: kube_client::Client,
    namespace: String,
    lease_name: String,
    identity: String,
) -> LeaderGate {
    let gate = LeaderGate::new();
    let gate_for_task = gate.clone();
    tokio::spawn(async move {
        let api: Api<Lease> = Api::namespaced(client, &namespace);
        loop {
            match try_acquire_or_renew(&api, &lease_name, &identity).await {
                Ok(true) => gate_for_task.set(true),
                Ok(false) => gate_for_task.set(false),
                Err(e) => {
                    tracing::warn!(%e, "lease loop error; assuming follower");
                    gate_for_task.set(false);
                }
            }
            tokio::time::sleep(LEASE_RENEW_INTERVAL).await;
        }
    });
    gate
}

async fn try_acquire_or_renew(
    api: &Api<Lease>,
    name: &str,
    identity: &str,
) -> Result<bool, kube_client::Error> {
    let existing = match api.get(name).await {
        Ok(l) => Some(l),
        Err(kube_client::Error::Api(e)) if e.code == 404 => None,
        Err(e) => return Err(e),
    };

    let now = jiff::Timestamp::now();
    let now_micro = MicroTime(now);

    match existing {
        None => {
            let lease = Lease {
                metadata: kube_client::api::ObjectMeta {
                    name: Some(name.to_string()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(identity.to_string()),
                    lease_duration_seconds: Some(LEASE_TTL.as_secs() as i32),
                    acquire_time: Some(now_micro.clone()),
                    renew_time: Some(now_micro),
                    lease_transitions: Some(1),
                    ..Default::default()
                }),
            };
            match api.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                // Lost the race — refetch and re-evaluate on next tick.
                Err(kube_client::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e),
            }
        }
        Some(lease) => {
            let spec = lease.spec.clone().unwrap_or_default();
            let holder = spec.holder_identity.as_deref().unwrap_or_default();
            let ttl = spec
                .lease_duration_seconds
                .map(|s| Duration::from_secs(s.max(0) as u64))
                .unwrap_or(LEASE_TTL);
            let expired = match spec.renew_time.as_ref() {
                Some(rt) => now.duration_since(rt.0).unsigned_abs() > ttl,
                None => true,
            };

            // SSA Apply with force=true *removes* fields a manager previously
            // owned but omits from the new patch. Renew and steal must therefore
            // send the full set of fields every tick (acquireTime + leaseTransitions
            // included), or those fields disappear after the first renew.
            if holder == identity {
                let acquire_time = spec.acquire_time.unwrap_or_else(|| now_micro.clone());
                let transitions = spec.lease_transitions.unwrap_or(0);
                let patch = serde_json::json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "spec": {
                        "holderIdentity": identity,
                        "leaseDurationSeconds": LEASE_TTL.as_secs() as i32,
                        "acquireTime": acquire_time,
                        "renewTime": now_micro,
                        "leaseTransitions": transitions,
                    }
                });
                api.patch(
                    name,
                    &PatchParams::apply("rivers-operator").force(),
                    &Patch::Apply(&patch),
                )
                .await?;
                Ok(true)
            } else if expired {
                let transitions = spec.lease_transitions.unwrap_or(0) + 1;
                let patch = serde_json::json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "spec": {
                        "holderIdentity": identity,
                        "leaseDurationSeconds": LEASE_TTL.as_secs() as i32,
                        "acquireTime": now_micro,
                        "renewTime": now_micro,
                        "leaseTransitions": transitions,
                    }
                });
                api.patch(
                    name,
                    &PatchParams::apply("rivers-operator").force(),
                    &Patch::Apply(&patch),
                )
                .await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}
