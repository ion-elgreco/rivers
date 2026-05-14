"""Comprehensive rivers demo showcasing all APIs.

Run with:
    rivers dev examples.demo_project.pipeline --storage-path .rivers/demo-storage/

Or materialize directly:
    rivers materialize examples.demo_project.pipeline --storage-path .rivers/demo-storage/
"""

import hashlib
import json
import logging
import os
import pickle
import time
from datetime import datetime

import obstore.store
from pydantic import BaseModel
from rivers import (
    Asset,
    AssetDef,
    AssetExecutionContext,
    AutomationCondition,
    BackfillRequest,
    BackfillStrategy,
    BashTask,
    CodeRepository,
    Executor,
    Hook,
    HookContext,
    Job,
    MetadataValue,
    Observation,
    OutputContext,
    PartitionKey,
    PartitionMapping,
    PartitionsDefinition,
    PickleIOHandler,
    RunQueueConfig,
    RunRequest,
    ScheduleEvaluationContext,
    ScheduleStatus,
    SelfDependency,
    SensorEvaluationContext,
    SensorResult,
    SensorStatus,
    SkipReason,
    TagConcurrencyLimit,
    Task,
)

from rivers import Schedule, Sensor

# =============================================================================
# Configuration (Config + Pydantic)
# =============================================================================


class _IngestionSettings(BaseModel):
    source_system: str = "demo"
    batch_size: int = 100
    include_inactive: bool = False


class _AnalyticsSettings(BaseModel):
    revenue_threshold: float = 50.0
    top_n_products: int = 5


# =============================================================================
# IO Handlers
# =============================================================================


class VersionedPickleIOHandler(PickleIOHandler):
    """PickleIOHandler that registers a data version (content hash) on each write."""

    def handle_output(self, context: OutputContext, obj: object) -> None:
        data_version = hashlib.sha256(pickle.dumps(obj)).hexdigest()
        context.register_data_version(data_version)
        super().handle_output(context, obj)


_deployment = os.environ.get("RIVERS_DEPLOYMENT", "dev")

if _deployment == "cloud":
    _s3_store = obstore.store.S3Store(
        bucket=os.environ.get("RIVERS_S3_BUCKET", "rivers-io"),
        config={
            "endpoint": os.environ.get("RIVERS_S3_ENDPOINT", "http://minio.rivers.svc:9000"),
            "access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
        },
        client_options={"allow_http": "true"},
    )
    raw_io = VersionedPickleIOHandler(store=_s3_store, prefix="raw")
    processed_io = VersionedPickleIOHandler(store=_s3_store, prefix="processed")
    output_io = VersionedPickleIOHandler(store=_s3_store, prefix="output")
else:
    _io_root = os.path.join(os.getcwd(), ".rivers", "demo-io")
    os.makedirs(_io_root, exist_ok=True)
    _local_store = obstore.store.LocalStore(_io_root)
    raw_io = VersionedPickleIOHandler(store=_local_store, prefix="raw")
    processed_io = VersionedPickleIOHandler(store=_local_store, prefix="processed")
    output_io = VersionedPickleIOHandler(store=_local_store, prefix="output")



# =============================================================================
# Hooks
# =============================================================================


@Hook.success
def log_success(context: HookContext):
    print(f"[HOOK] Asset '{context.asset_name}' succeeded (run={context.run_id})")


@Hook.failure(name="alert_on_failure")
def alert_failure(context: HookContext):
    print(f"[HOOK] Asset '{context.asset_name}' FAILED: {context.error}")


# =============================================================================
# Partitions Definitions
# =============================================================================

region_partitions = PartitionsDefinition.static_(["us-east", "us-west", "eu-west"])

daily_partitions = PartitionsDefinition.daily(
    start=datetime(2025, 1, 1),
)

multi_partitions = PartitionsDefinition.multi(
    {
        "region": PartitionsDefinition.static_(["us", "eu"]),
        "tier": PartitionsDefinition.static_(["free", "pro", "enterprise"]),
    }
)

dynamic_customer_partitions = PartitionsDefinition.dynamic("customers")


# =============================================================================
# External Assets
# =============================================================================

external_weather_data = Asset.external(
    name="external_weather_data",
    io_handler=raw_io,
    tags=["external", "weather"],
    kinds="api",
    group="external_sources",
    metadata={"source": "weather-api", "refresh": "hourly"},
)


# =============================================================================
# Data Ingestion Layer (source assets with Config)
# =============================================================================


@Asset(
    io_handler=raw_io,
    tags=["ingestion", "raw"],
    kinds="source",
    group="data_ingestion",
    code_version="1.0",
    metadata={"source_system": "crm", "format": "json"},
    hooks=[log_success, alert_failure],
    pool="database",
)
def raw_users() -> dict:
    """Simulate raw user data ingestion using pipeline config."""
    settings = _IngestionSettings()
    users = [
        {"id": 1, "name": "Alice", "region": "us-east", "active": True, "tier": "pro"},
        {"id": 2, "name": "Bob", "region": "us-west", "active": True, "tier": "free"},
        {
            "id": 3,
            "name": "Charlie",
            "region": "eu-west",
            "active": False,
            "tier": "pro",
        },
        {
            "id": 4,
            "name": "Diana",
            "region": "us-east",
            "active": True,
            "tier": "enterprise",
        },
        {"id": 5, "name": "Eve", "region": "us-west", "active": True, "tier": "free"},
        {"id": 6, "name": "Frank", "region": "eu-west", "active": True, "tier": "pro"},
    ]
    if not settings.include_inactive:
        users = [u for u in users if u["active"]]
    return {"users": users, "source": settings.source_system}


@Asset(
    io_handler=raw_io,
    tags=["ingestion", "raw"],
    kinds="source",
    group="data_ingestion",
    code_version="1.0",
    metadata={"source_system": "orders_db"},
    pool="database",
)
def raw_orders() -> dict:
    """Simulate raw order data ingestion."""
    return {
        "orders": [
            {
                "id": 101,
                "user_id": 1,
                "product": "Widget",
                "amount": 29.99,
                "date": "2025-03-01",
            },
            {
                "id": 102,
                "user_id": 2,
                "product": "Gadget",
                "amount": 49.99,
                "date": "2025-03-01",
            },
            {
                "id": 103,
                "user_id": 1,
                "product": "Widget",
                "amount": 29.99,
                "date": "2025-03-02",
            },
            {
                "id": 104,
                "user_id": 3,
                "product": "Gizmo",
                "amount": 19.99,
                "date": "2025-03-02",
            },
            {
                "id": 105,
                "user_id": 4,
                "product": "Widget",
                "amount": 29.99,
                "date": "2025-03-03",
            },
            {
                "id": 106,
                "user_id": 1,
                "product": "Gadget",
                "amount": 49.99,
                "date": "2025-03-03",
            },
            {
                "id": 107,
                "user_id": 5,
                "product": "Gizmo",
                "amount": 19.99,
                "date": "2025-03-03",
            },
            {
                "id": 108,
                "user_id": 6,
                "product": "Widget",
                "amount": 29.99,
                "date": "2025-03-04",
            },
        ]
    }


@Asset(
    io_handler=raw_io,
    tags=["ingestion", "raw"],
    kinds="source",
    group="data_ingestion",
    code_version="1.0",
    pool="database",
)
def raw_products() -> dict:
    """Simulate raw product catalog."""
    return {
        "products": [
            {"name": "Widget", "category": "Hardware", "price": 29.99, "margin": 0.35},
            {
                "name": "Gadget",
                "category": "Electronics",
                "price": 49.99,
                "margin": 0.42,
            },
            {"name": "Gizmo", "category": "Hardware", "price": 19.99, "margin": 0.28},
        ]
    }


# =============================================================================
# Processing Layer (transforms with context + rich metadata)
# =============================================================================


@Asset(
    io_handler=processed_io,
    tags=["processing"],
    kinds="transform",
    group="data_processing",
    code_version="2.1",
)
def active_users(context: AssetExecutionContext, raw_users: dict) -> dict:
    """Filter to active users only, with rich metadata."""
    active = [u for u in raw_users["users"] if u["active"]]
    context.log.info(
        "Filtered %d -> %d active users", len(raw_users["users"]), len(active)
    )
    context.add_output_metadata(
        {
            "total_users": MetadataValue.int(len(raw_users["users"])),
            "active_users": MetadataValue.int(len(active)),
            "filter_rate": MetadataValue.percentage(
                len(active) / len(raw_users["users"])
            ),
        }
    )
    return {"users": active}


@Asset(
    io_handler=processed_io,
    tags=["processing"],
    kinds="transform",
    group="data_processing",
    code_version="1.0",
)
def enriched_orders(
    context: AssetExecutionContext, raw_orders: dict, raw_products: dict
) -> dict:
    """Join orders with product catalog and add metadata."""
    product_lookup = {p["name"]: p for p in raw_products["products"]}
    enriched = []
    for order in raw_orders["orders"]:
        product = product_lookup.get(order["product"], {})
        enriched.append(
            {
                **order,
                "category": product.get("category", "Unknown"),
                "margin": product.get("margin", 0),
            }
        )
    context.add_output_metadata(
        {
            "order_count": MetadataValue.int(len(enriched)),
            "products_matched": MetadataValue.int(
                sum(1 for o in enriched if o["category"] != "Unknown")
            ),
            "query": MetadataValue.sql(
                "SELECT o.*, p.category FROM orders o JOIN products p ON o.product = p.name",
                dialect="postgres",
            ),
        }
    )
    return {"orders": enriched}


# =============================================================================
# Partitioned Assets (static partitions)
# =============================================================================


@Asset(
    io_handler=processed_io,
    tags=["processing", "partitioned"],
    kinds="transform",
    group="regional",
    partitions_def=region_partitions,
)
def regional_users(context: AssetExecutionContext, active_users: dict) -> dict:
    """Filter users by region partition."""
    region = context.partition_key
    filtered = [u for u in active_users["users"] if u["region"] == region]
    context.add_output_metadata(
        {
            "region": MetadataValue.text(region),
            "user_count": MetadataValue.int(len(filtered)),
        }
    )
    return {"users": filtered, "region": region}


@Asset(
    io_handler=processed_io,
    tags=["analytics", "partitioned"],
    kinds="metric",
    group="regional",
    partitions_def=region_partitions,
    deps=[AssetDef.input("regional_users", partition_mapping=PartitionMapping.identity())],
)
def regional_revenue(
    context: AssetExecutionContext, regional_users: dict, enriched_orders: dict
) -> dict:
    """Compute revenue per region partition."""
    user_ids = {u["id"] for u in regional_users["users"]}
    region_orders = [o for o in enriched_orders["orders"] if o["user_id"] in user_ids]
    total = sum(o["amount"] for o in region_orders)
    context.add_output_metadata(
        {
            "region": MetadataValue.text(context.partition_key),
            "revenue": MetadataValue.float_(total),
            "order_count": MetadataValue.int(len(region_orders)),
        }
    )
    return {
        "region": regional_users["region"],
        "revenue": total,
        "orders": len(region_orders),
    }


# =============================================================================
# Multi-Asset (multiple outputs from one function)
# =============================================================================


@Asset.from_multi(
    output_defs=[
        AssetDef(
            "order_stats",
            tags=["analytics"],
            kinds="metric",
            group="analytics",
            io_handler=output_io,
        ),
        AssetDef(
            "product_rankings",
            tags=["analytics"],
            kinds="metric",
            group="analytics",
            io_handler=output_io,
        ),
    ],
    name="compute_analytics",
    code_version="1.0",
)
def compute_analytics(enriched_orders: dict) -> tuple[dict, dict]:
    """Compute multiple analytics outputs in a single pass."""
    orders = enriched_orders["orders"]

    # Order stats
    total_revenue = sum(o["amount"] for o in orders)
    total_orders = len(orders)
    avg_order = total_revenue / total_orders if total_orders else 0

    # Product rankings
    product_rev: dict[str, float] = {}
    for o in orders:
        product_rev[o["product"]] = product_rev.get(o["product"], 0) + o["amount"]
    rankings = sorted(product_rev.items(), key=lambda x: -x[1])

    return (
        {
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "avg_order_value": avg_order,
        },
        {"rankings": [{"product": p, "revenue": r} for p, r in rankings]},
    )


# =============================================================================
# Graph Asset (nested composition of tasks + assets)
# =============================================================================


@Task(name="validate_data", tags=["validation"])
def validate_data(enriched_orders: dict) -> dict:
    """Validate order data — returns validation report."""
    errors = []
    for order in enriched_orders["orders"]:
        if order["amount"] <= 0:
            errors.append(f"Order {order['id']}: non-positive amount")
        if order["category"] == "Unknown":
            errors.append(f"Order {order['id']}: unknown product category")
    return {"valid": len(errors) == 0, "error_count": len(errors), "errors": errors}


@Task(name="compute_margins", tags=["finance"])
def compute_margins(enriched_orders: dict) -> dict:
    """Compute profit margins from enriched orders."""
    margins = []
    for order in enriched_orders["orders"]:
        profit = order["amount"] * order.get("margin", 0)
        margins.append(
            {"order_id": order["id"], "revenue": order["amount"], "profit": profit}
        )
    total_profit = sum(m["profit"] for m in margins)
    return {"margins": margins, "total_profit": total_profit}


@Asset.from_graph(
    name="validated_pipeline",
    tags=["pipeline", "validated"],
    kinds="pipeline",
    group="pipelines",
    io_handler=output_io,
    metadata={"description": "End-to-end validated order pipeline"},
)
def validated_pipeline(enriched_orders: dict):
    """Graph asset composing validation + margin tasks."""
    validate_data(enriched_orders)
    compute_margins(enriched_orders)


# =============================================================================
# SelfDependency (incremental processing)
# =============================================================================


@Asset(
    io_handler=processed_io,
    tags=["analytics", "incremental"],
    kinds="metric",
    group="analytics",
    code_version="1.0",
)
def cumulative_revenue(self: SelfDependency[dict], enriched_orders: dict) -> dict:
    """Accumulate revenue across materializations using SelfDependency."""
    prev = self.get_inner()
    current_total = sum(o["amount"] for o in enriched_orders["orders"])
    if prev is None:
        return {"cumulative_total": current_total, "materializations": 1}
    return {
        "cumulative_total": prev["cumulative_total"] + current_total,
        "materializations": prev["materializations"] + 1,
    }


# =============================================================================
# Automation Conditions — End-to-End Pipeline
#
# This section demonstrates a realistic data pipeline where the daemon
# automatically materializes assets based on declared conditions.
#
# Pipeline topology:
#
#   [ext_market_feed] ──(observed)──┐
#                                   ├──> [market_snapshot] ──> [risk_report]
#   [ext_positions_feed] ──(obs.)──┘          │                     │
#                                             v                     v
#                                     [daily_pnl] ──────> [executive_dashboard]
#
# Condition strategy:
#   - External feeds: observed by the daemon (no condition needed)
#   - market_snapshot: eager — refreshes whenever feeds update
#   - daily_pnl: on_cron — runs at market close, but only when deps are ready
#   - risk_report: on_missing — bootstraps automatically, then updates eagerly
#   - executive_dashboard: custom condition — fires when all deps are fresh
#     and no upstream failures
# =============================================================================


# ── External data feeds (observed by daemon on cron Schedule) ──
#
# External assets represent data produced outside of rivers. The observe
# function checks for new data and writes it into the IO handler so
# downstream assets can read it. The daemon calls observe() automatically
# when the automation_condition fires (e.g., on a cron Schedule).


@Asset.external(
    name="ext_market_feed",
    io_handler=raw_io,
    tags=["external", "market-data"],
    kinds="api",
    group="automation_pipeline",
    metadata={"source": "market-data-api", "refresh": "realtime"},
    automation_condition=AutomationCondition.on_cron("*/5 * * * *"),
)
def ext_market_feed(context: AssetExecutionContext) -> Observation:
    """Observe external market data API and persist via IO handler for downstream."""
    data = {
        "prices": {"AAPL": 185.0, "GOOGL": 142.0, "MSFT": 415.0},
        "timestamp": time.time(),
    }
    raw_io.handle_output(OutputContext(asset_name="ext_market_feed"), data)
    return Observation(data_version=str(int(time.time())))


@Asset.external(
    name="ext_positions_feed",
    io_handler=raw_io,
    tags=["external", "positions"],
    kinds="api",
    group="automation_pipeline",
    metadata={"source": "positions-service", "refresh": "5min"},
    automation_condition=AutomationCondition.on_cron("*/5 * * * *"),
)
def ext_positions_feed(context: AssetExecutionContext) -> Observation:
    """Observe external positions service and persist via IO handler for downstream."""
    data = {
        "positions": [
            {"symbol": "AAPL", "qty": 100},
            {"symbol": "GOOGL", "qty": 50},
            {"symbol": "MSFT", "qty": 75},
        ],
    }
    raw_io.handle_output(OutputContext(asset_name="ext_positions_feed"), data)
    return Observation(data_version=str(int(time.time())))


# ── Tier 1: Eager — auto-refreshes as soon as upstream feeds update ──


@Asset(
    io_handler=output_io,
    tags=["market-data", "automated"],
    kinds="transform",
    group="automation_pipeline",
    code_version="1.0",
    automation_condition=AutomationCondition.eager(),
)
def market_snapshot(
    context: AssetExecutionContext,
    ext_market_feed: dict,
    ext_positions_feed: dict,
) -> dict:
    """Merge market prices with current positions.

    Condition: eager — re-materializes whenever either external feed updates.
    The daemon detects the upstream change via AnyDepsUpdated and triggers this
    asset automatically (unless it's already in progress).
    """
    prices = ext_market_feed.get(
        "prices", {"AAPL": 185.0, "GOOGL": 142.0, "MSFT": 415.0}
    )
    positions = ext_positions_feed.get(
        "positions",
        [
            {"symbol": "AAPL", "qty": 100},
            {"symbol": "GOOGL", "qty": 50},
            {"symbol": "MSFT", "qty": 75},
        ],
    )
    snapshot = []
    for pos in positions:
        price = prices.get(pos["symbol"], 0)
        snapshot.append(
            {
                "symbol": pos["symbol"],
                "qty": pos["qty"],
                "price": price,
                "market_value": pos["qty"] * price,
            }
        )
    total_value = sum(s["market_value"] for s in snapshot)
    context.add_output_metadata(
        {
            "position_count": MetadataValue.int(len(snapshot)),
            "total_market_value": MetadataValue.float_(total_value),
        }
    )
    return {"snapshot": snapshot, "total_value": total_value, "timestamp": time.time()}


# ── Tier 2: On-cron — runs at market close (4:30 PM ET), waits for deps ──


@Asset(
    io_handler=output_io,
    tags=["finance", "automated"],
    kinds="metric",
    group="automation_pipeline",
    code_version="1.0",
    automation_condition=AutomationCondition.on_cron("30 16 * * 1-5").__and__(
        ~AutomationCondition.any_deps_missing()
    ),
)
def daily_pnl(context: AssetExecutionContext, market_snapshot: dict) -> dict:
    """Compute end-of-day P&L from latest market snapshot.

    Condition: on_cron("30 16 * * 1-5") — fires at 4:30 PM on weekdays.
    The daemon waits until the cron tick passes AND all upstream deps are
    materialized (~Missing guard). If market_snapshot hasn't been materialized
    yet, this asset waits rather than running with stale data.
    """
    snapshot = market_snapshot.get("snapshot", [])
    # Simulated previous close prices
    prev_close = {"AAPL": 183.0, "GOOGL": 140.5, "MSFT": 412.0}
    pnl_items = []
    for pos in snapshot:
        prev = prev_close.get(pos["symbol"], pos["price"])
        change = (pos["price"] - prev) * pos["qty"]
        pnl_items.append(
            {
                "symbol": pos["symbol"],
                "qty": pos["qty"],
                "prev_close": prev,
                "current": pos["price"],
                "pnl": change,
            }
        )
    total_pnl = sum(p["pnl"] for p in pnl_items)
    context.add_output_metadata(
        {
            "total_pnl": MetadataValue.float_(total_pnl),
            "pnl_breakdown": MetadataValue.md(
                "| Symbol | Qty | Prev | Current | P&L |\n|--------|-----|------|---------|-----|\n"
                + "\n".join(
                    f"| {p['symbol']} | {p['qty']} | ${p['prev_close']:.2f} | ${p['current']:.2f} | ${p['pnl']:+.2f} |"
                    for p in pnl_items
                )
            ),
        }
    )
    return {"pnl": pnl_items, "total_pnl": total_pnl}


# ── Tier 2: On-missing + eager combo — bootstraps then stays fresh ──


@Asset(
    io_handler=output_io,
    tags=["risk", "automated"],
    kinds="metric",
    group="automation_pipeline",
    code_version="1.0",
    automation_condition=(
        AutomationCondition.on_missing() | AutomationCondition.eager()
    ).with_label("bootstrap_then_eager"),
)
def risk_report(context: AssetExecutionContext, market_snapshot: dict) -> dict:
    """Compute VaR and risk metrics from market snapshot.

    Condition: on_missing | eager — first materialization happens as soon as
    market_snapshot exists (on_missing fires). After that, eager keeps it
    refreshed whenever market_snapshot updates. This two-phase pattern is
    useful when you want both automatic bootstrapping AND continuous freshness.
    """
    snapshot = market_snapshot.get("snapshot", [])
    total_value = sum(s["market_value"] for s in snapshot)
    # Simplified VaR: 2% of total portfolio value (real would use returns dist)
    var_95 = total_value * 0.02
    concentration = {}
    for s in snapshot:
        concentration[s["symbol"]] = (
            s["market_value"] / total_value if total_value else 0
        )
    max_concentration = max(concentration.values()) if concentration else 0
    context.add_output_metadata(
        {
            "var_95": MetadataValue.float_(var_95),
            "max_concentration": MetadataValue.percentage(max_concentration),
            "portfolio_value": MetadataValue.float_(total_value),
        }
    )
    return {
        "var_95": var_95,
        "concentration": concentration,
        "max_concentration_symbol": max(concentration, key=concentration.get)
        if concentration
        else None,
    }


# ── Tier 3: Custom composite — waits for all deps fresh + no failures ──


@Asset(
    io_handler=output_io,
    tags=["executive", "automated"],
    kinds="metric",
    group="automation_pipeline",
    code_version="1.0",
    automation_condition=(
        AutomationCondition.all_deps_match(AutomationCondition.newly_updated())
        & ~AutomationCondition.in_progress()
        & ~AutomationCondition.any_deps_in_progress()
        & AutomationCondition.all_deps_match(~AutomationCondition.execution_failed())
    ).with_label("all_deps_fresh_no_failures"),
)
def executive_dashboard(
    context: AssetExecutionContext,
    daily_pnl: dict,
    risk_report: dict,
) -> dict:
    """Aggregate P&L and risk into an executive summary.

    Condition: custom composite
      - all_deps_match(newly_updated()) — only fires when every dep has freshly materialized
      - ~in_progress — doesn't double-fire if already running
      - ~any_deps_in_progress — waits for upstream to finish
      - all_deps_match(~execution_failed) — never runs if any dep has failed

    This is the most defensive condition pattern: it guarantees the dashboard
    only refreshes with fully consistent, non-failed upstream data.
    """
    total_pnl = daily_pnl.get("total_pnl", 0)
    var_95 = risk_report.get("var_95", 0)
    pnl_items = daily_pnl.get("pnl", [])
    concentration = risk_report.get("concentration", {})

    winners = [p for p in pnl_items if p["pnl"] > 0]
    losers = [p for p in pnl_items if p["pnl"] < 0]

    dashboard = {
        "total_pnl": total_pnl,
        "var_95": var_95,
        "risk_reward_ratio": abs(total_pnl / var_95) if var_95 else 0,
        "winners": len(winners),
        "losers": len(losers),
        "top_concentration": risk_report.get("max_concentration_symbol"),
        "generated_at": time.time(),
    }

    context.add_output_metadata(
        {
            "total_pnl": MetadataValue.float_(total_pnl),
            "var_95": MetadataValue.float_(var_95),
            "summary": MetadataValue.json(json.dumps(dashboard, indent=2)),
        }
    )
    return dashboard


# ── Simple presets for reference ──


@Asset(
    io_handler=output_io,
    tags=["analytics", "automated"],
    kinds="metric",
    group="analytics",
    code_version="1.0",
    automation_condition=AutomationCondition.eager(),
)
def auto_refresh_stats(enriched_orders: dict) -> dict:
    """Auto-materializes eagerly when dependencies update."""
    orders = enriched_orders["orders"]
    by_category: dict[str, int] = {}
    for o in orders:
        by_category[o["category"]] = by_category.get(o["category"], 0) + 1
    return {"category_counts": by_category, "total": len(orders)}


@Asset(
    io_handler=output_io,
    tags=["reporting"],
    kinds="metric",
    group="analytics",
    automation_condition=(
        AutomationCondition.any_deps_updated().newly_true()
        & ~AutomationCondition.any_deps_in_progress()
    ).with_label("deps_ready"),
)
def custom_condition_report(enriched_orders: dict) -> dict:
    """Demonstrates a custom composite automation condition."""
    return {
        "report_generated_at": time.time(),
        "order_count": len(enriched_orders["orders"]),
    }


# =============================================================================
# Analytics Layer (uses context for logging + metadata)
# =============================================================================


@Asset(
    io_handler=output_io,
    tags=["analytics", "reporting"],
    kinds="metric",
    group="analytics",
    code_version="1.0",
    hooks=[log_success],
)
def user_order_summary(
    context: AssetExecutionContext, active_users: dict, enriched_orders: dict
) -> dict:
    """Compute per-user order summary with rich metadata."""
    active_ids = {u["id"] for u in active_users["users"]}
    summary: dict[int, dict] = {}
    for order in enriched_orders["orders"]:
        if order["user_id"] in active_ids:
            uid = order["user_id"]
            if uid not in summary:
                summary[uid] = {"user_id": uid, "total_orders": 0, "total_spent": 0.0}
            summary[uid]["total_orders"] += 1
            summary[uid]["total_spent"] += order["amount"]

    result = list(summary.values())
    top_spender = max(result, key=lambda x: x["total_spent"]) if result else None

    context.log.info("Computed summary for %d users", len(result))
    context.add_output_metadata(
        {
            "user_count": MetadataValue.int(len(result)),
            "top_spender": MetadataValue.json(json.dumps(top_spender)),
            "summary_table": MetadataValue.md(
                "| User | Orders | Spent |\n|------|--------|-------|\n"
                + "\n".join(
                    f"| {s['user_id']} | {s['total_orders']} | ${s['total_spent']:.2f} |"
                    for s in result
                )
            ),
        }
    )
    return {"summaries": result}


@Asset(
    io_handler=output_io,
    tags=["analytics", "reporting"],
    kinds="metric",
    group="analytics",
    code_version="1.0",
)
def product_revenue(context: AssetExecutionContext, enriched_orders: dict) -> dict:
    """Compute revenue by product with metadata."""
    revenue: dict[str, dict] = {}
    for order in enriched_orders["orders"]:
        product = order["product"]
        if product not in revenue:
            revenue[product] = {
                "product": product,
                "units": 0,
                "revenue": 0.0,
                "profit": 0.0,
            }
        revenue[product]["units"] += 1
        revenue[product]["revenue"] += order["amount"]
        revenue[product]["profit"] += order["amount"] * order.get("margin", 0)

    sorted_rev = sorted(revenue.values(), key=lambda x: -x["revenue"])
    context.add_output_metadata(
        {
            "product_count": MetadataValue.int(len(sorted_rev)),
            "total_revenue": MetadataValue.float_(
                sum(p["revenue"] for p in sorted_rev)
            ),
            "breakdown": MetadataValue.code_block(
                json.dumps(sorted_rev, indent=2), language="json"
            ),
        }
    )
    return {"revenue": sorted_rev}


@Asset(
    io_handler=output_io,
    tags=["analytics"],
    kinds="metric",
    group="analytics",
    code_version="1.0",
)
def daily_stats(enriched_orders: dict) -> dict:
    """Compute daily aggregate order statistics."""
    by_date: dict[str, dict] = {}
    for o in enriched_orders["orders"]:
        date = o.get("date", "unknown")
        if date not in by_date:
            by_date[date] = {"date": date, "revenue": 0.0, "orders": 0}
        by_date[date]["revenue"] += o["amount"]
        by_date[date]["orders"] += 1

    total = sum(o["amount"] for o in enriched_orders["orders"])
    count = len(enriched_orders["orders"])
    return {
        "total_revenue": total,
        "total_orders": count,
        "avg_order": total / count if count else 0,
        "by_date": sorted(by_date.values(), key=lambda x: x["date"]),
    }


# =============================================================================
# BashTask
# =============================================================================

export_report = BashTask(
    name="export_report",
    command=["echo", "Report exported successfully"],
    env={"REPORT_FORMAT": "json"},
    tags=["export", "bash"],
)


# =============================================================================
# Slow Pipeline (for testing Gantt timeline)
# =============================================================================


@Asset(
    io_handler=output_io,
    tags=["slow"],
    kinds="source",
    group="slow_pipeline",
    pool="compute",
)
def slow_step_a(
    ctx: AssetExecutionContext,
) -> dict:
    """First step: 10 second sleep."""
    ctx.log.info("[slow_step_a] Generating 100 data points...")
    time.sleep(10)
    data = list(range(100))
    ctx.log.info(f"[slow_step_a] Done. Produced {len(data)} items.")
    return {"step": "A", "data": data}


@Asset(
    io_handler=output_io,
    tags=["slow"],
    kinds="transform",
    group="slow_pipeline",
    pool="compute",
    pool_slots=2,
)
def slow_step_b(ctx: AssetExecutionContext, slow_step_a: dict) -> dict:
    """Second step: 20 second sleep."""
    ctx.log.info(f"[slow_step_b] Received {len(slow_step_a['data'])} items from step A")
    logging.info("[slow_step_b] Doubling values...")
    time.sleep(20)
    data = [x * 2 for x in slow_step_a["data"]]
    ctx.log.info(f"[slow_step_b] Done. Max value: {max(data)}")
    return {"step": "B", "data": data}


@Asset(
    io_handler=output_io,
    tags=["slow"],
    kinds="transform",
    group="slow_pipeline",
    pool="compute",
)
def slow_step_c(ctx: AssetExecutionContext, slow_step_b: dict) -> dict:
    """Third step: 5 second sleep."""
    print("hello world")
    ctx.log.info(f"[slow_step_c] Incrementing {len(slow_step_b['data'])} values")
    time.sleep(5)
    data = [x + 1 for x in slow_step_b["data"]]
    ctx.log.info(f"[slow_step_c] Done. Sum: {sum(data)}")
    return {"step": "C", "data": data}


@Asset(
    io_handler=output_io,
    tags=["slow"],
    kinds="metric",
    group="slow_pipeline",
    pool=["compute", "database"],
)
def slow_step_d(ctx: AssetExecutionContext, slow_step_c: dict) -> dict:
    """Fourth step: 30 second sleep."""
    ctx.log.info(f"Computing final total from {len(slow_step_c['data'])} values...")
    time.sleep(30)
    total = sum(slow_step_c["data"])
    ctx.log.info(f"Final total: {total}")
    return {"step": "D", "total": total}


# =============================================================================
# Metadata Showcase (demonstrates all MetadataValue types in the UI)
# =============================================================================


@Asset(
    io_handler=output_io,
    tags=["demo", "metadata"],
    kinds="showcase",
    group="demo",
    code_version="1.0",
    metadata={"purpose": "Visual test for all metadata display types"},
)
def metadata_showcase(context: AssetExecutionContext) -> dict:
    """Asset that emits every MetadataValue type for visual inspection in the UI."""
    context.add_output_metadata(
        {
            "text_value": MetadataValue.text("Hello, rivers!"),
            "int_value": MetadataValue.int(42),
            "float_value": MetadataValue.float_(3.14159),
            "bool_true": MetadataValue.bool_(True),
            "bool_false": MetadataValue.bool_(False),
            "url_value": MetadataValue.url("https://github.com/adriangb/rivers"),
            "path_value": MetadataValue.path("/data/warehouse/orders/2025-03-25/"),
            "json_value": MetadataValue.json(
                json.dumps(
                    {"users": 150, "active": True, "regions": ["us", "eu"]}, indent=2
                )
            ),
            "markdown_value": MetadataValue.md(
                "## Pipeline Summary\n\n"
                "- **Total rows**: 1,234\n"
                "- **Quality score**: 98.5%\n\n"
                "| Stage | Status | Duration |\n"
                "|-------|--------|----------|\n"
                "| Extract | Done | 2.1s |\n"
                "| Transform | Done | 5.3s |\n"
                "| Load | Done | 1.0s |\n"
            ),
            "sql_value": MetadataValue.sql(
                "SELECT u.name, COUNT(o.id) AS order_count\n"
                "FROM users u\n"
                "JOIN orders o ON o.user_id = u.id\n"
                "WHERE o.date >= '2025-01-01'\n"
                "GROUP BY u.name\n"
                "ORDER BY order_count DESC\n"
                "LIMIT 10;"
            ),
            "code_block_python": MetadataValue.code_block(
                "def transform(df):\n"
                "    return df.filter(pl.col('active') == True)\n"
                "               .with_columns(pl.col('amount').round(2))",
                language="python",
            ),
            "timestamp_value": MetadataValue.timestamp(time.time()),
            "duration_value": MetadataValue.duration(127.5),
            "bytes_small": MetadataValue.bytes(1536),
            "bytes_large": MetadataValue.bytes(2_147_483_648),
            "percentage_low": MetadataValue.percentage(0.234),
            "percentage_high": MetadataValue.percentage(0.971),
            "data_version": MetadataValue.data_version("sha256:a1b2c3d4e5f6"),
            "null_value": MetadataValue.null(),
            "date_range": MetadataValue.date_range(
                datetime(2025, 1, 1), datetime(2025, 3, 25)
            ),
        }
    )
    return {"showcase": "all metadata types emitted"}


# =============================================================================
# Jobs
# =============================================================================

ingestion_job = Job(
    "ingestion",
    assets=[raw_users, raw_orders, raw_products],
    executor=Executor.in_process(),
)

analytics_job = Job(
    "analytics",
    assets=[
        raw_users,
        raw_orders,
        raw_products,
        active_users,
        enriched_orders,
        user_order_summary,
        product_revenue,
        daily_stats,
    ],
    executor=Executor.in_process(),
)

slow_pipeline_job = Job(
    "slow_pipeline",
    assets=[slow_step_a, slow_step_b, slow_step_c, slow_step_d],
    executor=Executor.in_process(),
)

metadata_showcase_job = Job(
    "metadata_showcase",
    assets=[metadata_showcase],
    executor=Executor.in_process(),
)

full_pipeline_job = Job(
    "full_pipeline",
    assets=[
        raw_users,
        raw_orders,
        raw_products,
        active_users,
        enriched_orders,
        user_order_summary,
        product_revenue,
        daily_stats,
        validated_pipeline,
        cumulative_revenue,
        auto_refresh_stats,
        custom_condition_report,
        export_report,
    ],
    executor=Executor.parallel(max_workers=4),
)


# =============================================================================
# Schedules
# =============================================================================


@Schedule(
    cron_schedule="0 6 * * *",
    job_name="ingestion",
    name="daily_ingestion_schedule",
    default_status=ScheduleStatus.Running,
    timezone="US/Eastern",
    tags={"team": "data-eng"},
    description="Ingest raw data every morning at 6am ET",
)
def daily_ingestion_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=f"ingest-{context.scheduled_execution_time}",
        tags={"triggered_by": "Schedule", "Schedule": context.schedule_name},
    )


@Schedule(
    cron_schedule="0 8 * * *",
    job_name="analytics",
    name="daily_analytics_schedule",
    default_status=ScheduleStatus.Stopped,
    timezone="US/Eastern",
    description="Run analytics pipeline every morning at 8am ET",
)
def daily_analytics_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=f"analytics-{context.scheduled_execution_time}",
        tags={"triggered_by": "Schedule"},
    )


@Schedule(
    cron_schedule="*/2 * * * *",
    job_name="slow_pipeline",
    name="slow_pipeline_schedule",
    default_status=ScheduleStatus.Running,
    description="Run the slow pipeline every 2 minutes",
)
def slow_pipeline_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=f"slow-{context.scheduled_execution_time}",
        tags={"triggered_by": "Schedule"},
    )


@Schedule(
    cron_schedule="0 * * * *",
    job_name="ingestion",
    name="hourly_skip_example",
    default_status=ScheduleStatus.Running,
    description="Demonstrates conditional Schedule skipping",
)
def hourly_skip_example(context: ScheduleEvaluationContext):
    hour = int(context.scheduled_execution_time.split("T")[1].split(":")[0])
    if hour < 6 or hour > 22:
        return SkipReason(f"Outside business hours (hour={hour})")
    return RunRequest(run_key=f"hourly-{context.scheduled_execution_time}")


# =============================================================================
# Sensors
# =============================================================================


@Sensor(
    job_name="analytics",
    name="new_data_sensor",
    minimum_interval="60s",
    default_status=SensorStatus.Running,
    description="Triggers analytics when new data arrives",
    tags={"team": "data-eng"},
    asset_selection=["enriched_orders", "user_order_summary", "product_revenue"],
)
def new_data_sensor(context: SensorEvaluationContext) -> SensorResult:
    last_seen = int(context.cursor) if context.cursor else 0
    current_count = 8  # Simulated: in reality, check external source
    if current_count > last_seen:
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=f"Sensor-{current_count}",
                    tags={"triggered_by": "Sensor", "record_count": str(current_count)},
                )
            ],
            cursor=str(current_count),
        )
    return SensorResult(
        skip_reason=f"No new data (count={current_count}, last_seen={last_seen})",
        cursor=context.cursor,
    )


@Sensor(
    name="file_watcher_sensor",
    minimum_interval="30s",
    default_status=SensorStatus.Running,
    description="Demonstrates a Sensor with no job (asset-targeted only)",
    asset_selection=["raw_users", "raw_orders"],
)
def file_watcher_sensor(context: SensorEvaluationContext) -> SensorResult:
    return SensorResult(skip_reason="No new files detected")


# =============================================================================
# Backfill Showcase
# =============================================================================
# Demonstrates all backfill patterns: manual backfills, eager automation with
# different strategies (multi_run, single_run, per_dimension), and sensor-
# triggered backfills.

# --- Daily event ingest: manually backfillable ---
# This is the typical "reprocess 30 days of data" use case.
# Backfill via CLI:
#   rivers backfill examples.demo_project.pipeline \
#       --assets daily_events --from 2025-01-01 --to 2025-01-31

@Asset(
    name="daily_events",
    partitions_def=daily_partitions,
    io_handler=raw_io,
    tags=["backfill", "ingest"],
    kinds="source",
    group="backfill_demo",
)
def daily_events(context: AssetExecutionContext) -> dict:
    """Raw daily events — one partition per day."""
    date = context.partition.key.key[0]
    return {"date": date, "events": 1000 + hash(date) % 500}


# --- Daily aggregates: eager, multi_run (default) ---
# Each partition runs independently. When daily_events is updated,
# the daemon triggers one run per stale partition.

@Asset(
    name="daily_aggregates",
    partitions_def=daily_partitions,
    io_handler=processed_io,
    deps=[AssetDef.input("daily_events", partition_mapping=PartitionMapping.identity())],
    automation_condition=AutomationCondition.eager(),
    tags=["backfill", "processing"],
    kinds="transform",
    group="backfill_demo",
)
def daily_aggregates(context: AssetExecutionContext, daily_events: dict) -> dict:
    """Aggregated daily metrics — triggered eagerly per partition."""
    return {
        "date": daily_events["date"],
        "total_events": daily_events["events"],
        "avg_per_hour": daily_events["events"] / 24,
    }


# --- Monthly rollup: eager, single_run ---
# Processes all updated partitions in one batch. Efficient for SQL-like
# "INSERT INTO ... SELECT ... WHERE month = ..." operations.
# Backfill via:
#   repo.backfill(
#       selection=["monthly_rollup"],
#       partition_range=PartitionKeyRange.single("2025-01-01", "2025-01-31"),
#       strategy=BackfillStrategy.single_run(),
#   )

monthly_partitions = PartitionsDefinition.static_([
    "2025-01", "2025-02", "2025-03", "2025-04",
    "2025-05", "2025-06", "2025-07", "2025-08",
    "2025-09", "2025-10", "2025-11", "2025-12",
])

@Asset(
    name="monthly_rollup",
    partitions_def=monthly_partitions,
    io_handler=processed_io,
    backfill_strategy=BackfillStrategy.single_run(),
    automation_condition=AutomationCondition.eager(),
    tags=["backfill", "aggregation"],
    kinds="metric",
    group="backfill_demo",
)
def monthly_rollup(context: AssetExecutionContext) -> dict:
    """Monthly rollup — uses single_run strategy for batch efficiency.

    When multiple months need reprocessing, the daemon batches them
    into a single backfill instead of one run per month.
    """
    keys = context.partition.keys
    return {
        "months": [k.key[0] for k in keys],
        "total_months": len(keys),
        "status": "rolled_up",
    }


# --- Regional daily metrics: eager, per_dimension ---
# Region iterates (one run per region), dates batched within each run.
# Backfill via:
#   repo.backfill(
#       selection=["regional_daily_metrics"],
#       partition_range=PartitionKeyRange.multi({
#           "region": ["us", "eu"],
#           "date": ("2025-01-01", "2025-01-31"),
#       }),
#       strategy=BackfillStrategy.per_dimension(
#           multi_run=["region"], single_run=["date"]
#       ),
#   )

regional_daily_partitions = PartitionsDefinition.multi({
    "region": PartitionsDefinition.static_(["us", "eu", "apac"]),
    "date": PartitionsDefinition.static_(["2025-01", "2025-02", "2025-03"]),
})

@Asset(
    name="regional_daily_metrics",
    partitions_def=regional_daily_partitions,
    io_handler=processed_io,
    backfill_strategy=BackfillStrategy.per_dimension(
        multi_run=["region"],
        single_run=["date"],
    ),
    automation_condition=AutomationCondition.eager(),
    tags=["backfill", "regional"],
    kinds="metric",
    group="backfill_demo",
)
def regional_daily_metrics(context: AssetExecutionContext) -> dict:
    """Regional daily metrics — per_dimension strategy.

    When backfilled, produces one run per region (3 runs for us/eu/apac),
    each processing all date partitions in one shot.
    """
    keys = context.partition.keys
    regions = set()
    dates = set()
    for k in keys:
        regions.update(k.keys.get("region", []))
        dates.update(k.keys.get("date", []))
    return {
        "regions": sorted(regions),
        "dates": sorted(dates),
        "num_partitions": len(keys),
    }


# --- Sensor-triggered backfill ---
# A sensor that detects data quality issues and triggers a targeted backfill.

@Sensor(
    name="data_quality_sensor",
    asset_selection=["validated_pipeline"],
    minimum_interval="60s",
    default_status=SensorStatus.Running,
)
def data_quality_sensor(context: SensorEvaluationContext):
    """Detects data quality issues and triggers targeted backfills.

    In production, this would check a monitoring system for bad partitions.
    Here we simulate by checking the cursor — first tick triggers a backfill.
    """
    if context.cursor is not None:
        return SensorResult(cursor=context.cursor)

    # First evaluation: simulate finding bad partitions that need reprocessing
    return SensorResult(
        run_requests=[
            BackfillRequest(
                selection=["daily_events"],
                partition_keys=[
                    PartitionKey.single("2025-01-15"),
                    PartitionKey.single("2025-01-16"),
                    PartitionKey.single("2025-01-17"),
                ],
                max_concurrency=2,
            ),
        ],
        cursor="checked",
    )


# --- Backfill job for manual trigger ---

backfill_demo_job = Job(
    name="backfill_demo",
    assets=[daily_events, daily_aggregates],
    executor=Executor.in_process(),
)


# --- Multi-partition demo job ---
# Click Execute in the UI → the dialog renders one selector per dimension
# (region + date). Picking multiple values per dim fires one run per
# combination via the cartesian product.

multi_partition_demo_job = Job(
    name="multi_partition_demo",
    assets=[regional_daily_metrics],
    executor=Executor.in_process(),
)


# =============================================================================
# Repository
# =============================================================================

all_assets = [
    # External
    external_weather_data,
    # Ingestion
    raw_users,
    raw_orders,
    raw_products,
    # Processing
    active_users,
    enriched_orders,
    # Partitioned
    regional_users,
    regional_revenue,
    # Multi-asset
    compute_analytics,
    # Graph asset
    validated_pipeline,
    # Self-dependency
    cumulative_revenue,
    # Automation pipeline (end-to-end)
    ext_market_feed,
    ext_positions_feed,
    market_snapshot,
    daily_pnl,
    risk_report,
    executive_dashboard,
    # Simple automation presets
    auto_refresh_stats,
    custom_condition_report,
    # Analytics
    user_order_summary,
    product_revenue,
    daily_stats,
    # Slow pipeline
    slow_step_a,
    slow_step_b,
    slow_step_c,
    slow_step_d,
    # Metadata showcase
    metadata_showcase,
    # Backfill showcase
    daily_events,
    daily_aggregates,
    monthly_rollup,
    regional_daily_metrics,
]

if _deployment == "cloud":
    from rivers import RunBackendConfig

    _repo_kwargs = dict(
        default_executor=Executor.kubernetes(),
        run_backend=RunBackendConfig.kubernetes(),
    )
else:
    _repo_kwargs = dict(
        default_executor=Executor.in_process(),
    )

repo = CodeRepository(
    assets=all_assets,
    tasks=[validate_data, compute_margins, export_report],
    jobs=[
        ingestion_job,
        analytics_job,
        full_pipeline_job,
        slow_pipeline_job,
        metadata_showcase_job,
        backfill_demo_job,
        multi_partition_demo_job,
    ],
    schedules=[
        daily_ingestion_schedule,
        daily_analytics_schedule,
        slow_pipeline_schedule,
        hourly_skip_example,
    ],
    sensors=[new_data_sensor, file_watcher_sensor, data_quality_sensor],
    run_queue=RunQueueConfig(
        max_concurrent_runs=3,
        tag_concurrency_limits=[
            TagConcurrencyLimit(key="slow", limit=1),
            TagConcurrencyLimit(key="ingestion", limit=2),
        ],
    ),
    pool_limits={
        "database": 2,
        "compute": 4,
    },
    **_repo_kwargs,
)
