"""A second code location that builds on assets of the demo code location.

In k3d it runs as the `demo-downstream` CodeLocation. Locally, start the demo
under the name `demo`, then this module on the storage it prints:

    rivers dev examples.demo_project.pipeline --code-location-id demo
    rivers dev examples.demo_project.downstream --code-location-id demo-downstream \\
        --surreal-endpoint ws://127.0.0.1:<port> --grpc-port 3011 --no-ui
"""

import os
import time

from rivers import (
    Asset,
    AssetDef,
    AssetExecutionContext,
    AutomationCondition,
    CodeRepository,
    MetadataValue,
    PartitionMapping,
    PartitionsDefinition,
    RunQueueConfig,
)

from .shared import output_io, pickle_io, processed_io, repo_kwargs

DEMO = os.environ.get("RIVERS_DEMO_CODE_LOCATION", "demo")

downstream_io = pickle_io("downstream")

# The same definition as `region_partitions` in the demo.
region_partitions = PartitionsDefinition.static_(["us-east", "us-west", "eu-west"])


# =============================================================================
# Remote assets: materialized by the demo code location
# =============================================================================

enriched_orders = Asset.remote(
    "enriched_orders",
    code_location=DEMO,
    io_handler=processed_io,
    group="from_demo",
)

regional_revenue = Asset.remote(
    "regional_revenue",
    code_location=DEMO,
    io_handler=processed_io,
    partitions_def=region_partitions,
    group="from_demo",
)

# The demo stores its daily P&L under its own key; here it is `demo_pnl`.
demo_pnl = Asset.remote(
    "demo_pnl",
    code_location=DEMO,
    asset_key="daily_pnl",
    io_handler=output_io,
    group="from_demo",
)

# Lineage only: no handler, so no asset here loads it.
market_snapshot = Asset.remote(
    "market_snapshot",
    code_location=DEMO,
    group="from_demo",
)


# =============================================================================
# Assets of this code location
# =============================================================================


@Asset(
    io_handler=downstream_io,
    tags=["reporting"],
    kinds="metric",
    group="order_reporting",
    automation_condition=AutomationCondition.eager(),
)
def category_report(context: AssetExecutionContext, enriched_orders: dict) -> dict:
    """Revenue per product category. Runs each time the demo updates its orders."""
    revenue: dict[str, float] = {}
    for order in enriched_orders["orders"]:
        revenue[order["category"]] = (
            revenue.get(order["category"], 0.0) + order["amount"]
        )
    context.add_output_metadata(
        {
            "categories": MetadataValue.int(len(revenue)),
            "revenue": MetadataValue.float_(sum(revenue.values())),
        }
    )
    return revenue


@Asset(
    io_handler=downstream_io,
    tags=["planning", "partitioned"],
    kinds="metric",
    group="order_reporting",
    partitions_def=region_partitions,
    deps=[
        AssetDef.input(
            "regional_revenue", partition_mapping=PartitionMapping.identity()
        )
    ],
    automation_condition=AutomationCondition.eager(),
)
def regional_targets(context: AssetExecutionContext, regional_revenue: dict) -> dict:
    """Next quarter's revenue target per region: 10% over the demo's revenue."""
    target = round(regional_revenue["revenue"] * 1.1, 2)
    context.add_output_metadata({"target": MetadataValue.float_(target)})
    return {"region": context.partition_key, "target": target}


@Asset(
    io_handler=downstream_io,
    tags=["finance"],
    kinds="metric",
    group="market_watch",
    automation_condition=AutomationCondition.eager(),
)
def pnl_alerts(demo_pnl: dict) -> dict:
    """Positions that lost money at the demo's last close."""
    losers = [p["symbol"] for p in demo_pnl["pnl"] if p["pnl"] < 0]
    return {"losers": losers, "total_pnl": demo_pnl["total_pnl"]}


@Asset(
    io_handler=downstream_io,
    tags=["market-data"],
    kinds="metric",
    group="market_watch",
    deps=[AssetDef.dep("market_snapshot")],
    automation_condition=AutomationCondition.eager(),
)
def snapshot_heartbeat(context: AssetExecutionContext) -> dict:
    """Records when the demo's market snapshot last changed."""
    context.log.info("market_snapshot changed in code location %s", DEMO)
    return {"seen_at": time.time()}


repo = CodeRepository(
    assets=[
        enriched_orders,
        regional_revenue,
        demo_pnl,
        market_snapshot,
        category_report,
        regional_targets,
        pnl_alerts,
        snapshot_heartbeat,
    ],
    # In the cluster, only a queued run starts as a Run with its own pod.
    run_queue=RunQueueConfig(),
    **repo_kwargs(),
)
