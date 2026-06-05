import rivers as rs


@rs.Asset
def raw_data(context: rs.AssetExecutionContext):
    context.add_output_metadata({"rows": 1000})
    return {"users": 100, "events": 5000}


@rs.Asset
def summary(raw_data: dict):
    return f"{raw_data['users']} users, {raw_data['events']} events"
