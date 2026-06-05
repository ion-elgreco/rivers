# Resources

Resources are shared, injectable dependencies (database connections, API clients, etc.) available across assets, tasks, schedules, and sensors.

## Defining a resource

Extend `rivers.Resource` (a `pydantic_settings.BaseSettings` subclass) with optional lifecycle hooks:

```python
import rivers as rs

class DatabaseResource(rs.Resource):
    connection_string: str  # explicit or from CONNECTION_STRING env var
    pool_size: int = 5

    def setup(self):
        self._pool = create_pool(self.connection_string, self.pool_size)

    def teardown(self):
        self._pool.close()
```

`setup()` and `teardown()` are optional — only override them if your resource needs initialization or cleanup.

## Registering resources

Pass resources to `CodeRepository` as a dict:

```python
repo = rs.CodeRepository(
    assets=[my_asset],
    resources={
        "db": DatabaseResource(connection_string="postgresql://..."),
        "api": APIClient(base_url="https://api.example.com"),
    },
)
```

## Injection

Resources are injected by matching parameter names to resource keys:

```python
@rs.Asset
def my_asset(context: rs.AssetExecutionContext, db: DatabaseResource, api: APIClient):
    data = api.fetch("/data")
    db._pool.execute("INSERT ...", data)
    return data
```

The injection resolution order is:

1. Context (first param, if it matches a context type)
2. `self` — self-dependency
3. Upstream asset name match (in-memory result)
4. Upstream asset name match (via io_handler)
5. Resource key match
6. Error — unknown parameter

Asset names take precedence over resource keys. rivers warns at resolve time if a resource key shadows an asset name.

## Schedule and sensor injection

Schedules and sensors also support resource injection:

```python
@rs.Schedule(cron_schedule="0 * * * *", job_name="hourly_job")
def my_schedule(context: rs.ScheduleEvaluationContext, db: DatabaseResource):
    if db.check_condition():
        return rs.RunRequest()
    return rs.SkipReason("No new data")
```

## Lifecycle

```text
CodeRepository.__init__()
  └─ stores raw resource instances

CodeRepository.resolve()
  └─ calls resource.setup() for each resource

materialize() / evaluate_schedule() / evaluate_sensor()
  └─ injects resource instances into function calls

CodeRepository shutdown (context manager __exit__ or shutdown())
  └─ calls resource.teardown() for each resource
```

Resources are initialized once at resolve time and shared across all executions. They are not re-created per run.

## IOHandler as resource reference

`io_handler` on an asset can reference a resource by string key:

```python
class S3Handler(rs.BaseIOHandler):
    bucket: str
    prefix: str

    def handle_output(self, context, obj): ...
    def load_input(self, context): ...

@rs.Asset(io_handler="s3")  # references resources["s3"]
def my_asset():
    return data

repo = rs.CodeRepository(
    assets=[my_asset],
    resources={"s3": S3Handler(bucket="my-bucket", prefix="assets/")},
)
```

String references are resolved to the actual handler at `resolve()` time.

## Testing

Override resources in tests by passing a different dict:

```python
def test_my_pipeline():
    mock_db = DatabaseResource(connection_string="sqlite:///:memory:")
    repo = rs.CodeRepository(
        assets=[my_asset],
        resources={"db": mock_db},
    )
    result = repo.materialize()
    assert result.success
```

## Validation

rivers validates all resource references at resolve time:

- Asset/task function parameters that don't match an upstream asset or resource key raise `ConfigurationError`
- Schedule/sensor evaluation function parameters that don't match a resource key raise `ConfigurationError`
- IOHandler string references that don't match a resource key raise `ConfigurationError`
