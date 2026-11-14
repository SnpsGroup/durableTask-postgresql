# Connection and Data Source Configuration

The provider can create its own Npgsql pool or use an application-owned
`NpgsqlDataSource`. Both paths require the PostgreSQL composite mappings used by
checkpoints and activity completion.

## 1. Connection model

### 1.1 Provider-owned pool

With `ConnectionString` settings, the constructor builds one data source and
registers its mappings. `ConfigureDataSource` can customize the builder before
the provider applies its mappings and task hub name. `StopAsync` and `Dispose`
dispose this pool.

`AutoDeploySchema` optionally deploys over a separate raw connection. This avoids
loading the runtime pool's PostgreSQL type metadata before the composite types
exist. Deployment also holds a session advisory lock across multiple
transactions, so its connection must remain checked out for the entire deploy.

### 1.2 Application-owned pool

Set `DataSource`, or use the DI overload accepting an `NpgsqlDataSource`. The
service uses that exact pool and never disposes it, including when startup fails.
The application or its DI container retains ownership and disposes the pool
after its consumers have stopped.

Before calling `Build`, register provider mappings with
`builder.UseDurableTaskPostgreSql(schemaName)`. This public helper registers the
internal CLR composite types without exposing them as public contracts:

| PostgreSQL type | Use |
| --- | --- |
| `message_id` | Acknowledge consumed messages |
| `history_event` | Persist orchestration history |
| `orchestration_event` | Route orchestration and timer messages |
| `task_event` | Schedule activities |
| `task_result` | Complete activities |

The helper schema must match `settings.SchemaName`. `StartAsync` validates all
five mappings by serializing typed empty arrays without changing data. A pool
that connects successfully but lacks these mappings fails during startup,
before an orchestration reaches its first checkpoint.

### 1.3 Task hub routing

In the default `TaskHubMode = '0'`, PostgreSQL resolves the task hub from the
connection's `application_name`. For an owned pool, the provider sets this from
`TaskHubName`. For an external pool, it cannot change the pool's connection
configuration: if `TaskHubName` is supplied, it must match the external pool's
`ApplicationName`, or construction fails.

The application may use the same pool for its ordinary SQL and one configured
task hub. Sharing the pool does not modify application queries. Do not mutate
session `application_name` in a connection initializer. Use separately configured
pools for task hubs requiring different application names. If `TaskHubName` is
omitted, the external pool's application name controls routing, with the database
fallback of `default` when it is empty.

Task hub routing is not an authorization boundary. The separate runtime role
model controls database privileges; neither pool sharing nor an application name
enforces tenant isolation against direct SQL access.

## 2. Schema deployment and metadata

Deploy the schema before the external data source opens its first physical
connection, including connections used by other application components. Npgsql
loads and caches database type metadata on that first connection. Building the
data source alone does not load this metadata, but executing even `SELECT 1`
does. Creating the composite types afterwards does not refresh that snapshot.

If a pool has already cached metadata before deployment, reload its types with
Npgsql's `ReloadTypesAsync`, or recreate the pool after deployment. Mappings are
schema-specific: use `UseDurableTaskPostgreSql("orders_dt")` and
`SchemaName = "orders_dt"` together for a custom schema.

`DataSource` cannot be combined with `ConfigureDataSource` or `AutoDeploySchema`.
Configure the external builder yourself and deploy separately with a schema
owner. The runtime connection string is optional when supplying `DataSource`.

The SQL Server reference provider constructs `SqlConnection` objects from a
connection string and has no equivalent public pool or client composite mapping
contract; pool ownership is a PostgreSQL/Npgsql integration concern.

## 3. Lifecycle

Stop the worker before stopping or disposing the service. An externally supplied
pool continues to work afterwards. Do not dispose that pool until all application
and provider consumers have stopped. A service which has been stopped is not
restartable; construct a new service if needed, reusing the external pool.

Configure settings before constructing the service. Changing settings afterwards
is unsupported because connections and polling strategies are initialized during
construction.

## 4. Examples

### 4.1 Share a configured application pool

Deploy the provider schema first using the schema owner, then create the runtime
pool and share it with the application:

```csharp
using DurableTask.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

var builder = new NpgsqlDataSourceBuilder(
    "Host=localhost;Database=app;Username=runtime;Password=...;Application Name=orders");
builder.UseDurableTaskPostgreSql("dt");
await using var dataSource = builder.Build();

var services = new ServiceCollection();
services.AddLogging();
// Register the same instance for application SQL consumers if desired.
services.AddSingleton(dataSource);
services.AddDurableTaskPostgreSql(dataSource, settings =>
{
    settings.TaskHubName = "orders";
    settings.SchemaName = "dt";
});

using var serviceProvider = services.BuildServiceProvider();
var service = serviceProvider.GetRequiredService<PostgreSqlOrchestrationService>();
await service.StartAsync();
// Start the TaskHubWorker and execute application work here.
await service.StopAsync();
// dataSource remains usable until the application disposes it.
```

Do not build a pool first and then attempt to configure its composite mappings:
Npgsql builders are the configuration boundary. For ASP.NET hosting, register a
data source factory in DI and call the helper inside that factory before `Build`.

### 4.2 Configure a provider-owned pool

Use the existing callback when only shared configuration is needed:

```csharp
services.AddDurableTaskPostgreSql(new PostgreSqlOrchestrationServiceSettings
{
    ConnectionString = "Host=localhost;Database=app;Username=runtime",
    TaskHubName = "orders",
    ConfigureDataSource = builder =>
    {
        builder.UseLoggerFactory(loggerFactory);
        builder.UsePeriodicPasswordProvider(
            (_, cancellationToken) => GetAccessTokenAsync(cancellationToken),
            successRefreshInterval: TimeSpan.FromMinutes(50),
            failureRefreshInterval: TimeSpan.FromSeconds(5));
    }
});
```

`ConnectionString` is applied before the callback. Omit `Password` and `Passfile`
when registering a password provider; Npgsql rejects supplying both. This is a
trusted extension point running as the host application. The provider reapplies
composite mappings and `TaskHubName`, but allows other builder changes such as
database, host, search path, logging, certificates and tracing.

The callback does not apply to schema deployment, which uses a raw connection.
Authentication available only through the callback therefore requires deployment
out of band with `AutoDeploySchema = false`.

## 5. Verification

`ExternalDataSourceIntegrationTests` runs complete activity/checkpoint lifecycles
with both the default and a custom schema, verifies task hub routing and confirms
that application SQL still succeeds after provider stop/dispose. It also proves
missing mappings fail during startup while leaving the application pool usable.
Unit tests cover the DI overload, conflicting settings and mismatched task hubs.
