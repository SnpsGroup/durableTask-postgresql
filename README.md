# PostgreSQL Provider for Durable Task Framework (Standalone)

`DurableTask.PostgreSQL` is a PostgreSQL-backed storage provider for the
[Durable Task Framework (DTFx)](https://github.com/Azure/durabletask),
focused on the standalone runtime scenario.

This repository follows the same overall direction as
[microsoft/durabletask-mssql](https://github.com/microsoft/durabletask-mssql),
but targets PostgreSQL and currently concentrates on standalone DTFx support.

## Why this provider

- **PostgreSQL portability**: run on local environments, managed cloud
  PostgreSQL services, or self-hosted clusters.
- **Data ownership**: orchestration runtime data lives in your database and can
  be managed with your existing backup and governance processes.
- **Operational visibility**: SQL scripts and schema are part of this
  repository, making runtime behavior easier to inspect and operate.

## Package

| Package | Description |
| ------- | ----------- |
| `DurableTask.PostgreSQL` | Standalone DTFx provider for PostgreSQL |

Project file:
`src/DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj`

## Current scope

- Standalone DTFx backend (`IOrchestrationService` and
  `IOrchestrationServiceClient`)
- PostgreSQL schema + logic scripts shipped with the package
- Integration-test coverage for core orchestration flows

## Feature support & known limitations

This is a port in progress toward parity with the
[microsoft/durabletask-mssql](https://github.com/microsoft/durabletask-mssql)
1.x reference. To set expectations clearly:

**Supported and integration-tested:**
- Create / get-state / get-history for orchestrations
- Activity execution (schedule + complete)
- Durable timers
- ContinueAsNew (orchestration loops with a new execution id)
- Sub-orchestration (nested orchestrations with result propagation)
- External events raised via the client, with the orchestrator waiting via the `OnEvent` pattern
- Purge by filter and by instance id
- Orchestration query (`GetOrchestrationWithQueryAsync`)
- Multi-tenancy via task hubs (`TaskHubName`)
- Concurrent-deploy safety (schema creation retries on contention)

**Known limitations (not yet at parity):**
- **Durable entities**: not implemented (the MSSQL provider's entity feature has
  no PostgreSQL equivalent yet). Tracked for 1.1.0.
- **Schema migrations**: forward migrations are supported. The baseline
  (`schema.postgresql.sql`) is applied idempotently for fresh installs; subsequent
  upgrades are applied from embedded `Scripts/migrations/migration-{semver}.postgresql.sql`
  resources in semantic-version order, each recorded in `dt.versions`. Migrations must
  use idempotent DDL (`ADD COLUMN IF NOT EXISTS`, `CREATE TYPE IF NOT EXISTS`, etc.).
- **Tags**: `OrchestrationState.Tags` is a non-null empty dictionary but tags are
  not persisted (no schema column).
- **Least-privilege role**: no `dt_runtime`-style role/`GRANT` model yet; connect
  with a user that can create the schema.

If any of the above is a blocker for your use case, please open an issue.

## Quick start

Install package:

```bash
dotnet add package DurableTask.PostgreSQL
```

Register with DI:

```csharp
using DurableTask.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddDurableTaskPostgreSql(new PostgreSqlOrchestrationServiceSettings
{
    ConnectionString = "Host=localhost;Port=5433;Database=durabletask;Username=postgres;Password=postgres",
    SchemaName = "dt",
    TaskHubName = "sample-hub",
    AutoDeploySchema = true
});
```

## Polling and backoff

The provider polls PostgreSQL for work. When a poll finds nothing, it waits before trying again,
using randomized exponential backoff between `MinWorkItemPollingInterval` and
`WorkItemPollingInterval`. This matters because each poll runs as a transaction that commits even
when it returns no rows, so polling without a delay inflates `pg_stat_database.xact_commit` and
puts needless load on the server while the task hub is idle.

The backoff does not delay real work: creating an instance, raising an external event, completing
an activity, or checkpointing an orchestration that enqueued work all signal the relevant poller,
which wakes immediately and resets to the minimum interval.

| Setting | Default | Purpose |
| ------- | ------- | ------- |
| `MinWorkItemPollingInterval` | 50 ms | Delay after the first poll that finds no work, and the interval returned to once work appears. |
| `WorkItemPollingInterval` | 1 s | Upper bound. An idle worker settles here (one poll per interval, per dispatcher). |
| `DeltaBackoffWorkItemPollingInterval` | 50 ms | Growth increment applied as successive polls come back empty. |

Raise `WorkItemPollingInterval` to reduce load from idle workers; lower
`MinWorkItemPollingInterval` to shave latency on a busy task hub. `MinWorkItemPollingInterval`
must not exceed `WorkItemPollingInterval` — the constructor rejects that combination.

## Database scripts

The provider ships SQL scripts at:

- `src/DurableTask.PostgreSQL/Scripts/schema.postgresql.sql`
- `src/DurableTask.PostgreSQL/Scripts/logic.postgresql.sql`

These scripts are also packed in NuGet as content files.

## Local development

Restore/build:

```bash
dotnet restore src/DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj
dotnet build src/DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj -c Release
```

Run tests:

```bash
dotnet test tests/DurableTask.PostgreSQL.Tests/DurableTask.PostgreSQL.Tests.csproj -c Release
```

By default, integration tests use:

`Host=localhost;Port=5433;Database=durabletask;Username=postgres;Password=postgres`

Or set `POSTGRES_CONNECTION_STRING` to override.

## Sample app

A minimal consumer exists in:

`samples/ConsumerApp`

Run it with:

```bash
dotnet run --project samples/ConsumerApp/ConsumerApp.csproj
```

## Releases

NuGet publishing is automated by GitHub Actions in
`.github/workflows/publish-nuget.yml`.

Triggers:
- Pull Request and Push (main/vnext/desenv): build + pack validation
- Tag push `v*`: build + publish to NuGet.org
- Manual (`workflow_dispatch`): optional publish when `publish=true`

For trusted publishing, configure:
- NuGet.org trusted publisher for this GitHub repository/workflow
- GitHub secret `NUGET_ORG_USERNAME` with the owner username at NuGet.org

## Contributing

Contributions are welcome. Open an issue to discuss proposals and behavior
changes before large PRs.

Please review [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## License

MIT. See [LICENSE](LICENSE).
