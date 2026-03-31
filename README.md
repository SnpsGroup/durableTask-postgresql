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
