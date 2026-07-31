# AGENTS.md

Guidance for AI agents (and contributors) working on this repository.

## Project

`DurableTask.PostgreSQL` is a PostgreSQL backend for the standalone Durable Task
Framework (DTFx) — a port of `microsoft/durabletask-mssql` to PostgreSQL using
Npgsql. See `README.md` for usage and `lessonLearned.md` for hard-won gotchas.

## Local upstream sources

This provider is a port, so the upstream projects are the authoritative reference
when fixing bugs or closing feature gaps. They live alongside this repo on disk:

| Reference | Local path | Use it to |
|---|---|---|
| **DTFx core** (`Microsoft.Azure.DurableTask.Core`) | `D:\Code\durabletask-projects\durabletask` | Understand the runtime contracts (`IOrchestrationService`, `TaskHubWorker`, history events, message routing) the provider must satisfy. |
| **SQL Server provider** (`microsoft/durabletask-mssql`) | `D:\Code\durabletask-projects\durabletask-mssql` | The 1.x "stable" reference implementation. Mirror its behavior (SQL logic, C# message routing) when porting/fixing PostgreSQL equivalents. |

When fixing a provider bug, first read the corresponding MSSQL code path, then
port the same behavior to PostgreSQL (C# + `Scripts/*.sql`). See the
`lessonLearned.md` entries on inter-orchestration message routing for a concrete
example of why cross-referencing the MSSQL reference matters.

## Testing

- Integration tests run against a live PostgreSQL. Locally a `shared-postgres`
  (postgres:17-alpine) container is expected on port 5432
  (`Username=root;Password=root;Database=postgres`), overridable via the
  `POSTGRES_CONNECTION_STRING` env var.
- CI (`.github/workflows/tests.yml`) spins up a postgres:17 service container so
  the integration suite actually runs.
- Each integration test class uses a unique `TaskHubName` to avoid cross-class
  instance residue; see `lessonLearned.md`.
