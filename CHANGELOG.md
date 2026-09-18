# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **`OrchestrationState.CompletedTime` / `CreatedTime` / `LastUpdatedTime` had
  `DateTimeKind.Unspecified` when the underlying column was NULL** (GitHub #8, residue of #3).
  The reader already normalized non-NULL values to UTC, but the NULL fallback used
  `default(DateTime)`, whose `Kind` is `Unspecified`, so the DTFx v2 gRPC sidecar's
  `Timestamp.FromDateTime` threw `ArgumentException`. Since `completed_time` is NULL by design for
  every non-terminal instance, this affected any read of a Running, Pending, or Suspended
  orchestration — `GetOrchestrationStateAsync` (both overloads), `WaitForOrchestrationAsync`, and
  the query paths. The fallback now carries an explicit UTC kind while keeping the same ticks, so
  existing `== default` checks are unaffected.

## [1.0.0] - 2026-07-30

First stable release. The provider runs the full standalone DTFx lifecycle end-to-end — create,
activity execution, durable timers, ContinueAsNew, sub-orchestration, external events, purge, query,
multi-tenancy, and forward schema migrations — with the integration test suite running in CI
against PostgreSQL 17. See the README "Feature support & known limitations" section for what is not
yet at parity with the MSSQL reference (durable entities, least-privilege role, tags persistence).

### Fixed
- **`PurgeInstanceStateAsync(PurgeInstanceFilter)` crashed at runtime.** It called
  `purge_instance_state_by_time` passing a status CSV string where the function expected a
  `SMALLINT`, throwing `PostgresException (invalid input syntax for type smallint)`. It now mirrors
  the MSSQL provider: collects matching instance IDs via the paginated query, then deletes them in
  batches. Status-set filtering (multiple statuses) is now supported.
- **`GetOrchestrationWithQueryAsync` and `GetManyOrchestrationsAsync` could fail with
  `42883: function ... does not exist`.** Parameters were sent untyped (`integer`/`text`/
  `timestamp without time zone`), which did not resolve the `query_many_orchestrations` overload.
  Parameters are now typed explicitly (`SMALLINT`/`VARCHAR`/`TIMESTAMPTZ`) and `DateTime` values are
  normalized to UTC.
- **History-event timestamps materialized from JSON had `DateTimeKind != Utc`** (GitHub #3). The
  DTFx v2 gRPC sidecar's `Timestamp.FromDateTime` requires UTC and threw. Timestamps are now
  normalized to UTC on materialization (`HistoryEvent.Timestamp`, `TimerCreatedEvent.FireAt`,
  `TimerFiredEvent.FireAt`, `DistributedTraceContext.ActivityStartTime`).
- **`OrchestrationState.Tags` was `null`** (GitHub #3), causing `ArgumentNullException` in consumers
  that copy tags into a non-null collection. It now defaults to an empty dictionary. (Tags are not
  yet persisted — see limitations.)
- **Concurrent schema deployment could deadlock / unique-violate** (`23505`/`40P01`) when multiple
  service instances or parallel test classes started against the same database. `DeploySchemaAsync`
  now retries on these SQLSTATEs since the scripts are idempotent.
- **Sub-orchestration never completed** — the parent stayed Running forever. Two root causes: (1)
  the `lock_next_orchestration` history/new-event JSON omitted `parentInstanceId`, so
  `OrchestrationRuntimeState.ParentInstance` was null and the runtime never emitted a
  `SubOrchestrationInstanceCompletedEvent`; and (2) the leaf's `ExecutionStarted` row was written
  with `task_id = -1` (ExecutionStarted's own EventId is always -1), so the runtime read
  `ParentInstance.TaskScheduleId = -1` and built the completion with `TaskScheduledId = -1`, which
  matched no pending `SubOrchestrationInstanceCreated`. The parent link is now propagated and
  `GetTaskEventId` persists the parent's schedule id for `ExecutionStarted`. Sub-orchestration now
  works end-to-end.
- **`CompleteTaskOrchestrationWorkItemAsync` filtered inter-orchestration messages to
  `ExecutionStartedEvent`**, dropping sub-orchestration completions. All orchestrator-emitted
  messages are now forwarded, mirroring the MSSQL provider.
- **ContinueAsNew was not honored** — the orchestration completed on its first execution instead
  of re-activating with the next input/execution id. The `continuedAsNewMessage` is now routed
  through `checkpoint_orchestration`, which detects the execution-id change and re-activates the
  instance. ContinueAsNew now works end-to-end.
- **Querying orchestrations by task hub name now throws `NotSupportedException`** explicitly
  (mirrors MSSQL) instead of silently ignoring the filter.

### Changed
- Schema version string bumped from `0.1.0-poc` to `1.0.0` to match the package version.
- Removed unused helpers (`AddArrayParameter`, `ExecuteNonQueryAsync`, `ExecuteReaderAsync`) and
  stale internal documentation that described a long-superseded POC.

### Added
- **Forward schema migrations.** The baseline schema is applied idempotently; subsequent upgrades
  are applied from embedded `Scripts/migrations/migration-{semver}.postgresql.sql` resources in
  semantic-version order, each recorded in `dt.versions`. Enables a clean 1.0.0 → 1.x upgrade path.
- Integration test coverage for activity execution, durable timers, sub-orchestration, ContinueAsNew,
  external events (via the DTFx `OnEvent` + `TaskCompletionSource` pattern — DTFx has no built-in
  `WaitForExternalEvent<T>()`), purge-by-filter, orchestration query, non-null `Tags`, end-to-end
  orchestration completion, and the migration runner.
- Unit coverage for `PostgreSqlUtils.GetHistoryEvent` UTC materialization.
- A GitHub Actions workflow (`.github/workflows/tests.yml`) that runs the test suite against a
  `postgres:17` service container.
- `AGENTS.md` documenting the local upstream sources (DTFx core, MSSQL provider) used as the porting
  reference.

### Tests
- Integration tests now each use a unique task hub to avoid cross-class instance residue, and run
  serialized via an xUnit collection.

### Known limitations (documented in README; tracked for 1.1.0 as #4 and #5)
- Durable entities (#4), a least-privilege role model with a `SECURITY DEFINER` audit (#5), and tags
  persistence are not yet at parity with the MSSQL reference. See the README "Feature support &
  known limitations" section.



## [1.0.0-alpha.3] - 2026-07-04

### Fixed
- **SchemaName setting was not honored end-to-end.** The embedded SQL scripts and many inline SQL
  commands were hard-coded to the default schema `dt`. When consumers configured a non-default
  `SchemaName` (or relied on the previous default `dtf`), deployment created objects in `dt` while
  runtime queries targeted the configured schema, causing `schema "..." does not exist` and
  `composite type ... does not exist` errors. The provider now rewrites the embedded scripts to use
  the configured schema at deploy time and uses `{_settings.SchemaName}` in all inline SQL
  statements. The default schema remains `dt` for backwards compatibility.

## [1.0.0-alpha.2] - 2026-06-16

Critical bug-fix release addressing the orchestration abandonment defect reported by the
Menshen team (see `docs/sre/bug-report-durabletask-postgresql.md` in the core repo). With
1.0.0-alpha.1, **no orchestration could execute past the first `ScheduleTask` call** and
`GetOrchestrationStateAsync` threw `IndexOutOfRangeException`. Both are now resolved, and
several additional defects surfaced (and fixed) during end-to-end verification against
PostgreSQL 17.

### Fixed

**Reported defects (from the bug report):**

- **Orchestrations abandoned after `ScheduleTask`** (root cause). `dt.checkpoint_orchestration`
  and `dt.complete_tasks` declare their event/task parameters as PostgreSQL composite arrays
  (`dt.history_event[]`, `dt.task_event[]`, …), but the C# runtime was sending hand-built JSON
  strings as `text`. PostgreSQL cannot cast `text` to a composite array, so every checkpoint
  threw `22P02`/`42804`; the DTFx `WorkItemDispatcher` swallowed the error and only emitted the
  generic "Abandoning orchestration work item" warning. Fixed by registering the composite types
  via `NpgsqlDataSourceBuilder.MapComposite<...>` (the records already existed in
  `PostgreSqlTypes.cs` as dead code — the intended design, mirroring the MSSQL provider's TVPs)
  and sending typed arrays. After the fix, history and activity-task rows are persisted and the
  orchestration reaches `Completed`.
- **`GetOrchestrationStateAsync` threw `IndexOutOfRangeException`**. `dt.query_single_orchestration`
  was missing the `parent_instance_id` column its `RETURNS TABLE` clause (present in
  `query_many_orchestrations`). Added it to both the signature and the inner `SELECT`.
- **`TaskHubName` from C# settings was ignored**. The setting was only logged. The constructor now
  propagates it as the Npgsql connection `ApplicationName`, and the schema default for
  `TaskHubMode` changed from `'1'` (`CURRENT_USER`) to `'0'` (`application_name`), so the C#
  `TaskHubName` is authoritative. Existing databases keep their current mode
  (`ON CONFLICT DO NOTHING`).
- **`AutoDeploySchema` could not upgrade functions whose `RETURNS` shape changed** (`42P13:
  cannot change return type of existing function`). `logic.postgresql.sql` now drops each
  function (`DROP FUNCTION IF EXISTS ... CASCADE`) before its `CREATE OR REPLACE`, and
  `DeploySchemaAsync` deploys schema + logic atomically in a single transaction.
- **Checkpoint/completion errors were invisible.** The DTFx dispatcher swallows the inner
  exception and only logs "Abandoning". The provider now wraps `checkpoint_orchestration` and
  `complete_tasks` in `try/catch` that logs the full `PostgresException` (including `SqlState`)
  before re-throwing. (This is what made the additional defects below diagnosable.)

**Additional defects found during end-to-end verification:**

- **`DateTimeOffset` with a non-UTC offset** (e.g. the local offset on DTFx history `Timestamp`s)
  could not be written to `timestamptz` (`ArgumentException`). All `Timestamp`/`VisibleTime`
  values are now normalized to UTC via `.ToUniversalTime()`.
- **Payload round-trip corruption.** `PostgreSqlUtils.GetPayloadText` called `GetString()` on
  string-valued `payloadText` JSON, unwrapping the quotes — so an input of `"World"` deserialized
  to bare `World`, which then failed to write to the JSONB column / re-deserialize. Now returns
  `GetRawText()` to keep payloads in their serialized form.
- **`dt.complete_tasks` used `SELECT DISTINCT ... FOR UPDATE OF i`**, which PostgreSQL forbids
  (`0A000: FOR UPDATE is not allowed with DISTINCT clause`). Activity completion therefore always
  failed, leaving activity tasks permanently locked. Rewritten to lock a single matching instance
  row directly via an `EXISTS` subquery.
- **`LockNextTaskActivityWorkItem` cast `payload_text` to `JsonElement`** (`(JsonElement)reader.GetValue(...)`),
  but Npgsql returns the JSONB column as `string` → `InvalidCastException`. The task got locked
  in the database but the work item was never returned, so the task stayed locked and the activity
  worker polled forever. Now reads the column as `string`.

### Verified

End-to-end against `postgres:17-alpine`: the repro orchestration (`HelloOrchestration` scheduling
a `HelloActivity`) now reaches `Completed` with output `"Hello World!"`, with rows in `dt.history`
and the activity task consumed from `dt.new_tasks`. DurableTask.Core is unchanged.

## [1.0.0-alpha] - YYYY-MM-DD
### Added
- Initial setup and OSS extraction.
