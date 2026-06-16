# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

No changes yet.

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
