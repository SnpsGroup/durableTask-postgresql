# Connection and Data Source Configuration

How `DurableTask.PostgreSQL` obtains PostgreSQL connections, what it requires of
them, and how to supply your own Npgsql configuration without breaking the
provider.

This document covers a PostgreSQL-only concern. The SQL Server reference
provider (`microsoft/durabletask-mssql`) has no equivalent: it builds a
`SqlConnection` per operation from a connection string (`internal
SqlOrchestrationServiceSettings.CreateConnection()`,
`SqlOrchestrationServiceSettings.cs:223`), so there is no pool object to share
and no client-side type mappings to register. Do not look for parity on those
two points.

Note that mssql *does* expose some Npgsql-equivalent configuration directly —
`SqlOrchestrationServiceSettings.LoggerFactory` is a public settable property
(`SqlOrchestrationServiceSettings.cs:211`). So the gap in section 2 around
logging is a gap against the reference provider too, not a PostgreSQL-specific
trade-off.

---

## 1. How connections work today

`PostgreSqlOrchestrationService` owns exactly one `NpgsqlDataSource`
(`PostgreSqlOrchestrationService.cs:30`), built in the constructor
(`:88-117`) from `PostgreSqlOrchestrationServiceSettings.ConnectionString` (plus the optional `ConfigureDataSource` callback; see 4.2). Every
runtime database operation — 21 call sites — acquires its connection through
`_dataSource.OpenConnectionAsync(...)`. There are no connections constructed
outside this data source on the runtime path.

The constructor does three things to that data source that the provider depends
on. All three are requirements, not conveniences.

### 1.1 Task hub isolation via `ApplicationName`

`TaskHubName` is propagated to PostgreSQL as the connection's `ApplicationName`
(`:77-81`, and re-asserted after `ConfigureDataSource` at `:98-101`).

The schema reads it back. `dt.current_task_hub()`
(`Scripts/schema.postgresql.sql:200-228`) resolves the task hub as:

| `TaskHubMode` in `dt.global_settings` | Task hub source |
| --- | --- |
| `'0'` (default, seeded at `schema.postgresql.sql:189-191`) | `current_setting('application_name', true)` |
| `'1'` | `CURRENT_USER` |
| neither / NULL / empty result | literal `'default'` |

Every stored function that touches task hub data calls
`dt.current_task_hub()` — 17 call sites in `logic.postgresql.sql`. The task hub
is therefore a property of **the connection**, not of a parameter the provider
passes. A connection with the wrong `application_name` reads and writes a
different task hub's data, and does so successfully.

Names longer than 50 characters are rewritten as the first 16 characters plus
`'__'` plus the MD5 of the full name (`schema.postgresql.sql:229-233`). This is
deterministic, so two long names sharing a 16-character prefix still resolve to
distinct hubs.

Be aware the margin is exactly zero: 16 + 2 + 32 = 50, and the function is
declared `RETURNS VARCHAR(50)` (`schema.postgresql.sql:201`). The truncation
point is the `RETURN` (`:235`), not the local — the local `v_task_hub` is
`VARCHAR(150)` (`:208`) and holds the full hashed value fine. Widening the
16-character prefix would therefore produce a value that fits the local, passes
unnoticed through the function body, and is then silently cut at the return
boundary — losing MD5 characters and reintroducing collisions for names sharing
a prefix. Treat the 16 as load-bearing, and change it only together with the
return type.

### 1.2 Composite type mappings

`checkpoint_orchestration` and `complete_tasks` take arrays of PostgreSQL
composite types. The constructor registers five mappings (`:112-116`):

| CLR type (`PostgreSqlTypes.cs`) | PostgreSQL type | Defined at |
| --- | --- | --- |
| `PostgreSqlTypes.MessageId` | `<schema>.message_id` | `logic.postgresql.sql:745` |
| `PostgreSqlTypes.HistoryEvent` | `<schema>.history_event` | `logic.postgresql.sql:753` |
| `PostgreSqlTypes.OrchestrationEvent` | `<schema>.orchestration_event` | `logic.postgresql.sql:775` |
| `PostgreSqlTypes.TaskEvent` | `<schema>.task_event` | `logic.postgresql.sql:794` |
| `PostgreSqlTypes.TaskResult` | `<schema>.task_result` | `logic.postgresql.sql:642` |

Note the composite types live in **`logic.postgresql.sql`**, not
`schema.postgresql.sql`. `DeploySchemaScriptsAsync`
(`PostgreSqlOrchestrationService.cs:1654`) runs both in one transaction, schema
first and logic second, so the types only exist once that transaction commits.

The composite name is qualified with `SchemaName`, so the mappings are
schema-specific. A data source mapped for schema `dt` is not valid for a service
configured with `SchemaName = "orders_dt"`.

### 1.3 Type information is loaded and pinned on the first physical connection

A data source loads the database's type information when it opens its **first
physical connection**, and pins it for the data source's lifetime. Composite
mappings registered via `MapComposite` resolve against that snapshot on first
use — not at `Build()`, and not per connection.

> Verified empirically against PostgreSQL 17.9 / Npgsql 10.0.2 (net10.0) on
> 2026-11-04. Note that Npgsql does not attempt composite OID resolution eagerly
> at connect time: opening a connection and running a non-composite query with
> the types absent succeeds without error. What the first physical connection
> does is load and **pin the type snapshot** for the data source; the mapping
> failure surfaces later, on first composite use (see 3.4).

Three consequences, each measured:

- The `<schema>.*` composite types must already exist by the time the data
  source opens its first connection — **any** connection, including one that
  never touches a composite.
- `Build()` alone pins nothing. A data source built before the types exist is
  still healthy provided it opens no connection until after they do.
- A mapping failure surfaces on first composite use, not at construction.

This is why `DeploySchemaAsync` deliberately does **not** use `_dataSource`. It
constructs a raw `NpgsqlConnection` (`:1540-1547`) from the connection string
with `ApplicationName` re-applied from `TaskHubName`. At deploy time the `dt.*`
types do not exist yet, so letting the deploy be the data source's first
connection pins a snapshot without them and breaks every later checkpoint.

Note that type-cache avoidance is **not the only** reason for the raw
connection, and a future change must not remove the carve-out on the assumption
that it is. The deploy also holds a *session-scoped* `pg_advisory_lock` across
several transactions (`:1564-1573`, `:1582-1587`); that requires a connection
whose physical session is under the provider's control for the whole deploy,
which a pooled connection does not guarantee.

### 1.4 Pool ownership

The provider owns the pool and disposes it:

- `StopAsync()` calls `await _dataSource.DisposeAsync()` (`:188`)
- `Dispose()` calls `_dataSource.Dispose()` (`:1888-1892`)

Disposing an `NpgsqlDataSource` closes every connection in its pool and makes
the object unusable. Today this is correct because the pool is private to the
service.

---

## 2. Current limits

Known constraints of the connection model as of 1.1.0.

| Limit | Detail |
| --- | --- |
| **One pool per service instance** | The data source is created in the constructor and never replaced. Two services against the same database (different task hubs or schemas) mean two pools. |
| **Npgsql configuration is reachable, but only through `ConfigureDataSource`** | Since 1.1.0, anything configurable through `NpgsqlDataSourceBuilder` — client certificates, `UsePasswordProvider` / `UsePeriodicPasswordProvider` (for example AWS RDS IAM or Azure Entra token rotation), `UseLoggerFactory`, `ConfigureTracing`, `UsePhysicalConnectionInitializer` — is available via the `ConfigureDataSource` callback (4.2). There is still no dedicated setting per feature; in particular the mssql provider exposes `LoggerFactory` as a first-class setting, where here it goes through the callback. Before 1.1.0 only what fits in a connection string was available. |
| **Pool cannot be shared with the host application** | An application already using PostgreSQL as its data store runs a second, independent pool for the task hub. Connection budget on the server is consumed twice, and `max_connections` planning must account for both. This is the substance of issue #7. |
| **`ApplicationName` is reserved by the provider** | When `TaskHubName` is set, any `ApplicationName` in the supplied connection string is overwritten (`:77-81`), and re-applied after the `ConfigureDataSource` callback (`:98-101`) so a callback cannot repoint it either. It is not available for application-level connection tagging on provider connections. |
| **`TaskHubName` is silently optional** | When `TaskHubName` is null or empty, the connection string is left untouched and the effective task hub becomes whatever `application_name` Npgsql sends by default, or `CURRENT_USER` under `TaskHubMode = '1'`, or `'default'`. There is no validation and no warning. |
| **Composite CLR types are `internal`** | `PostgreSqlTypes` is `internal static` (`PostgreSqlTypes.cs:10`) and all seven of its records are `internal sealed` — the five mapped composites plus the read-path result types `OrchestrationLockResult` (`:16`) and `TaskLockResult` (`:30`). An external caller cannot call `MapComposite<T>` for them. **This is the hard blocker on accepting a pre-built `NpgsqlDataSource` from the caller:** the caller has no way to satisfy requirement 1.2. |
| **`DeploySchemaAsync` needs the connection string** | It cannot be served by an injected data source (see 1.3). `ConnectionString` remains required whenever `AutoDeploySchema` is enabled. |
| **`AutoDeploySchema` is incompatible with callback-based auth** | The schema-deploy connection is built from the connection string plus `ApplicationName` only (`:1540-1544`, opened at `:1546-1547`) — it does not go through `NpgsqlDataSourceBuilder`, so a password provider, token provider, or client-certificate callback does not apply to it. A deployment that authenticates only via such a callback (AWS RDS IAM, Azure Entra) must deploy the schema out of band and leave `AutoDeploySchema = false` (its default). Deliberate: routing the deploy through a temporary data source would invoke the caller's configuration callback a second time. |

---

## 3. Points of attention

Failure modes to understand before changing the connection model. Each is a
real consequence of the mechanics in section 1, not a hypothetical.

### 3.1 Disposing a shared pool takes down the host application

If the provider accepts a caller-supplied data source and keeps its current
disposal behavior, `StopAsync()` or `Dispose()` destroys a pool the rest of the
application is still using. Every other component loses database access, with no
indication of the cause. Pool ownership must be tracked explicitly — a service
may only dispose a data source it built.

### 3.2 A missing composite mapping fails at checkpoint, not at startup

A data source without the mappings from 1.2 connects fine, passes `StartAsync()`
(which only runs `SELECT <schema>.current_task_hub()`, `:174-176`), and executes
every read path. It fails the first time an orchestration checkpoints or an
activity batch completes — i.e. under load, after the service appears healthy.

### 3.3 A wrong `ApplicationName` corrupts task hub isolation silently

This is the most dangerous failure in this document, because nothing errors.
Under the default `TaskHubMode = '0'`, a connection whose `application_name`
does not match the intended `TaskHubName` operates against a different task hub.
An application pool typically sets `ApplicationName` to the application's own
name, so the natural outcome of sharing it is a task hub silently named after
the application. Work is written to one hub and never picked up by the worker
polling another. Instances appear stuck; no exception is raised anywhere.

A pool shared across both the application and the provider cannot carry two
different `application_name` values — it is a connection-string-level setting
fixed at pool construction.

### 3.4 Type cache poisoning is permanent for the data source

Per 1.3, if a data source with composite mappings opens its first connection
before the types exist, the pinned snapshot lacks them and every later composite
use on that data source fails — **deterministically, for the data source's
lifetime**. Creating the types afterwards does not help, and the state is not
per-connection: a brand-new physical connection from the same data source still
resolves against the pinned snapshot and still fails.

Recovery requires `ReloadTypesAsync()` or rebuilding the data source. Ordering
between schema deployment and first connection is therefore load-bearing — and
an injected data source already in use by the application has, by definition,
already opened its first connection.

Observed failure modes, both on a pinned-without-types data source:

| Call shape | Exception |
| --- | --- |
| Parameter with `DataTypeName = "<schema>.<type>[]"` (what the provider uses — `:638-645`, `:925-926`) | `NotSupportedException`: *The data type name '`<schema>`._`<type>`' could not be found in the types that were loaded by Npgsql* |
| Parameter relying on the `MapComposite<T>` CLR mapping alone | `InvalidCastException`: *Writing values of '`<T>`' is not supported for parameters having no NpgsqlDbType or DataTypeName* |

Both paths break, so the mapping genuinely fails to register rather than only
the explicit-name lookup failing.

> Verified empirically against PostgreSQL 17.9 / Npgsql 10.0.2 on 2026-11-04,
> including the discriminating case: connection A held open so the pool was
> forced to create a second backend (distinct `pg_backend_pid`), with the
> round-trip on that new backend still failing. This rules out the alternative
> hypothesis that the state is per-physical-connection and self-heals as the
> pool churns — which would have made the failure flaky instead of
> deterministic. `ReloadTypesAsync()` recovers it, as this section claims.

### 3.5 Schema-qualified mappings do not survive a `SchemaName` change

Mappings are registered against `SchemaName` at construction. Reusing one data
source across services with different `SchemaName` values is invalid: the
composite names do not match.

### 3.6 Connection budget is a server-side limit

Sharing a pool halves connection consumption, which is the operational
motivation behind #7. Conversely, two pools require `max_connections` headroom
for both. On managed PostgreSQL with low connection ceilings, or behind a
transaction-mode pooler such as PgBouncer, this matters. Note that composite
types and `application_name`-based isolation both depend on session-level state,
so transaction-mode pooling is independently problematic for this provider.

---

## 4. Recommended configuration patterns

Ordered from least to most coupling with the host application.

### 4.1 Connection string only (current behavior)

```csharp
services.AddDurableTaskPostgreSql(new PostgreSqlOrchestrationServiceSettings
{
    ConnectionString = "Host=localhost;Database=durabletask;Username=app;Password=...",
    SchemaName = "dt",
    TaskHubName = "orders-hub",
    AutoDeploySchema = true
});
```

The provider builds and owns the pool, sets `ApplicationName` from
`TaskHubName`, registers all composite mappings, and deploys the schema through
a separate raw connection. Nothing to get wrong. Use this unless you have a
concrete reason not to.

### 4.2 Configure the provider's pool (recommended when you need Npgsql features)

Supply configuration; the provider still builds and owns the pool.

```csharp
services.AddDurableTaskPostgreSql(new PostgreSqlOrchestrationServiceSettings
{
    // No Password= — see the credential conflict note below.
    ConnectionString = "Host=db.example.com;Database=durabletask;Username=app",
    TaskHubName = "orders-hub",
    ConfigureDataSource = builder =>
    {
        // Signature per Npgsql 10.0.2: the callback receives the connection
        // string builder, and the refresh/failure intervals are TimeSpans.
        builder.UsePeriodicPasswordProvider(
            (csb, ct) => tokenProvider.GetTokenAsync(ct),
            TimeSpan.FromMinutes(50),
            TimeSpan.FromSeconds(5));
        builder.UseLoggerFactory(loggerFactory);
    }
});
```

**Credential conflict.** The provider applies `ConnectionString` to the builder *before* the
callback runs, so the two must not both supply a credential. With `Password` (or `Passfile`) in
the connection string *and* a password provider registered in the callback, Npgsql 10.0.2
throws at `Build()` — i.e. inside the service constructor:

```
NotSupportedException: When registering a password provider, a password or password file
may not be set.
```

Measured against Npgsql 10.0.2: omitting `Password` with the provider registered builds fine.
This matters because rotating-token auth is the main reason to reach for this callback, and the
quick-start connection string in `README.md` does include a password — so copying both together
fails immediately.

This reaches every `NpgsqlDataSourceBuilder` capability — auth token rotation,
client certificates, tracing, physical connection initializers — while the
provider retains control of pool ownership (1.4) and of the two settings it
re-applies after the callback. The callback runs **before** the provider applies
its own composite mappings and `ApplicationName`, so those two win over caller
configuration.

What that does and does not guarantee:

- **Defended.** `ApplicationName` (re-applied after the callback, so a shared
  host config helper that sets it to the application's name cannot silently
  repoint the task hub) and the five composite mappings.
- **Not defended.** Every other connection-string key. A callback setting
  `Database`, `Host`, or `SearchPath` takes effect. `SearchPath` is the one to
  watch: the composite mappings are schema-qualified against `SchemaName`
  (1.2), so changing it shifts object resolution while the mappings stay put.

Treat `ConfigureDataSource` as a **trusted** extension point, not a sandbox. It
runs as the host application, which already owns the connection string — so
this is a boundary of intent, not of privilege.

Pool ownership stays with the provider, so disposal semantics are unchanged.

> Status: available since 1.1.0 (issue #7). Not available in 1.0.4 or earlier.

### 4.3 Share the application's pool (advanced, opt-in)

Only when connection budget genuinely requires one pool, and only with these
conditions met:

1. The data source registers all five composite mappings for the **same**
   `SchemaName` the service uses.
2. The data source's `ApplicationName` equals the service's `TaskHubName`
   exactly — which means the application's own connections are also tagged with
   the task hub name, not the application name.
3. The schema already exists before the data source opens its first connection,
   or `ReloadTypesAsync()` is called after deployment.
4. The caller disposes the data source; the provider must not.

Condition 2 is the one that usually disqualifies this pattern: it forces the
whole application to adopt the task hub name as its `application_name`, losing
application-level connection attribution in `pg_stat_activity`.

Condition 1 additionally requires the composite CLR types to be public — they
are still `internal` as of 1.1.0 (see section 2), so this pattern is **not
expressible today** and would require exporting the type mappings as public API.

> Status: not implemented, and not recommended. Documented so the trade-off is
> explicit rather than rediscovered.

---

## 5. Verifying your configuration

### Confirm the effective task hub

```sql
-- Run on a connection from the pool the provider uses.
SELECT dt.current_task_hub();
```

It must equal your `TaskHubName`. If it returns `'default'`, your application
name, or a database user name, task hub isolation is not working as intended
(see 3.3).

### Confirm which application names are connecting

```sql
SELECT application_name, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY application_name
ORDER BY 2 DESC;
```

### Confirm the composite types exist in the configured schema

```sql
SELECT t.typname
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'dt'  -- your SchemaName
  AND t.typname IN ('message_id', 'history_event',
                    'orchestration_event', 'task_event', 'task_result')
ORDER BY t.typname;
```

Five rows expected. Fewer means `logic.postgresql.sql` did not complete (the
composites are created there, not in the schema script — see 1.2), and
checkpoints will fail per 3.2.

### Confirm connection count per pool

```sql
SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();
```

Compare against `SHOW max_connections`. Two pools (application plus provider) is
the expected baseline with patterns 4.1 and 4.2.

---

## References

| Concern | Location |
| --- | --- |
| Data source construction and mappings | `PostgreSqlOrchestrationService.cs:77-117` |
| `ApplicationName` / `TaskHubName` propagation | `PostgreSqlOrchestrationService.cs:77-81`, `:98-101` |
| Task hub resolution | `Scripts/schema.postgresql.sql:200-232` |
| Pool disposal | `PostgreSqlOrchestrationService.cs:188`, `:1888-1892` |
| Schema deploy on a raw connection | `PostgreSqlOrchestrationService.cs:1540-1547` |
| Composite CLR types | `PostgreSqlTypes.cs` |
| Settings surface | `PostgreSqlOrchestrationServiceSettings.cs` |
| Upstream SQL Server equivalent (none) | `durabletask-mssql/src/DurableTask.SqlServer/SqlOrchestrationServiceSettings.cs:223` (`CreateConnection()`); `LoggerFactory` at `:211` |
