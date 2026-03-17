# DurableTask.PostgreSQL - C# Provider Integration

## Overview

C# provider for DurableTask Framework using PostgreSQL 17+ as backend storage.

**Status**: Alpha - core operations implemented and baseline-synced with upstream commit tracking

---

## Quick Start

### 1. Install Package

**Option A: Install from local source (development)**
```bash
# Add project reference
dotnet add reference ../DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj
```

**Option B: Install from NuGet package (production)**
```bash
# Build and pack the package first
cd src/DurableTask.PostgreSQL
.\build-package.ps1 -Version "1.0.0-alpha"

# Install from local artifacts
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha --source ../../artifacts/packages
```

**Option C: Install from private NuGet feed**
```bash
dotnet add package DurableTask.PostgreSQL --version 1.0.0
```

### 2. Deploy Schema

```bash
psql -U janus -d janus_sga -f Scripts/schema.postgresql.sql
psql -U janus -d janus_sga -f Scripts/logic.postgresql.sql
```

### 3. Register in DI

```csharp
using DurableTask.PostgreSQL;

// In Program.cs or Startup.cs
services.AddDurableTaskPostgreSql(
    connectionString: "Host=localhost;Database=janus_sga;Username=janus;Password=***",
    configure: settings =>
    {
        settings.TaskHubName = "JanusHub";
        settings.MaxConcurrentOrchestrations = 10;
        settings.LockTimeout = TimeSpan.FromMinutes(5);
    });
```

### 4. Use with OrleansDtfIntegrationService

```csharp
// In OrleansDtfIntegrationService.cs
public class OrleansDtfIntegrationService
{
    private readonly IOrchestrationService _orchestrationService;
    private readonly TaskHubClient _taskHubClient;

    public OrleansDtfIntegrationService(
        IOrchestrationService orchestrationService,  // PostgreSqlOrchestrationService injected
        IOrchestrationServiceClient orchestrationServiceClient)
    {
        _orchestrationService = orchestrationService;

        _taskHubClient = new TaskHubClient(orchestrationServiceClient);
    }

    public async Task StartAsync()
    {
        await _orchestrationService.CreateAsync().ConfigureAwait(false);
        await _orchestrationService.StartAsync().ConfigureAwait(false);

        // Start TaskHubWorker
        var worker = new TaskHubWorker(_orchestrationService);
        worker.AddTaskOrchestrations(typeof(CorrelationWorkflow));
        // ... register activities
        await worker.StartAsync().ConfigureAwait(false);
    }
}
```

---

## Configuration Options

```csharp
public sealed class PostgreSqlOrchestrationServiceSettings
{
    // Required
    public required string ConnectionString { get; init; }

    // Optional
    public string? TaskHubName { get; init; }  // Default: current user
    public string SchemaName { get; init; } = "dt";
    public int MaxConcurrentOrchestrations { get; init; } = 10;
    public int MaxConcurrentActivities { get; init; } = 10;
    public TimeSpan LockRenewalInterval { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan LockTimeout { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan WorkItemPollingInterval { get; init; } = TimeSpan.FromSeconds(1);
    public bool AutoDeploySchema { get; init; } = false;
    public string WorkerId { get; init; } = $"{MachineName}-{ProcessId}";
}
```

---

## Implemented Operations

### ✅ Core (Working)
- `CreateTaskOrchestrationAsync` - Create new workflow instance
- `GetOrchestrationStateAsync` - Query instance state
- `LockNextTaskOrchestrationWorkItemAsync` - Dequeue orchestration work
- `LockNextTaskActivityWorkItem` - Dequeue activity work

### ✅ Implemented in Service
- `CompleteTaskOrchestrationWorkItemAsync`
- `CompleteTaskActivityWorkItemAsync`
- `RenewTaskOrchestrationWorkItemLockAsync`
- `SendTaskOrchestrationMessageAsync`
- `ForceTerminateTaskOrchestrationAsync`
- `PurgeOrchestrationHistoryAsync`
- `GetOrchestrationHistoryAsync`
- `WaitForOrchestrationAsync`

---

## Type Mappings

### PostgreSQL → C#

| PostgreSQL Type | C# Type | Notes |
|-----------------|---------|-------|
| `VARCHAR(100)` | `string` | Instance/Execution IDs |
| `TIMESTAMP WITH TIME ZONE` | `DateTimeOffset` | Always UTC |
| `JSONB` | `JsonDocument` / `string` | Payloads |
| `UUID` | `Guid` | Payload IDs |
| `BIGINT` | `long` | Sequence numbers |
| Composite types | Record classes | See `PostgreSqlTypes.cs` |

### DurableTask Events → PostgreSQL

| DurableTask Event | PostgreSQL Event Type |
|-------------------|----------------------|
| `ExecutionStartedEvent` | `ExecutionStarted` |
| `ExecutionCompletedEvent` | `ExecutionCompleted` |
| `TaskScheduledEvent` | `TaskScheduled` |
| `TaskCompletedEvent` | `TaskCompleted` |
| `TaskFailedEvent` | `TaskFailed` |
| `TimerCreatedEvent` | `TimerCreated` |
| `TimerFiredEvent` | `TimerFired` |
| `SubOrchestrationInstanceCreatedEvent` | `ExecutionStarted` (sub) |
| `EventRaisedEvent` | `EventRaised` |

---

## Known Limitations (Alpha)

1. **Checkpoint Not Fully Implemented**
   - `CompleteTaskOrchestrationWorkItemAsync` needs complex mapping
   - Requires converting `OrchestrationRuntimeState` → PostgreSQL types
   - Splitting messages into history/orchestration/task events

2. **History Parsing**
   - `LockNextTaskOrchestrationWorkItemAsync` returns JSON
   - Needs conversion to `HistoryEvent[]` for DTFx
   - Event deserialization logic pending

3. **No External Events**
   - `RaiseEvent` not implemented
   - Cannot send messages to running orchestrations

4. **No Purge/Cleanup**
   - Old instances accumulate
   - Manual cleanup required

5. **No Split-Brain Detection Yet**
   - PK violation handling not wired up in C#
   - PostgreSQL will throw exception correctly

---

## Next Steps to Complete

### Priority 1: Checkpoint Implementation

```csharp
// CompleteTaskOrchestrationWorkItemAsync needs:
1. Map OrchestrationRuntimeState → HistoryEvents[]
2. Map outboundMessages → OrchestrationEvents[]
3. Map timerMessages → TaskEvents[]
4. Call dt.checkpoint_orchestration() with arrays
5. Handle exceptions (split-brain, etc.)
```

### Priority 2: Complete Tasks

```csharp
// CompleteTaskActivityWorkItemAsync needs:
1. Map TaskMessage → TaskResult
2. Call dt.complete_tasks() with arrays
3. Handle race conditions
```

### Priority 3: History Deserialization

```csharp
// LockNextTaskOrchestrationWorkItemAsync needs:
1. Parse JSON history → HistoryEvent[]
2. Create TaskOrchestrationWorkItem correctly
3. Set locked timestamp for timeout tracking
```

---

## Testing

### Manual Test

```csharp
// Create instance
var client = serviceProvider.GetRequiredService<IOrchestrationServiceClient>();
var instance = await client.CreateTaskOrchestrationAsync(
    new TaskMessage
    {
        OrchestrationInstance = new OrchestrationInstance
        {
            InstanceId = "test-001"
        },
        Event = new ExecutionStartedEvent(1, "{ \"test\": true }")
        {
            Name = "TestWorkflow",
            Version = "v1"
        }
    });

// Query state
var state = await client.GetOrchestrationStateAsync("test-001", null);
Console.WriteLine($"Status: {state.OrchestrationStatus}");
```

### Integration Test

Run existing DurableTask tests against PostgreSQL backend.

---

## Performance Considerations

- **Connection Pooling**: Npgsql handles automatically via `NpgsqlDataSource`
- **Lock Contention**: `SKIP LOCKED` prevents blocking
- **JSONB Overhead**: Slightly slower than binary, but queryable
- **Batch Operations**: Arrays minimize round-trips

---

## Troubleshooting

### "Function does not exist"
- Schema not deployed
- Run `schema.postgresql.sql` and `logic.postgresql.sql`

### "Connection timeout"
- Check connection string
- Verify PostgreSQL is running
- Check firewall rules

### "Cannot convert JSON"
- Payload must be valid JSON
- Use `JsonSerializer.Serialize()` before passing

### "Unique violation on dt.history"
- Split-brain detected (2 workers processing same instance)
- Expected behavior - DTFx will retry on different instance

---

## Architecture Diagram

```
┌─────────────────────────────────────────────┐
│         DurableTask Framework               │
│  (TaskHubWorker + TaskHubClient)            │
└──────────────┬──────────────────────────────┘
               │
               │ IOrchestrationService
               │ IOrchestrationServiceClient
               ↓
┌─────────────────────────────────────────────┐
│   PostgreSqlOrchestrationService            │
│   - LockNext (orchestrations + tasks)       │
│   - CreateInstance                          │
│   - Checkpoint (TODO: full impl)            │
│   - CompleteTasks (TODO: full impl)         │
└──────────────┬──────────────────────────────┘
               │
               │ Npgsql (Connection Pool)
               ↓
┌─────────────────────────────────────────────┐
│         PostgreSQL 17+                      │
│   Schema: dt                                │
│   - create_instance()                       │
│   - lock_next_orchestration()               │
│   - lock_next_task()                        │
│   - checkpoint_orchestration()              │
│   - complete_tasks()                        │
│   - renew_*_locks()                         │
└─────────────────────────────────────────────┘
```

---

## References

- [DurableTask Framework](https://github.com/Azure/durabletask)
- [DurableTask.SqlServer (Reference)](https://github.com/microsoft/durabletask-mssql)
- [Npgsql Documentation](https://www.npgsql.org/doc/)
- [PostgreSQL 17 Docs](https://www.postgresql.org/docs/17/)

---

**Last Updated**: 2026-02-14
**Version**: 0.2.0-alpha
**Author**: Janus SGA Team
