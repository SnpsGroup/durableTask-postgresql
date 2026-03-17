# DurableTask.PostgreSQL

PostgreSQL provider for the Durable Task Framework (DTFx).

## Status: Alpha (Baseline Synced) ✅

This is a **work-in-progress** port of [DurableTask.SqlServer](https://github.com/microsoft/durabletask-mssql) to PostgreSQL 17+.

**Current Version**: 1.0.0-alpha  
**Baseline Upstream**: `microsoft/durabletask-mssql@d48539ea3e132cb192415f8f8f499990d0a3d0ba`  
**Last Updated**: 2026-03-03  
**Status**: ✅ core orchestration lifecycle implemented for end-to-end workflows

## What's Implemented (POC)

### ✅ Schema
- Complete database schema with 6 tables
- Indexes optimized for PostgreSQL (partial indexes, INCLUDE columns)
- JSONB payload storage for better querying
- 3 utility functions (CurrentTaskHub, GetScaleMetric, GetScaleRecommendation)
- Denormalized view (v_instances)

### ✅ Core Procedures (8/20) - **END-TO-END WORKFLOWS SUPPORTED**
1. **`create_instance`** - Create orchestration with deduplication ✅
2. **`lock_next_orchestration`** - Dequeue orchestration work (CRITICAL) ✅
3. **`lock_next_task`** - Dequeue activity task work (CRITICAL) ✅
4. **`query_single_orchestration`** - Query instance state ✅
5. **`checkpoint_orchestration`** - **Atomic state transition (CRITICAL)** ✅
6. **`complete_tasks`** - Mark tasks completed ✅
7. **`renew_orchestration_locks`** - Heartbeat for orchestrations ✅
8. **`renew_task_locks`** - Heartbeat for tasks ✅

### ⚠️ Remaining Hardening
- Expand parity tests against latest upstream SQL Server provider behavior
- Finalize open-source metadata (`RepositoryUrl`, release governance, changelog policy)
- Add weekly upstream sync review (see `references/monitor-durabletask-mssql.ps1`)

## Key Design Decisions

### 1. JSONB for Payloads
**Changed**: `VARCHAR(MAX)` → `JSONB`

**Rationale**:
- Validates JSON on insert
- Enables querying nested fields
- GIN indexes for fast searches
- PostgreSQL native type

### 2. SKIP LOCKED for Concurrency
**Changed**: `WITH (READPAST)` → `FOR UPDATE SKIP LOCKED`

**Rationale**:
- PostgreSQL 9.5+ feature
- Prevents deadlocks in high-concurrency dequeue
- Same semantics as SQL Server READPAST

### 3. Composite Return Types
**Changed**: Multiple result sets → Single composite type

**Rationale**:
- PostgreSQL doesn't support multiple result sets directly
- Composite types are type-safe
- Alternative: OUT parameters or separate calls

**Example**:
```sql
-- SQL Server (3 result sets)
SELECT ... -- Result #1: Events
SELECT ... -- Result #2: Instance
SELECT ... -- Result #3: History

-- PostgreSQL (composite type)
RETURNS orchestration_lock_result AS (
    new_events JSONB,
    instance_id VARCHAR(100),
    history JSONB
)
```

### 4. Table Access Order (Deadlock Prevention)
Consistent ordering documented in comments:
- CreateInstance: Payloads → Instances → NewEvents
- LockNextOrchestration: Instances → NewEvents → Payloads → History
- LockNextTask: NewTasks → Payloads

## Installation

### 1. Install Package

**Option A: From Local NuGet Feed (Development)**
```bash
# Configure local feed (one-time setup)
cat > nuget.config << EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="LocalPackages" value="D:\Code\LocalPackage" />
  </packageSources>
</configuration>
EOF

# Install package
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha
```

See [LOCAL-NUGET.md](LOCAL-NUGET.md) for detailed setup instructions.

**Option B: From Project Reference**
```bash
dotnet add reference ../DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj
```

### 2. Deploy Schema
```bash
psql -U janus -d janus_sga -f Scripts/schema.postgresql.sql
```

### 3. Deploy Logic
```bash
psql -U janus -d janus_sga -f Scripts/logic.postgresql.sql
```

### 4. Verify
```sql
SELECT * FROM dt.versions;
-- Should show: 0.1.0-poc

SELECT dt.current_task_hub();
-- Should return task hub name
```

## Usage Examples

### Create Instance
```sql
SELECT dt.create_instance(
    p_name := 'MyOrchestration',
    p_version := 'v1',
    p_input_text := '{"key": "value"}'
);
-- Returns: instance_id (UUID)
```

### Lock Next Orchestration
```sql
SELECT * FROM dt.lock_next_orchestration(
    p_batch_size := 10,
    p_locked_by := 'worker-001',
    p_lock_expiration := NOW() + INTERVAL '5 minutes'
);
-- Returns: composite result with events + history
```

### Lock Next Task
```sql
SELECT * FROM dt.lock_next_task(
    p_locked_by := 'worker-001',
    p_lock_expiration := NOW() + INTERVAL '5 minutes'
);
-- Returns: table with task details
```

### Query Instance
```sql
SELECT * FROM dt.query_single_orchestration('my-instance-id');
-- Returns: instance details with payloads
```

## Testing

### Manual Testing
```sql
-- Create test instance
SELECT dt.create_instance(
    p_name := 'TestWorkflow',
    p_input_text := '{"test": true}'
) AS instance_id \gset

-- Query it back
SELECT * FROM dt.query_single_orchestration(:'instance_id');

-- Try to dequeue
SELECT * FROM dt.lock_next_orchestration(10, 'test-worker', NOW() + INTERVAL '5 minutes');
```

### Integration Tests
See: `tests/DurableTask.PostgreSQL.Tests/`
(To be created)

## Performance Considerations

### Indexes
- Partial indexes on `runtime_status` (only active instances)
- INCLUDE columns to avoid index-only scans
- GIN indexes for JSONB payloads (optional)

### Connection Pooling
Use Npgsql connection pooling:
```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);
// Reuse dataSource across requests
```

### Lock Lease Duration
Recommended: 5 minutes for orchestrations, 2 minutes for tasks

## Conversion Notes

### From SQL Server
Key differences compared to the SQL Server provider:

| SQL Server | PostgreSQL |
|------------|------------|
| `NEWID()` | `gen_random_uuid()` |
| `GETUTCDATE()` | `NOW()` |
| `STRING_SPLIT()` | `string_to_array()` |
| `@@ROWCOUNT` | `GET DIAGNOSTICS ... = ROW_COUNT` |
| `WITH (READPAST)` | `FOR UPDATE SKIP LOCKED` |

## Next Steps (Roadmap)

**Phase 2**: Implement remaining P0 procedures
- [ ] `_CheckpointOrchestration` (most complex)
- [ ] `_CompleteTasks`
- [ ] `_RenewOrchestrationLocks`
- [ ] `_RenewTaskLocks`

**Phase 3**: C# Provider
- [ ] `PostgreSqlOrchestrationService` class
- [ ] Implement `IOrchestrationService` interface
- [ ] Connection management with Npgsql

**Phase 4**: Testing
- [ ] Unit tests for each procedure
- [ ] Integration tests with Testcontainers
- [ ] Concurrency stress tests
- [ ] Performance benchmarks

**Phase 5**: Production Readiness
- [ ] Complete all 20 procedures
- [ ] Migration guide from SQL Server
- [ ] Monitoring and observability
- [ ] Open-source publication

## Contributing

This project is an open-source PostgreSQL provider for the Durable Task Framework.

## References

- [DurableTask Framework](https://github.com/Azure/durabletask)
- [DurableTask.SqlServer](https://github.com/microsoft/durabletask-mssql)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/17/)
- [Npgsql Provider](https://www.npgsql.org/)

## License

MIT License (same as upstream DurableTask.SqlServer)
