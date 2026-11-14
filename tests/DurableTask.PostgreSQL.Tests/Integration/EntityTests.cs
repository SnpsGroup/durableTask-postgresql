using DurableTask.Core;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;
using static DurableTask.Core.Entities.EntityBackendQueries;

namespace DurableTask.PostgreSQL.Tests.Integration;

[Collection("integration")]
public sealed class EntityTests
{
    [Fact]
    public async Task Cleanup_RevalidatesMetadataAndPreservesLockedOrQueuedEntities()
    {
        Assert.True(await IsDatabaseAvailableAsync(ConnectionString));
        using var service = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = ConnectionString, TaskHubName = "EntityClean" + Guid.NewGuid().ToString("N"),
            AutoDeploySchema = true, UseSeparateQueueForEntityWorkItems = true,
            EntityMessageReorderWindowInMinutes = 0,
        }, NullLogger<PostgreSqlOrchestrationService>.Instance);
        await service.CreateAsync();
        var entity = new EntityId(nameof(CounterEntity), Guid.NewGuid().ToString("N"));
        // Seed a completed empty scheduler candidate. No worker is active, so
        // dispatcher locks and future queued messages can be tested deterministically.
        await using var connection = await service.DataSource.OpenConnectionAsync();
        await using (var seed = new NpgsqlCommand("""
            WITH status AS (
                INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text)
                VALUES (dt.current_task_hub(), $1, gen_random_uuid(), '{}'::jsonb)
                RETURNING payload_id)
            INSERT INTO dt.instances (task_hub, instance_id, name, version, runtime_status,
                custom_status_payload_id, last_updated_time)
            SELECT dt.current_task_hub(), $1, 'CounterEntity', '', 'Running', payload_id, NOW() - INTERVAL '1 hour'
            FROM status
            """, connection))
        {
            seed.Parameters.AddWithValue(entity.ToString());
            await seed.ExecuteNonQueryAsync();
        }
        var stale = (await service.GetOrchestrationStateAsync(entity.ToString(), (string?)null))!;
        var cutoff = DateTime.UtcNow;

        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await using var acquire = new NpgsqlCommand(
                "SELECT 1 FROM dt.instances WHERE task_hub=dt.current_task_hub() AND instance_id=$1 FOR UPDATE", connection, transaction);
            acquire.Parameters.AddWithValue(entity.ToString());
            await acquire.ExecuteScalarAsync();
            Assert.Equal(0, await service.TryPurgeEmptyEntityAsync(entity.ToString(), stale.OrchestrationInstance.ExecutionId,
                stale.LastUpdatedTime, cutoff, default));
            await transaction.CommitAsync();
        }

        // An unexpired dispatcher lease must also prevent cleanup after its SQL
        // row lock has been released.
        await using (var lease = new NpgsqlCommand(
            "UPDATE dt.instances SET lock_expiration=NOW()+INTERVAL '1 hour' WHERE task_hub=dt.current_task_hub() AND instance_id=$1", connection))
        {
            lease.Parameters.AddWithValue(entity.ToString());
            await lease.ExecuteNonQueryAsync();
        }
        Assert.Equal(0, await service.TryPurgeEmptyEntityAsync(entity.ToString(), stale.OrchestrationInstance.ExecutionId,
            stale.LastUpdatedTime, cutoff, default));
        await using (var release = new NpgsqlCommand(
            "UPDATE dt.instances SET lock_expiration=NULL WHERE task_hub=dt.current_task_hub() AND instance_id=$1", connection))
        {
            release.Parameters.AddWithValue(entity.ToString());
            await release.ExecuteNonQueryAsync();
        }
        // A stale execution token may never remove a newer scheduler execution.
        Assert.Equal(0, await service.TryPurgeEmptyEntityAsync(entity.ToString(), "old-execution",
            stale.LastUpdatedTime, cutoff, default));
        await using (var modify = new NpgsqlCommand(
            "UPDATE dt.instances SET last_updated_time=NOW()-INTERVAL '30 minutes' WHERE task_hub=dt.current_task_hub() AND instance_id=$1", connection))
        {
            modify.Parameters.AddWithValue(entity.ToString());
            await modify.ExecuteNonQueryAsync();
        }
        Assert.Equal(0, await service.TryPurgeEmptyEntityAsync(entity.ToString(), stale.OrchestrationInstance.ExecutionId,
            stale.LastUpdatedTime, cutoff, default));
        await using (var restore = new NpgsqlCommand(
            "UPDATE dt.instances SET last_updated_time=$2 WHERE task_hub=dt.current_task_hub() AND instance_id=$1", connection))
        {
            restore.Parameters.AddWithValue(entity.ToString());
            restore.Parameters.AddWithValue(stale.LastUpdatedTime);
            await restore.ExecuteNonQueryAsync();
        }

        var due = DateTime.UtcNow.AddSeconds(2);
        await service.SendTaskOrchestrationMessageAsync(ClientEntityHelpers.EmitOperationSignal(
            new OrchestrationInstance { InstanceId = entity.ToString() }, Guid.NewGuid(), "add", "1", (due, due)).AsTaskMessage());
        Assert.Equal(0, await service.TryPurgeEmptyEntityAsync(entity.ToString(), stale.OrchestrationInstance.ExecutionId,
            stale.LastUpdatedTime, cutoff, default));
        Assert.Equal(0, (await service.EntityBackendQueries.CleanEntityStorageAsync(
            new CleanEntityStorageRequest { RemoveEmptyEntities = true })).EmptyEntitiesRemoved);

        var worker = new TaskHubWorker(service, NullLoggerFactory.Instance).AddTaskEntities(typeof(CounterEntity));
        await worker.StartAsync();
        try
        {
            await WaitForStateAsync(service, entity, "1");
        }
        finally
        {
            await worker.StopAsync(isForced: true);
        }
    }

    [Fact]
    public async Task SignalsCallsQueriesAndCleanup_RunThroughEntityDispatcher()
    {
        Assert.True(await IsDatabaseAvailableAsync(ConnectionString), "PostgreSQL is required for entity integration coverage.");
        using var service = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = ConnectionString, TaskHubName = "Entities" + Guid.NewGuid().ToString("N"), AutoDeploySchema = true,
            UseSeparateQueueForEntityWorkItems = true, EntityMessageReorderWindowInMinutes = 0,
            MaxEntityOperationBatchSize = 2, MaxConcurrentTaskEntityWorkItems = 4,
        }, NullLogger<PostgreSqlOrchestrationService>.Instance);
        await service.CreateAsync();
        var worker = new TaskHubWorker(service, NullLoggerFactory.Instance)
            .AddTaskEntities(typeof(CounterEntity))
            .AddTaskOrchestrations(typeof(CallCounter), typeof(LockCounter));
        var id = new EntityId(nameof(CounterEntity), Guid.NewGuid().ToString("N"));
        await worker.StartAsync();
        try
        {
            Assert.Null(await service.EntityBackendQueries.GetEntityAsync(id));
            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => SignalAsync(service, id, "add", "1")));
            await WaitForStateAsync(service, id, "8");

            var client = new TaskHubClient(service);
            var caller = await client.CreateOrchestrationInstanceAsync(typeof(CallCounter), id.ToString());
            var completed = await client.WaitForOrchestrationAsync(caller, TimeSpan.FromSeconds(30));
            Assert.Equal(OrchestrationStatus.Completed, completed.OrchestrationStatus);
            Assert.Equal("10", completed.Output);
            await WaitForStateAsync(service, id, "10");

            // LastModified is intentionally later than creation: this must not be
            // implemented as a CreatedTime predicate (the upstream shortcut).
            var instanceState = await service.GetOrchestrationStateAsync(id.ToString(), (string?)null);
            var metadata = await service.EntityBackendQueries.GetEntityAsync(id, includeState: true);
            Assert.NotNull(metadata);
            Assert.Equal(instanceState!.LastUpdatedTime, metadata.Value.LastModifiedTime);
            var entities = new List<EntityMetadata>();
            string? continuation = null;
            do
            {
                var page = await service.EntityBackendQueries.QueryEntitiesAsync(new EntityQuery
                {
                    InstanceIdStartsWith = id.ToString(), IncludeState = true, PageSize = 1,
                    LastModifiedFrom = instanceState.CreatedTime.AddTicks(1), ContinuationToken = continuation,
                }, default);
                entities.AddRange(page.Results);
                continuation = page.ContinuationToken;
            } while (continuation != null);
            Assert.Single(entities);
            Assert.Equal("10", entities[0].SerializedState);

            await SignalAsync(service, id, "delete", null);
            await WaitForStateAsync(service, id, null);
            Assert.Null(await service.EntityBackendQueries.GetEntityAsync(id));
            var cleanup = await service.EntityBackendQueries.CleanEntityStorageAsync(new CleanEntityStorageRequest { RemoveEmptyEntities = true });
            Assert.True(cleanup.EmptyEntitiesRemoved >= 1);
            Assert.Null(await service.GetOrchestrationStateAsync(id.ToString(), (string?)null));

            // Exercise implicit creation through checkpoint-emitted entity calls,
            // rather than pre-creating every entity with a client signal.
            var fresh = new EntityId(nameof(CounterEntity), Guid.NewGuid().ToString("N"));
            var freshCaller = await client.CreateOrchestrationInstanceAsync(typeof(CallCounter), fresh.ToString());
            var freshCompleted = await client.WaitForOrchestrationAsync(freshCaller, TimeSpan.FromSeconds(30));
            Assert.Equal(OrchestrationStatus.Completed, freshCompleted.OrchestrationStatus);
            Assert.Equal("2", freshCompleted.Output);
            await WaitForStateAsync(service, fresh, "2");

            // A completed owner that neglected to release its lock must be
            // repairable without discarding the entity's application state.
            var locker = await client.CreateOrchestrationInstanceAsync(typeof(LockCounter), fresh.ToString());
            Assert.Equal(OrchestrationStatus.Completed,
                (await client.WaitForOrchestrationAsync(locker, TimeSpan.FromSeconds(30))).OrchestrationStatus);
            Assert.Equal(locker.InstanceId, (await service.EntityBackendQueries.GetEntityAsync(fresh))!.Value.LockedBy);
            var unlocked = await service.EntityBackendQueries.CleanEntityStorageAsync(new CleanEntityStorageRequest { ReleaseOrphanedLocks = true });
            Assert.Equal(1, unlocked.OrphanedLocksReleased);
            for (int attempt = 0; attempt < 150; attempt++)
            {
                if ((await service.EntityBackendQueries.GetEntityAsync(fresh))!.Value.LockedBy == null) break;
                await Task.Delay(100);
            }
            var unlockedEntity = await service.EntityBackendQueries.GetEntityAsync(fresh, includeState: true);
            Assert.Null(unlockedEntity!.Value.LockedBy);
            Assert.Equal("2", unlockedEntity.Value.SerializedState);

            var delayed = new EntityId(nameof(CounterEntity), Guid.NewGuid().ToString("N"));
            var due = DateTime.UtcNow.AddSeconds(2);
            await service.SendTaskOrchestrationMessageAsync(ClientEntityHelpers.EmitOperationSignal(
                new OrchestrationInstance { InstanceId = delayed.ToString() }, Guid.NewGuid(), "add", "1", (due, due)).AsTaskMessage());
            Assert.Null(await service.EntityBackendQueries.GetEntityAsync(delayed));
            await WaitForStateAsync(service, delayed, "1");
            Assert.True((await service.EntityBackendQueries.GetEntityAsync(delayed))!.Value.LastModifiedTime >= due,
                "Scheduled signal was executed before its due time.");
        }
        finally
        {
            await worker.StopAsync(isForced: true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DispatchFilters_KeepEntitiesAndOrchestrationsInTheirQueues(bool separate)
    {
        Assert.True(await IsDatabaseAvailableAsync(ConnectionString));
        using var service = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = ConnectionString, TaskHubName = "EntityQueue" + separate,
            AutoDeploySchema = true, UseSeparateQueueForEntityWorkItems = separate,
        }, NullLogger<PostgreSqlOrchestrationService>.Instance);
        await service.CreateAsync();
        var entity = new EntityId(nameof(CounterEntity), Guid.NewGuid().ToString("N"));
        await SignalAsync(service, entity, "add", "1");
        var client = new TaskHubClient(service);
        var orchestration = await client.CreateOrchestrationInstanceAsync(typeof(CallCounter), entity.ToString());
        var ordinary = await service.LockNextOrchestrationWorkItemAsync(TimeSpan.Zero, default);
        Assert.Equal(orchestration.InstanceId, ordinary.InstanceId);
        var entityWork = await service.LockNextTaskOrchestrationWorkItemAsync(TimeSpan.Zero, default);
        if (separate)
        {
            Assert.Null(entityWork);
            entityWork = await service.LockNextEntityWorkItemAsync(TimeSpan.Zero, default);
        }
        Assert.NotNull(entityWork);
        Assert.Equal(entity.ToString(), entityWork.InstanceId);
        Assert.NotNull(entityWork.NewMessages.FirstOrDefault(m => m.Event is DurableTask.Core.History.ExecutionStartedEvent));
        await service.AbandonTaskOrchestrationWorkItemAsync(ordinary);
        await service.AbandonTaskOrchestrationWorkItemAsync(entityWork);
        await service.PurgeInstanceStateAsync(orchestration.InstanceId);
        await service.PurgeInstanceStateAsync(entity.ToString());
    }

    private static Task SignalAsync(PostgreSqlOrchestrationService service, EntityId id, string operation, string? input)
        => service.SendTaskOrchestrationMessageAsync(ClientEntityHelpers.EmitOperationSignal(
            new OrchestrationInstance { InstanceId = id.ToString() }, Guid.NewGuid(), operation, input, null).AsTaskMessage());

    private static async Task WaitForStateAsync(PostgreSqlOrchestrationService service, EntityId id, string? state)
    {
        for (int attempt = 0; attempt < 150; attempt++)
        {
            var metadata = await service.EntityBackendQueries.GetEntityAsync(id, includeState: true, includeStateless: true);
            if (metadata.HasValue && metadata.Value.SerializedState == state) return;
            await Task.Delay(100);
        }
        var actual = await service.GetOrchestrationStateAsync(id.ToString(), (string?)null);
        Assert.Fail($"Entity did not reach state {state ?? "<deleted>"}; status {actual?.Status}, scheduler {actual?.Input}.");
    }

    public sealed class CounterEntity : TaskEntity
    {
        public override Task<EntityBatchResult> ExecuteOperationBatchAsync(EntityBatchRequest request)
        {
            int value = request.EntityState == null ? 0 : int.Parse(request.EntityState);
            string? state = request.EntityState;
            var results = new List<OperationResult>();
            foreach (var operation in request.Operations!)
            {
                if (operation.Operation == "delete") state = null;
                else
                {
                    value += int.Parse(operation.Input!);
                    state = value.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }
                results.Add(new OperationResult { Result = state });
            }
            return Task.FromResult(new EntityBatchResult { EntityState = state, Results = results, Actions = [] });
        }
    }

    public sealed class CallCounter : TaskOrchestration<int, string, Dictionary<string, string>, string>
    {
        private readonly TaskCompletionSource<int> _response = new();
        public override async Task<int> RunTask(OrchestrationContext context, string input)
        {
            context.SendEvent(new OrchestrationInstance { InstanceId = input }, "op", new
            {
                op = "add", input = "2", id = Guid.Parse("35d4f92b-02c3-4c80-8ebe-428a939a0001"),
                parent = context.OrchestrationInstance.InstanceId,
                parentExecution = context.OrchestrationInstance.ExecutionId,
            });
            return await _response.Task;
        }
        public override void OnEvent(OrchestrationContext context, string name, Dictionary<string, string> input)
        {
            _response.TrySetResult(int.Parse(input["result"]));
        }
    }

    public sealed class LockCounter : TaskOrchestration<int, string, Dictionary<string, string>, string>
    {
        private readonly TaskCompletionSource<int> _response = new();
        public override async Task<int> RunTask(OrchestrationContext context, string input)
        {
            context.SendEvent(new OrchestrationInstance { InstanceId = input }, "op", new
            {
                id = Guid.Parse("35d4f92b-02c3-4c80-8ebe-428a939a0002"), lockset = new[] { EntityId.FromString(input) },
                parent = context.OrchestrationInstance.InstanceId, parentExecution = context.OrchestrationInstance.ExecutionId,
            });
            return await _response.Task;
        }
        public override void OnEvent(OrchestrationContext context, string name, Dictionary<string, string> input)
            => _response.TrySetResult(0);
    }
}
