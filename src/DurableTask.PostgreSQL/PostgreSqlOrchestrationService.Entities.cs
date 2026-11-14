using DurableTask.Core;
using DurableTask.Core.Entities;
using Npgsql;

namespace DurableTask.PostgreSQL;

public sealed partial class PostgreSqlOrchestrationService
{
    /// <summary>Capabilities and dispatch limits for durable entities.</summary>
    public EntityBackendProperties EntityBackendProperties => new()
    {
        EntityMessageReorderWindow = TimeSpan.FromMinutes(_settings.EntityMessageReorderWindowInMinutes),
        MaxEntityOperationBatchSize = _settings.MaxEntityOperationBatchSize,
        MaxConcurrentTaskEntityWorkItems = _settings.MaxConcurrentTaskEntityWorkItems,
        SupportsImplicitEntityDeletion = false,
        MaximumSignalDelayTime = TimeSpan.MaxValue,
        UseSeparateQueueForEntityWorkItems = _settings.UseSeparateQueueForEntityWorkItems,
    };

    /// <summary>Entity metadata queries and maintenance operations.</summary>
    public EntityBackendQueries EntityBackendQueries => new EntityPostgreSqlBackendQueries(this);

    /// <inheritdoc />
    public async Task<TaskOrchestrationWorkItem> LockNextOrchestrationWorkItemAsync(
        TimeSpan receiveTimeout, CancellationToken cancellationToken)
        => (await LockNextWorkItemAsync(receiveTimeout, cancellationToken, 0).ConfigureAwait(false))!;

    /// <inheritdoc />
    public async Task<TaskOrchestrationWorkItem> LockNextEntityWorkItemAsync(
        TimeSpan receiveTimeout, CancellationToken cancellationToken)
        => (await LockNextWorkItemAsync(receiveTimeout, cancellationToken, 1).ConfigureAwait(false))!;

    // A metadata query is only a candidate selection. Revalidate under the same
    // instance-row lock used by signal/checkpoint producers before removing data.
    internal async Task<int> TryPurgeEmptyEntityAsync(string instanceId, string executionId,
        DateTime lastUpdatedTime, DateTime cutoff, CancellationToken cancellation)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellation).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(
            System.Data.IsolationLevel.ReadCommitted, cancellation).ConfigureAwait(false);
        await using var query = new NpgsqlCommand($"""
            SELECT 1
            FROM {_settings.SchemaName}.instances i
            WHERE i.task_hub = {_settings.SchemaName}.current_task_hub() AND i.instance_id = $1
              AND i.execution_id = $2 AND i.last_updated_time = $3 AND i.last_updated_time < $4
              AND (i.lock_expiration IS NULL OR i.lock_expiration <= NOW())
            FOR UPDATE OF i SKIP LOCKED
            """, connection, transaction);
        query.Parameters.AddWithValue(instanceId);
        query.Parameters.AddWithValue(executionId);
        query.Parameters.AddWithValue(lastUpdatedTime);
        query.Parameters.AddWithValue(cutoff);
        if (await query.ExecuteScalarAsync(cancellation).ConfigureAwait(false) == null) return 0;

        // Use a fresh READ COMMITTED statement snapshot after taking the row lock.
        // A producer may have committed between the first snapshot and lock acquisition.
        // Producers now hold this same row lock through their event insertion, so no new
        // work can appear after this check until cleanup commits or rolls back.
        await using var eligibility = new NpgsqlCommand($"""
            SELECT p.text::text
            FROM {_settings.SchemaName}.instances i
            JOIN {_settings.SchemaName}.payloads p ON p.task_hub = i.task_hub
              AND p.instance_id = i.instance_id AND p.payload_id = i.custom_status_payload_id
            WHERE i.task_hub = {_settings.SchemaName}.current_task_hub() AND i.instance_id = $1
              AND NOT EXISTS (SELECT 1 FROM {_settings.SchemaName}.new_events e
                  WHERE e.task_hub = i.task_hub AND e.instance_id = i.instance_id)
            """, connection, transaction);
        eligibility.Parameters.AddWithValue(instanceId);
        var serializedStatus = await eligibility.ExecuteScalarAsync(cancellation).ConfigureAwait(false) as string;
        if (serializedStatus == null) return 0;
        var status = ClientEntityHelpers.GetEntityStatus(serializedStatus);
        if (status == null || status.EntityExists || status.LockedBy != null || status.BacklogQueueSize != 0) return 0;

        await using var purge = new NpgsqlCommand(
            $"SELECT {_settings.SchemaName}.purge_instance_state_by_id($1)", connection, transaction);
        purge.Parameters.AddWithValue(new[] { instanceId });
        int removed = Convert.ToInt32(await purge.ExecuteScalarAsync(cancellation).ConfigureAwait(false),
            System.Globalization.CultureInfo.InvariantCulture);
        await transaction.CommitAsync(cancellation).ConfigureAwait(false);
        return removed;
    }
}
