#pragma warning disable CA1812 // Avoid uninstantiated internal classes (used by Npgsql composite type mapping)
using System.Text.Json;

namespace DurableTask.PostgreSQL;

/// <summary>
/// Composite type mappings for PostgreSQL DurableTask provider.
/// These correspond to types defined in logic.postgresql.sql
/// </summary>
internal static class PostgreSqlTypes
{
    /// <summary>
    /// Result from dt.lock_next_orchestration function.
    /// Maps to dt.orchestration_lock_result composite type.
    /// </summary>
    internal sealed record OrchestrationLockResult
    {
        public string? InstanceId { get; init; }
        public string? ExecutionId { get; init; }
        public string? RuntimeStatus { get; init; }
        public string? ParentInstanceId { get; init; }
        public string? Version { get; init; }
        public JsonDocument? NewEvents { get; init; }
        public JsonDocument? History { get; init; }
    }

    /// <summary>
    /// Task lock result from dt.lock_next_task function.
    /// </summary>
    internal sealed record TaskLockResult
    {
        public long SequenceNumber { get; init; }
        public required string InstanceId { get; init; }
        public string? ExecutionId { get; init; }
        public string? Name { get; init; }
        public required string EventType { get; init; }
        public int TaskId { get; init; }
        public DateTimeOffset? VisibleTime { get; init; }
        public DateTimeOffset Timestamp { get; init; }
        public int DequeueCount { get; init; }
        public string? Version { get; init; }
        public JsonDocument? PayloadText { get; init; }
        public int WaitTimeSeconds { get; init; }
        public string? TraceContext { get; init; }
    }

    /// <summary>
    /// History event for bulk insert.
    /// Maps to dt.history_event composite type.
    /// </summary>
    internal sealed record HistoryEvent
    {
        public required string InstanceId { get; init; }
        public required string ExecutionId { get; init; }
        public long SequenceNumber { get; init; }
        public required string EventType { get; init; }
        public string? Name { get; init; }
        public string? RuntimeStatus { get; init; }
        public int? TaskId { get; init; }
        public DateTimeOffset Timestamp { get; init; }
        public bool IsPlayed { get; init; }
        public DateTimeOffset? VisibleTime { get; init; }
        public string? Reason { get; init; }
        public string? PayloadText { get; init; }
        public Guid? PayloadId { get; init; }
        public string? ParentInstanceId { get; init; }
        public string? Version { get; init; }
        public string? TraceContext { get; init; }
    }

    /// <summary>
    /// Orchestration event for bulk insert.
    /// Maps to dt.orchestration_event composite type.
    /// </summary>
    internal sealed record OrchestrationEvent
    {
        public required string InstanceId { get; init; }
        public string? ExecutionId { get; init; }
        public required string EventType { get; init; }
        public string? Name { get; init; }
        public string? RuntimeStatus { get; init; }
        public int? TaskId { get; init; }
        public DateTimeOffset? VisibleTime { get; init; }
        public string? Reason { get; init; }
        public string? PayloadText { get; init; }
        public Guid? PayloadId { get; init; }
        public string? ParentInstanceId { get; init; }
        public string? Version { get; init; }
        public string? TraceContext { get; init; }
    }

    /// <summary>
    /// Task event for bulk insert.
    /// Maps to dt.task_event composite type.
    /// </summary>
    internal sealed record TaskEvent
    {
        public required string InstanceId { get; init; }
        public string? ExecutionId { get; init; }
        public string? Name { get; init; }
        public required string EventType { get; init; }
        public int TaskId { get; init; }
        public DateTimeOffset? VisibleTime { get; init; }
        public string? LockedBy { get; init; }
        public DateTimeOffset? LockExpiration { get; init; }
        public string? Reason { get; init; }
        public string? PayloadText { get; init; }
        public Guid? PayloadId { get; init; }
        public string? Version { get; init; }
        public string? TraceContext { get; init; }
    }

    /// <summary>
    /// Task result for task completion.
    /// Maps to dt.task_result composite type.
    /// </summary>
    internal sealed record TaskResult
    {
        public required string InstanceId { get; init; }
        public required string ExecutionId { get; init; }
        public string? Name { get; init; }
        public required string EventType { get; init; }
        public int TaskId { get; init; }
        public DateTimeOffset? VisibleTime { get; init; }
        public string? PayloadText { get; init; }
        public Guid? PayloadId { get; init; }
        public string? Reason { get; init; }
        public string? TraceContext { get; init; }
    }

    /// <summary>
    /// Message ID for event deletion.
    /// Maps to dt.message_id composite type.
    /// </summary>
    internal sealed record MessageId
    {
        public required string InstanceId { get; init; }
        public long SequenceNumber { get; init; }
    }
}
