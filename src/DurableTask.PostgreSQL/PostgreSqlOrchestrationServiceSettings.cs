namespace DurableTask.PostgreSQL;

/// <summary>
/// Configuration settings for PostgreSqlOrchestrationService.
/// </summary>
public sealed class PostgreSqlOrchestrationServiceSettings
{
    /// <summary>
    /// PostgreSQL connection string.
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// Task hub name. When set, it is propagated to PostgreSQL as the connection's
    /// ApplicationName and used as the task hub when TaskHubMode = '0' (the default).
    /// When null/empty, the task hub falls back to the current database user
    /// (CURRENT_USER, i.e. TaskHubMode = '1').
    /// </summary>
    public string? TaskHubName { get; init; }

    /// <summary>
    /// Schema name for DurableTask tables.
    /// Default: "dt"
    /// </summary>
    public string SchemaName { get; init; } = "dt";

    /// <summary>
    /// Maximum number of work items to fetch per poll.
    /// Default: 10
    /// </summary>
    public int MaxConcurrentOrchestrations { get; init; } = 10;

    /// <summary>
    /// Maximum number of activity tasks to fetch per poll.
    /// Default: 10
    /// </summary>
    public int MaxConcurrentActivities { get; init; } = 10;

    /// <summary>
    /// Lock renewal interval (heartbeat).
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan LockRenewalInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Lock timeout duration.
    /// Default: 5 minutes
    /// </summary>
    public TimeSpan LockTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Upper bound for the work-item polling interval. When a dispatcher finds no work it
    /// backs off exponentially from <see cref="MinWorkItemPollingInterval"/> toward this value,
    /// so an idle task hub settles at one poll per interval instead of polling continuously.
    /// A dispatcher that finds work (or is signaled by a checkpoint that enqueued work) resets
    /// to the minimum immediately, so this does not add latency to a busy task hub.
    /// Default: 1 second
    /// </summary>
    public TimeSpan WorkItemPollingInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Lower bound for the work-item polling interval — the delay after the first poll that
    /// finds no work, and the interval a dispatcher returns to once work appears.
    /// Must not be greater than <see cref="WorkItemPollingInterval"/>.
    /// Default: 50 milliseconds
    /// </summary>
    public TimeSpan MinWorkItemPollingInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Increment applied to each successive empty poll while backing off from
    /// <see cref="MinWorkItemPollingInterval"/> to <see cref="WorkItemPollingInterval"/>.
    /// Default: 50 milliseconds
    /// </summary>
    public TimeSpan DeltaBackoffWorkItemPollingInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Enable automatic schema deployment on startup.
    /// Default: false (manual deployment recommended)
    /// </summary>
    public bool AutoDeploySchema { get; init; }

    /// <summary>
    /// Worker identifier for lock ownership.
    /// Default: Machine name + Process ID
    /// </summary>
    public string WorkerId { get; init; } = $"{Environment.MachineName}-{Environment.ProcessId}";
}
