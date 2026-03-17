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
    /// Task hub name. If null, will use current database user.
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
    /// Work item polling interval.
    /// Default: 1 second
    /// </summary>
    public TimeSpan WorkItemPollingInterval { get; init; } = TimeSpan.FromSeconds(1);

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
