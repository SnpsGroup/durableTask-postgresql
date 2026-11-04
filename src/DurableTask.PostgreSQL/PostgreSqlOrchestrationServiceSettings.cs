using Npgsql;

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
    /// Optional hook to configure the Npgsql data source the service builds, for Npgsql
    /// features that a connection string cannot reach: password providers (managed-identity
    /// or IAM token auth), client certificates, logging and tracing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Invoked once per service instance, during construction, after the connection string
    /// is applied and <em>before</em> the provider registers its composite type mappings and
    /// re-applies <see cref="TaskHubName"/> as the connection's <c>ApplicationName</c>. The
    /// provider's own requirements therefore take precedence over anything set here: a
    /// callback cannot remove the type mappings, and — when <see cref="TaskHubName"/> is set —
    /// cannot repoint the task hub. When <see cref="TaskHubName"/> is null or empty the
    /// provider has no name to re-apply, so a callback setting <c>ApplicationName</c> does
    /// then determine the task hub.
    /// </para>
    /// <para>
    /// Because <see cref="ConnectionString"/> is applied to the builder first, the connection
    /// string and the callback must not both supply the same credential. Npgsql throws
    /// <see cref="NotSupportedException"/> ("When registering a password provider, a password
    /// or password file may not be set") if the connection string carries <c>Password</c> or
    /// <c>Passfile</c> and the callback registers a password provider — omit the password from
    /// the connection string in that case.
    /// </para>
    /// <para>
    /// This is a trusted extension point, not a sandbox. It runs with the privileges of the
    /// host application, which already owns the connection string; settings the provider does
    /// not re-apply — notably <c>Database</c>, <c>Host</c> and <c>SearchPath</c> — are not
    /// defended and will take effect.
    /// </para>
    /// <para>
    /// Does not apply to schema deployment (<see cref="AutoDeploySchema"/>), which uses a raw
    /// connection built from <see cref="ConnectionString"/>. A deployment that needs
    /// callback-based authentication must deploy the schema out of band. See
    /// <see href="https://github.com/SnpsGroup/durableTask-postgresql/blob/main/src/DurableTask.PostgreSQL/DATASOURCE.md">DATASOURCE.md</see>
    /// sections 2 and 4.2.
    /// </para>
    /// </remarks>
    public Action<NpgsqlDataSourceBuilder>? ConfigureDataSource { get; init; }

    /// <summary>
    /// Task hub name. When set, it is propagated to PostgreSQL as the connection's
    /// ApplicationName and used as the task hub when TaskHubMode = '0' (the default).
    /// When null/empty, ApplicationName is left untouched and dt.current_task_hub() uses whatever
    /// application_name the connection carries, or 'default' if empty (or CURRENT_USER when TaskHubMode = '1').
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
