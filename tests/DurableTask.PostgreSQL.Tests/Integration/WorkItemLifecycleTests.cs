using System.Reflection;
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Tests for work item abandon/release logging behavior (GitHub Issue #2).
/// </summary>
[Collection("integration")]
public sealed class WorkItemLifecycleTests : IDisposable
{
    private readonly List<LogEntry> _logs;
    private readonly ILoggerFactory _loggerFactory;
    private readonly PostgreSqlOrchestrationService _service;
    private readonly NpgsqlDataSource _deadDataSource;

    public WorkItemLifecycleTests()
    {
        _logs = [];
        var builder = LoggerFactory.Create(b =>
        {
            b.SetMinimumLevel(LogLevel.Debug);
            b.AddProvider(new TestLoggerProvider(_logs));
        });
        _loggerFactory = builder;

        // Create a data source pointing to a non-existent host with minimal timeout.
        // Any attempt to open a connection will fail quickly.
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(
            "Host=127.0.0.1;Port=59999;Database=test;Username=test;Password=test;Timeout=1;CommandTimeout=1");
        _deadDataSource = dataSourceBuilder.Build();

        var settings = new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = "Host=127.0.0.1;Port=59999;Database=test;Username=test;Password=test;Timeout=1;CommandTimeout=1",
            TaskHubName = "TestHub",
        };

        var logger = _loggerFactory.CreateLogger<PostgreSqlOrchestrationService>();
        _service = new PostgreSqlOrchestrationService(settings, logger);

        // Replace the real data source with our dead one
        var dataSourceField = typeof(PostgreSqlOrchestrationService)
            .GetField("_dataSource", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(dataSourceField);
        dataSourceField.SetValue(_service, _deadDataSource);
    }

    public void Dispose()
    {
        _deadDataSource.Dispose();
        _loggerFactory.Dispose();
    }

    /// <summary>
    /// ReleaseTaskOrchestrationWorkItemAsync should NOT log at Warning level.
    /// It should log at Debug (which is invisible at default Information/Warning level).
    /// Verifies the fix for Issue #2: normal release was logging misleading "Abandoning" warning.
    /// </summary>
    [Fact]
    public async Task ReleaseTaskOrchestrationWorkItemAsync_LogsAtDebug_NotWarning()
    {
        // Arrange
        var workItem = CreateOrchestrationWorkItem("test-instance-1", "exec-1");

        // Act
        await _service.ReleaseTaskOrchestrationWorkItemAsync(workItem);

        // Assert: No Warning or Error logs should exist
        var warningsOrErrors = _logs
            .Where(l => l.LogLevel >= LogLevel.Warning)
            .ToList();
        Assert.Empty(warningsOrErrors);

        // Assert: No "Abandoning" message at any level
        var abandoningLogs = _logs
            .Where(l => l.Message.Contains("Abandoning", StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.Empty(abandoningLogs);
    }

    /// <summary>
    /// ReleaseTaskOrchestrationWorkItemAsync and AbandonTaskOrchestrationWorkItemAsync
    /// must NOT be the same codepath. Release must not log "Abandoning".
    /// </summary>
    [Fact]
    public async Task ReleaseDoesNotLogAbandoning()
    {
        // Arrange
        var workItem = CreateOrchestrationWorkItem("test-instance-release", "exec-release");

        // Act
        await _service.ReleaseTaskOrchestrationWorkItemAsync(workItem);

        // Assert: No "Abandoning" substring in any log
        Assert.DoesNotContain(_logs, l =>
            l.Message.Contains("Abandoning", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// AbandonTaskOrchestrationWorkItemAsync should log at Warning with instance ID
    /// and "(will retry)" before attempting DB operation.
    /// Even when the DB is unreachable, the diagnostic log must be emitted first.
    /// </summary>
    [Fact]
    public async Task AbandonTaskOrchestrationWorkItemAsync_LogsWarningWithRetry()
    {
        // Arrange
        var workItem = CreateOrchestrationWorkItem("test-instance-2", "exec-2");

        // Act & Assert: The DB call will throw, but the log must happen before that
        await Assert.ThrowsAnyAsync<Exception>(
            () => _service.AbandonTaskOrchestrationWorkItemAsync(workItem));

        // Assert: A Warning log with "Abandoning" and "(will retry)" was written
        var abandonWarning = _logs
            .FirstOrDefault(l => l.LogLevel == LogLevel.Warning
                                 && l.Message.Contains("Abandoning orchestration work item", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(abandonWarning);
        Assert.Contains("(will retry)", abandonWarning.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("test-instance-2", abandonWarning.Message);
    }

    /// <summary>
    /// AbandonTaskActivityWorkItem (sync) should log at Warning with "(will retry)".
    /// This is the non-async version with no DB operations.
    /// </summary>
    [Fact]
    public void AbandonTaskActivityWorkItem_LogsWarningWithRetry()
    {
        // Arrange
        var workItem = CreateActivityWorkItem("test-activity-id");

        // Act
        _service.AbandonTaskActivityWorkItem(workItem);

        // Assert: A Warning log with "Abandoning" and "(will retry)" was written
        var abandonWarning = _logs
            .FirstOrDefault(l => l.LogLevel == LogLevel.Warning
                                 && l.Message.Contains("Abandoning activity work item", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(abandonWarning);
        Assert.Contains("(will retry)", abandonWarning.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("test-activity-id", abandonWarning.Message);
    }

    /// <summary>
    /// AbandonTaskActivityWorkItemAsync should log at Warning with "(will retry)".
    /// Same pattern as the orchestration abandon: log before DB call.
    /// </summary>
    [Fact]
    public async Task AbandonTaskActivityWorkItemAsync_LogsWarningWithRetry()
    {
        // Arrange
        var workItem = CreateActivityWorkItem("test-activity-id-2");

        // Act & Assert: The DB call will throw, but the log must happen before that
        await Assert.ThrowsAnyAsync<Exception>(
            () => _service.AbandonTaskActivityWorkItemAsync(workItem));

        // Assert: A Warning log with "Abandoning" and "(will retry)" was written
        var abandonWarning = _logs
            .FirstOrDefault(l => l.LogLevel == LogLevel.Warning
                                 && l.Message.Contains("Abandoning activity work item", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(abandonWarning);
        Assert.Contains("(will retry)", abandonWarning.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("test-activity-id-2", abandonWarning.Message);
    }

    private static TaskOrchestrationWorkItem CreateOrchestrationWorkItem(string instanceId, string executionId)
    {
        return new TaskOrchestrationWorkItem
        {
            InstanceId = instanceId,
            LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
            NewMessages = [],
            OrchestrationRuntimeState = new OrchestrationRuntimeState(
            [
                new ExecutionStartedEvent(-1, null)
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = instanceId,
                        ExecutionId = executionId,
                    },
                },
            ]),
        };
    }

    private static TaskActivityWorkItem CreateActivityWorkItem(string id)
    {
        return new TaskActivityWorkItem
        {
            Id = id,
            TaskMessage = new TaskMessage
            {
                SequenceNumber = 42,
                Event = new TaskScheduledEvent(1)
                {
                    Name = "TestActivity",
                },
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "test-instance",
                    ExecutionId = "test-execution",
                },
            },
        };
    }
}
