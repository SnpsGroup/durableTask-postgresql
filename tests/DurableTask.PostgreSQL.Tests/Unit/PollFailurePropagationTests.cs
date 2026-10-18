using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

/// <summary>
/// Regression coverage for the interaction between the work-item polling loop and connection
/// failures. The lock methods poll internally until <c>receiveTimeout</c> elapses, so a failure
/// must escape the loop: if it were swallowed and reported as "no work available", a database
/// outage would be indistinguishable from an idle queue and the loop would retry (and re-log)
/// for the full timeout, starving the DTFx dispatcher's own fetch-exception backoff.
/// </summary>
public sealed class PollFailurePropagationTests
{
    // Port 1 is reserved/unused, so connecting fails fast without needing a live server.
    private const string UnreachableConnectionString =
        "Host=127.0.0.1;Port=1;Database=nonexistent;Username=none;Password=none;Timeout=2";

    private static PostgreSqlOrchestrationService CreateService() =>
        new(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = UnreachableConnectionString,
                TaskHubName = "poll-failure-tests",
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance);

    [Fact]
    public async Task LockNextOrchestration_ConnectionFailure_PropagatesWithoutExhaustingTimeout()
    {
        using PostgreSqlOrchestrationService service = CreateService();

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(() =>
            service.LockNextTaskOrchestrationWorkItemAsync(
                TimeSpan.FromSeconds(30),
                CancellationToken.None));
        stopwatch.Stop();

        // Swallowing the failure would make this retry for the full 30s receiveTimeout.
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(25),
            $"Expected the failure to surface promptly, but the call took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task LockNextActivity_ConnectionFailure_PropagatesWithoutExhaustingTimeout()
    {
        using PostgreSqlOrchestrationService service = CreateService();

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(() =>
            service.LockNextTaskActivityWorkItem(
                TimeSpan.FromSeconds(30),
                CancellationToken.None));
        stopwatch.Stop();

        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(25),
            $"Expected the failure to surface promptly, but the call took {stopwatch.Elapsed}.");
    }

    [Fact]
    public void Constructor_RejectsMinimumPollingIntervalAboveMaximum()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlOrchestrationService(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = UnreachableConnectionString,
                MinWorkItemPollingInterval = TimeSpan.FromSeconds(5),
                WorkItemPollingInterval = TimeSpan.FromSeconds(1),
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance));
    }

    [Fact]
    public void Constructor_RejectsNegativePollingInterval()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PostgreSqlOrchestrationService(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = UnreachableConnectionString,
                MinWorkItemPollingInterval = TimeSpan.FromMilliseconds(-1),
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance));
    }
}
