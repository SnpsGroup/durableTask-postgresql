using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.PostgreSQL.Tests.Utils;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Regression coverage for GitHub Issue #13: an event delivered to an instance that already
/// reached a terminal state left the provider permanently stuck.
///
/// Two defects were in play on that path:
/// <list type="number">
/// <item>The discard ran a second command on the connection that still had the
/// <c>lock_next_orchestration</c> reader open, throwing <c>NpgsqlOperationInProgressException</c>.</item>
/// <item>The discard only cleared the lock and never deleted the events, so the instance kept
/// satisfying <c>lock_next_orchestration</c>'s "has pending events" join and was re-locked and
/// re-discarded on every poll, forever.</item>
/// </list>
/// Fixing only the first turns the crash into a silent infinite loop, so the assertions below
/// cover the queue being drained, not just the absence of an exception.
/// </summary>
[Collection("integration")]
public sealed class TerminalInstanceDiscardTests : IAsyncLifetime
{
    private const string TaskHubName = "TerminalDiscardHub";

    private TestService? _testService;
    private TaskHubWorker? _worker;
    private TaskHubClient? _client;
    private bool _isDatabaseAvailable;

    public async Task InitializeAsync()
    {
        _isDatabaseAvailable = await IsDatabaseAvailableAsync(ConnectionString);
        if (!_isDatabaseAvailable)
        {
            return;
        }

        _testService = new TestService(ConnectionString, taskHubName: TaskHubName);
        await _testService.InitializeAsync();

        _client = new TaskHubClient(_testService.ClientService);
        _worker = new TaskHubWorker(_testService.OrchestrationService, _testService.LoggerFactory)
            .AddTaskOrchestrations(new NamedOrchestrationCreator(nameof(NoOpOrchestration), string.Empty, new NoOpOrchestration()));
        await _worker.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_worker != null)
        {
            await _worker.StopAsync(isForced: true);
        }

        if (_testService != null)
        {
            await _testService.DisposeAsync();
        }
    }

    /// <summary>
    /// An external event raised against a completed instance must be discarded and its queue
    /// drained, leaving the instance unlocked and ineligible for the next poll.
    /// </summary>
    [Fact]
    public async Task EventForTerminalInstance_IsDiscarded_AndQueueDrained()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // Run to completion so the instance lands in a terminal status. This is the state that
        // makes the work item undeliverable: lock_next_orchestration still hands it out, but the
        // dispatcher cannot replay it.
        string instanceId = Guid.NewGuid().ToString();
        OrchestrationInstance instance = await _client!.CreateOrchestrationInstanceAsync(
            name: nameof(NoOpOrchestration), version: string.Empty, instanceId, input: null);

        OrchestrationState state = await _client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));
        Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);

        // Queue an event the completed instance can never consume. This mirrors the issue's
        // reproduction: a TimerFired still in flight when the orchestration completed.
        await _client.RaiseEventAsync(instance, "neverConsumed", "payload");
        Assert.True(
            await WaitForNewEventCountAsync(instanceId, expected: 1, TimeSpan.FromSeconds(10)),
            "the raised event should have been queued in new_events");

        // The worker polls continuously, so the discard happens on its own. Before the fix this
        // never reached zero: the events stayed queued and the instance was re-locked forever.
        Assert.True(
            await WaitForNewEventCountAsync(instanceId, expected: 0, TimeSpan.FromSeconds(30)),
            "the discard should have deleted the queued events for the terminal instance");

        // The lock must be released too, otherwise the instance stays pinned to a dead worker.
        Assert.Null(await GetLockedByAsync(instanceId));

        // The crash this issue was reported for: a second command on the connection that still
        // had the lock_next_orchestration reader open.
        Assert.DoesNotContain(
            _testService!.LogEntries,
            l => l.Message.Contains("NpgsqlOperationInProgressException", StringComparison.OrdinalIgnoreCase)
                 || (l.Exception?.GetType().Name.Contains("OperationInProgress", StringComparison.OrdinalIgnoreCase) ?? false));

        // The queue is drained, so the instance stops being handed out. A provider that merely
        // unlocked would keep logging this warning on every poll.
        int discardWarnings = CountDiscardWarnings(instanceId);
        await Task.Delay(TimeSpan.FromSeconds(5));
        Assert.Equal(discardWarnings, CountDiscardWarnings(instanceId));
    }

    /// <summary>
    /// The SQL function must be idempotent: discarding an instance with nothing queued is a
    /// no-op returning zero, not an error. The provider can reach this through a racing poll.
    /// </summary>
    [Fact]
    public async Task DiscardFunction_WithNoQueuedEvents_ReturnsZero()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT dt.discard_events_and_unlock_instance($1)", connection);
        cmd.Parameters.AddWithValue($"does-not-exist-{Guid.NewGuid()}");

        Assert.Equal(0, Convert.ToInt32(await cmd.ExecuteScalarAsync()));
    }

    private static async Task<bool> WaitForNewEventCountAsync(string instanceId, int expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow.Add(timeout);
        while (DateTime.UtcNow < deadline)
        {
            if (await GetNewEventCountAsync(instanceId) == expected)
            {
                return true;
            }

            await Task.Delay(250);
        }

        return await GetNewEventCountAsync(instanceId) == expected;
    }

    private static async Task<int> GetNewEventCountAsync(string instanceId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM dt.new_events WHERE task_hub = $1 AND instance_id = $2", connection);
        cmd.Parameters.AddWithValue(TaskHubName);
        cmd.Parameters.AddWithValue(instanceId);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private static async Task<string?> GetLockedByAsync(string instanceId)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT locked_by FROM dt.instances WHERE task_hub = $1 AND instance_id = $2", connection);
        cmd.Parameters.AddWithValue(TaskHubName);
        cmd.Parameters.AddWithValue(instanceId);

        return await cmd.ExecuteScalarAsync() as string;
    }

    private int CountDiscardWarnings(string instanceId) =>
        _testService!.LogEntries.Count(l =>
            l.Message.Contains("Discarding", StringComparison.OrdinalIgnoreCase)
            && l.Message.Contains(instanceId, StringComparison.Ordinal));
}
