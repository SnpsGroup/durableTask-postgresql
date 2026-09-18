using DurableTask.Core;
using DurableTask.Core.Query;
using DurableTask.PostgreSQL.Tests.Utils;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Integration tests for GitHub issue #8 (residue of #3): the <see cref="OrchestrationState"/>
/// timestamps must round-trip with <see cref="DateTimeKind.Utc"/> on every read path, including
/// for non-terminal instances whose <c>completed_time</c> column is NULL by design
/// (<c>logic.postgresql.sql</c>: <c>completed_time = CASE WHEN v_is_completed THEN NOW() ELSE NULL END</c>).
///
/// Before the fix these materialized as <c>default(DateTime)</c> — <see cref="DateTimeKind.Unspecified"/> —
/// which makes the DTFx v2 gRPC sidecar's <c>Timestamp.FromDateTime</c> throw
/// <see cref="ArgumentException"/> when Argus polls a suspended instance.
/// </summary>
[Collection("integration")]
public sealed class OrchestrationStateTimestampTests : IAsyncLifetime
{
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

        _testService = new TestService(ConnectionString, taskHubName: "StateTimestampTestsHub");
        await _testService.InitializeAsync();

        _client = new TaskHubClient(_testService.ClientService);
        _worker = new TaskHubWorker(_testService.OrchestrationService, _testService.LoggerFactory)
            .AddTaskOrchestrations(new NamedOrchestrationCreator(
                nameof(PendingEventOrchestration), string.Empty, new PendingEventOrchestration()));
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
    /// Asserts every timestamp on a materialized state carries <see cref="DateTimeKind.Utc"/>.
    /// This is exactly the precondition <c>Timestamp.FromDateTime</c> enforces in the sidecar.
    /// </summary>
    private static void AssertTimestampsAreUtc(OrchestrationState state, string readPath)
    {
        Assert.Equal(DateTimeKind.Utc, state.CreatedTime.Kind);
        Assert.Equal(DateTimeKind.Utc, state.LastUpdatedTime.Kind);

        // The load-bearing one: NULL in the database for any non-terminal instance.
        Assert.True(
            state.CompletedTime.Kind == DateTimeKind.Utc,
            $"{readPath}: CompletedTime.Kind was {state.CompletedTime.Kind}, expected Utc " +
            "(issue #8 — the NULL fallback used default(DateTime), which is Unspecified).");
    }

    [Fact]
    public async Task SuspendedInstance_TimestampsAreUtc_OnAllReadPaths()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // Start an orchestration that blocks on an external event, so it never completes and
        // completed_time stays NULL, then suspend it — the Argus scenario from issue #8
        // (polling GetInstance at a human-approval checkpoint).
        string instanceId = Guid.NewGuid().ToString();
        OrchestrationInstance instance = await _client!.CreateOrchestrationInstanceAsync(
            nameof(PendingEventOrchestration), string.Empty, instanceId, input: null);

        OrchestrationState? running = await WaitForStatusAsync(
            instanceId,
            s => s == OrchestrationStatus.Running || s == OrchestrationStatus.Pending);
        Assert.NotNull(running);

        await _client.SuspendInstanceAsync(instance, "issue #8 regression");

        OrchestrationState? suspended = await WaitForStatusAsync(
            instanceId, s => s == OrchestrationStatus.Suspended);
        Assert.NotNull(suspended);
        Assert.Equal(OrchestrationStatus.Suspended, suspended!.OrchestrationStatus);

        // Sanity-check the precondition this regression depends on: the instance really is
        // non-terminal, so completed_time is NULL and the fallback path is the one under test.
        Assert.Equal(default, suspended.CompletedTime);

        // Read path 1: GetOrchestrationStateAsync(instanceId, executionId)
        AssertTimestampsAreUtc(suspended, "GetOrchestrationStateAsync(instanceId, executionId)");

        // Read path 2: GetOrchestrationStateAsync(instanceId, allExecutions)
        IList<OrchestrationState> allExecutions = await _testService!.ClientService
            .GetOrchestrationStateAsync(instanceId, allExecutions: true);
        OrchestrationState listed = Assert.Single(allExecutions);
        AssertTimestampsAreUtc(listed, "GetOrchestrationStateAsync(instanceId, allExecutions)");

        // Read path 3: GetOrchestrationWithQueryAsync -> OrchestrationQueryResult
        var query = new OrchestrationQuery
        {
            PageSize = 100,
            FetchInputsAndOutputs = false,
            CreatedTimeFrom = DateTime.UtcNow.AddDays(-1),
            CreatedTimeTo = DateTime.UtcNow.AddDays(1),
            InstanceIdPrefix = instanceId,
        };

        OrchestrationQueryResult result = await _testService.ClientService
            .GetOrchestrationWithQueryAsync(query, CancellationToken.None);

        OrchestrationState queried = Assert.Single(
            result.OrchestrationState, r => r.OrchestrationInstance.InstanceId == instanceId);
        AssertTimestampsAreUtc(queried, "GetOrchestrationWithQueryAsync");
    }

    [Fact]
    public async Task CompletedInstance_TimestampsAreUtc()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // The terminal case: completed_time is non-NULL, so this covers the reader path rather
        // than the NULL fallback. Both must yield Utc.
        string instanceId = Guid.NewGuid().ToString();
        OrchestrationInstance instance = await _client!.CreateOrchestrationInstanceAsync(
            nameof(PendingEventOrchestration), string.Empty, instanceId, input: null);

        await WaitForStatusAsync(
            instanceId,
            s => s == OrchestrationStatus.Running || s == OrchestrationStatus.Pending);

        await _client.RaiseEventAsync(instance, PendingEventOrchestration.EventName, "done");

        OrchestrationState state = await _client.WaitForOrchestrationAsync(
            instance, TimeSpan.FromSeconds(30));

        Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
        Assert.NotEqual(default, state.CompletedTime);
        AssertTimestampsAreUtc(state, "WaitForOrchestrationAsync");
    }

    /// <summary>
    /// Polls until the instance reaches a status matching <paramref name="predicate"/>, or returns
    /// null on timeout. WaitForOrchestrationAsync only returns on terminal states, so it cannot be
    /// used to observe a Running/Suspended instance.
    /// </summary>
    private async Task<OrchestrationState?> WaitForStatusAsync(
        string instanceId,
        Func<OrchestrationStatus, bool> predicate)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            OrchestrationState? state = await _testService!.ClientService
                .GetOrchestrationStateAsync(instanceId, executionId: null);

            if (state != null && predicate(state.OrchestrationStatus))
            {
                return state;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        return null;
    }

    /// <summary>
    /// Blocks on an external event so the instance stays non-terminal (completed_time NULL) until
    /// the test either suspends it or raises the event.
    /// </summary>
    private sealed class PendingEventOrchestration : TaskOrchestration<string?, string?>
    {
        public const string EventName = "Approve";

        private TaskCompletionSource<string?>? _handle;

        public override async Task<string?> RunTask(OrchestrationContext context, string? input)
        {
            this._handle = new TaskCompletionSource<string?>();
            string? value = await this._handle.Task;
            this._handle = null;
            return value;
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (string.Equals(name, EventName, StringComparison.Ordinal))
            {
                this._handle?.TrySetResult(input);
            }
        }
    }
}
