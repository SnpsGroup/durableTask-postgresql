using DurableTask.Core;
using DurableTask.PostgreSQL.Tests.Utils;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Integration tests for <see cref="IOrchestrationServiceClient.PurgeInstanceStateAsync(PurgeInstanceFilter)"/>.
///
/// Regression coverage for the bug where that overload called <c>purge_instance_state_by_time</c>
/// passing a status CSV string where the SQL function expected a SMALLINT, throwing a
/// <see cref="PostgresException"/> at runtime.
/// </summary>
[Collection("integration")]
public sealed class PurgeTests : IAsyncLifetime
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

        _testService = new TestService(ConnectionString, taskHubName: "PurgeTestsHub");
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

    [Fact]
    public async Task PurgeInstanceState_ByFilter_DeletesCompletedInstances()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // Run an orchestration to completion so it lands in a terminal (Completed) status,
        // which is what the purge filter matches on.
        string instanceId = Guid.NewGuid().ToString();
        OrchestrationInstance instance = await _client!.CreateOrchestrationInstanceAsync(
            name: nameof(NoOpOrchestration), version: string.Empty, instanceId, input: null);

        OrchestrationState state = await _client.WaitForOrchestrationAsync(
            instance, TimeSpan.FromSeconds(30));
        Assert.NotNull(state);
        Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);

        // This previously threw PostgresException (invalid input syntax for type smallint)
        // because a status CSV was passed where a SMALLINT was expected.
        var filter = new PurgeInstanceFilter(
            createdTimeFrom: DateTime.UtcNow.AddDays(-1),
            createdTimeTo: null,
            runtimeStatus: new[] { OrchestrationStatus.Completed });

        PurgeResult result = await _testService!.ClientService.PurgeInstanceStateAsync(filter);

        Assert.True(result.DeletedInstanceCount >= 1);

        // The purged instance must no longer exist.
        OrchestrationState? after = await _testService.ClientService.GetOrchestrationStateAsync(instanceId, executionId: null);
        Assert.Null(after);
    }
}
