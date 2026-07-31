using DurableTask.Core;
using DurableTask.Core.Query;
using DurableTask.PostgreSQL.Tests.Utils;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Integration tests for <see cref="IOrchestrationServiceClient.GetOrchestrationWithQueryAsync"/>.
/// Regression coverage for the bug where untyped parameters (integer/text/timestamp-without-tz)
/// failed to resolve the <c>query_many_orchestrations</c> overload (PostgreSQL error 42883).
/// </summary>
[Collection("integration")]
public sealed class QueryTests : IAsyncLifetime
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

        _testService = new TestService(ConnectionString, taskHubName: "QueryTestsHub");
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
    public async Task GetOrchestrationWithQuery_ReturnsMatchingInstances()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // Run an orchestration to completion so a query by Completed status can find it.
        string instanceId = Guid.NewGuid().ToString();
        OrchestrationInstance instance = await _client!.CreateOrchestrationInstanceAsync(
            nameof(NoOpOrchestration), string.Empty, instanceId, input: null);

        OrchestrationState state = await _client.WaitForOrchestrationAsync(
            instance, TimeSpan.FromSeconds(30));
        Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);

        // This previously threw PostgresException (42883: function ... does not exist) because
        // untyped int/string/timestamp parameters did not match the function signature.
        var query = new OrchestrationQuery
        {
            PageSize = 100,
            FetchInputsAndOutputs = false,
            CreatedTimeFrom = DateTime.UtcNow.AddDays(-1),
            CreatedTimeTo = DateTime.UtcNow.AddDays(1),
            InstanceIdPrefix = instanceId,
            RuntimeStatus = new List<OrchestrationStatus> { OrchestrationStatus.Completed },
        };

        OrchestrationQueryResult result = await _testService!.ClientService
            .GetOrchestrationWithQueryAsync(query, CancellationToken.None);

        Assert.NotEmpty(result.OrchestrationState);
        Assert.Contains(result.OrchestrationState, r => r.OrchestrationInstance.InstanceId == instanceId);
    }
}
