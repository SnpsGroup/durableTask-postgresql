using DurableTask.Core;
using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;
using static DurableTask.PostgreSQL.Tests.Utils.JsonPayload;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// End-to-end integration tests for
/// <see cref="PostgreSqlOrchestrationServiceSettings.ConfigureDataSource"/> (GitHub issue #7).
/// The unit tests cover the connection string the provider builds; these prove caller
/// configuration actually reaches real physical connections <em>and</em> that the provider's
/// composite type mappings still resolve afterwards — the failure mode that only appears at
/// checkpoint, after the service has already reported healthy. They must therefore run a full
/// orchestration, not merely assert on startup.
/// </summary>
[Collection("integration")]
public sealed class DataSourceConfigurationIntegrationTests : IAsyncLifetime
{
    private const string HubName = "ConfigureDataSourceTestsHub";

    private bool _isDatabaseAvailable;

    public async Task InitializeAsync() =>
        _isDatabaseAvailable = await IsDatabaseAvailableAsync(ConnectionString);

    public Task DisposeAsync() => Task.CompletedTask;

    private static PostgreSqlOrchestrationService CreateService(
        Action<NpgsqlDataSourceBuilder>? configureDataSource) =>
        new(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = ConnectionString,
                TaskHubName = HubName,
                AutoDeploySchema = true,
                ConfigureDataSource = configureDataSource,
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance);

    /// <summary>
    /// A callback registering a physical connection initializer must be honored on real
    /// connections, and the orchestration must still complete — create, activity, checkpoint,
    /// complete — which is what exercises the composite-typed arrays.
    /// </summary>
    [Fact]
    public async Task ConfigureDataSource_PhysicalConnectionInitializer_RunsAndOrchestrationCompletes()
    {
        if (!_isDatabaseAvailable) return;

        // Counted inside the callback rather than asserted via a session GUC: Npgsql sends
        // DISCARD ALL when a connection returns to the pool, so a GUC would survive or not
        // depending on pool reuse — flaky, and failing for a reason unrelated to this test.
        int initializerRuns = 0;

        var service = CreateService(builder => builder.UsePhysicalConnectionInitializer(
            // Throwing from the sync initializer also catches accidental synchronous opening,
            // which the provider never does; all its I/O goes through OpenConnectionAsync.
            _ => throw new NotSupportedException("Synchronous connection opening is not expected."),
            async connection =>
            {
                Interlocked.Increment(ref initializerRuns);
                await using var cmd = new NpgsqlCommand("SELECT 1", connection);
                await cmd.ExecuteNonQueryAsync();
            }));

        TaskHubWorker? worker = null;
        try
        {
            await service.CreateAsync();
            await service.StartAsync();

            worker = new TaskHubWorker(service, NullLoggerFactory.Instance);
            worker.AddTaskOrchestrations(
                new NamedOrchestrationCreator(nameof(EchoOrchestration), string.Empty, new EchoOrchestration()));
            worker.AddTaskActivities(
                new NamedActivityCreator(nameof(EchoActivity), string.Empty, new EchoActivity()));
            await worker.StartAsync();

            var client = new TaskHubClient(service);
            OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                nameof(EchoOrchestration), string.Empty, Guid.NewGuid().ToString(), "World");

            OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal("Hello, World!", FromJson<string>(state.Output));
            Assert.True(initializerRuns > 0, "The caller's connection initializer never ran.");
        }
        finally
        {
            if (worker != null)
            {
                await worker.StopAsync(isForced: true);
            }
            await service.StopAsync();
            service.Dispose();
        }
    }

    /// <summary>
    /// The server-side half of the ApplicationName re-assert: with a callback present that sets
    /// ApplicationName, current_task_hub() must still resolve to TaskHubName. The unit test
    /// covers the connection string; this covers what PostgreSQL actually reports.
    /// </summary>
    [Fact]
    public async Task ConfigureDataSource_SettingApplicationName_CurrentTaskHubIsStillTaskHubName()
    {
        if (!_isDatabaseAvailable) return;

        var service = CreateService(
            builder => builder.ConnectionStringBuilder.ApplicationName = "SomeHostApplication");

        try
        {
            await service.CreateAsync();
            await service.StartAsync();

            await using var connection = await service.DataSource.OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT dt.current_task_hub()", connection);
            var taskHub = await cmd.ExecuteScalarAsync() as string;

            Assert.Equal(HubName, taskHub);
        }
        finally
        {
            await service.StopAsync();
            service.Dispose();
        }
    }

    private sealed class EchoOrchestration : TaskOrchestration
    {
        public override async Task<string?> Execute(OrchestrationContext context, string? input) =>
            await context.ScheduleTask<string>(nameof(EchoActivity), string.Empty, FromJson<string>(input));

        public override string GetStatus() => string.Empty;

        public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
    }

    private sealed class EchoActivity : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input) => $"Hello, {input}!";
    }
}
