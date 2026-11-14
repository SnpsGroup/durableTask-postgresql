using DurableTask.Core;
using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;
using static DurableTask.PostgreSQL.Tests.Utils.JsonPayload;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>Exercises the checkpoint serialization and ownership boundary of an application-owned pool.</summary>
[Collection("integration")]
public sealed class ExternalDataSourceIntegrationTests : IAsyncLifetime
{
    private bool _isDatabaseAvailable;

    public async Task InitializeAsync() => _isDatabaseAvailable = await IsDatabaseAvailableAsync(ConnectionString);

    public Task DisposeAsync() => Task.CompletedTask;

    [Theory]
    [InlineData("dt", "ExternalDataSourceHub")]
    [InlineData("external_source_test", "ExternalDataSourceCustomHub")]
    public async Task ExternalDataSource_CheckpointCompletesAndPoolSurvivesStopAndDispose(string schemaName, string hubName)
    {
        if (!_isDatabaseAvailable) return;
        await DeployAsync(schemaName, hubName);

        var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString) { ApplicationName = hubName }.ConnectionString;
        await using var dataSource = new NpgsqlDataSourceBuilder(connectionString).UseDurableTaskPostgreSql(schemaName).Build();
        var service = CreateService(dataSource, schemaName, hubName);
        TaskHubWorker? worker = null;
        try
        {
            await service.StartAsync();
            Assert.Same(dataSource, service.DataSource);

            worker = new TaskHubWorker(service, NullLoggerFactory.Instance);
            worker.AddTaskOrchestrations(new NamedOrchestrationCreator(nameof(EchoOrchestration), string.Empty, new EchoOrchestration()));
            worker.AddTaskActivities(new NamedActivityCreator(nameof(EchoActivity), string.Empty, new EchoActivity()));
            await worker.StartAsync();

            var client = new TaskHubClient(service);
            var instance = await client.CreateOrchestrationInstanceAsync(nameof(EchoOrchestration), string.Empty, Guid.NewGuid().ToString(), "shared pool");
            var state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal("Hello, shared pool!", FromJson<string>(state.Output));
            await using (var connection = await dataSource.OpenConnectionAsync())
            {
                await using var command = new NpgsqlCommand($"SELECT {schemaName}.current_task_hub()", connection);
                Assert.Equal(hubName, await command.ExecuteScalarAsync());
            }
        }
        finally
        {
            if (worker != null) await worker.StopAsync(isForced: true);
            await service.StopAsync();
            service.Dispose();
        }

        // The rest of the application must keep using the exact same pool after provider shutdown.
        await using var applicationConnection = await dataSource.OpenConnectionAsync();
        await using var applicationCommand = new NpgsqlCommand("SELECT 1", applicationConnection);
        Assert.Equal(1, await applicationCommand.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExternalDataSource_MissingCompositeMappingsFailDuringStartupWithoutDisposingPool()
    {
        if (!_isDatabaseAvailable) return;
        const string hubName = "ExternalMissingMappingsHub";
        await DeployAsync("dt", hubName);

        var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString) { ApplicationName = hubName }.ConnectionString;
        await using var dataSource = new NpgsqlDataSourceBuilder(connectionString).Build();
        using (var service = CreateService(dataSource, "dt", hubName))
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync());
            Assert.Contains("UseDurableTaskPostgreSql", exception.Message);
        }

        await using var applicationConnection = await dataSource.OpenConnectionAsync();
        await using var applicationCommand = new NpgsqlCommand("SELECT 1", applicationConnection);
        Assert.Equal(1, await applicationCommand.ExecuteScalarAsync());
    }

    private static PostgreSqlOrchestrationService CreateService(NpgsqlDataSource dataSource, string schemaName, string hubName) =>
        new(new PostgreSqlOrchestrationServiceSettings { DataSource = dataSource, SchemaName = schemaName, TaskHubName = hubName },
            NullLogger<PostgreSqlOrchestrationService>.Instance);

    private static async Task DeployAsync(string schemaName, string hubName)
    {
        using var deployment = new PostgreSqlOrchestrationService(
            new PostgreSqlOrchestrationServiceSettings { ConnectionString = ConnectionString, SchemaName = schemaName, TaskHubName = hubName, AutoDeploySchema = true },
            NullLogger<PostgreSqlOrchestrationService>.Instance);
        await deployment.CreateAsync();
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
