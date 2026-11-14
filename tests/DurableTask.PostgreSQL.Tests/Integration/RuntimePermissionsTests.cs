using DurableTask.Core;
using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;
using static DurableTask.PostgreSQL.Tests.Utils.JsonPayload;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>Exercises the real provider using an unrelated, non-owner PostgreSQL login.</summary>
[Collection("integration")]
public sealed class RuntimePermissionsTests : IAsyncLifetime
{
    private readonly string _customSchema = $"dt_permissions_{Guid.NewGuid():N}";
    private readonly List<string> _runtimeUsers = [];
    private bool _available;

    public async Task InitializeAsync() => _available = await IsDatabaseAvailableAsync(ConnectionString);

    public async Task DisposeAsync()
    {
        if (!_available) return;
        await ExecuteOwnerAsync($"DROP SCHEMA IF EXISTS {_customSchema} CASCADE");
        foreach (string user in _runtimeUsers)
        {
            await ExecuteOwnerAsync($"DROP ROLE IF EXISTS {user}");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RestrictedRuntime_CompletesQueriesAndPurges_WithoutDdlOrConfigurationWrites(bool customSchema)
    {
        if (!_available) return;
        string schema = customSchema ? _customSchema : "dt";
        await DeployAsync(schema, permissions: false);
        // Simulate an upgrade from the former owner-identity helper, then repeat deployment.
        await ExecuteOwnerAsync($"ALTER FUNCTION {schema}.current_task_hub() SECURITY DEFINER");
        await DeployAsync(schema, permissions: true);
        await DeployAsync(schema, permissions: true);

        string runtimeConnection = await CreateRuntimeLoginAsync();
        string hub = $"PermissionsHub{Guid.NewGuid():N}";
        using var service = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = runtimeConnection,
            SchemaName = schema,
            TaskHubName = hub,
            AutoDeploySchema = false,
        }, NullLogger<PostgreSqlOrchestrationService>.Instance);
        await service.StartAsync();

        var worker = new TaskHubWorker(service, NullLoggerFactory.Instance);
        worker.AddTaskOrchestrations(new NamedOrchestrationCreator(nameof(EchoOrchestration), string.Empty, new EchoOrchestration()));
        worker.AddTaskActivities(new NamedActivityCreator(nameof(EchoActivity), string.Empty, new EchoActivity()));
        await worker.StartAsync();
        try
        {
            var client = new TaskHubClient(service);
            OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                nameof(EchoOrchestration), string.Empty, Guid.NewGuid().ToString(), "restricted");
            OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal("Hello, restricted!", FromJson<string>(state.Output));
            Assert.NotEmpty(await service.GetOrchestrationHistoryAsync(instance.InstanceId, instance.ExecutionId));
            Assert.NotNull(await service.GetOrchestrationStateAsync(instance.InstanceId, instance.ExecutionId));
            Assert.Equal(1, (await service.PurgeInstanceStateAsync(instance.InstanceId)).DeletedInstanceCount);

            await using var runtime = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(runtimeConnection) { Pooling = false }.ConnectionString);
            await runtime.OpenAsync();
            foreach (string forbiddenSql in new[]
            {
                $"CREATE TABLE {schema}.forbidden (id int)",
                $"DROP TABLE {schema}.instances",
                $"UPDATE {schema}.global_settings SET value='1' WHERE name='TaskHubMode'",
                $"INSERT INTO {schema}.versions (semantic_version) VALUES ('unauthorized')",
                $"ALTER FUNCTION {schema}.current_task_hub() SECURITY DEFINER",
            })
            {
                await using var forbidden = new NpgsqlCommand(forbiddenSql, runtime);
                var error = await Assert.ThrowsAsync<PostgresException>(() => forbidden.ExecuteNonQueryAsync());
                Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, error.SqlState);
            }
        }
        finally
        {
            await worker.StopAsync(isForced: true);
            await service.StopAsync();
        }
    }

    [Fact]
    public async Task UserBasedTaskHub_UsesEachRuntimeIdentity_AfterUpgradeFromDefiner()
    {
        if (!_available) return;
        await DeployAsync(_customSchema, permissions: false);
        await ExecuteOwnerAsync($"ALTER FUNCTION {_customSchema}.current_task_hub() SECURITY DEFINER");
        await DeployAsync(_customSchema, permissions: true);
        await ExecuteOwnerAsync($"UPDATE {_customSchema}.global_settings SET value='1' WHERE name='TaskHubMode'");

        string instanceId = Guid.NewGuid().ToString();
        for (int userIndex = 0; userIndex < 2; userIndex++)
        {
            string runtimeConnection = await CreateRuntimeLoginAsync();
            var builder = new NpgsqlConnectionStringBuilder(runtimeConnection) { Pooling = false };
            await using var runtime = new NpgsqlConnection(builder.ConnectionString);
            await runtime.OpenAsync();
            await using var cmd = new NpgsqlCommand($"SELECT {_customSchema}.current_task_hub()", runtime);
            Assert.Equal(builder.Username, await cmd.ExecuteScalarAsync());

            using var service = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = runtimeConnection,
                SchemaName = _customSchema,
                TaskHubName = "IgnoredInUserMode",
            }, NullLogger<PostgreSqlOrchestrationService>.Instance);
            string input = $"user-{userIndex}";
            var client = new TaskHubClient(service);
            await client.CreateOrchestrationInstanceAsync(nameof(EchoOrchestration), string.Empty, instanceId, input);
            OrchestrationState? state = await service.GetOrchestrationStateAsync(instanceId, executionId: null);
            Assert.NotNull(state);
            Assert.Equal(input, FromJson<string>(state.Input));
        }
    }

    private async Task DeployAsync(string schema, bool permissions)
    {
        using var deployer = new PostgreSqlOrchestrationService(new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = ConnectionString,
            SchemaName = schema,
            TaskHubName = "PermissionsDeployment",
            AutoDeploySchema = true,
            DeployRuntimePermissions = permissions,
        }, NullLogger<PostgreSqlOrchestrationService>.Instance);
        await deployer.CreateAsync();
    }

    private async Task<string> CreateRuntimeLoginAsync()
    {
        string user = $"dt_runtime_test_{Guid.NewGuid():N}";
        string password = Guid.NewGuid().ToString("N");
        await ExecuteOwnerAsync($"CREATE ROLE {user} LOGIN PASSWORD '{password}' IN ROLE dt_runtime");
        _runtimeUsers.Add(user);
        return new NpgsqlConnectionStringBuilder(ConnectionString)
        {
            Username = user,
            Password = password,
            ApplicationName = "RestrictedApplication",
        }.ConnectionString;
    }

    private static async Task ExecuteOwnerAsync(string sql)
    {
        await using var owner = new NpgsqlConnection(ConnectionString);
        await owner.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, owner);
        await cmd.ExecuteNonQueryAsync();
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
