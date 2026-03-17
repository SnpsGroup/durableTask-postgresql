using System.Text.Json;
using System.Net.Sockets;
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.PostgreSQL.Tests.Utils;
using Npgsql;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Integration;

public sealed class OrchestrationsTests : IAsyncLifetime
{
    private TestService? _testService;
    private bool _isDatabaseAvailable;
    private static readonly string ConnectionString = 
        Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING") 
        ?? "Host=localhost;Port=5433;Database=durabletask;Username=postgres;Password=postgres";

    public async Task InitializeAsync()
    {
        _isDatabaseAvailable = await IsDatabaseAvailableAsync();
        if (!_isDatabaseAvailable)
        {
            return;
        }

        _testService = new TestService(ConnectionString);
        await _testService.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        if (_testService != null)
        {
            await _testService.DisposeAsync();
        }
    }

    [Fact]
    public async Task CreateInstance_BasicOrchestration_CreatesSuccessfully()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        var client = _testService!.ClientService;

        var instanceId = Guid.NewGuid().ToString();
        var executionId = Guid.NewGuid().ToString();

        await client.CreateTaskOrchestrationAsync(new TaskMessage
        {
            OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId
            },
            Event = new ExecutionStartedEvent(-1, JsonSerializer.Serialize("test input"))
            {
                Name = "TestOrchestration",
                Version = "1.0",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId
                }
            }
        });

        await Task.Delay(500);

        var state = await client.GetOrchestrationStateAsync(instanceId, null);

        Assert.NotNull(state);
        Assert.Equal(instanceId, state.OrchestrationInstance.InstanceId);
    }

    [Fact]
    public async Task GetOrchestrationState_ExistingInstance_ReturnsState()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        var client = _testService!.ClientService;

        var instanceId = Guid.NewGuid().ToString();
        await client.CreateTaskOrchestrationAsync(new TaskMessage
        {
            OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
            Event = new ExecutionStartedEvent(-1, JsonSerializer.Serialize("test"))
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId }
            }
        });

        await Task.Delay(500);

        var state = await client.GetOrchestrationStateAsync(instanceId, null);

        Assert.NotNull(state);
        Assert.Equal(instanceId, state.OrchestrationInstance.InstanceId);
    }

    [Fact]
    public async Task GetOrchestrationState_NonExistingInstance_ReturnsNull()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        var client = _testService!.ClientService;

        var state = await client.GetOrchestrationStateAsync("non-existing-instance", null);

        Assert.Null(state);
    }

    private static async Task<bool> IsDatabaseAvailableAsync()
    {
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                Timeout = 3,
                CommandTimeout = 3
            };

            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync();
            return true;
        }
        catch (Exception ex) when (
            ex is NpgsqlException ||
            ex is SocketException ||
            ex is TimeoutException ||
            ex is InvalidOperationException)
        {
            return false;
        }
    }
}
