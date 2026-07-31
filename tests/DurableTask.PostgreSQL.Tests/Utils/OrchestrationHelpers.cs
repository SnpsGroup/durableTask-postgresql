using System.Net.Sockets;
using System.Text.Json;
using DurableTask.Core;
using Npgsql;

namespace DurableTask.PostgreSQL.Tests.Utils;

/// <summary>
/// Shared test infrastructure for integration tests, mirroring the MSSQL provider's
/// <c>Utils/</c> helpers (named creators + DB probe). Orchestrations are defined as small
/// concrete <see cref="TaskOrchestration"/> subclasses per behavior (as the existing
/// CompletedOrchestration does), kept inline in each test file.
/// </summary>
public static class OrchestrationHelpers
{
    /// <summary>
    /// Probes whether a PostgreSQL backend is reachable. Integration tests early-return (no-op)
    /// when this is false so the suite can run in environments without a database.
    /// </summary>
    public static async Task<bool> IsDatabaseAvailableAsync(string connectionString)
    {
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Timeout = 3,
                CommandTimeout = 3,
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

    /// <summary>Default connection string, overridable via the POSTGRES_CONNECTION_STRING env var.</summary>
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
        ?? "Host=localhost;Port=5433;Database=durabletask;Username=postgres;Password=postgres";
}

/// <summary>
/// Registers a single named orchestrator on a <see cref="TaskHubWorker"/>. Wraps a fixed
/// instance, mirroring the MSSQL <c>TestObjectCreator&lt;T&gt;</c>.
/// </summary>
public sealed class NamedOrchestrationCreator : ObjectCreator<TaskOrchestration>
{
    private readonly TaskOrchestration _orchestration;

    public NamedOrchestrationCreator(string name, string version, TaskOrchestration orchestration)
    {
        this.Name = name;
        this.Version = version;
        this._orchestration = orchestration;
    }

    public override TaskOrchestration Create() => this._orchestration;
}

/// <summary>
/// Registers a single named activity on a <see cref="TaskHubWorker"/>.
/// </summary>
public sealed class NamedActivityCreator : ObjectCreator<TaskActivity>
{
    private readonly TaskActivity _activity;

    public NamedActivityCreator(string name, string version, TaskActivity activity)
    {
        this.Name = name;
        this.Version = version;
        this._activity = activity;
    }

    public override TaskActivity Create() => this._activity;
}

/// <summary>Serializes an orchestration input/output value to the JSON string DTFx expects.</summary>
public static class JsonPayload
{
    public static string ToJson<T>(T value) => JsonSerializer.Serialize(value);
    public static T FromJson<T>(string? json) => json is null ? default! : JsonSerializer.Deserialize<T>(json)!;
}

/// <summary>A trivial orchestration that completes immediately, producing a Completed instance.</summary>
public sealed class NoOpOrchestration : TaskOrchestration
{
    public override Task<string?> Execute(OrchestrationContext context, string? input) => Task.FromResult<string?>(null);
    public override string GetStatus() => string.Empty;
    public override void RaiseEvent(OrchestrationContext context, string name, string input) { }
}
