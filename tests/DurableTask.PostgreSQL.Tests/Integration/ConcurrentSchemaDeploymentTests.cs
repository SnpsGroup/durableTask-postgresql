using DurableTask.PostgreSQL.Tests.Utils;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Covers concurrent first-deploy of the schema (GitHub #10). Several workers starting at once
/// against a fresh database all call <see cref="PostgreSqlOrchestrationService.CreateAsync()"/>
/// with <c>AutoDeploySchema = true</c>; without serialization they collide on catalog locks and
/// fail with a deadlock (40P01) or a unique violation (23505).
///
/// These tests deploy into their own throwaway schemas rather than the shared <c>dt</c> schema,
/// because the race only exists on a genuinely empty catalog.
/// </summary>
[Collection("integration")]
public sealed class ConcurrentSchemaDeploymentTests : IAsyncLifetime
{
    private const string SchemaPrefix = "dt_concurrent_deploy_test";

    private bool _isDatabaseAvailable;
    private readonly List<string> _schemasToDrop = [];

    public async Task InitializeAsync()
    {
        _isDatabaseAvailable = await IsDatabaseAvailableAsync(ConnectionString);
    }

    public async Task DisposeAsync()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        foreach (var schema in _schemasToDrop)
        {
            await using var cmd = new NpgsqlCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE", connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// The acceptance criterion from GitHub #10: concurrent <c>CreateAsync</c> calls on a fresh
    /// database all succeed — one deploys, the others wait — instead of deadlocking.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Concurrent_first_deploy_succeeds_for_every_worker(int workerCount)
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        string schema = await CreateThrowawaySchemaNameAsync($"parallel{workerCount}");

        var services = Enumerable.Range(0, workerCount)
            .Select(_ => CreateService(schema))
            .ToList();

        try
        {
            // Release all workers at once: this is what produced the 40P01 deadlock.
            var barrier = new TaskCompletionSource();
            var deployments = services
                .Select(async service =>
                {
                    await barrier.Task;
                    await service.CreateAsync();
                })
                .ToList();

            barrier.SetResult();

            // Assert.All over awaited tasks would hide which worker failed; awaiting the whole
            // set surfaces the first exception with its original stack (e.g. the PostgresException).
            await Task.WhenAll(deployments);
        }
        finally
        {
            foreach (var service in services)
            {
                service.Dispose();
            }
        }

        // Every worker reported success, so the schema must actually be complete — a deploy that
        // silently gave up would leave these missing.
        Assert.True(await TableExistsAsync(schema, "instances"));
        Assert.True(await TableExistsAsync(schema, "versions"));
    }

    /// <summary>
    /// A second deploy over an already-deployed schema must be a no-op, not a failure: the
    /// blocked workers in the concurrent case take exactly this path once the winner commits.
    /// </summary>
    [Fact]
    public async Task Redeploy_over_existing_schema_succeeds()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        string schema = await CreateThrowawaySchemaNameAsync("redeploy");

        using (var first = CreateService(schema))
        {
            await first.CreateAsync();
        }

        using (var second = CreateService(schema))
        {
            await second.CreateAsync();
        }

        Assert.True(await TableExistsAsync(schema, "instances"));
    }

    /// <summary>
    /// The advisory lock key must be schema-scoped, otherwise unrelated task hubs sharing a
    /// database serialize their deploys against each other.
    /// </summary>
    [Fact]
    public void Deployment_lock_key_is_stable_and_schema_scoped()
    {
        long dt = PostgreSqlOrchestrationService.GetDeploymentLockKey("dt");
        long other = PostgreSqlOrchestrationService.GetDeploymentLockKey("dt_other");

        Assert.Equal(dt, PostgreSqlOrchestrationService.GetDeploymentLockKey("dt"));
        Assert.NotEqual(dt, other);
    }

    private static PostgreSqlOrchestrationService CreateService(string schema)
    {
        var settings = new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = ConnectionString,
            TaskHubName = schema,
            SchemaName = schema,
            AutoDeploySchema = true,
        };

        return new PostgreSqlOrchestrationService(settings, NullLogger<PostgreSqlOrchestrationService>.Instance);
    }

    /// <summary>
    /// Produces a unique schema name and drops any residue from an earlier run, so each test
    /// starts against a truly empty catalog (the only state where the race reproduces).
    /// </summary>
    private async Task<string> CreateThrowawaySchemaNameAsync(string suffix)
    {
        string schema = $"{SchemaPrefix}_{suffix}";
        _schemasToDrop.Add(schema);

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand($"DROP SCHEMA IF EXISTS {schema} CASCADE", connection);
        await cmd.ExecuteNonQueryAsync();

        return schema;
    }

    private static async Task<bool> TableExistsAsync(string schema, string table)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            connection);
        cmd.Parameters.AddWithValue(schema);
        cmd.Parameters.AddWithValue(table);

        return (long)(await cmd.ExecuteScalarAsync() ?? 0L) == 1;
    }
}
