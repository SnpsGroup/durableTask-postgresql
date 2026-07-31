using DurableTask.PostgreSQL.Tests.Utils;
using Npgsql;
using Xunit;
using static DurableTask.PostgreSQL.Tests.Utils.OrchestrationHelpers;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// Verifies the forward-migration runner: an embedded migration-{semver}.postgresql.sql whose
/// version is newer than the baseline is applied on startup and recorded in dt.versions.
/// </summary>
[Collection("integration")]
public sealed class MigrationTests : IAsyncLifetime
{
    private TestService? _testService;
    private bool _isDatabaseAvailable;

    public async Task InitializeAsync()
    {
        _isDatabaseAvailable = await IsDatabaseAvailableAsync(ConnectionString);
        if (!_isDatabaseAvailable)
        {
            return;
        }

        _testService = new TestService(ConnectionString, taskHubName: "MigrationTestsHub");
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
    public async Task Startup_AppliesPendingMigrationsAndRecordsVersion()
    {
        if (!_isDatabaseAvailable)
        {
            return;
        }

        // The migration-1.1.0.postgresql.sql script adds a description column.
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using var colCmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='dt' AND table_name='instances' AND column_name='description'",
            connection);
        var colCount = (long)(await colCmd.ExecuteScalarAsync() ?? 0);
        Assert.Equal(1, colCount);

        await using var verCmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM dt.versions WHERE semantic_version='1.1.0'",
            connection);
        var verCount = (long)(await verCmd.ExecuteScalarAsync() ?? 0);
        Assert.Equal(1, verCount);
    }
}
