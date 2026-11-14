using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="PostgreSqlOrchestrationServiceSettings.ConfigureDataSource"/>
/// (GitHub issue #7). These assert on the connection string of the data source the service
/// built, read through the <c>internal</c> <see cref="PostgreSqlOrchestrationService.DataSource"/>
/// seam, so no database is needed — constructing the service does not open a connection.
/// </summary>
public sealed class DataSourceConfigurationUnitTests
{
    private const string BaseConnectionString =
        "Host=unit-test-host;Port=5432;Database=durabletask;Username=u;Password=p";

    private const string HubName = "ConfigureDataSourceUnitHub";

    private static PostgreSqlOrchestrationService CreateService(
        Action<NpgsqlDataSourceBuilder>? configureDataSource,
        string? taskHubName = HubName) =>
        new(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = BaseConnectionString,
                TaskHubName = taskHubName,
                ConfigureDataSource = configureDataSource,
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance);

    [Fact]
    public void ConfigureDataSource_IsInvokedExactlyOnce_DuringConstruction()
    {
        int invocations = 0;

        using var service = CreateService(_ => invocations++);

        Assert.Equal(1, invocations);
    }

    [Fact]
    public void ConfigureDataSource_ReceivesConnectionStringAlreadyApplied()
    {
        string? observedDatabase = null;

        using var service = CreateService(
            builder => observedDatabase = builder.ConnectionStringBuilder.Database);

        Assert.Equal("durabletask", observedDatabase);
    }

    /// <summary>
    /// The regression test for the ApplicationName re-assert. A caller callback that sets
    /// ApplicationName — the realistic case being a shared host configuration helper setting
    /// it to the application's name — must not repoint the task hub, because under the default
    /// TaskHubMode = '0' the schema's current_task_hub() reads application_name. Without the
    /// re-assert this silently routes work to a different hub: no exception, instances simply
    /// never progress. Fails if the re-assert is removed from the constructor.
    /// </summary>
    [Fact]
    public void ConfigureDataSource_SettingApplicationName_DoesNotChangeEffectiveTaskHub()
    {
        using var service = CreateService(
            builder => builder.ConnectionStringBuilder.ApplicationName = "SomeHostApplication");

        var effective = new NpgsqlConnectionStringBuilder(service.DataSource.ConnectionString);
        Assert.Equal(HubName, effective.ApplicationName);
    }

    /// <summary>
    /// Guards the documented trust boundary rather than a defense: ConfigureDataSource runs as
    /// the host application, which already owns the connection string, so settings the provider
    /// does not re-apply do take effect. Pinning this keeps the behavior from being mistaken
    /// for a bug, and makes it a deliberate decision to change.
    /// </summary>
    [Fact]
    public void ConfigureDataSource_SettingsOtherThanApplicationName_AreNotOverridden()
    {
        using var service = CreateService(builder =>
        {
            builder.ConnectionStringBuilder.SearchPath = "some_other_schema";
            builder.ConnectionStringBuilder.CommandTimeout = 123;
        });

        var effective = new NpgsqlConnectionStringBuilder(service.DataSource.ConnectionString);
        Assert.Equal("some_other_schema", effective.SearchPath);
        Assert.Equal(123, effective.CommandTimeout);
        Assert.Equal(HubName, effective.ApplicationName);
    }

    [Fact]
    public void ConfigureDataSource_Null_LeavesConnectionStringUnchanged()
    {
        using var withCallback = CreateService(_ => { });
        using var withoutCallback = CreateService(configureDataSource: null);

        Assert.Equal(withoutCallback.DataSource.ConnectionString, withCallback.DataSource.ConnectionString);
    }

    /// <summary>
    /// With no TaskHubName the provider must leave ApplicationName alone (under the default
    /// TaskHubMode = '0', dt.current_task_hub() then uses whatever application_name the connection carries, or 'default' if empty). The re-assert is conditional for this reason;
    /// this pins that a callback can still set it in that configuration.
    /// </summary>
    [Fact]
    public void ConfigureDataSource_WithoutTaskHubName_CallerApplicationNameSurvives()
    {
        using var service = CreateService(
            builder => builder.ConnectionStringBuilder.ApplicationName = "SomeHostApplication",
            taskHubName: null);

        var effective = new NpgsqlConnectionStringBuilder(service.DataSource.ConnectionString);
        Assert.Equal("SomeHostApplication", effective.ApplicationName);
    }

    /// <summary>
    /// Pins the credential conflict documented in DATASOURCE.md section 4.2: the provider applies
    /// ConnectionString to the builder before the callback runs, so a connection string carrying
    /// a password plus a callback registering a password provider throws from Build() — inside
    /// the constructor. This is the main reason to use the callback (rotating-token auth) crossed
    /// with the quick start's connection string, so it is worth guarding rather than only
    /// documenting. If a future Npgsql relaxes this, the test fails and the docs get revisited.
    /// </summary>
    [Fact]
    public void ConfigureDataSource_PasswordProviderWithPasswordInConnectionString_Throws()
    {
        var thrown = Assert.Throws<NotSupportedException>(() => CreateService(
            builder => builder.UsePeriodicPasswordProvider(
                (_, _) => ValueTask.FromResult("token"),
                successRefreshInterval: TimeSpan.FromMinutes(50),
                failureRefreshInterval: TimeSpan.FromSeconds(5))));

        Assert.Contains("password provider", thrown.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The documented workaround for the conflict above: omit the password from the connection
    /// string and let the provider callback supply it. Construction must succeed.
    /// </summary>
    [Fact]
    public void ConfigureDataSource_PasswordProviderWithoutPasswordInConnectionString_Succeeds()
    {
        using var service = new PostgreSqlOrchestrationService(
            new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = "Host=unit-test-host;Port=5432;Database=durabletask;Username=u",
                TaskHubName = HubName,
                ConfigureDataSource = builder => builder.UsePeriodicPasswordProvider(
                    (_, _) => ValueTask.FromResult("token"),
                    successRefreshInterval: TimeSpan.FromMinutes(50),
                    failureRefreshInterval: TimeSpan.FromSeconds(5)),
            },
            NullLogger<PostgreSqlOrchestrationService>.Instance);

        var effective = new NpgsqlConnectionStringBuilder(service.DataSource.ConnectionString);
        Assert.Equal(HubName, effective.ApplicationName);
    }

    [Fact]
    public void ConfigureDataSource_ThrowingCallback_SurfacesFromConstructor()
    {
        var failure = new InvalidOperationException("callback failed");

        var thrown = Assert.Throws<InvalidOperationException>(
            () => CreateService(_ => throw failure));

        Assert.Same(failure, thrown);
    }
}
