using DurableTask.Core;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

public sealed class ServiceCollectionExtensionsTests
{
    private const string ConnectionString =
        "Host=unit-test-host;Database=durabletask;Username=u;Password=p";

    [Fact]
    public void ConfigureCallback_AppliesSettingsToResolvedService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        int callbackInvocations = 0;
        int dataSourceInvocations = 0;
        services.AddDurableTaskPostgreSql(ConnectionString, settings =>
        {
            callbackInvocations++;
            settings.TaskHubName = "ConfiguredViaDi";
            settings.MaxConcurrentOrchestrations = 17;
            settings.MaxConcurrentActivities = 23;
            settings.MinWorkItemPollingInterval = TimeSpan.FromMilliseconds(80);
            settings.WorkItemPollingInterval = TimeSpan.FromSeconds(2);
            settings.DeltaBackoffWorkItemPollingInterval = TimeSpan.FromMilliseconds(70);
            settings.ConfigureDataSource = builder =>
            {
                dataSourceInvocations++;
                builder.ConnectionStringBuilder.MaxPoolSize = 31;
            };
        });

        using var provider = services.BuildServiceProvider();
        var service = provider.GetRequiredService<PostgreSqlOrchestrationService>();
        var configuredSettings = provider.GetRequiredService<PostgreSqlOrchestrationServiceSettings>();

        Assert.Same(service, provider.GetRequiredService<IOrchestrationService>());
        Assert.Same(service, provider.GetRequiredService<IOrchestrationServiceClient>());
        Assert.Same(service, provider.GetRequiredService<DurableTask.Core.Entities.IEntityOrchestrationService>());
        Assert.Equal(17, service.MaxConcurrentTaskOrchestrationWorkItems);
        Assert.Equal(23, service.MaxConcurrentTaskActivityWorkItems);
        Assert.Equal(TimeSpan.FromMilliseconds(80), configuredSettings.MinWorkItemPollingInterval);
        Assert.Equal(TimeSpan.FromSeconds(2), configuredSettings.WorkItemPollingInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(70), configuredSettings.DeltaBackoffWorkItemPollingInterval);
        var effectiveConnection = new NpgsqlConnectionStringBuilder(service.DataSource.ConnectionString);
        Assert.Equal("ConfiguredViaDi", effectiveConnection.ApplicationName);
        Assert.Equal(31, effectiveConnection.MaxPoolSize);
        Assert.Equal(1, callbackInvocations);
        Assert.Equal(1, dataSourceInvocations);
    }

    [Fact]
    public void ConfigureCallback_InvalidPollingSettingsAreValidatedDuringResolution()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDurableTaskPostgreSql(ConnectionString, settings =>
        {
            settings.MinWorkItemPollingInterval = TimeSpan.FromSeconds(2);
            settings.WorkItemPollingInterval = TimeSpan.FromSeconds(1);
        });

        using var provider = services.BuildServiceProvider();
        Assert.Throws<ArgumentOutOfRangeException>(
            () => provider.GetRequiredService<PostgreSqlOrchestrationService>());
    }
}
