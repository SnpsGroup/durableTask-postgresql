using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

public sealed class ExternalDataSourceUnitTests
{
    private const string ConnectionString = "Host=unit-test-host;Database=durabletask;Username=u;Password=p;Application Name=SharedHub";

    [Fact]
    public void BuilderExtension_ValidatesArgumentsAndReturnsBuilder()
    {
        Assert.Throws<ArgumentNullException>(() => NpgsqlDataSourceBuilderExtensions.UseDurableTaskPostgreSql(null!));
        var builder = new NpgsqlDataSourceBuilder(ConnectionString);
        Assert.Throws<ArgumentException>(() => builder.UseDurableTaskPostgreSql(" "));
        Assert.Same(builder, builder.UseDurableTaskPostgreSql());
    }

    [Fact]
    public void ExternalDataSource_DiResolvesSameSourceWithoutConnectionString()
    {
        using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString).UseDurableTaskPostgreSql().Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDurableTaskPostgreSql(dataSource, settings => settings.TaskHubName = "SharedHub");
        using var provider = services.BuildServiceProvider();

        var service = provider.GetRequiredService<PostgreSqlOrchestrationService>();
        Assert.Same(dataSource, service.DataSource);
        Assert.Same(dataSource, provider.GetRequiredService<PostgreSqlOrchestrationServiceSettings>().DataSource);
    }

    [Fact]
    public void ExternalDataSource_MismatchedTaskHubIsRejected()
    {
        using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString).UseDurableTaskPostgreSql().Build();
        var settings = new PostgreSqlOrchestrationServiceSettings { DataSource = dataSource, TaskHubName = "DifferentHub" };

        Assert.Throws<ArgumentException>(() => new PostgreSqlOrchestrationService(settings, NullLogger<PostgreSqlOrchestrationService>.Instance));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ExternalDataSource_RejectsProviderOwnedConfiguration(bool autoDeploy)
    {
        using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString).UseDurableTaskPostgreSql().Build();
        var settings = new PostgreSqlOrchestrationServiceSettings
        {
            DataSource = dataSource,
            AutoDeploySchema = autoDeploy,
            ConfigureDataSource = autoDeploy ? null : _ => { },
        };

        Assert.Throws<ArgumentException>(() => new PostgreSqlOrchestrationService(settings, NullLogger<PostgreSqlOrchestrationService>.Instance));
    }
}
