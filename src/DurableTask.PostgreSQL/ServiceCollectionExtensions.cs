using DurableTask.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DurableTask.PostgreSQL;

/// <summary>
/// Extension methods for registering PostgreSqlOrchestrationService in DI.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds PostgreSqlOrchestrationService as the DurableTask backend.
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="connectionString">PostgreSQL connection string</param>
    /// <param name="configure">Optional configuration action</param>
    /// <returns>Service collection for chaining</returns>
    public static IServiceCollection AddDurableTaskPostgreSql(
        this IServiceCollection services,
        string connectionString,
        Action<PostgreSqlOrchestrationServiceSettings>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.AddSingleton<PostgreSqlOrchestrationServiceSettings>(sp =>
        {
            var settings = new PostgreSqlOrchestrationServiceSettings
            {
                ConnectionString = connectionString
            };

            configure?.Invoke(settings);

            return settings;
        });

        services.AddSingleton<PostgreSqlOrchestrationService>(sp =>
        {
            var settings = sp.GetRequiredService<PostgreSqlOrchestrationServiceSettings>();
            var logger = sp.GetRequiredService<ILogger<PostgreSqlOrchestrationService>>();

            return new PostgreSqlOrchestrationService(settings, logger);
        });

        // Register as both IOrchestrationService and IOrchestrationServiceClient
        services.AddSingleton<IOrchestrationService>(sp =>
            sp.GetRequiredService<PostgreSqlOrchestrationService>());

        services.AddSingleton<IOrchestrationServiceClient>(sp =>
            sp.GetRequiredService<PostgreSqlOrchestrationService>());

        return services;
    }

    /// <summary>
    /// Adds PostgreSqlOrchestrationService with settings object.
    /// </summary>
    public static IServiceCollection AddDurableTaskPostgreSql(
        this IServiceCollection services,
        PostgreSqlOrchestrationServiceSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        services.AddSingleton(settings);

        services.AddSingleton<PostgreSqlOrchestrationService>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<PostgreSqlOrchestrationService>>();
            return new PostgreSqlOrchestrationService(settings, logger);
        });

        services.AddSingleton<IOrchestrationService>(sp =>
            sp.GetRequiredService<PostgreSqlOrchestrationService>());

        services.AddSingleton<IOrchestrationServiceClient>(sp =>
            sp.GetRequiredService<PostgreSqlOrchestrationService>());

        return services;
    }
}
