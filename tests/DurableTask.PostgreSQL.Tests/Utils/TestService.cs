using Microsoft.Extensions.Logging;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Utils;

using System.Text.Json;
using DurableTask.Core;
using DurableTask.Core.History;

public sealed class TestService : IAsyncLifetime
{
    private readonly string _connectionString;
    private ILoggerFactory _loggerFactory;
    private ILogger<TestService> _logger;
    private PostgreSqlOrchestrationService? _orchestrationService;
    private PostgreSqlOrchestrationService? _clientService;
    private readonly List<LogEntry> _logs = [];

    public TestService(string connectionString)
    {
        _connectionString = connectionString;
        var builder = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddDebug();
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(new TestLoggerProvider(_logs));
        });
        _loggerFactory = builder;
        _logger = _loggerFactory.CreateLogger<TestService>();
    }

    public IList<LogEntry> LogEntries => _logs;
    public ILoggerFactory LoggerFactory => _loggerFactory;
    public PostgreSqlOrchestrationService OrchestrationService => _orchestrationService!;
    public PostgreSqlOrchestrationService ClientService => _clientService!;

    public async Task InitializeAsync()
    {
        var settings = new PostgreSqlOrchestrationServiceSettings
        {
            ConnectionString = _connectionString,
            TaskHubName = "TestHub",
            MaxConcurrentOrchestrations = 10,
            MaxConcurrentActivities = 10,
            AutoDeploySchema = true,
        };

        _orchestrationService = new PostgreSqlOrchestrationService(settings, _loggerFactory.CreateLogger<PostgreSqlOrchestrationService>());
        _clientService = new PostgreSqlOrchestrationService(settings, _loggerFactory.CreateLogger<PostgreSqlOrchestrationService>());

        await _orchestrationService.CreateAsync();
        await _orchestrationService.StartAsync();
        await _clientService.StartAsync();

        _logger.LogInformation("TestService initialized");
    }

    public async Task DisposeAsync()
    {
        if (_orchestrationService != null)
        {
            await _orchestrationService.StopAsync();
            _orchestrationService.Dispose();
        }

        if (_clientService != null)
        {
            await _clientService.StopAsync();
            _clientService.Dispose();
        }

        _loggerFactory.Dispose();
    }
}

public sealed class TestLoggerProvider : ILoggerProvider
{
    private readonly List<LogEntry> _logs;

    public TestLoggerProvider(List<LogEntry> logs)
    {
        _logs = logs;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new TestLogger(categoryName, _logs);
    }

    public void Dispose() { }
}

public sealed class TestLogger : ILogger
{
    private readonly string _categoryName;
    private readonly List<LogEntry> _logs;

    public TestLogger(string categoryName, List<LogEntry> logs)
    {
        _categoryName = categoryName;
        _logs = logs;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        _logs.Add(new LogEntry
        {
            Timestamp = DateTime.UtcNow,
            LogLevel = logLevel,
            Category = _categoryName,
            Message = formatter(state, exception),
            Exception = exception
        });
    }
}

public class LogEntry
{
    public DateTime Timestamp { get; init; }
    public LogLevel LogLevel { get; init; }
    public string Category { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public Exception? Exception { get; init; }
}

public static class LogAssert
{
    public static void NoWarningsOrErrors(List<LogEntry> logs)
    {
        var warningsOrErrors = logs.Where(l => l.LogLevel >= LogLevel.Warning).ToList();
        Assert.Empty(warningsOrErrors);
    }

    public static void Sequence(List<LogEntry> logs, params Action<LogEntry>[] assertions)
    {
        Assert.Equal(assertions.Length, logs.Count);

        for (int i = 0; i < assertions.Length; i++)
        {
            assertions[i](logs[i]);
        }
    }

    public static Action<LogEntry> Contains(string message)
    {
        return entry => Assert.Contains(message, entry.Message);
    }

    public static Action<LogEntry> NotContains(string message)
    {
        return entry => Assert.DoesNotContain(message, entry.Message);
    }
}
