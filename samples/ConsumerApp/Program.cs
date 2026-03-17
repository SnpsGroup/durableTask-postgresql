#pragma warning disable CA1303
using System;
using DurableTask.PostgreSQL;

namespace ConsumerApp;

public static class Program
{
    public static void Main()
    {
        Console.WriteLine("Testing DurableTask.PostgreSQL Consumer...");
        
        var serviceType = typeof(PostgreSqlOrchestrationService);
        var settingsType = typeof(PostgreSqlOrchestrationServiceSettings);
        
        Console.WriteLine($"Successfully loaded Types: {serviceType.Name}, {settingsType.Name}");
        Console.WriteLine("All types successfully loaded and parsed from the local NuGet package!");
    }
}
