using Xunit;

namespace DurableTask.PostgreSQL.Tests.Integration;

/// <summary>
/// All integration tests share a single physical PostgreSQL instance and the default
/// <c>dt</c> schema/task hub, so they must run sequentially. Putting them in one collection
/// disables xUnit's class-level parallelization between them, avoiding races on shared
/// schema state and work-item locking.
/// </summary>
[CollectionDefinition("integration")]
public sealed class IntegrationTestCollection
{
}
