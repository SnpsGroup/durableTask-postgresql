using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

/// <summary>
/// Unit tests for the sentinel used when an <c>OrchestrationState</c> timestamp column is NULL.
/// Regression coverage for GitHub issue #8 (residue of #3): <c>CompletedTime</c>,
/// <c>CreatedTime</c> and <c>LastUpdatedTime</c> must carry <see cref="DateTimeKind.Utc"/> even
/// when absent, so the DTFx v2 gRPC sidecar's <c>Timestamp.FromDateTime</c> does not throw.
/// </summary>
/// <remarks>
/// <c>PostgreSqlUtils.GetOrchestrationState</c> itself takes a sealed <c>NpgsqlDataReader</c>,
/// which cannot be faked; the end-to-end read path is covered by
/// <c>Integration.OrchestrationStateTimestampTests</c> against a real database. These tests pin
/// the sentinel's two load-bearing properties instead.
/// </remarks>
public sealed class OrchestrationStateTimestampTests
{
    [Fact]
    public void UnsetUtcTimestamp_IsUtcKind()
    {
        // default(DateTime).Kind is Unspecified — the exact defect behind issue #8.
        Assert.Equal(DateTimeKind.Unspecified, default(DateTime).Kind);

        Assert.Equal(DateTimeKind.Utc, PostgreSqlUtils.UnsetUtcTimestamp.Kind);
    }

    [Fact]
    public void UnsetUtcTimestamp_KeepsDefaultTicks()
    {
        // Equality on DateTime ignores Kind, so callers that test `== default` to detect an
        // absent timestamp keep working. Guards against "fixing" the Kind with something like
        // DateTime.MinValue.ToUniversalTime(), which shifts the ticks by the local offset.
        Assert.Equal(0L, PostgreSqlUtils.UnsetUtcTimestamp.Ticks);
        Assert.Equal(default, PostgreSqlUtils.UnsetUtcTimestamp);
    }
}
