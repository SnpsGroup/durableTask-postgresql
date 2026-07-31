using System.Text.Json;
using DurableTask.Core.History;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="PostgreSqlUtils.GetHistoryEvent"/>. Regression coverage for
/// GitHub issue #3 part 1: history-event timestamps materialized from JSON must have
/// <see cref="DateTimeKind.Utc"/> so the DTFx v2 gRPC sidecar's
/// <c>Timestamp.FromDateTime</c> does not throw.
/// </summary>
public sealed class HistoryEventMaterializationTests
{
    private static JsonElement Parse(string json) =>
        JsonDocument.Parse(json).RootElement;

    [Fact]
    public void GetHistoryEvent_Timestamp_IsUtc()
    {
        // PostgreSQL jsonb serializes timestamptz as an ISO string without a guaranteed
        // 'Z' suffix; JsonElement.GetDateTime then yields DateTimeKind.Unspecified/Local.
        // The provider must normalize to Utc.
        JsonElement element = Parse(/*lang=json,strict*/
            "{\"eventType\":\"ExecutionStarted\",\"taskId\":-1,\"timestamp\":\"2026-07-30T12:34:56.789\"}");

        HistoryEvent e = PostgreSqlUtils.GetHistoryEvent(element);

        Assert.Equal(DateTimeKind.Utc, e.Timestamp.Kind);
    }

    [Fact]
    public void GetHistoryEvent_TimerFireAt_IsUtc()
    {
        JsonElement element = Parse(/*lang=json,strict*/
            "{\"eventType\":\"TimerCreated\",\"taskId\":5,\"visibleTime\":\"2026-07-30T12:34:56\",\"timestamp\":\"2026-07-30T12:34:56\"}");

        var timer = Assert.IsType<TimerCreatedEvent>(PostgreSqlUtils.GetHistoryEvent(element));

        Assert.Equal(DateTimeKind.Utc, timer.FireAt.Kind);
        Assert.Equal(DateTimeKind.Utc, timer.Timestamp.Kind);
    }
}
