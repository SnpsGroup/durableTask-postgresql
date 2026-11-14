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
    [Theory]
    [InlineData("2026-11-09T02:00:00.123+00:00")]
    [InlineData("2026-11-09T12:00:00.123+10:00")]
    [InlineData("2026-11-08T21:00:00.123-05:00")]
    [InlineData("2026-11-09T02:00:00.123Z")]
    [InlineData("2026-11-09T02:00:00.123")]
    public void GetHistoryEvent_PreservesUtcInstant_ForHistoryTimersAndTrace(string timestamp)
    {
        // Offset-free legacy JSON represents UTC. Offset-bearing JSON must preserve
        // the instant regardless of the worker's local timezone (issue #18).
        var expected = new DateTime(2026, 11, 9, 2, 0, 0, 123, DateTimeKind.Utc);
        foreach (string eventType in new[] { "ExecutionStarted", "TaskScheduled", "TimerCreated", "TimerFired" })
        {
            var element = JsonSerializer.SerializeToElement(new
            {
                eventType,
                taskId = 5,
                timestamp,
                visibleTime = timestamp,
                traceContext = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            });
            HistoryEvent historyEvent = PostgreSqlUtils.GetHistoryEvent(element);

            AssertUtcInstant(historyEvent.Timestamp, expected);
            switch (historyEvent)
            {
                case TimerCreatedEvent created:
                    AssertUtcInstant(created.FireAt, expected);
                    break;
                case TimerFiredEvent fired:
                    AssertUtcInstant(fired.FireAt, expected);
                    break;
                case ExecutionStartedEvent started:
                    Assert.NotNull(started.ParentTraceContext);
                    Assert.Equal(new DateTimeOffset(expected), started.ParentTraceContext.ActivityStartTime);
                    break;
                case TaskScheduledEvent scheduled:
                    Assert.NotNull(scheduled.ParentTraceContext);
                    Assert.Equal(new DateTimeOffset(expected), scheduled.ParentTraceContext.ActivityStartTime);
                    break;
            }
        }
    }

    private static void AssertUtcInstant(DateTime actual, DateTime expected)
    {
        Assert.Equal(expected, actual);
        Assert.Equal(DateTimeKind.Utc, actual.Kind);
    }

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
