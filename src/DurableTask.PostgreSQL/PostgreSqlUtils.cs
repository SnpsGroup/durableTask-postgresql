#pragma warning disable CA2007 // ConfigureAwait
#pragma warning disable CA1849 // Call async methods
using System.Text.Json;
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Tracing;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace DurableTask.PostgreSQL;

static class PostgreSqlUtils
{
    public static TaskMessage GetTaskMessage(JsonElement eventElement, string instanceId, string? executionId, long sequenceNumber)
    {
        var eventType = eventElement.GetProperty("eventType").GetString() ?? throw new InvalidOperationException("EventType is required");
        
        return new TaskMessage
        {
            SequenceNumber = sequenceNumber,
            Event = GetHistoryEvent(eventElement),
            OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            },
        };
    }

    public static HistoryEvent GetHistoryEvent(JsonElement reader, bool isOrchestrationHistory = false)
    {
        var eventTypeString = reader.GetProperty("eventType").GetString() 
            ?? throw new InvalidOperationException("EventType is required");
        
        if (!Enum.TryParse<EventType>(eventTypeString, out var eventType))
        {
            throw new InvalidOperationException($"Unknown event type '{eventTypeString}'.");
        }

        int eventId = GetTaskId(reader);

        HistoryEvent historyEvent;
        switch (eventType)
        {
            case EventType.ContinueAsNew:
                historyEvent = new ContinueAsNewEvent(eventId, GetPayloadText(reader));
                break;
            case EventType.EventRaised:
                historyEvent = new EventRaisedEvent(eventId, GetPayloadText(reader))
                {
                    Name = GetName(reader),
                    ParentTraceContext = GetTraceContext(reader),
                };
                break;
            case EventType.EventSent:
                historyEvent = new EventSentEvent(eventId)
                {
                    Input = GetPayloadText(reader),
                    Name = GetName(reader),
                    InstanceId = GetStringProperty(reader, "instanceId"),
                };
                break;
            case EventType.ExecutionCompleted:
                historyEvent = new ExecutionCompletedEvent(
                    eventId,
                    result: GetPayloadText(reader),
                    orchestrationStatus: OrchestrationStatus.Completed);
                break;
            case EventType.ExecutionFailed:
                string? executionFailedResult = null;
                if (!TryGetFailureDetails(reader, out FailureDetails? executionFailedDetails))
                {
                    executionFailedResult = GetPayloadText(reader);
                }

                historyEvent = new ExecutionCompletedEvent(
                    eventId,
                    result: executionFailedResult,
                    orchestrationStatus: OrchestrationStatus.Failed,
                    failureDetails: executionFailedDetails);
                break;
            case EventType.ExecutionStarted:
                historyEvent = new ExecutionStartedEvent(eventId, GetPayloadText(reader))
                {
                    Name = GetName(reader),
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = GetStringProperty(reader, "instanceId"),
                        ExecutionId = GetStringProperty(reader, "executionId"),
                    },
                    Version = GetVersion(reader),
                    ParentTraceContext = GetTraceContext(reader),
                };
                string? parentInstanceId = GetStringProperty(reader, "parentInstanceId");
                if (parentInstanceId != null)
                {
                    ((ExecutionStartedEvent)historyEvent).ParentInstance = new ParentInstance
                    {
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = parentInstanceId
                        },
                        TaskScheduleId = GetTaskId(reader)
                    };
                }
                break;
            case EventType.ExecutionTerminated:
                historyEvent = new ExecutionTerminatedEvent(eventId, GetPayloadText(reader));
                break;
            case EventType.GenericEvent:
                historyEvent = new GenericEvent(eventId, GetPayloadText(reader));
                break;
            case EventType.OrchestratorCompleted:
                historyEvent = new OrchestratorCompletedEvent(eventId);
                break;
            case EventType.OrchestratorStarted:
                historyEvent = new OrchestratorStartedEvent(eventId);
                break;
            case EventType.SubOrchestrationInstanceCompleted:
                historyEvent = new SubOrchestrationInstanceCompletedEvent(eventId: -1, GetTaskId(reader), GetPayloadText(reader));
                break;
            case EventType.SubOrchestrationInstanceCreated:
                historyEvent = new SubOrchestrationInstanceCreatedEvent(eventId)
                {
                    Input = GetPayloadText(reader),
                    InstanceId = "",
                    Name = GetName(reader),
                    Version = null,
                };
                break;
            case EventType.SubOrchestrationInstanceFailed:
                string? subOrchFailedReason = null;
                string? subOrchFailedDetails = null;
                if (!TryGetFailureDetails(reader, out FailureDetails? subOrchFailureDetails))
                {
                    subOrchFailedReason = GetReason(reader);
                    subOrchFailedDetails = GetPayloadText(reader);
                }

                historyEvent = new SubOrchestrationInstanceFailedEvent(
                    eventId: -1,
                    taskScheduledId: GetTaskId(reader),
                    reason: subOrchFailedReason,
                    details: subOrchFailedDetails,
                    failureDetails: subOrchFailureDetails);
                break;
            case EventType.TaskCompleted:
                historyEvent = new TaskCompletedEvent(
                    eventId: -1,
                    taskScheduledId: GetTaskId(reader),
                    result: GetPayloadText(reader));
                break;
            case EventType.TaskFailed:
                string? taskFailedReason = null;
                string? taskFailedDetails = null;
                if (!TryGetFailureDetails(reader, out FailureDetails? taskFailureDetails))
                {
                    taskFailedReason = GetReason(reader);
                    taskFailedDetails = GetPayloadText(reader);
                }

                historyEvent = new TaskFailedEvent(
                    eventId: -1,
                    taskScheduledId: GetTaskId(reader),
                    reason: taskFailedReason,
                    details: taskFailedDetails,
                    failureDetails: taskFailureDetails);
                break;
            case EventType.TaskScheduled:
                historyEvent = new TaskScheduledEvent(eventId)
                {
                    Input = GetPayloadText(reader),
                    Name = GetName(reader),
                    Version = GetVersion(reader),
                    ParentTraceContext = GetTraceContext(reader),
                };
                break;
            case EventType.TimerCreated:
                historyEvent = new TimerCreatedEvent(eventId)
                {
                    FireAt = GetVisibleTime(reader) ?? DateTime.MinValue,
                };
                break;
            case EventType.TimerFired:
                historyEvent = new TimerFiredEvent(eventId: -1)
                {
                    FireAt = GetVisibleTime(reader) ?? DateTime.MinValue,
                    TimerId = GetTaskId(reader),
                };
                break;
            case EventType.ExecutionSuspended:
                historyEvent = new ExecutionSuspendedEvent(eventId, GetPayloadText(reader));
                break;
            case EventType.ExecutionResumed:
                historyEvent = new ExecutionResumedEvent(eventId, GetPayloadText(reader));
                break;
            default:
                throw new InvalidOperationException($"Don't know how to interpret '{eventType}'.");
        }

        if (reader.TryGetProperty("timestamp", out var timestampElement))
        {
            historyEvent.Timestamp = timestampElement.GetDateTime();
        }
        
        if (isOrchestrationHistory && reader.TryGetProperty("isPlayed", out var isPlayedElement))
        {
            historyEvent.IsPlayed = isPlayedElement.GetBoolean();
        }
        
        return historyEvent;
    }

    static bool TryGetFailureDetails(JsonElement reader, out FailureDetails? details)
    {
        string? text = GetPayloadText(reader);
        if (string.IsNullOrEmpty(text) || text![0] != '{')
        {
            details = null;
            return false;
        }

        return TryDeserializeFailureDetails(text, out details);
    }

    public static OrchestrationState GetOrchestrationState(NpgsqlDataReader reader)
    {
        ParentInstance? parentInstance = null;
        string? parentInstanceId = GetStringOrNull(reader, "parent_instance_id");
        if (parentInstanceId != null)
        {
            parentInstance = new ParentInstance
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = parentInstanceId
                }
            };
        }

        var state = new OrchestrationState
        {
            CompletedTime = GetUtcDateTime(reader, "completed_time") ?? default,
            CreatedTime = GetUtcDateTime(reader, "created_time") ?? default,
            Input = GetJsonStringOrNull(reader, "input_text"),
            LastUpdatedTime = GetUtcDateTime(reader, "last_updated_time") ?? default,
            Name = GetStringOrNull(reader, "name"),
            Version = GetStringOrNull(reader, "version"),
            OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = GetStringOrNull(reader, "instance_id") ?? string.Empty,
                ExecutionId = GetStringOrNull(reader, "execution_id"),
            },
            OrchestrationStatus = GetRuntimeStatus(reader),
            Status = GetJsonStringOrNull(reader, "custom_status_text"),
            ParentInstance = parentInstance
        };

        string? rawOutput = GetJsonStringOrNull(reader, "output_text");
        if (rawOutput != null)
        {
            if (state.OrchestrationStatus == OrchestrationStatus.Failed &&
                TryDeserializeFailureDetails(rawOutput, out FailureDetails? failureDetails))
            {
                state.FailureDetails = failureDetails;
            }
            else
            {
                state.Output = rawOutput;
            }
        }

        return state;
    }

    static DateTime? GetVisibleTime(JsonElement reader)
    {
        if (reader.TryGetProperty("visibleTime", out var visibleTimeElement) && visibleTimeElement.ValueKind != JsonValueKind.Null)
        {
            return visibleTimeElement.GetDateTime();
        }
        return null;
    }

    static string? GetPayloadText(JsonElement reader)
    {
        if (reader.TryGetProperty("payloadText", out var payloadElement) && payloadElement.ValueKind != JsonValueKind.Null)
        {
            if (payloadElement.ValueKind == JsonValueKind.String)
            {
                return payloadElement.GetString();
            }
            else if (payloadElement.ValueKind != JsonValueKind.Null)
            {
                return payloadElement.GetRawText();
            }
        }
        return null;
    }

    static string? GetName(JsonElement reader)
    {
        if (reader.TryGetProperty("name", out var nameElement) && nameElement.ValueKind != JsonValueKind.Null)
        {
            return nameElement.GetString();
        }
        return null;
    }

    static int GetTaskId(JsonElement reader)
    {
        if (reader.TryGetProperty("taskId", out var taskIdElement) && taskIdElement.ValueKind != JsonValueKind.Null)
        {
            return taskIdElement.GetInt32();
        }
        return -1;
    }

    static long GetSequenceNumber(JsonElement reader)
    {
        if (reader.TryGetProperty("sequenceNumber", out var seqElement) && seqElement.ValueKind != JsonValueKind.Null)
        {
            return seqElement.GetInt64();
        }
        return -1;
    }

    static string? GetVersion(JsonElement reader)
    {
        if (reader.TryGetProperty("version", out var versionElement) && versionElement.ValueKind != JsonValueKind.Null)
        {
            return versionElement.GetString();
        }
        return null;
    }

    static string? GetReason(JsonElement reader)
    {
        if (reader.TryGetProperty("reason", out var reasonElement) && reasonElement.ValueKind != JsonValueKind.Null)
        {
            return reasonElement.GetString();
        }
        return null;
    }

    static string? GetStringProperty(JsonElement reader, string propertyName)
    {
        if (reader.TryGetProperty(propertyName, out var element) && element.ValueKind != JsonValueKind.Null)
        {
            return element.GetString();
        }
        return null;
    }

    static DistributedTraceContext? GetTraceContext(JsonElement reader)
    {
        if (!reader.TryGetProperty("traceContext", out var traceElement) || traceElement.ValueKind == JsonValueKind.Null || string.IsNullOrEmpty(traceElement.GetString()))
        {
            return null;
        }

        var text = traceElement.GetString()!;
        var parts = text.Split('\n', 2);
        var traceContext = new DistributedTraceContext(traceParent: parts[0]);

        if (parts.Length > 1)
        {
            traceContext.TraceState = parts[1];
        }

        if (reader.TryGetProperty("timestamp", out var timestampElement))
        {
            traceContext.ActivityStartTime = timestampElement.GetDateTime();
        }

        return traceContext;
    }

    static OrchestrationStatus GetRuntimeStatus(NpgsqlDataReader reader)
    {
        var runtimeStatus = GetStringOrNull(reader, "runtime_status");
        return Enum.Parse<OrchestrationStatus>(runtimeStatus ?? "Pending");
    }

    static string? GetStringOrNull(NpgsqlDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    static string? GetJsonStringOrNull(NpgsqlDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        var value = reader.GetValue(ordinal);
        if (value is JsonElement jsonElement)
        {
            return jsonElement.ValueKind == JsonValueKind.Null ? null : jsonElement.GetRawText();
        }
        
        return value?.ToString();
    }

    static DateTime? GetUtcDateTime(NpgsqlDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        var dateTime = reader.GetDateTime(ordinal);
        return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
    }

    static bool TryDeserializeFailureDetails(string jsonText, out FailureDetails? failureDetails)
    {
        try
        {
            failureDetails = JsonSerializer.Deserialize<FailureDetails>(jsonText);
            return failureDetails?.ErrorType != null;
        }
        catch (JsonException)
        {
            failureDetails = null;
            return false;
        }
    }

    public static NpgsqlParameter AddArrayParameter<T>(this NpgsqlParameterCollection parameters, string parameterName, T[]? values, NpgsqlDbType dbType)
    {
        var parameter = parameters.Add(parameterName, dbType);
        if (values != null && values.Length > 0)
        {
            parameter.Value = values;
        }
        else
        {
            parameter.Value = Array.Empty<T>();
        }
        return parameter;
    }

    public static async Task<int> ExecuteNonQueryAsync(
        NpgsqlCommand command,
        ILogger logger,
        string? instanceId = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing command for instance {InstanceId}", instanceId ?? "unknown");
            throw;
        }
    }

    public static async Task<NpgsqlDataReader> ExecuteReaderAsync(
        NpgsqlCommand command,
        ILogger logger,
        string? instanceId = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing reader for instance {InstanceId}", instanceId ?? "unknown");
            throw;
        }
    }
}
