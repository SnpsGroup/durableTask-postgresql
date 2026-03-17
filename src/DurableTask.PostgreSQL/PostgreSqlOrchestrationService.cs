#pragma warning disable CA2007 // ConfigureAwait
#pragma warning disable CA2100 // SQL injection review
#pragma warning disable CA1849 // Call async methods
#pragma warning disable CA1062 // Validate arguments
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Query;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Text.Json;

namespace DurableTask.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of IOrchestrationService for DurableTask Framework.
/// Provides durable workflow orchestration using PostgreSQL 17+ as the backend.
/// </summary>
public sealed class PostgreSqlOrchestrationService : IOrchestrationService, IOrchestrationServiceClient, IDisposable
{
    private readonly PostgreSqlOrchestrationServiceSettings _settings;
    private readonly ILogger<PostgreSqlOrchestrationService> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly CancellationTokenSource _shutdownTokenSource = new();

    public PostgreSqlOrchestrationService(
        PostgreSqlOrchestrationServiceSettings settings,
        ILogger<PostgreSqlOrchestrationService> logger)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Create data source (connection pool)
        _dataSource = NpgsqlDataSource.Create(_settings.ConnectionString);

        _logger.LogInformation(
            "PostgreSqlOrchestrationService initialized with TaskHub={TaskHub}, WorkerId={WorkerId}",
            _settings.TaskHubName ?? "default",
            _settings.WorkerId);
    }

    // =============================================================================
    // IOrchestrationService Implementation
    // =============================================================================

    public int MaxConcurrentTaskOrchestrationWorkItems => _settings.MaxConcurrentOrchestrations;
    public int MaxConcurrentTaskActivityWorkItems => _settings.MaxConcurrentActivities;
    public int TaskOrchestrationDispatcherCount => 1; // Single dispatcher for simplicity
    public int TaskActivityDispatcherCount => 1;
    public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

    public Task CreateAsync() => CreateAsync(recreateInstanceStore: false);

    public async Task CreateAsync(bool recreateInstanceStore)
    {
        if (recreateInstanceStore)
        {
            _logger.LogWarning("Recreate instance store requested - this will DROP all data!");
            throw new NotSupportedException("Recreate not supported in production. Deploy schema manually.");
        }

        if (_settings.AutoDeploySchema)
        {
            _logger.LogInformation("Auto-deploying schema...");
            await DeploySchemaAsync().ConfigureAwait(false);
        }

        _logger.LogInformation("PostgreSqlOrchestrationService created successfully");
    }

    public Task CreateIfNotExistsAsync() => CreateAsync(recreateInstanceStore: false);

    public async Task StartAsync()
    {
        _logger.LogInformation("Starting PostgreSqlOrchestrationService...");

        // Verify connectivity
        await using var connection = await _dataSource.OpenConnectionAsync(_shutdownTokenSource.Token).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.current_task_hub()", connection);
        var taskHub = await cmd.ExecuteScalarAsync(_shutdownTokenSource.Token).ConfigureAwait(false) as string;

        _logger.LogInformation("Connected to PostgreSQL. TaskHub={TaskHub}", taskHub);
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping PostgreSqlOrchestrationService...");

        _shutdownTokenSource.Cancel();

        await _dataSource.DisposeAsync().ConfigureAwait(false);

        _logger.LogInformation("PostgreSqlOrchestrationService stopped");
    }

    public Task DeleteAsync() => DeleteAsync(deleteInstanceStore: false);

    public Task DeleteAsync(bool deleteInstanceStore)
    {
        if (deleteInstanceStore)
        {
            throw new NotSupportedException("Delete instance store not supported. Drop schema manually.");
        }
        return Task.CompletedTask;
    }

    public async Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(
                "SELECT * FROM dt.lock_next_orchestration($1, $2, $3)",
                connection);

            cmd.Parameters.AddWithValue(_settings.MaxConcurrentOrchestrations);
            cmd.Parameters.AddWithValue(_settings.WorkerId);
            cmd.Parameters.AddWithValue(lockExpiration);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return null; // No work available
            }

            var instanceId = reader.GetString(reader.GetOrdinal("instance_id"));
            var executionId = reader.GetString(reader.GetOrdinal("execution_id"));
            var runtimeStatus = reader.GetString(reader.GetOrdinal("runtime_status"));

            // Parse new events JSON
            var newEventsJson = reader.GetString(reader.GetOrdinal("new_events"));
            var newEventsArray = JsonSerializer.Deserialize<JsonElement[]>(newEventsJson) ?? [];

            // Parse history JSON
            var historyJson = reader.GetString(reader.GetOrdinal("history"));
            var historyArray = JsonSerializer.Deserialize<JsonElement[]>(historyJson) ?? [];

            _logger.LogDebug(
                "Locked orchestration {InstanceId} (execution {ExecutionId}) with {EventCount} events and {HistoryCount} history items",
                instanceId, executionId, newEventsArray.Length, historyArray.Length);

            // Build task messages from new events
            var messages = new List<TaskMessage>(newEventsArray.Length);
            foreach (var eventElement in newEventsArray)
            {
                var sequenceNumber = eventElement.TryGetProperty("sequenceNumber", out var seqEl) 
                    ? seqEl.GetInt64() 
                    : 0;
                var eventInstanceId = eventElement.TryGetProperty("instanceId", out var instEl) 
                    ? instEl.GetString() 
                    : instanceId;
                var eventExecutionId = eventElement.TryGetProperty("executionId", out var execEl) 
                    ? execEl.GetString() 
                    : executionId;

                var taskMessage = PostgreSqlUtils.GetTaskMessage(eventElement, eventInstanceId ?? instanceId, eventExecutionId, sequenceNumber);
                messages.Add(taskMessage);
            }

            // Build history events
            var history = new List<HistoryEvent>(historyArray.Length);
            foreach (var historyElement in historyArray)
            {
                var historyEvent = PostgreSqlUtils.GetHistoryEvent(historyElement, isOrchestrationHistory: true);
                history.Add(historyEvent);
            }

            var runtimeState = new OrchestrationRuntimeState(history);

            // Determine orchestration name and instance
            string orchestrationName;
            OrchestrationInstance instance;
            if (runtimeState.ExecutionStartedEvent != null)
            {
                orchestrationName = runtimeState.Name;
                instance = runtimeState.OrchestrationInstance!;
            }
            else if (messages.Count > 0 && messages[0].Event is ExecutionStartedEvent startedEvent)
            {
                orchestrationName = startedEvent.Name;
                instance = startedEvent.OrchestrationInstance;
            }
            else
            {
                orchestrationName = "(Unknown)";
                instance = new OrchestrationInstance { InstanceId = instanceId, ExecutionId = executionId };
            }

            // Check if instance is in a terminal state
            var isRunning = runtimeStatus == "Running" || runtimeStatus == "Suspended" || runtimeStatus == "Pending";
            if (!isRunning)
            {
                _logger.LogWarning(
                    "Target orchestration {InstanceId} is in {Status} state. Discarding {EventCount} events.",
                    instanceId, runtimeStatus, messages.Count);

                // Discard events and release lock
                await using var discardCmd = new NpgsqlCommand(
                    "UPDATE dt.instances SET locked_by = NULL, lock_expiration = NULL WHERE instance_id = $1",
                    connection);
                discardCmd.Parameters.AddWithValue(instanceId);
                await discardCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                return null;
            }

            return new ExtendedOrchestrationWorkItem(orchestrationName, instance)
            {
                InstanceId = instanceId,
                LockedUntilUtc = lockExpiration.DateTime,
                NewMessages = messages,
                OrchestrationRuntimeState = runtimeState,
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error locking next orchestration work item");
            return null;
        }
    }

    sealed class ExtendedOrchestrationWorkItem : TaskOrchestrationWorkItem
    {
        public ExtendedOrchestrationWorkItem(string name, OrchestrationInstance instance)
        {
            this.Name = name;
            this.Instance = instance;
        }

        public string Name { get; }
        public OrchestrationInstance Instance { get; }
    }

    public async Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(
                "SELECT * FROM dt.lock_next_task($1, $2)",
                connection);

            cmd.Parameters.AddWithValue(_settings.WorkerId);
            cmd.Parameters.AddWithValue(lockExpiration);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return null; // No work available
            }

            var sequenceNumber = reader.GetInt64(reader.GetOrdinal("sequence_number"));
            var instanceId = reader.GetString(reader.GetOrdinal("instance_id"));
            var executionId = reader.GetString(reader.GetOrdinal("execution_id"));
            var name = reader.GetString(reader.GetOrdinal("name"));
            var taskId = reader.GetInt32(reader.GetOrdinal("task_id"));
            var dequeueCount = reader.GetInt32(reader.GetOrdinal("dequeue_count"));
            var version = reader.IsDBNull(reader.GetOrdinal("version")) ? null : reader.GetString(reader.GetOrdinal("version"));

            JsonElement? payloadText = null;
            if (!reader.IsDBNull(reader.GetOrdinal("payload_text")))
            {
                payloadText = (JsonElement)reader.GetValue(reader.GetOrdinal("payload_text"));
            }

            _logger.LogDebug(
                "Locked task {SequenceNumber} for instance {InstanceId} (TaskId={TaskId})",
                sequenceNumber, instanceId, taskId);

            // Create TaskScheduledEvent
            var scheduledEvent = new TaskScheduledEvent(taskId)
            {
                Name = name,
                Version = version,
                Input = payloadText?.GetRawText(),
            };

            var taskMessage = new TaskMessage
            {
                SequenceNumber = sequenceNumber,
                Event = scheduledEvent,
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                },
            };

            return new TaskActivityWorkItem
            {
                Id = $"{instanceId}:{taskId:X16}",
                TaskMessage = taskMessage,
                LockedUntilUtc = lockExpiration.DateTime,
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error locking next task activity");
            return null;
        }
    }

    public async Task CompleteTaskOrchestrationWorkItemAsync(
        TaskOrchestrationWorkItem workItem,
        OrchestrationRuntimeState newOrchestrationRuntimeState,
        IList<TaskMessage>? outboundMessages,
        IList<TaskMessage>? orchestratorMessages,
        IList<TaskMessage>? timerMessages,
        TaskMessage? continuedAsNewMessage,
        OrchestrationState? orchestrationState)
    {
        if (orchestrationState is null || !newOrchestrationRuntimeState.IsValid)
        {
            return;
        }

        _logger.LogDebug(
            "Checkpointing orchestration {InstanceId}, status={Status}, newEvents={EventCount}",
            workItem.InstanceId, orchestrationState.OrchestrationStatus, newOrchestrationRuntimeState.NewEvents?.Count ?? 0);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.checkpoint_orchestration($1, $2, $3, $4, $5, $6, $7, $8)", connection);

        var instance = newOrchestrationRuntimeState.OrchestrationInstance!;
        var newEvents = newOrchestrationRuntimeState.NewEvents ?? [];
        var allEvents = newOrchestrationRuntimeState.Events;
        int nextSequenceNumber = allEvents.Count - newEvents.Count;

        cmd.Parameters.AddWithValue(workItem.InstanceId);
        cmd.Parameters.AddWithValue(instance.ExecutionId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(orchestrationState.OrchestrationStatus.ToString());
        
        // Custom status - serialize to JSON
        var customStatus = orchestrationState.Status;
        if (!string.IsNullOrEmpty(customStatus))
        {
            cmd.Parameters.AddWithValue(customStatus);
        }
        else
        {
            cmd.Parameters.AddWithValue(DBNull.Value);
        }

        // Deleted events (message IDs)
        var deletedEvents = workItem.NewMessages?.Select(m => 
            $"(\"{m.OrchestrationInstance.InstanceId}\",{m.SequenceNumber})").ToArray() ?? [];
        if (deletedEvents.Length > 0)
        {
            cmd.Parameters.AddWithValue($"{{{string.Join(",", deletedEvents)}}}");
        }
        else
        {
            cmd.Parameters.AddWithValue("{}");
        }

        // New history events
        var historyEvents = new List<string>();
        foreach (var evt in newEvents)
        {
            var historyJson = BuildHistoryEventJson(evt, instance, nextSequenceNumber++);
            historyEvents.Add(historyJson);
        }
        if (historyEvents.Count > 0)
        {
            cmd.Parameters.AddWithValue($"[{string.Join(",", historyEvents)}]");
        }
        else
        {
            cmd.Parameters.AddWithValue("[]");
        }

        // New orchestration events (sub-orchestrations) + timer messages
        var orchestrationEvents = new List<string>();
        if (orchestratorMessages != null)
        {
            foreach (var msg in orchestratorMessages)
            {
                if (msg.Event is ExecutionStartedEvent)
                {
                    orchestrationEvents.Add(BuildOrchestrationEventJson(msg));
                }
            }
        }
        if (timerMessages != null)
        {
            foreach (var msg in timerMessages)
            {
                if (msg.Event is TimerCreatedEvent || msg.Event is TimerFiredEvent)
                {
                    orchestrationEvents.Add(BuildOrchestrationEventJson(msg));
                }
            }
        }
        if (orchestrationEvents.Count > 0)
        {
            cmd.Parameters.AddWithValue($"[{string.Join(",", orchestrationEvents)}]");
        }
        else
        {
            cmd.Parameters.AddWithValue("[]");
        }

        // New task events (activity tasks)
        var taskEvents = new List<string>();
        if (outboundMessages != null)
        {
            foreach (var msg in outboundMessages)
            {
                if (msg.Event is TaskScheduledEvent scheduledEvent)
                {
                    taskEvents.Add(BuildTaskEventJson(msg, scheduledEvent));
                }
            }
        }
        if (taskEvents.Count > 0)
        {
            cmd.Parameters.AddWithValue($"[{string.Join(",", taskEvents)}]");
        }
        else
        {
            cmd.Parameters.AddWithValue("[]");
        }

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "Checkpoint completed for orchestration {InstanceId}",
            workItem.InstanceId);
    }

    static string BuildHistoryEventJson(HistoryEvent evt, OrchestrationInstance instance, int sequenceNumber)
    {
        var eventType = evt.EventType.ToString();
        var name = GetEventName(evt);
        var version = GetEventVersion(evt);
        var reason = GetEventReason(evt);
        var payloadText = GetEventPayload(evt);
        var taskId = GetTaskEventId(evt);
        var visibleTime = GetEventVisibleTime(evt);
        var isPlayed = evt.IsPlayed;
        var parentInstanceId = GetParentInstanceId(evt);
        var traceContext = GetTraceContext(evt);

        return $@"{{
            ""instanceId"":""{instance.InstanceId}"",
            ""executionId"":""{instance.ExecutionId}"",
            ""sequenceNumber"":{sequenceNumber},
            ""eventType"":""{eventType}"",
            ""name"":{name},
            ""runtimeStatus"":""{GetRuntimeStatus(evt)}"",
            ""taskId"":{taskId},
            ""timestamp"":""{DateTime.UtcNow:O}"",
            ""isPlayed"":{isPlayed.ToString().ToUpperInvariant()},
            ""visibleTime"":{visibleTime},
            ""reason"":{reason},
            ""payloadText"":{payloadText},
            ""payloadId"":null,
            ""parentInstanceId"":{parentInstanceId},
            ""version"":{version},
            ""traceContext"":{traceContext}
        }}";
    }

    static string BuildOrchestrationEventJson(TaskMessage msg)
    {
        var eventType = msg.Event.EventType.ToString();
        var name = GetEventName(msg.Event);
        var version = GetEventVersion(msg.Event);
        var reason = GetEventReason(msg.Event);
        var payloadText = GetEventPayload(msg.Event);
        var taskId = GetTaskEventId(msg.Event);
        var visibleTime = GetEventVisibleTime(msg.Event);
        var parentInstanceId = GetParentInstanceId(msg.Event);
        var traceContext = GetTraceContext(msg.Event);

        return $@"{{
            ""instanceId"":""{msg.OrchestrationInstance.InstanceId}"",
            ""executionId"":""{msg.OrchestrationInstance.ExecutionId}"",
            ""eventType"":""{eventType}"",
            ""name"":{name},
            ""runtimeStatus"":""Pending"",
            ""taskId"":{taskId},
            ""visibleTime"":{visibleTime},
            ""reason"":{reason},
            ""payloadText"":{payloadText},
            ""payloadId"":null,
            ""parentInstanceId"":{parentInstanceId},
            ""version"":{version},
            ""traceContext"":{traceContext}
        }}";
    }

    static string BuildTaskEventJson(TaskMessage msg, TaskScheduledEvent scheduledEvent)
    {
        var name = scheduledEvent.Name ?? "Unknown";
        var version = scheduledEvent.Version ?? "";
        var input = scheduledEvent.Input ?? "";
        var traceContext = GetTraceContext(msg.Event);

        return $@"{{
            ""instanceId"":""{msg.OrchestrationInstance.InstanceId}"",
            ""executionId"":""{msg.OrchestrationInstance.ExecutionId}"",
            ""name"":""{name}"",
            ""eventType"":""TaskScheduled"",
            ""taskId"":{scheduledEvent.EventId},
            ""visibleTime"":null,
            ""lockedBy"":null,
            ""lockExpiration"":null,
            ""reason"":null,
            ""payloadText"":{EscapeJsonString(input)},
            ""payloadId"":null,
            ""version"":""{version}"",
            ""traceContext"":{traceContext}
        }}";
    }

    static string? GetEventName(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.EventRaised => ((EventRaisedEvent)evt).Name,
            EventType.EventSent => ((EventSentEvent)evt).Name,
            EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).Name,
            EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)evt).Name,
            EventType.TaskScheduled => ((TaskScheduledEvent)evt).Name,
            _ => null,
        } is string s ? $"\"{s}\"" : "null";
    }

    static string? GetEventVersion(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).Version,
            EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)evt).Version,
            EventType.TaskScheduled => ((TaskScheduledEvent)evt).Version,
            _ => null,
        } is string s ? $"\"{s}\"" : "null";
    }

    static string? GetEventReason(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.ExecutionTerminated => ((ExecutionTerminatedEvent)evt).Input,
            EventType.TaskFailed => ((TaskFailedEvent)evt).Reason,
            EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)evt).Reason,
            _ => null,
        } is string s ? $"\"{EscapeJsonString(s)}\"" : "null";
    }

    static string? GetEventPayload(HistoryEvent evt)
    {
        string? payload = evt.EventType switch
        {
            EventType.ContinueAsNew => ((ContinueAsNewEvent)evt).Result,
            EventType.EventRaised => ((EventRaisedEvent)evt).Input,
            EventType.EventSent => ((EventSentEvent)evt).Input,
            EventType.ExecutionCompleted => ((ExecutionCompletedEvent)evt).Result,
            EventType.ExecutionFailed => ((ExecutionCompletedEvent)evt).Result,
            EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).Input,
            EventType.ExecutionTerminated => ((ExecutionTerminatedEvent)evt).Input,
            EventType.GenericEvent => ((GenericEvent)evt).Data,
            EventType.SubOrchestrationInstanceCompleted => ((SubOrchestrationInstanceCompletedEvent)evt).Result,
            EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)evt).Input,
            EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)evt).Details,
            EventType.TaskCompleted => ((TaskCompletedEvent)evt).Result,
            EventType.TaskFailed => ((TaskFailedEvent)evt).Details,
            EventType.TaskScheduled => ((TaskScheduledEvent)evt).Input,
            _ => null,
        };
        
        return payload != null ? EscapeJsonString(payload) : "null";
    }

    static int GetTaskEventId(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.TaskCompleted => ((TaskCompletedEvent)evt).TaskScheduledId,
            EventType.TaskFailed => ((TaskFailedEvent)evt).TaskScheduledId,
            EventType.SubOrchestrationInstanceCompleted => ((SubOrchestrationInstanceCompletedEvent)evt).TaskScheduledId,
            EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)evt).TaskScheduledId,
            EventType.TimerFired => ((TimerFiredEvent)evt).TimerId,
            _ => evt.EventId,
        };
    }

    static string? GetEventVisibleTime(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.TimerCreated => ((TimerCreatedEvent)evt).FireAt.ToString("O"),
            EventType.TimerFired => ((TimerFiredEvent)evt).FireAt.ToString("O"),
            _ => "null",
        };
    }

    static string? GetRuntimeStatus(HistoryEvent evt)
    {
        return evt.EventType switch
        {
            EventType.ExecutionCompleted => ((ExecutionCompletedEvent)evt).OrchestrationStatus.ToString(),
            EventType.ExecutionFailed => ((ExecutionCompletedEvent)evt).OrchestrationStatus.ToString(),
            _ => "null",
        };
    }

    static string? GetParentInstanceId(HistoryEvent evt)
    {
        if (evt.EventType == EventType.ExecutionStarted)
        {
            var parent = ((ExecutionStartedEvent)evt).ParentInstance;
            return parent != null ? $"\"{parent.OrchestrationInstance.InstanceId}\"" : "null";
        }
        return "null";
    }

    static string? GetTraceContext(HistoryEvent evt)
    {
        if (evt is ISupportsDurableTraceContext traceEvent && traceEvent.ParentTraceContext != null)
        {
            return $"\"{traceEvent.ParentTraceContext.TraceParent}\"";
        }
        return "null";
    }

    static string EscapeJsonString(string? s)
    {
        if (s == null) 
        {
            return "null";
        }

        return $"\"{s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal).Replace("\n", "\\n", StringComparison.Ordinal).Replace("\r", "\\r", StringComparison.Ordinal)}\"";
    }

    public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
    {
        _logger.LogDebug(
            "Completing task activity for instance {InstanceId}, event type={EventType}",
            workItem.TaskMessage.OrchestrationInstance.InstanceId, responseMessage.Event.EventType);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.complete_tasks($1, $2)", connection);

        // Sequence numbers to complete
        cmd.Parameters.AddWithValue(new[] { workItem.TaskMessage.SequenceNumber });

        // Build task result
        var result = BuildTaskResultJson(responseMessage);
        cmd.Parameters.AddWithValue($"[{result}]");

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        var instance = workItem.TaskMessage.OrchestrationInstance;
        _logger.LogDebug(
            "Task activity completed for instance {InstanceId}",
            instance.InstanceId);
    }

    static string BuildTaskResultJson(TaskMessage msg)
    {
        var instance = msg.OrchestrationInstance;
        var eventType = msg.Event.EventType.ToString();
        var name = GetEventName(msg.Event);
        var payloadText = GetEventPayload(msg.Event);
        var reason = GetEventReason(msg.Event);
        var traceContext = GetTraceContext(msg.Event);
        
        int taskId = msg.Event.EventId;
        if (msg.Event is TaskCompletedEvent completed)
        {
            taskId = completed.TaskScheduledId;
        }
        else if (msg.Event is TaskFailedEvent failed)
        {
            taskId = failed.TaskScheduledId;
        }

        return $@"{{
            ""instanceId"":""{instance.InstanceId}"",
            ""executionId"":""{instance.ExecutionId}"",
            ""name"":{name},
            ""eventType"":""{eventType}"",
            ""taskId"":{taskId},
            ""visibleTime"":null,
            ""payloadText"":{payloadText},
            ""payloadId"":null,
            ""reason"":{reason},
            ""traceContext"":{traceContext}
        }}";
    }

    public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
    {
        // Release lock by not renewing
        _logger.LogWarning("Abandoning orchestration work item {InstanceId}", workItem.InstanceId);
        return Task.CompletedTask;
    }

    public Task AbandonTaskActivityWorkItem(TaskActivityWorkItem workItem)
    {
        _logger.LogWarning("Abandoning activity work item");
        return Task.CompletedTask;
    }

    public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
    {
        return AbandonTaskOrchestrationWorkItemAsync(workItem);
    }

    public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
    {
        var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.renew_orchestration_locks($1, $2)", connection);

        cmd.Parameters.AddWithValue(workItem.InstanceId);
        cmd.Parameters.AddWithValue(lockExpiration);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        workItem.LockedUntilUtc = lockExpiration.DateTime;
    }

    public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
    {
        return false; // No limit for now
    }

    public int GetDelayInSecondsAfterOnProcessException(Exception exception)
    {
        return 10; // Retry after 10 seconds
    }

    public int GetDelayInSecondsAfterOnFetchException(Exception exception)
    {
        return 5; // Retry after 5 seconds
    }

    // =============================================================================
    // IOrchestrationServiceClient Implementation
    // =============================================================================

    public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
    {
        ArgumentNullException.ThrowIfNull(creationMessage);
        ArgumentNullException.ThrowIfNull(creationMessage.OrchestrationInstance);

        var instance = creationMessage.OrchestrationInstance;
        var startEvent = creationMessage.Event as ExecutionStartedEvent
            ?? throw new ArgumentException("Creation message must contain ExecutionStartedEvent");

        var createdInstanceId = await CreateTaskOrchestrationCoreAsync(
            instance,
            startEvent,
            dedupeStatuses: null).ConfigureAwait(false);

        _logger.LogInformation(
            "Created orchestration instance {InstanceId} (name={Name}, version={Version})",
            createdInstanceId, startEvent.Name, startEvent.Version);
    }

    public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[]? dedupeStatuses)
    {
        ArgumentNullException.ThrowIfNull(creationMessage);
        ArgumentNullException.ThrowIfNull(creationMessage.OrchestrationInstance);

        var instance = creationMessage.OrchestrationInstance;
        var startEvent = creationMessage.Event as ExecutionStartedEvent
            ?? throw new ArgumentException("Creation message must contain ExecutionStartedEvent");

        await CreateTaskOrchestrationCoreAsync(instance, startEvent, dedupeStatuses).ConfigureAwait(false);
    }

    public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(message.OrchestrationInstance);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.add_orchestration_event($1, $2, $3, $4, $5)", connection);

        var eventType = message.Event.EventType.ToString();
        string? name = null;
        string? payloadText = null;
        string? traceContext = null;

        if (message.Event is EventSentEvent sentEvent)
        {
            name = sentEvent.Name;
            payloadText = sentEvent.Input;
        }
        else if (message.Event is EventRaisedEvent raisedEvent)
        {
            name = raisedEvent.Name;
            payloadText = raisedEvent.Input;
        }

        if (message.Event is ISupportsDurableTraceContext traceEvent)
        {
            traceContext = SerializeTraceContext(traceEvent.ParentTraceContext);
        }

        cmd.Parameters.AddWithValue(message.OrchestrationInstance.InstanceId);
        cmd.Parameters.AddWithValue(eventType);
        cmd.Parameters.AddWithValue(name ?? (object)DBNull.Value);
        
        if (payloadText != null)
        {
            cmd.Parameters.AddWithValue(ToJsonElement(payloadText));
        }
        else
        {
            cmd.Parameters.AddWithValue(DBNull.Value);
        }
        
        cmd.Parameters.AddWithValue(traceContext ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "Sent message to orchestration {InstanceId}, event type={EventType}",
            message.OrchestrationInstance.InstanceId, eventType);
    }

    public async Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            "SELECT * FROM dt.query_single_orchestration($1)",
            connection);

        cmd.Parameters.AddWithValue(instanceId);

        await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        if (!await reader.ReadAsync().ConfigureAwait(false))
        {
            return null;
        }

        var orchestrationState = PostgreSqlUtils.GetOrchestrationState(reader);

        _logger.LogDebug(
            "Retrieved orchestration state for {InstanceId}: {Status}",
            instanceId,
            orchestrationState.OrchestrationStatus);

        if (!string.IsNullOrWhiteSpace(executionId) &&
            !string.Equals(orchestrationState.OrchestrationInstance.ExecutionId, executionId, StringComparison.Ordinal))
        {
            return null;
        }

        return orchestrationState;
    }

    public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
    {
        var state = await GetOrchestrationStateAsync(instanceId, null).ConfigureAwait(false);
        if (state == null)
        {
            return Array.Empty<OrchestrationState>();
        }
        return new[] { state };
    }

    public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string? executionId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.get_instance_history($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(executionId ?? (object)DBNull.Value);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return result?.ToString() ?? "[]";
    }

    public async Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.purge_instance_state_by_time($1, $2)", connection);

        cmd.Parameters.AddWithValue(thresholdDateTimeUtc);
        cmd.Parameters.AddWithValue((short)timeRangeFilterType);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task<PurgeResult> PurgeInstanceStateAsync(string instanceId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.purge_instance_state_by_id($1)", connection);

        cmd.Parameters.AddWithValue(new[] { instanceId });

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return new PurgeResult(Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture));
    }

    public async Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);

        var statusFilter = purgeInstanceFilter.RuntimeStatus?.Any() == true
            ? string.Join(",", purgeInstanceFilter.RuntimeStatus)
            : null;

        await using var cmd = new NpgsqlCommand(
            "SELECT dt.purge_instance_state_by_time($1, $2)",
            connection);

        cmd.Parameters.AddWithValue(purgeInstanceFilter.CreatedTimeTo ?? DateTime.MaxValue);
        cmd.Parameters.AddWithValue(statusFilter ?? (object)DBNull.Value);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return new PurgeResult(Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture));
    }

    public async Task<OrchestrationQueryResult> GetOrchestrationWithQueryAsync(
        OrchestrationQuery query,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT * FROM dt.query_many_orchestrations($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);

        var createdTimeFrom = query.CreatedTimeFrom != default ? query.CreatedTimeFrom : DateTime.MinValue;
        var createdTimeTo = query.CreatedTimeTo != default ? query.CreatedTimeTo : DateTime.MaxValue;

        int pageNumber = 0;
        if (!string.IsNullOrWhiteSpace(query.ContinuationToken) && int.TryParse(query.ContinuationToken, out int parsedPage))
        {
            pageNumber = parsedPage;
        }

        cmd.Parameters.AddWithValue(query.PageSize > 0 ? query.PageSize : 100);
        cmd.Parameters.AddWithValue(pageNumber);
        cmd.Parameters.AddWithValue(query.FetchInputsAndOutputs);
        cmd.Parameters.AddWithValue(query.FetchInputsAndOutputs);
        cmd.Parameters.AddWithValue(createdTimeFrom!);
        cmd.Parameters.AddWithValue(createdTimeTo!);
        cmd.Parameters.AddWithValue(query.InstanceIdPrefix ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(false);

        if (query.RuntimeStatus?.Count > 0)
        {
            cmd.Parameters.AddWithValue(string.Join(",", query.RuntimeStatus));
        }
        else
        {
            cmd.Parameters.AddWithValue(DBNull.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        var results = new List<OrchestrationState>(query.PageSize);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var state = PostgreSqlUtils.GetOrchestrationState(reader);
            results.Add(state);
        }

        string? continuationToken = results.Count == query.PageSize 
            ? (pageNumber + 1).ToString(System.Globalization.CultureInfo.InvariantCulture) 
            : null;
        return new OrchestrationQueryResult(results, continuationToken);
    }

    public async Task<IReadOnlyCollection<OrchestrationState>> GetManyOrchestrationsAsync(
        PostgreSqlOrchestrationQuery query,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT * FROM dt.query_many_orchestrations($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);

        cmd.Parameters.AddWithValue(query.PageSize);
        cmd.Parameters.AddWithValue(query.PageNumber);
        cmd.Parameters.AddWithValue(query.FetchInput);
        cmd.Parameters.AddWithValue(query.FetchOutput);
        cmd.Parameters.AddWithValue(query.CreatedTimeFrom);
        cmd.Parameters.AddWithValue(query.CreatedTimeTo);
        cmd.Parameters.AddWithValue(query.InstanceIdPrefix ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(query.ExcludeSubOrchestrations);

        if (query.StatusFilter?.Count > 0)
        {
            cmd.Parameters.AddWithValue(string.Join(",", query.StatusFilter));
        }
        else
        {
            cmd.Parameters.AddWithValue(DBNull.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        var results = new List<OrchestrationState>(query.PageSize);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var state = PostgreSqlUtils.GetOrchestrationState(reader);
            results.Add(state);
        }

        return results;
    }

    public async Task RewindTaskOrchestrationAsync(string instanceId, string reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.rewind_instance($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(reason ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogInformation("Rewound orchestration {InstanceId}, reason={Reason}", instanceId, reason);
    }

    public async Task<int> GetRecommendedReplicaCountAsync(int? currentReplicaCount = null, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            "SELECT dt.get_scale_recommendation($1, $2)",
            connection);

        cmd.Parameters.AddWithValue(_settings.MaxConcurrentOrchestrations);
        cmd.Parameters.AddWithValue(_settings.MaxConcurrentActivities);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        var recommendedCount = Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);

        if (currentReplicaCount.HasValue && currentReplicaCount != recommendedCount)
        {
            _logger.LogInformation(
                "Scale recommendation: current={Current}, recommended={Recommended}",
                currentReplicaCount, recommendedCount);
        }

        return recommendedCount;
    }

    public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string? reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.terminate_instance($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(reason ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogInformation("Terminated orchestration {InstanceId}, reason={Reason}", instanceId, reason);
    }

    public async Task<OrchestrationState> WaitForOrchestrationAsync(
        string instanceId,
        string? executionId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = timeout < TimeSpan.MaxValue && timeout >= TimeSpan.Zero
            ? new CancellationTokenSource(timeout)
            : new CancellationTokenSource();

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
            timeoutCts.Token,
            cancellationToken);

        while (!combinedCts.Token.IsCancellationRequested)
        {
            var state = await this.GetOrchestrationStateAsync(instanceId, executionId).ConfigureAwait(false);

            if (state?.OrchestrationStatus == OrchestrationStatus.Completed ||
                state?.OrchestrationStatus == OrchestrationStatus.Failed ||
                state?.OrchestrationStatus == OrchestrationStatus.Terminated)
            {
                return state;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), combinedCts.Token).ConfigureAwait(false);
        }

        throw new OperationCanceledException();
    }

    // =============================================================================
    // Helper Methods
    // =============================================================================

    private async Task<string?> CreateTaskOrchestrationCoreAsync(
        OrchestrationInstance instance,
        ExecutionStartedEvent startEvent,
        OrchestrationStatus[]? dedupeStatuses)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            "SELECT dt.create_instance($1, $2, $3, $4, $5, $6, $7, $8)",
            connection);

        cmd.Parameters.AddWithValue(startEvent.Name);
        cmd.Parameters.AddWithValue(startEvent.Version ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(instance.InstanceId);
        cmd.Parameters.AddWithValue(instance.ExecutionId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(startEvent.Input ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(startEvent.ScheduledStartTime ?? (object)DBNull.Value);

        if (dedupeStatuses?.Length > 0)
        {
            cmd.Parameters.AddWithValue(string.Join(",", dedupeStatuses));
        }
        else
        {
            cmd.Parameters.AddWithValue(DBNull.Value);
        }

        cmd.Parameters.AddWithValue(SerializeTraceContext(startEvent.ParentTraceContext) ?? (object)DBNull.Value);
        return await cmd.ExecuteScalarAsync().ConfigureAwait(false) as string;
    }

    private static JsonElement ToJsonElement(string payloadText)
    {
        if (string.IsNullOrWhiteSpace(payloadText))
        {
            return JsonSerializer.SerializeToElement(string.Empty);
        }

        try
        {
            return JsonSerializer.Deserialize<JsonElement>(payloadText);
        }
        catch (JsonException)
        {
            return JsonSerializer.SerializeToElement(payloadText);
        }
    }

    private static string? SerializeTraceContext(DurableTask.Core.Tracing.DistributedTraceContext? traceContext)
    {
        if (traceContext == null || string.IsNullOrWhiteSpace(traceContext.TraceParent))
        {
            return null;
        }

        return string.IsNullOrWhiteSpace(traceContext.TraceState)
            ? traceContext.TraceParent
            : $"{traceContext.TraceParent}\n{traceContext.TraceState}";
    }

    private async Task DeploySchemaAsync()
    {
        var baseDir = AppContext.BaseDirectory;
        
        var schemaPath = Path.Combine(baseDir, "Scripts", "schema.postgresql.sql");
        var logicPath = Path.Combine(baseDir, "Scripts", "logic.postgresql.sql");

        if (!File.Exists(schemaPath))
        {
            schemaPath = Path.Combine(baseDir, "DurableTask.PostgreSQL", "Scripts", "schema.postgresql.sql");
        }
        if (!File.Exists(logicPath))
        {
            logicPath = Path.Combine(baseDir, "DurableTask.PostgreSQL", "Scripts", "logic.postgresql.sql");
        }

        if (!File.Exists(schemaPath) || !File.Exists(logicPath))
        {
            throw new FileNotFoundException($"Schema scripts not found. Searched in: {baseDir}");
        }

        var schemaSql = await File.ReadAllTextAsync(schemaPath).ConfigureAwait(false);
        var logicSql = await File.ReadAllTextAsync(logicPath).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);

        await using (var cmd = new NpgsqlCommand(schemaSql, connection))
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await using (var cmd = new NpgsqlCommand(logicSql, connection))
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        _logger.LogInformation("Schema deployed successfully");
    }

    public Task StopAsync(bool isForced)
    {
        _logger.LogInformation("Stopping PostgreSqlOrchestrationService (isForced={IsForced})", isForced);
        _shutdownTokenSource.Cancel();
        return Task.CompletedTask;
    }

    public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
    {
        var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.renew_task_locks($1, $2)", connection);

        cmd.Parameters.AddWithValue(new[] { workItem.TaskMessage.SequenceNumber });
        cmd.Parameters.AddWithValue(lockExpiration);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        workItem.LockedUntilUtc = lockExpiration.DateTime;
        return workItem;
    }

    public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
    {
        // Same behavior as DurableTask.SqlServer: no-op and return completed task.
        return Task.CompletedTask;
    }

    public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
    {
        if (messages == null || messages.Length == 0)
        {
            return;
        }

        foreach (var message in messages)
        {
            await SendTaskOrchestrationMessageAsync(message).ConfigureAwait(false);
        }

        _logger.LogDebug("Sent batch of {Count} messages", messages.Length);
    }

    public void Dispose()
    {
        _shutdownTokenSource?.Dispose();
        _dataSource?.Dispose();
    }
}
