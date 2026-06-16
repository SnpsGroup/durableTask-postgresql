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

        // Propagate TaskHubName to PostgreSQL by setting it as the connection's
        // ApplicationName. The schema's current_task_hub() uses application_name
        // when TaskHubMode = '0' (the default), so this makes TaskHubName govern
        // the task hub. When TaskHubName is null/empty we leave the connection
        // string untouched (falls back to CURRENT_USER per TaskHubMode = '1').
        var connectionBuilder = new NpgsqlConnectionStringBuilder(_settings.ConnectionString);
        if (!string.IsNullOrEmpty(_settings.TaskHubName))
        {
            connectionBuilder.ApplicationName = _settings.TaskHubName;
        }

        // Build the data source via NpgsqlDataSourceBuilder so we can register
        // composite type mappings. checkpoint_orchestration and complete_tasks
        // take arrays of PostgreSQL composite types (dt.history_event[],
        // dt.task_event[], etc.); the records in PostgreSqlTypes.cs map to them
        // 1:1 and are sent as typed arrays instead of JSON strings.
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionBuilder.ConnectionString);
        // Map the PostgreSQL composite types used by checkpoint_orchestration and
        // complete_tasks. Npgsql resolves each composite's OID on the data source's
        // first connection, so the dt.* types must already exist by then —
        // DeploySchemaAsync uses a raw NpgsqlConnection (not this data source) to
        // avoid populating the type cache before the schema is in place.
        dataSourceBuilder.MapComposite<PostgreSqlTypes.MessageId>($"{_settings.SchemaName}.message_id");
        dataSourceBuilder.MapComposite<PostgreSqlTypes.HistoryEvent>($"{_settings.SchemaName}.history_event");
        dataSourceBuilder.MapComposite<PostgreSqlTypes.OrchestrationEvent>($"{_settings.SchemaName}.orchestration_event");
        dataSourceBuilder.MapComposite<PostgreSqlTypes.TaskEvent>($"{_settings.SchemaName}.task_event");
        dataSourceBuilder.MapComposite<PostgreSqlTypes.TaskResult>($"{_settings.SchemaName}.task_result");
        _dataSource = dataSourceBuilder.Build();

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

            // payload_text is a JSONB column; Npgsql returns it as a string holding
            // the raw JSON (e.g. "\"World\""). Use it directly as the serialized input.
            string? payloadText = reader.IsDBNull(reader.GetOrdinal("payload_text"))
                ? null
                : reader.GetString(reader.GetOrdinal("payload_text"));

            _logger.LogDebug(
                "Locked task {SequenceNumber} for instance {InstanceId} (TaskId={TaskId})",
                sequenceNumber, instanceId, taskId);

            // Create TaskScheduledEvent
            var scheduledEvent = new TaskScheduledEvent(taskId)
            {
                Name = name,
                Version = version,
                Input = payloadText,
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

        var instance = newOrchestrationRuntimeState.OrchestrationInstance!;
        var newEvents = newOrchestrationRuntimeState.NewEvents ?? [];
        var allEvents = newOrchestrationRuntimeState.Events;
        int nextSequenceNumber = allEvents.Count - newEvents.Count;

        // Build the typed composite arrays that checkpoint_orchestration expects.
        // These are PostgreSQL composite types (dt.message_id, dt.history_event,
        // dt.orchestration_event, dt.task_event) mapped via NpgsqlDataSourceBuilder.
        // Sending JSON strings here fails because PostgreSQL cannot cast text to a
        // composite array, which was the root cause of the orchestration abandonment.
        PostgreSqlTypes.MessageId[] deletedEvents = (workItem.NewMessages ?? (IList<TaskMessage>)[])
            .Select(m => new PostgreSqlTypes.MessageId
            {
                InstanceId = m.OrchestrationInstance.InstanceId,
                SequenceNumber = m.SequenceNumber,
            })
            .ToArray();

        PostgreSqlTypes.HistoryEvent[] historyEvents = newEvents
            .Select((evt, i) => ToHistoryEventRecord(evt, instance, nextSequenceNumber + i))
            .ToArray();

        var orchestrationEvents = new List<PostgreSqlTypes.OrchestrationEvent>();
        if (orchestratorMessages != null)
        {
            foreach (var msg in orchestratorMessages)
            {
                if (msg.Event is ExecutionStartedEvent)
                {
                    orchestrationEvents.Add(ToOrchestrationEventRecord(msg));
                }
            }
        }
        if (timerMessages != null)
        {
            foreach (var msg in timerMessages)
            {
                if (msg.Event is TimerCreatedEvent || msg.Event is TimerFiredEvent)
                {
                    orchestrationEvents.Add(ToOrchestrationEventRecord(msg));
                }
            }
        }

        var taskEvents = new List<PostgreSqlTypes.TaskEvent>();
        if (outboundMessages != null)
        {
            foreach (var msg in outboundMessages)
            {
                if (msg.Event is TaskScheduledEvent scheduledEvent)
                {
                    taskEvents.Add(ToTaskEventRecord(msg, scheduledEvent));
                }
            }
        }

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.checkpoint_orchestration($1, $2, $3, $4, $5, $6, $7, $8)", connection);

        cmd.Parameters.AddWithValue(workItem.InstanceId);
        cmd.Parameters.AddWithValue(instance.ExecutionId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue(orchestrationState.OrchestrationStatus.ToString());

        // Custom status payload (TEXT / JSON). NULL when empty.
        var customStatus = orchestrationState.Status;
        cmd.Parameters.AddWithValue(!string.IsNullOrEmpty(customStatus) ? customStatus : (object)DBNull.Value);

        // Deleted events, history, orchestration, and task events as typed composite
        // arrays. Npgsql needs the element DataTypeName (with the [] suffix) to write
        // arrays of mapped composite types.
        var pDeleted = cmd.Parameters.AddWithValue(deletedEvents);
        pDeleted.DataTypeName = $"{_settings.SchemaName}.message_id[]";
        var pHistory = cmd.Parameters.AddWithValue(historyEvents);
        pHistory.DataTypeName = $"{_settings.SchemaName}.history_event[]";
        var pOrch = cmd.Parameters.AddWithValue(orchestrationEvents.ToArray());
        pOrch.DataTypeName = $"{_settings.SchemaName}.orchestration_event[]";
        var pTask = cmd.Parameters.AddWithValue(taskEvents.ToArray());
        pTask.DataTypeName = $"{_settings.SchemaName}.task_event[]";

        try
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (PostgresException ex)
        {
            // Split-brain (duplicate execution) manifests as a primary key violation
            // on dt.history and is expected. Re-throw everything else after logging
            // the full error, because the DTFx WorkItemDispatcher swallows exceptions
            // and otherwise only surfaces a generic "Abandoning" warning.
            if (!IsUniqueKeyViolation(ex))
            {
                _logger.LogError(ex,
                    "checkpoint_orchestration failed for instance {InstanceId} (SQLSTATE={SqlState})",
                    workItem.InstanceId, ex.SqlState);
            }

            throw;
        }

        _logger.LogDebug(
            "Checkpoint completed for orchestration {InstanceId}",
            workItem.InstanceId);
    }

    static bool IsUniqueKeyViolation(PostgresException ex)
        => ex.SqlState == PostgresErrorCodes.UniqueViolation;

    static PostgreSqlTypes.HistoryEvent ToHistoryEventRecord(HistoryEvent evt, OrchestrationInstance instance, int sequenceNumber)
    {
        string? payloadText = ToJsonText(GetEventPayloadValue(evt));
        string? reason = GetEventReasonValue(evt);
        return new PostgreSqlTypes.HistoryEvent
        {
            InstanceId = instance.InstanceId,
            ExecutionId = instance.ExecutionId,
            SequenceNumber = sequenceNumber,
            EventType = evt.EventType.ToString(),
            Name = GetEventNameValue(evt),
            RuntimeStatus = GetRuntimeStatusValue(evt),
            TaskId = GetTaskEventId(evt),
            // Npgsql requires UTC (offset 0) when writing DateTimeOffset to
            // timestamptz; DurableTask.Core Timestamps may carry a local offset.
            Timestamp = evt.Timestamp.ToUniversalTime(),
            IsPlayed = evt.IsPlayed,
            VisibleTime = ToUtc(GetEventVisibleTimeValue(evt)),
            Reason = reason,
            PayloadText = payloadText,
            // The checkpoint inserts a payloads row when payload_text or reason is
            // non-null; that row's payload_id is NOT NULL, so we generate one here.
            PayloadId = (payloadText != null || reason != null) ? Guid.NewGuid() : null,
            ParentInstanceId = GetParentInstanceIdValue(evt),
            Version = GetEventVersionValue(evt),
            TraceContext = GetTraceContextValue(evt),
        };
    }

    static PostgreSqlTypes.OrchestrationEvent ToOrchestrationEventRecord(TaskMessage msg)
    {
        var evt = msg.Event;
        string? payloadText = ToJsonText(GetEventPayloadValue(evt));
        return new PostgreSqlTypes.OrchestrationEvent
        {
            InstanceId = msg.OrchestrationInstance.InstanceId,
            ExecutionId = msg.OrchestrationInstance.ExecutionId,
            EventType = evt.EventType.ToString(),
            Name = GetEventNameValue(evt),
            RuntimeStatus = "Pending",
            TaskId = GetTaskEventId(evt),
            VisibleTime = ToUtc(GetEventVisibleTimeValue(evt)),
            Reason = GetEventReasonValue(evt),
            PayloadText = payloadText,
            PayloadId = payloadText != null ? Guid.NewGuid() : null,
            ParentInstanceId = GetParentInstanceIdValue(evt),
            Version = GetEventVersionValue(evt),
            TraceContext = GetTraceContextValue(evt),
        };
    }

    static DateTimeOffset? ToUtc(DateTimeOffset? value) => value?.ToUniversalTime();

    // The payload_text columns are JSONB, so a non-null payload must be valid JSON.
    // DurableTask.Core payloads are arbitrary strings (e.g. a deserialized "World"
    // from history replay, which is not valid JSON). Serialize such values as JSON
    // strings. Payloads that are already valid JSON (objects/arrays/quoted strings
    // coming straight from the create path) are passed through unchanged.
    static string? ToJsonText(string? payload)
    {
        if (payload is null)
        {
            return null;
        }

        try
        {
            // Validate it's already JSON; if so, keep as-is.
            using var doc = JsonDocument.Parse(payload);
            return payload;
        }
        catch (JsonException)
        {
            // Not JSON — wrap as a JSON string so the JSONB column accepts it.
            return JsonSerializer.Serialize(payload);
        }
    }

    static PostgreSqlTypes.TaskEvent ToTaskEventRecord(TaskMessage msg, TaskScheduledEvent scheduledEvent)
    {
        string? payloadText = ToJsonText(scheduledEvent.Input);
        return new PostgreSqlTypes.TaskEvent
        {
            InstanceId = msg.OrchestrationInstance.InstanceId,
            ExecutionId = msg.OrchestrationInstance.ExecutionId,
            Name = scheduledEvent.Name,
            EventType = EventType.TaskScheduled.ToString(),
            TaskId = scheduledEvent.EventId,
            VisibleTime = null,
            Reason = null,
            PayloadText = payloadText,
            PayloadId = payloadText != null ? Guid.NewGuid() : null,
            Version = scheduledEvent.Version,
            TraceContext = GetTraceContextValue(msg.Event),
        };
    }

    static PostgreSqlTypes.TaskResult ToTaskResultRecord(TaskMessage msg)
    {
        var instance = msg.OrchestrationInstance;
        var evt = msg.Event;
        string? payloadText = ToJsonText(GetEventPayloadValue(evt));

        int taskId = evt.EventId;
        if (evt is TaskCompletedEvent completed)
        {
            taskId = completed.TaskScheduledId;
        }
        else if (evt is TaskFailedEvent failed)
        {
            taskId = failed.TaskScheduledId;
        }

        return new PostgreSqlTypes.TaskResult
        {
            InstanceId = instance.InstanceId,
            ExecutionId = instance.ExecutionId,
            Name = GetEventNameValue(evt),
            EventType = evt.EventType.ToString(),
            TaskId = taskId,
            VisibleTime = null,
            PayloadText = payloadText,
            PayloadId = payloadText != null ? Guid.NewGuid() : null,
            Reason = GetEventReasonValue(evt),
            TraceContext = GetTraceContextValue(evt),
        };
    }

    // ---- Raw-value extractors (return the underlying value, not a JSON literal) ----

    static string? GetEventNameValue(HistoryEvent evt) => evt.EventType switch
    {
        EventType.EventRaised => ((EventRaisedEvent)evt).Name,
        EventType.EventSent => ((EventSentEvent)evt).Name,
        EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).Name,
        EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)evt).Name,
        EventType.TaskScheduled => ((TaskScheduledEvent)evt).Name,
        _ => null,
    };

    static string? GetEventVersionValue(HistoryEvent evt) => evt.EventType switch
    {
        EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).Version,
        EventType.SubOrchestrationInstanceCreated => ((SubOrchestrationInstanceCreatedEvent)evt).Version,
        EventType.TaskScheduled => ((TaskScheduledEvent)evt).Version,
        _ => null,
    };

    static string? GetEventReasonValue(HistoryEvent evt) => evt.EventType switch
    {
        EventType.ExecutionTerminated => ((ExecutionTerminatedEvent)evt).Input,
        EventType.TaskFailed => ((TaskFailedEvent)evt).Reason,
        EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)evt).Reason,
        _ => null,
    };

    static string? GetEventPayloadValue(HistoryEvent evt) => evt.EventType switch
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

    static int GetTaskEventId(HistoryEvent evt) => evt.EventType switch
    {
        EventType.TaskCompleted => ((TaskCompletedEvent)evt).TaskScheduledId,
        EventType.TaskFailed => ((TaskFailedEvent)evt).TaskScheduledId,
        EventType.SubOrchestrationInstanceCompleted => ((SubOrchestrationInstanceCompletedEvent)evt).TaskScheduledId,
        EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)evt).TaskScheduledId,
        EventType.TimerFired => ((TimerFiredEvent)evt).TimerId,
        _ => evt.EventId,
    };

    static DateTimeOffset? GetEventVisibleTimeValue(HistoryEvent evt) => evt.EventType switch
    {
        EventType.TimerCreated => ((TimerCreatedEvent)evt).FireAt,
        EventType.TimerFired => ((TimerFiredEvent)evt).FireAt,
        _ => null,
    };

    static string? GetRuntimeStatusValue(HistoryEvent evt) => evt.EventType switch
    {
        EventType.ExecutionCompleted => ((ExecutionCompletedEvent)evt).OrchestrationStatus.ToString(),
        EventType.ExecutionFailed => ((ExecutionCompletedEvent)evt).OrchestrationStatus.ToString(),
        _ => null,
    };

    static string? GetParentInstanceIdValue(HistoryEvent evt)
    {
        if (evt.EventType == EventType.ExecutionStarted)
        {
            var parent = ((ExecutionStartedEvent)evt).ParentInstance;
            return parent?.OrchestrationInstance.InstanceId;
        }
        return null;
    }

    static string? GetTraceContextValue(HistoryEvent evt)
    {
        if (evt is ISupportsDurableTraceContext traceEvent && traceEvent.ParentTraceContext != null)
        {
            return traceEvent.ParentTraceContext.TraceParent;
        }
        return null;
    }

    public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
    {
        _logger.LogDebug(
            "Completing task activity for instance {InstanceId}, event type={EventType}",
            workItem.TaskMessage.OrchestrationInstance.InstanceId, responseMessage.Event.EventType);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("SELECT dt.complete_tasks($1, $2)", connection);

        // Sequence numbers to complete (BIGINT[])
        cmd.Parameters.AddWithValue(new[] { workItem.TaskMessage.SequenceNumber });

        // Task results as a typed dt.task_result[] array.
        var pResult = cmd.Parameters.AddWithValue(new[] { ToTaskResultRecord(responseMessage) });
        pResult.DataTypeName = $"{_settings.SchemaName}.task_result[]";

        try
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (PostgresException ex)
        {
            _logger.LogError(ex,
                "complete_tasks failed for instance {InstanceId}, event {EventId} (SQLSTATE={SqlState})",
                workItem.TaskMessage.OrchestrationInstance.InstanceId,
                workItem.TaskMessage.Event.EventId, ex.SqlState);
            throw;
        }

        var instance = workItem.TaskMessage.OrchestrationInstance;
        _logger.LogDebug(
            "Task activity completed for instance {InstanceId}",
            instance.InstanceId);
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

        // Deploy via a raw NpgsqlConnection instead of the pooled _dataSource.
        // The _dataSource has composite type mappings registered; its type cache is
        // built on first connection. If that first connection were this deploy
        // (before the dt.* types exist), the composite mappings would fail to
        // resolve later. Using a separate connection keeps _dataSource's cache clean
        // until the schema is in place.
        var connectionBuilder = new NpgsqlConnectionStringBuilder(_settings.ConnectionString);
        if (!string.IsNullOrEmpty(_settings.TaskHubName))
        {
            connectionBuilder.ApplicationName = _settings.TaskHubName;
        }

        await using var connection = new NpgsqlConnection(connectionBuilder.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        // Deploy schema + logic atomically. If either script fails (e.g. a
        // function signature change), the whole deployment is rolled back so the
        // database is not left in a half-upgraded state. The logic.sql script
        // drops functions up-front, so RETURN-type changes no longer fail with
        // SQLSTATE 42P13.
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        await using (var cmd = new NpgsqlCommand(schemaSql, connection, transaction))
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await using (var cmd = new NpgsqlCommand(logicSql, connection, transaction))
        {
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);

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
