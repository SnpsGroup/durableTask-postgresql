#pragma warning disable CA2007 // ConfigureAwait
#pragma warning disable CA2100 // SQL injection review
#pragma warning disable CA1849 // Call async methods
#pragma warning disable CA1062 // Validate arguments
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.Core.Query;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using System.Reflection;
using System.Text.Json;

namespace DurableTask.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of IOrchestrationService for DurableTask Framework.
/// Provides durable workflow orchestration using PostgreSQL 17+ as the backend.
/// </summary>
public sealed class PostgreSqlOrchestrationService : IOrchestrationService, IOrchestrationServiceClient, IDisposable
{
    /// <summary>
    /// The schema name the embedded SQL scripts are authored against. Deployments into a
    /// different schema rewrite this token; see <see cref="RewriteSchemaName"/>.
    /// </summary>
    private const string DefaultSchemaName = "dt";

    private readonly PostgreSqlOrchestrationServiceSettings _settings;
    private readonly ILogger<PostgreSqlOrchestrationService> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly CancellationTokenSource _shutdownTokenSource = new();

    /// <summary>
    /// Initializes a new instance of <see cref="PostgreSqlOrchestrationService"/> with the
    /// specified settings and logger. Constructs the Npgsql data source with composite type
    /// mappings for the configured PostgreSQL schema.
    /// </summary>
    /// <param name="settings">Service configuration. Must not be <c>null</c>.</param>
    /// <param name="logger">Logger for diagnostic output. Must not be <c>null</c>.</param>
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

    /// <inheritdoc cref="IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems" />
    public int MaxConcurrentTaskOrchestrationWorkItems => _settings.MaxConcurrentOrchestrations;

    /// <inheritdoc cref="IOrchestrationService.MaxConcurrentTaskActivityWorkItems" />
    public int MaxConcurrentTaskActivityWorkItems => _settings.MaxConcurrentActivities;

    /// <inheritdoc cref="IOrchestrationService.TaskOrchestrationDispatcherCount" />
    public int TaskOrchestrationDispatcherCount => 1; // Single dispatcher for simplicity

    /// <inheritdoc cref="IOrchestrationService.TaskActivityDispatcherCount" />
    public int TaskActivityDispatcherCount => 1;

    /// <inheritdoc cref="IOrchestrationService.EventBehaviourForContinueAsNew" />
    public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

    /// <inheritdoc cref="IOrchestrationService.CreateAsync()" />
    public Task CreateAsync() => CreateAsync(recreateInstanceStore: false);

    /// <inheritdoc cref="IOrchestrationService.CreateAsync(bool)" />
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

    /// <inheritdoc cref="IOrchestrationService.CreateIfNotExistsAsync()" />
    public Task CreateIfNotExistsAsync() => CreateAsync(recreateInstanceStore: false);

    /// <inheritdoc cref="IOrchestrationService.StartAsync()" />
    public async Task StartAsync()
    {
        _logger.LogInformation("Starting PostgreSqlOrchestrationService...");

        // Verify connectivity
        await using var connection = await _dataSource.OpenConnectionAsync(_shutdownTokenSource.Token).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.current_task_hub()", connection);
        var taskHub = await cmd.ExecuteScalarAsync(_shutdownTokenSource.Token).ConfigureAwait(false) as string;

        _logger.LogInformation("Connected to PostgreSQL. TaskHub={TaskHub}", taskHub);
    }

    /// <inheritdoc cref="IOrchestrationService.StopAsync()" />
    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping PostgreSqlOrchestrationService...");

        _shutdownTokenSource.Cancel();

        await _dataSource.DisposeAsync().ConfigureAwait(false);

        _logger.LogInformation("PostgreSqlOrchestrationService stopped");
    }

    /// <inheritdoc cref="IOrchestrationService.DeleteAsync()" />
    public Task DeleteAsync() => DeleteAsync(deleteInstanceStore: false);

    /// <inheritdoc cref="IOrchestrationService.DeleteAsync(bool)" />
    public Task DeleteAsync(bool deleteInstanceStore)
    {
        if (deleteInstanceStore)
        {
            throw new NotSupportedException("Delete instance store not supported. Drop schema manually.");
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync" />
    public async Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(
                $"SELECT * FROM {_settings.SchemaName}.lock_next_orchestration($1, $2, $3)",
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
                    $"UPDATE {_settings.SchemaName}.instances SET locked_by = NULL, lock_expiration = NULL WHERE instance_id = $1",
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

    /// <inheritdoc cref="IOrchestrationService.LockNextTaskActivityWorkItem" />
    public async Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(
                $"SELECT * FROM {_settings.SchemaName}.lock_next_task($1, $2)",
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

    /// <inheritdoc cref="IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync" />
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
            // Forward every orchestrator-emitted message. These target OTHER instances (e.g. a
            // parent receiving a SubOrchestrationInstanceCompletedEvent, or a ContinueAsNew
            // ExecutionStarted). Filtering to ExecutionStartedEvent alone dropped sub-orchestration
            // completions, leaving parents stuck in Running forever. The SQL gates instance
            // CREATION on event_type='ExecutionStarted', so non-start events are safely inserted
            // into new_events for their target instance without spawning phantom instances.
            foreach (var msg in orchestratorMessages)
            {
                orchestrationEvents.Add(ToOrchestrationEventRecord(msg));
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
        // ContinueAsNew: the runtime hands the next generation's ExecutionStartedEvent (with a new
        // execution id and the next input) as a standalone message. Route it into the orchestration
        // events so checkpoint_orchestration inserts it into new_events (waking the next generation)
        // and detects the execution-id change to clear prior history. Without this, the orchestration
        // completes on its first execution instead of continuing.
        if (continuedAsNewMessage != null)
        {
            orchestrationEvents.Add(ToOrchestrationEventRecord(continuedAsNewMessage));
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
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.checkpoint_orchestration($1, $2, $3, $4, $5, $6, $7, $8)", connection);

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
        // ExecutionStarted's own EventId is always -1 (DTFx dispatches it that way). For a
        // sub-orchestration's ExecutionStarted we must instead persist the PARENT's schedule id
        // (the EventId of the parent's SubOrchestrationInstanceCreatedEvent), which the runtime
        // reads back as runtimeState.ParentInstance.TaskScheduleId to build the completion event.
        // Storing -1 here left sub-orchestration completions unmatched (TaskScheduledId=-1).
        EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).ParentInstance?.TaskScheduleId ?? -1,
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

    /// <inheritdoc cref="IOrchestrationService.CompleteTaskActivityWorkItemAsync" />
    public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
    {
        _logger.LogDebug(
            "Completing task activity for instance {InstanceId}, event type={EventType}",
            workItem.TaskMessage.OrchestrationInstance.InstanceId, responseMessage.Event.EventType);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.complete_tasks($1, $2)", connection);

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


    /// <inheritdoc cref="IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync" />
    public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
    {
        _logger.LogWarning("Abandoning orchestration work item {InstanceId} (will retry)", workItem.InstanceId);

        // Reset lock immediately so the next worker can retry faster
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"UPDATE {_settings.SchemaName}.instances SET locked_by = NULL, lock_expiration = NULL WHERE instance_id = $1",
            connection);
        cmd.Parameters.AddWithValue(workItem.InstanceId);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="IOrchestrationService.AbandonTaskActivityWorkItem" />
    public Task AbandonTaskActivityWorkItem(TaskActivityWorkItem workItem)
    {
        _logger.LogWarning("Abandoning activity work item {Id} (will retry)", workItem.Id);
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync" />
    public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
    {
        // Lock is already released by checkpoint_orchestration; nothing to do.
        _logger.LogDebug("Releasing orchestration work item {InstanceId}", workItem.InstanceId);
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync" />
    public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
    {
        var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.renew_orchestration_locks($1, $2)", connection);

        cmd.Parameters.AddWithValue(workItem.InstanceId);
        cmd.Parameters.AddWithValue(lockExpiration);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        workItem.LockedUntilUtc = lockExpiration.DateTime;
    }

    /// <inheritdoc cref="IOrchestrationService.IsMaxMessageCountExceeded" />
    public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
    {
        return false; // No limit for now
    }

    /// <inheritdoc cref="IOrchestrationService.GetDelayInSecondsAfterOnProcessException" />
    public int GetDelayInSecondsAfterOnProcessException(Exception exception)
    {
        return 10; // Retry after 10 seconds
    }

    /// <inheritdoc cref="IOrchestrationService.GetDelayInSecondsAfterOnFetchException" />
    public int GetDelayInSecondsAfterOnFetchException(Exception exception)
    {
        return 5; // Retry after 5 seconds
    }

    // =============================================================================
    // IOrchestrationServiceClient Implementation
    // =============================================================================

    /// <inheritdoc cref="IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage)" />
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

    /// <inheritdoc cref="IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage, OrchestrationStatus[])" />
    public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[]? dedupeStatuses)
    {
        ArgumentNullException.ThrowIfNull(creationMessage);
        ArgumentNullException.ThrowIfNull(creationMessage.OrchestrationInstance);

        var instance = creationMessage.OrchestrationInstance;
        var startEvent = creationMessage.Event as ExecutionStartedEvent
            ?? throw new ArgumentException("Creation message must contain ExecutionStartedEvent");

        await CreateTaskOrchestrationCoreAsync(instance, startEvent, dedupeStatuses).ConfigureAwait(false);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync" />
    public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(message.OrchestrationInstance);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.add_orchestration_event($1, $2, $3, $4, $5)", connection);

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

    /// <inheritdoc cref="IOrchestrationServiceClient.GetOrchestrationStateAsync(string, string)" />
    public async Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"SELECT * FROM {_settings.SchemaName}.query_single_orchestration($1)",
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

    /// <inheritdoc cref="IOrchestrationServiceClient.GetOrchestrationStateAsync(string, bool)" />
    public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
    {
        var state = await GetOrchestrationStateAsync(instanceId, null).ConfigureAwait(false);
        if (state == null)
        {
            return Array.Empty<OrchestrationState>();
        }
        return new[] { state };
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.GetOrchestrationHistoryAsync" />
    public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string? executionId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.get_instance_history($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(executionId ?? (object)DBNull.Value);

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return result?.ToString() ?? "[]";
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync" />
    public async Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.purge_instance_state_by_time($1, $2)", connection);

        cmd.Parameters.AddWithValue(thresholdDateTimeUtc);
        cmd.Parameters.AddWithValue((short)timeRangeFilterType);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.PurgeInstanceStateAsync(string)" />
    public async Task<PurgeResult> PurgeInstanceStateAsync(string instanceId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.purge_instance_state_by_id($1)", connection);

        cmd.Parameters.AddWithValue(new[] { instanceId });

        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return new PurgeResult(Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.PurgeInstanceStateAsync(PurgeInstanceFilter)" />
    public async Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
    {
        // Mirrors the MSSQL provider: collect matching instance IDs via the paginated query,
        // then delete them in batches. Reusing GetManyOrchestrationsAsync gives full status-set
        // filtering (the by_time function only accepts a single SMALLINT status enum).
        var purgeQuery = new PostgreSqlOrchestrationQuery
        {
            PageSize = 1000,
            CreatedTimeFrom = purgeInstanceFilter.CreatedTimeFrom,
            CreatedTimeTo = purgeInstanceFilter.CreatedTimeTo ?? DateTime.MaxValue,
            FetchInput = false,
            FetchOutput = false,
            StatusFilter = purgeInstanceFilter.RuntimeStatus?.Any() == true
                ? new HashSet<OrchestrationStatus>(purgeInstanceFilter.RuntimeStatus)
                : null,
        };

        int totalPurgedCount = 0;
        while (true)
        {
            IReadOnlyCollection<OrchestrationState> results =
                await GetManyOrchestrationsAsync(purgeQuery, CancellationToken.None).ConfigureAwait(false);

            if (results.Count == 0)
            {
                break;
            }

            string[] instanceIds = results.Select(r => r.OrchestrationInstance.InstanceId).ToArray();

            await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(
                $"SELECT {_settings.SchemaName}.purge_instance_state_by_id($1)", connection);
            cmd.Parameters.AddWithValue(instanceIds);

            var deleted = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            totalPurgedCount += Convert.ToInt32(deleted, System.Globalization.CultureInfo.InvariantCulture);
        }

        return new PurgeResult(totalPurgedCount);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.GetOrchestrationWithQueryAsync" />
    public async Task<OrchestrationQueryResult> GetOrchestrationWithQueryAsync(
        OrchestrationQuery query,
        CancellationToken cancellationToken)
    {
        if (query.TaskHubNames?.Any() == true)
        {
            throw new NotSupportedException("Querying orchestrations by task hub name is not supported.");
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT * FROM {_settings.SchemaName}.query_many_orchestrations($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);

        var createdTimeFrom = query.CreatedTimeFrom ?? DateTime.MinValue;
        var createdTimeTo = query.CreatedTimeTo ?? DateTime.MaxValue;

        int pageNumber = 0;
        if (!string.IsNullOrWhiteSpace(query.ContinuationToken) && int.TryParse(query.ContinuationToken, out int parsedPage))
        {
            pageNumber = parsedPage;
        }

        // Parameters are positional ($1..$9) and typed explicitly so PostgreSQL can resolve the
        // function overload unambiguously (SMALLINT page_size, VARCHAR filters, timestamptz).
        // Untyped AddWithValue sends integer/text/timestamp-without-tz, which fails with 42883.
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Smallint, Value = (short)(query.PageSize > 0 ? query.PageSize : 100) });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = pageNumber });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = query.FetchInputsAndOutputs });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = query.FetchInputsAndOutputs });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz,
            Value = DateTime.SpecifyKind(createdTimeFrom, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz,
            Value = DateTime.SpecifyKind(createdTimeTo, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar,
            Value = query.InstanceIdPrefix ?? (object)DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = false });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar,
            Value = query.RuntimeStatus?.Count > 0 ? string.Join(",", query.RuntimeStatus) : DBNull.Value });

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

    /// <summary>
    /// Retrieves multiple orchestration instances matching the specified query parameters.
    /// This is a PostgreSQL-specific extension that supports richer filtering than the
    /// standard <see cref="IOrchestrationServiceClient.GetOrchestrationWithQueryAsync"/>.
    /// </summary>
    /// <param name="query">Query filters including pagination, time range, status, and ID prefix.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A read-only collection of matching orchestration states.</returns>
    public async Task<IReadOnlyCollection<OrchestrationState>> GetManyOrchestrationsAsync(
        PostgreSqlOrchestrationQuery query,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT * FROM {_settings.SchemaName}.query_many_orchestrations($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);

        // Parameters are positional ($1..$9 in the SQL) and typed explicitly so PostgreSQL can
        // resolve the function overload unambiguously (SMALLINT page_size, VARCHAR filters, etc.).
        // Untyped AddWithValue would send integer/text, which don't match the signature.
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Smallint, Value = (short)query.PageSize });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = query.PageNumber });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = query.FetchInput });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = query.FetchOutput });
        // query_many_orchestrations expects TIMESTAMP WITH TIME ZONE; normalize to UTC so any
        // caller works regardless of the DateTime.Kind.
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz,
            Value = DateTime.SpecifyKind(query.CreatedTimeFrom, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz,
            Value = DateTime.SpecifyKind(query.CreatedTimeTo, DateTimeKind.Utc) });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar,
            Value = query.InstanceIdPrefix ?? (object)DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = query.ExcludeSubOrchestrations });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar,
            Value = query.StatusFilter?.Count > 0 ? string.Join(",", query.StatusFilter) : DBNull.Value });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        var results = new List<OrchestrationState>(query.PageSize);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var state = PostgreSqlUtils.GetOrchestrationState(reader);
            results.Add(state);
        }

        return results;
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.RewindTaskOrchestrationAsync" />
    public async Task RewindTaskOrchestrationAsync(string instanceId, string reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.rewind_instance($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(reason ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogInformation("Rewound orchestration {InstanceId}, reason={Reason}", instanceId, reason);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.GetRecommendedReplicaCountAsync" />
    public async Task<int> GetRecommendedReplicaCountAsync(int? currentReplicaCount = null, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"SELECT {_settings.SchemaName}.get_scale_recommendation($1, $2)",
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

    /// <inheritdoc cref="IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync" />
    public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string? reason)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.terminate_instance($1, $2)", connection);

        cmd.Parameters.AddWithValue(instanceId);
        cmd.Parameters.AddWithValue(reason ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        _logger.LogInformation("Terminated orchestration {InstanceId}, reason={Reason}", instanceId, reason);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.WaitForOrchestrationAsync" />
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
            $"SELECT {_settings.SchemaName}.create_instance($1, $2, $3, $4, $5, $6, $7, $8)",
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

    private static async Task<string> GetEmbeddedScriptAsync(string scriptName)
    {
        Assembly assembly = typeof(PostgreSqlOrchestrationService).Assembly;
        string resourceName = $"{assembly.GetName().Name}.Scripts.{scriptName}";

        using Stream? resourceStream = assembly.GetManifestResourceStream(resourceName);
        if (resourceStream == null)
        {
            throw new ArgumentException($"Could not find assembly resource named '{resourceName}'.");
        }

        using var reader = new StreamReader(resourceStream);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }

    private async Task DeploySchemaAsync()
    {
        // SQL scripts are embedded as assembly resources (mirrors DurableTask.SqlServer),
        // so they are always available regardless of build or publish output.
        var schemaSql = await GetEmbeddedScriptAsync("schema.postgresql.sql").ConfigureAwait(false);
        var logicSql = await GetEmbeddedScriptAsync("logic.postgresql.sql").ConfigureAwait(false);

        // The embedded scripts are authored against the default 'dt' schema. Rewrite
        // them to use the configured schema name so SchemaName is honored end-to-end.
        schemaSql = RewriteSchemaName(schemaSql);
        logicSql = RewriteSchemaName(logicSql);

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

        // Concurrent deployments (parallel test classes, or multiple service instances starting
        // against the same DB) race on the DDL: two transactions both see the schema as missing,
        // and they collide on catalog locks — surfacing as a unique violation on pg_namespace /
        // pg_type (23505) or, more often with three or more racers, a deadlock (40P01).
        //
        // Retrying that collision does not work: the racers back off by the same amount and
        // return still aligned. Instead we serialize the whole deploy behind a session-scoped
        // advisory lock, mirroring how the SQL Server provider wraps its schema upgrade in
        // sys.sp_getapplock (DurableTask.SqlServer, SqlDbManager.AcquireDatabaseLockAsync).
        // Losers block until the winner finishes, then find the schema already in place and
        // no-op through the idempotent scripts.
        //
        // The lock is session-scoped rather than transaction-scoped because the deploy spans
        // several transactions (schema+logic, then one per migration); a transaction-scoped
        // lock would be released at the first commit and reopen the race for the migrations.
        await AcquireDeploymentLockAsync(connection).ConfigureAwait(false);
        try
        {
            await DeploySchemaScriptsAsync(connection, schemaSql, logicSql).ConfigureAwait(false);
            await ApplyMigrationsAsync(connection).ConfigureAwait(false);
        }
        finally
        {
            await ReleaseDeploymentLockAsync(connection).ConfigureAwait(false);
        }

        _logger.LogInformation("Schema '{SchemaName}' deployed successfully", _settings.SchemaName);
    }

    /// <summary>
    /// Takes the advisory lock that serializes schema deployment for this schema. Blocks until the
    /// lock is available, so concurrent first-deploys queue instead of deadlocking.
    /// </summary>
    private async Task AcquireDeploymentLockAsync(NpgsqlConnection connection)
    {
        long lockKey = GetDeploymentLockKey(_settings.SchemaName);

        await using var cmd = new NpgsqlCommand("SELECT pg_advisory_lock($1)", connection);
        cmd.Parameters.AddWithValue(lockKey);

        // No CommandTimeout: waiting is the desired behavior here. A deploy that is genuinely
        // stuck surfaces through the connection timeout rather than by failing fast on a
        // healthy-but-slow peer.
        cmd.CommandTimeout = 0;

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        stopwatch.Stop();

        _logger.LogDebug(
            "Acquired schema deployment lock {LockKey} for schema '{SchemaName}' after {ElapsedMs}ms",
            lockKey,
            _settings.SchemaName,
            stopwatch.ElapsedMilliseconds);
    }

    /// <summary>
    /// Releases the schema deployment advisory lock. Session-scoped advisory locks are also
    /// released when the connection closes, so a failure here is logged rather than thrown —
    /// it must not mask the deployment exception it may be unwinding.
    /// </summary>
    private async Task ReleaseDeploymentLockAsync(NpgsqlConnection connection)
    {
        long lockKey = GetDeploymentLockKey(_settings.SchemaName);

        try
        {
            await using var cmd = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", connection);
            cmd.Parameters.AddWithValue(lockKey);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            // The connection is closed or broken; PostgreSQL has already dropped the lock with
            // the session. Nothing to recover, and throwing would hide the original failure.
            _logger.LogWarning(
                ex,
                "Failed to explicitly release schema deployment lock {LockKey}; it is released with the session",
                lockKey);
        }
    }

    /// <summary>
    /// Derives the advisory lock key from the schema name, so task hubs deployed into different
    /// schemas of the same database do not serialize against each other. Uses FNV-1a (a stable,
    /// process-independent hash) because <see cref="string.GetHashCode()"/> is randomized per
    /// process and would give each worker a different key.
    /// </summary>
    internal static long GetDeploymentLockKey(string schemaName)
    {
        // Namespace the key so it cannot collide with advisory locks taken by application code
        // that happens to use a small integer key.
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        ulong hash = offsetBasis;
        foreach (char c in "DurableTask.PostgreSQL:schema-deploy:" + schemaName)
        {
            hash ^= c;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    private static async Task DeploySchemaScriptsAsync(NpgsqlConnection connection, string schemaSql, string logicSql)
    {
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
    }

    /// <summary>
    /// Applies pending forward migrations from embedded <c>Scripts/migrations/migration-{semver}.postgresql.sql</c>
    /// resources. The baseline (<c>schema.postgresql.sql</c>) is idempotent and creates everything for a fresh
    /// install; this runner handles subsequent upgrades by applying each migration whose version is newer than the
    /// newest row in <c>dt.versions</c>, recording each as it applies. Migrations must be idempotent-safe per the
    /// PostgreSQL convention (<c>ADD COLUMN IF NOT EXISTS</c>, <c>CREATE TYPE IF NOT EXISTS</c>, etc.).
    /// </summary>
    private async Task ApplyMigrationsAsync(NpgsqlConnection connection)
    {
        var assembly = typeof(PostgreSqlOrchestrationService).Assembly;
        const string prefix = ".Scripts.migrations.migration-";
        var migrationSuffix = ".postgresql.sql";

        // Discover migration scripts and parse their version from the resource name.
        var migrations = new List<(string ResourceForm, int[] Parts, string ResourceName)>();
        foreach (var name in assembly.GetManifestResourceNames())
        {
            int idx = name.IndexOf(prefix, StringComparison.Ordinal);
            if (idx < 0 || !name.EndsWith(migrationSuffix, StringComparison.Ordinal))
            {
                continue;
            }

            string versionPart = name.Substring(idx + prefix.Length, name.Length - idx - prefix.Length - migrationSuffix.Length);
            if (TryParseVersion(versionPart, out var parsed))
            {
                migrations.Add((parsed.ResourceForm, parsed.Parts, name));
            }
            else
            {
                _logger.LogWarning("Skipping migration with unparseable version: {Resource}", name);
            }
        }

        if (migrations.Count == 0)
        {
            return;
        }

        // Newest applied version recorded in dt.versions.
        string appliedMax = await GetMaxAppliedVersionAsync(connection).ConfigureAwait(false);
        var appliedComparison = ParseForComparison(appliedMax);

        foreach (var (resourceForm, parts, resourceName) in migrations.OrderBy(m => m.Parts, Comparer<int[]>.Create(CompareVersions)))
        {
            if (appliedComparison.Length > 0 && CompareVersions(parts, appliedComparison) <= 0)
            {
                continue; // already applied (or baseline covers it)
            }

            string sql = await GetEmbeddedResourceAsync(resourceName).ConfigureAwait(false);
            sql = RewriteSchemaName(sql);

            _logger.LogInformation("Applying schema migration {Version}", resourceForm);
            await using var tx = await connection.BeginTransactionAsync().ConfigureAwait(false);
            await using (var cmd = new NpgsqlCommand(sql, connection, tx))
            {
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            // Record the migration version.
            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {_settings.SchemaName}.versions (semantic_version) VALUES ($1) ON CONFLICT DO NOTHING",
                connection, tx))
            {
                cmd.Parameters.AddWithValue(resourceForm);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await tx.CommitAsync().ConfigureAwait(false);
        }
    }

    private async Task<string> GetMaxAppliedVersionAsync(NpgsqlConnection connection)
    {
        await using var cmd = new NpgsqlCommand(
            $"SELECT semantic_version FROM {_settings.SchemaName}.versions ORDER BY semantic_version DESC LIMIT 1",
            connection);
        var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
        return result as string ?? "0.0.0";
    }

    private static async Task<string> GetEmbeddedResourceAsync(string resourceName)
    {
        Assembly assembly = typeof(PostgreSqlOrchestrationService).Assembly;
        using Stream? stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Missing embedded resource '{resourceName}'.");
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }

    // Versions in resource names are dotted semver strings (e.g. "1.0.0", "1.1.0").
    // We parse them into a comparable form, tolerating pre-release suffixes by trimming them.
    private static bool TryParseVersion(string text, out (string ResourceForm, int[] Parts) parsed)
    {
        parsed = default;
        string core = text.Split('-', '+')[0];
        var parts = core.Split('.');
        if (parts.Length < 2) return false;
        var ints = new List<int>();
        foreach (var p in parts)
        {
            if (!int.TryParse(p, out var n)) return false;
            ints.Add(n);
        }
        while (ints.Count < 3) ints.Add(0);
        parsed = (text, ints.ToArray());
        return true;
    }

    private static int[] ParseForComparison(string text) =>
        TryParseVersion(text, out var parsed) ? parsed.Parts : Array.Empty<int>();

    private static int CompareVersions(int[] a, int[] b)
    {
        int len = Math.Max(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int av = i < a.Length ? a[i] : 0;
            int bv = i < b.Length ? b[i] : 0;
            if (av != bv) return av.CompareTo(bv);
        }
        return 0;
    }

    private string RewriteSchemaName(string sql)
    {
        // Rewrite the default schema name "dt" to the configured one, keeping the embedded
        // scripts stable while allowing consumers to choose any valid PostgreSQL identifier.
        //
        // This matches "dt" as a whole identifier, which covers every form the scripts use:
        // the qualifier ("dt.instances"), the CREATE SCHEMA statement, and bare references
        // such as "COMMENT ON SCHEMA dt". Matching only "dt." used to miss the last one, so a
        // non-default SchemaName deployed a schema and then failed with 3F000 trying to comment
        // on a "dt" that was never created.
        //
        // The lookarounds keep identifiers that merely contain "dt" intact: the lookbehind
        // rejects a preceding word character or "." (so "created_dt" and an already-qualified
        // "other.dt" are left alone), and the lookahead rejects a trailing word character
        // ("dt_other"). A following "." is allowed, which is what rewrites the qualifier.
        if (string.Equals(_settings.SchemaName, DefaultSchemaName, StringComparison.Ordinal))
        {
            return sql;
        }

        return System.Text.RegularExpressions.Regex.Replace(
            sql,
            $@"(?<![\w.]){DefaultSchemaName}(?![\w])",
            _settings.SchemaName,
            System.Text.RegularExpressions.RegexOptions.None,
            TimeSpan.FromSeconds(5));
    }

    /// <inheritdoc cref="IOrchestrationService.StopAsync(bool)" />
    public Task StopAsync(bool isForced)
    {
        _logger.LogInformation("Stopping PostgreSqlOrchestrationService (isForced={IsForced})", isForced);
        _shutdownTokenSource.Cancel();
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IOrchestrationService.RenewTaskActivityWorkItemLockAsync" />
    public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
    {
        var lockExpiration = DateTimeOffset.UtcNow.Add(_settings.LockTimeout);

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand($"SELECT {_settings.SchemaName}.renew_task_locks($1, $2)", connection);

        cmd.Parameters.AddWithValue(new[] { workItem.TaskMessage.SequenceNumber });
        cmd.Parameters.AddWithValue(lockExpiration);

        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        workItem.LockedUntilUtc = lockExpiration.DateTime;
        return workItem;
    }

    /// <inheritdoc cref="IOrchestrationService.AbandonTaskActivityWorkItemAsync" />
    public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
    {
        _logger.LogWarning("Abandoning activity work item {Id} (will retry)", workItem.Id);

        // Reset lock immediately so the next worker can retry faster
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"UPDATE {_settings.SchemaName}.new_tasks SET locked_by = NULL, lock_expiration = NULL WHERE sequence_number = $1",
            connection);
        cmd.Parameters.AddWithValue(workItem.TaskMessage.SequenceNumber);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync" />
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

    /// <summary>
    /// Disposes the PostgreSQL data source and shutdown token source, releasing all
    /// database connections and internal resources held by this service.
    /// </summary>
    public void Dispose()
    {
        _shutdownTokenSource?.Dispose();
        _dataSource?.Dispose();
    }
}
