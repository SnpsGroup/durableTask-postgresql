using Npgsql;

namespace DurableTask.PostgreSQL;

/// <summary>
/// Configures an application-owned Npgsql data source for DurableTask PostgreSQL.
/// </summary>
public static class NpgsqlDataSourceBuilderExtensions
{
    /// <summary>
    /// Registers the composite mappings required by DurableTask checkpoints and activity completion.
    /// </summary>
    /// <param name="builder">The builder used to create the shared data source.</param>
    /// <param name="schemaName">The schema deployed for the provider.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <remarks>
    /// Call before building the data source. Deploy the provider schema before opening its first
    /// connection, because Npgsql caches database type metadata. The caller retains ownership of
    /// the resulting data source. Use the same schema name in the orchestration service settings.
    /// </remarks>
    public static NpgsqlDataSourceBuilder UseDurableTaskPostgreSql(
        this NpgsqlDataSourceBuilder builder,
        string schemaName = "dt")
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaName);

        builder.MapComposite<PostgreSqlTypes.MessageId>($"{schemaName}.message_id");
        builder.MapComposite<PostgreSqlTypes.HistoryEvent>($"{schemaName}.history_event");
        builder.MapComposite<PostgreSqlTypes.OrchestrationEvent>($"{schemaName}.orchestration_event");
        builder.MapComposite<PostgreSqlTypes.TaskEvent>($"{schemaName}.task_event");
        builder.MapComposite<PostgreSqlTypes.TaskResult>($"{schemaName}.task_result");
        return builder;
    }

    // Sending all five typed arrays exercises the same serialization boundary as checkpoint and
    // complete_tasks without changing data. Connecting alone does not validate these mappings.
    internal static async Task ValidateDurableTaskMappingsAsync(
        NpgsqlConnection connection,
        string schemaName,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT cardinality($1), cardinality($2), cardinality($3), cardinality($4), cardinality($5)", connection);
        AddArray(command, $"{schemaName}.message_id[]", Array.Empty<PostgreSqlTypes.MessageId>());
        AddArray(command, $"{schemaName}.history_event[]", Array.Empty<PostgreSqlTypes.HistoryEvent>());
        AddArray(command, $"{schemaName}.orchestration_event[]", Array.Empty<PostgreSqlTypes.OrchestrationEvent>());
        AddArray(command, $"{schemaName}.task_event[]", Array.Empty<PostgreSqlTypes.TaskEvent>());
        AddArray(command, $"{schemaName}.task_result[]", Array.Empty<PostgreSqlTypes.TaskResult>());
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is InvalidCastException or InvalidOperationException or NotSupportedException or ArgumentException)
        {
            throw new InvalidOperationException(
                "The supplied data source cannot serialize the DurableTask composite types. " +
                "Call UseDurableTaskPostgreSql with the service schema on its builder, and deploy " +
                "the schema before the data source opens its first connection.", exception);
        }
    }

    private static void AddArray(NpgsqlCommand command, string dataTypeName, object value) =>
        command.Parameters.Add(new NpgsqlParameter { DataTypeName = dataTypeName, Value = value });
}
