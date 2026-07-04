using System;
using System.Collections.Generic;
using DurableTask.Core;

namespace DurableTask.PostgreSQL;

/// <summary>
/// Query parameters for retrieving orchestration instances from PostgreSQL.
/// Used by <see cref="PostgreSqlOrchestrationService.GetManyOrchestrationsAsync"/> for paginated queries.
/// </summary>
public class PostgreSqlOrchestrationQuery
{
    /// <summary>
    /// Maximum number of orchestration instances to return per page.
    /// Default: 100.
    /// </summary>
    public int PageSize { get; set; } = 100;

    /// <summary>
    /// Zero-based page number for paginated results.
    /// </summary>
    public int PageNumber { get; set; }

    /// <summary>
    /// Whether to include the orchestration input in the result set.
    /// Default: <c>true</c>.
    /// </summary>
    public bool FetchInput { get; set; } = true;

    /// <summary>
    /// Whether to include the orchestration output in the result set.
    /// Default: <c>true</c>.
    /// </summary>
    public bool FetchOutput { get; set; } = true;

    /// <summary>
    /// Lower bound (inclusive) for filtering by creation time (UTC). When left at the default,
    /// no lower bound filter is applied.
    /// </summary>
    public DateTime CreatedTimeFrom { get; set; }

    /// <summary>
    /// Upper bound (inclusive) for filtering by creation time (UTC). When left at the default,
    /// no upper bound filter is applied.
    /// </summary>
    public DateTime CreatedTimeTo { get; set; }

    /// <summary>
    /// Optional set of orchestration statuses to filter by. When <c>null</c>, all statuses
    /// are included.
    /// </summary>
    public ISet<OrchestrationStatus>? StatusFilter { get; init; }

    /// <summary>
    /// Optional prefix filter for the orchestration instance ID. When <c>null</c>, no prefix
    /// filtering is applied.
    /// </summary>
    public string? InstanceIdPrefix { get; set; }

    /// <summary>
    /// When <c>true</c>, sub-orchestration instances are excluded from the results.
    /// </summary>
    public bool ExcludeSubOrchestrations { get; set; }
}
