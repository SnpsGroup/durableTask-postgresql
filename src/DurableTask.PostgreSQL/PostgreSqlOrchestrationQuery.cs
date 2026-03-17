using System;
using System.Collections.Generic;
using DurableTask.Core;

namespace DurableTask.PostgreSQL;

public class PostgreSqlOrchestrationQuery
{
    public int PageSize { get; set; } = 100;
    public int PageNumber { get; set; }
    public bool FetchInput { get; set; } = true;
    public bool FetchOutput { get; set; } = true;
    public DateTime CreatedTimeFrom { get; set; }
    public DateTime CreatedTimeTo { get; set; }
    public ISet<OrchestrationStatus>? StatusFilter { get; init; }
    public string? InstanceIdPrefix { get; set; }
    public bool ExcludeSubOrchestrations { get; set; }
}
