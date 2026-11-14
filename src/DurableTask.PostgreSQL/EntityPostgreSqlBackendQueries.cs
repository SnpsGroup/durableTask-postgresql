using System.Diagnostics;
using System.Globalization;
using DurableTask.Core;
using DurableTask.Core.Entities;

namespace DurableTask.PostgreSQL;

// Mirrors the MSSQL backend query contract. Scheduler state is interpreted by
// the core helper, rather than assuming that entity state is itself a JSON object.
internal sealed class EntityPostgreSqlBackendQueries(PostgreSqlOrchestrationService service) : EntityBackendQueries
{
    public override async Task<EntityMetadata?> GetEntityAsync(EntityId id, bool includeState = false,
        bool includeStateless = false, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        var states = await service.GetOrchestrationStateAsync(id.ToString(), false).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        return GetMetadata(states.FirstOrDefault(), includeStateless, includeState);
    }

    public override async Task<EntityQueryResult> QueryEntitiesAsync(EntityQuery query, CancellationToken cancellation)
    {
        int page = ParsePage(query.ContinuationToken);
        int size = query.PageSize ?? 100;
        if (size <= 0 || size > short.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(query), "Page size must be between 1 and 32767.");
        var states = await service.GetManyOrchestrationsAsync(new PostgreSqlOrchestrationQuery
        {
            PageSize = size,
            PageNumber = page,
            InstanceIdPrefix = "@",
            CreatedTimeFrom = DateTime.MinValue,
            CreatedTimeTo = DateTime.MaxValue,
            FetchInput = query.IncludeState,
            FetchOutput = false,
        }, cancellation).ConfigureAwait(false);

        return new EntityQueryResult
        {
            Results = states.Where(s =>
                    s.OrchestrationInstance.InstanceId.StartsWith(query.InstanceIdStartsWith ?? "@", StringComparison.Ordinal)
                    && (!query.LastModifiedFrom.HasValue || s.LastUpdatedTime >= query.LastModifiedFrom.Value)
                    && (!query.LastModifiedTo.HasValue || s.LastUpdatedTime <= query.LastModifiedTo.Value))
                .Select(s => GetMetadata(s, query.IncludeTransient, query.IncludeState)).OfType<EntityMetadata>().ToArray(),
            ContinuationToken = states.Count < size ? null : checked(page + 1).ToString(CultureInfo.InvariantCulture),
        };
    }

    public override async Task<CleanEntityStorageResult> CleanEntityStorageAsync(
        CleanEntityStorageRequest request = default, CancellationToken cancellation = default)
    {
        int page = ParsePage(request.ContinuationToken);
        int removed = 0, unlocked = 0;
        var watch = Stopwatch.StartNew();
        var sentUnlocks = new HashSet<(string, string)>();
        do
        {
            cancellation.ThrowIfCancellationRequested();
            var states = await service.GetManyOrchestrationsAsync(new PostgreSqlOrchestrationQuery
            {
                PageSize = 100, PageNumber = page, InstanceIdPrefix = "@",
                CreatedTimeFrom = DateTime.MinValue, CreatedTimeTo = DateTime.MaxValue,
                FetchInput = false, FetchOutput = false,
            }, cancellation).ConfigureAwait(false);
            if (states.Count == 0)
                return new CleanEntityStorageResult { EmptyEntitiesRemoved = removed, OrphanedLocksReleased = unlocked };

            int removedOnPage = 0;
            foreach (var state in states)
            {
                cancellation.ThrowIfCancellationRequested();
                var status = ClientEntityHelpers.GetEntityStatus(state.Status);
                if (status == null) continue;
                string id = state.OrchestrationInstance.InstanceId;
                if (request.ReleaseOrphanedLocks && status.LockedBy is string owner && sentUnlocks.Add((id, owner)))
                {
                    var ownerState = (await service.GetOrchestrationStateAsync(owner, false).ConfigureAwait(false)).FirstOrDefault();
                    if (ownerState?.OrchestrationStatus is not (OrchestrationStatus.Running or OrchestrationStatus.Suspended or OrchestrationStatus.Pending))
                    {
                        await service.SendTaskOrchestrationMessageAsync(
                            ClientEntityHelpers.EmitUnlockForOrphanedLock(state.OrchestrationInstance, owner).AsTaskMessage()).ConfigureAwait(false);
                        unlocked++;
                    }
                }
                if (request.RemoveEmptyEntities && !status.EntityExists && status.LockedBy == null && status.BacklogQueueSize == 0
                    && DateTime.UtcNow - state.LastUpdatedTime > service.EntityBackendProperties.EntityMessageReorderWindow)
                {
                    removedOnPage += await service.TryPurgeEmptyEntityAsync(id, state.OrchestrationInstance.ExecutionId,
                        state.LastUpdatedTime, DateTime.UtcNow - service.EntityBackendProperties.EntityMessageReorderWindow,
                        cancellation).ConfigureAwait(false);
                }
            }
            removed += removedOnPage;
            // Purging shifts OFFSET pagination; keep the same page until it no longer shrinks.
            if (removedOnPage == 0) page++;
        } while (watch.Elapsed < TimeSpan.FromSeconds(5));
        return new CleanEntityStorageResult
        {
            EmptyEntitiesRemoved = removed, OrphanedLocksReleased = unlocked,
            ContinuationToken = page.ToString(CultureInfo.InvariantCulture),
        };
    }

    private static int ParsePage(string? token)
    {
        if (string.IsNullOrEmpty(token)) return 0;
        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out int page) || page < 0)
            throw new ArgumentException("Invalid entity continuation token.", nameof(token));
        return page;
    }

    private static EntityMetadata? GetMetadata(OrchestrationState? state, bool includeTransient, bool includeState)
    {
        if (state == null) return null;
        var status = ClientEntityHelpers.GetEntityStatus(state.Status);
        if (!includeTransient && status?.EntityExists != true) return null;
        return new EntityMetadata
        {
            EntityId = EntityId.FromString(state.OrchestrationInstance.InstanceId),
            LastModifiedTime = state.LastUpdatedTime,
            BacklogQueueSize = status?.BacklogQueueSize ?? 0,
            LockedBy = status?.LockedBy,
            SerializedState = includeState ? ClientEntityHelpers.GetEntityState(state.Input) : null,
        };
    }
}
