namespace DurableTask.PostgreSQL;

/// <summary>
/// A waitable "work may be available" signal for the polling dispatchers.
/// </summary>
/// <remarks>
/// Derived from the SQL Server provider's <c>AsyncAutoResetEvent</c>
/// (<c>DurableTask.SqlServer/Utils/AsyncAutoResetEvent.cs</c>), itself inspired by
/// <see href="https://devblogs.microsoft.com/pfxteam/building-async-coordination-primitives-part-2-asyncautoresetevent/">
/// Building Async Coordination Primitives, Part 2</see>. There is no equivalent primitive in the
/// framework or in DurableTask.Core's public surface (the Core and AzureStorage copies are both
/// internal), so it lives here. It deliberately differs from an auto-reset event in that
/// <see cref="Set"/> releases every waiter, not one; see the remarks on <see cref="Set"/>.
/// </remarks>
internal sealed class PollingSignal
{
    private readonly LinkedList<TaskCompletionSource<bool>> _waiters = new();

    /// <summary>
    /// Incremented by every <see cref="Set"/>. A waiter captures this before polling and passes
    /// it back to <see cref="WaitAsync"/>; a bumped value means a signal was raised while that
    /// waiter was busy, so it must poll again instead of sleeping. A plain boolean latch would
    /// be consumed by whichever poller waited first, parking the rest for a full interval.
    /// </summary>
    private ulong _generation;

    // ponytail: no "start signaled" overload — the only caller starts unsignaled. Add one if a
    // poller ever needs to skip its first wait.

    /// <summary>
    /// Reads the current signal generation. Call this before doing the work the signal relates
    /// to (i.e. before polling), and pass the result to <see cref="WaitAsync"/>.
    /// </summary>
    public ulong CurrentGeneration
    {
        get
        {
            lock (_waiters)
            {
                return _generation;
            }
        }
    }

    /// <summary>
    /// Waits until the signal is raised or <paramref name="timeout"/> elapses.
    /// </summary>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="observedGeneration">
    /// The generation observed by <see cref="CurrentGeneration"/> before the caller's last poll.
    /// If the generation has moved since, the signal was raised during that poll and this
    /// returns immediately.
    /// </param>
    /// <param name="cancellationToken">Aborts the wait on shutdown.</param>
    /// <returns><c>true</c> when the wait ended because the signal was raised;
    /// <c>false</c> when it ended because the timeout elapsed.</returns>
    public async Task<bool> WaitAsync(
        TimeSpan timeout,
        ulong observedGeneration,
        CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> tcs;

        lock (_waiters)
        {
            if (_generation != observedGeneration)
            {
                // Signaled while the caller was polling; don't sleep on stale information.
                return true;
            }

            if (timeout == TimeSpan.Zero)
            {
                return false;
            }

            tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.AddLast(tcs);
        }

        // Task.Delay observes the cancellation token, so a shutdown aborts the wait promptly
        // instead of sitting out the full backoff interval.
        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task delayTask = Task.Delay(timeout, delayCts.Token);
        Task winner = await Task.WhenAny(tcs.Task, delayTask).ConfigureAwait(false);

        if (winner == tcs.Task)
        {
            // Cancel the pending timer so it does not stay rooted for the full interval.
            delayCts.Cancel();
            return true;
        }

        lock (_waiters)
        {
            if (tcs.Task.IsCompleted)
            {
                // Signaled and timed out at about the same time; the signal wins so it is
                // not swallowed (Set() already removed this waiter from the list).
                return true;
            }

            // O(n) on a LinkedList, but n is the number of dispatchers waiting — a handful.
            _waiters.Remove(tcs);
        }

        // Surface shutdown as cancellation rather than as a normal timeout.
        cancellationToken.ThrowIfCancellationRequested();
        return false;
    }

    /// <summary>
    /// Raises the signal, releasing every current waiter and causing any poller that is
    /// mid-poll to skip its next sleep.
    /// </summary>
    /// <remarks>
    /// This releases <em>all</em> waiters rather than one, which is why this type is not a
    /// faithful auto-reset event: the signal means "work may be available", which is true for
    /// every waiter, not just the first in line. Today DTFx fetches with a single loop per
    /// dispatcher (<c>WorkItemDispatcher</c> gates fetches behind a semaphore), so there is
    /// normally one waiter per instance; broadcasting keeps the behavior correct if more than
    /// one poller ever shares a helper, instead of silently parking the others for a full
    /// interval while work sits in the queue.
    /// </remarks>
    public void Set()
    {
        lock (_waiters)
        {
            _generation++;

            if (_waiters.Count > 0)
            {
                // RunContinuationsAsynchronously (set at creation) keeps SetResult from running
                // a waiter's continuation inline on this thread while it holds the lock.
                foreach (TaskCompletionSource<bool> tcs in _waiters)
                {
                    tcs.SetResult(true);
                }

                _waiters.Clear();
            }
        }
    }

    public override string ToString()
    {
        lock (_waiters)
        {
            return $"Generation: {_generation}, Waiters: {_waiters.Count}";
        }
    }
}
