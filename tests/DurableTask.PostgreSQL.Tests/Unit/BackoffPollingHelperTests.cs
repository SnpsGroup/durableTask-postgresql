using System.Diagnostics;
using Xunit;

namespace DurableTask.PostgreSQL.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="BackoffPollingHelper"/>. Regression coverage for the idle polling
/// storm: <c>WorkItemPollingInterval</c> was declared but never consumed, so a worker with an
/// empty queue re-polled <c>lock_next_task</c> / <c>lock_next_orchestration</c> as fast as
/// PostgreSQL could answer. Each empty poll is an implicit transaction that still commits, so an
/// idle task hub produced thousands of commits per second.
/// </summary>
public sealed class BackoffPollingHelperTests
{
    private static BackoffPollingHelper CreateHelper(
        int minMs = 50,
        int maxMs = 1000,
        int deltaMs = 50) =>
        new(
            TimeSpan.FromMilliseconds(minMs),
            TimeSpan.FromMilliseconds(maxMs),
            TimeSpan.FromMilliseconds(deltaMs));

    [Fact]
    public async Task WaitAsync_EmptyPoll_ActuallyDelays()
    {
        // The core regression: an empty poll must cost wall-clock time, not return instantly.
        BackoffPollingHelper helper = CreateHelper(minMs: 100, maxMs: 1000);

        var stopwatch = Stopwatch.StartNew();
        bool signaled = await helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None);
        stopwatch.Stop();

        Assert.False(signaled); // Timed out rather than being woken.
        // Allow slack for timer granularity, but a busy-wait (0ms) fails this decisively.
        Assert.True(
            stopwatch.ElapsedMilliseconds >= 80,
            $"Expected the first idle wait to delay ~100ms, but it returned in {stopwatch.ElapsedMilliseconds}ms.");
    }

    [Fact]
    public async Task WaitAsync_SuccessiveEmptyPolls_BackOffTowardMaximum()
    {
        BackoffPollingHelper helper = CreateHelper(minMs: 1, maxMs: 50, deltaMs: 10);

        TimeSpan first = helper.CurrentInterval;
        for (int i = 0; i < 10; i++)
        {
            await helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None);
        }

        Assert.True(
            helper.CurrentInterval > first,
            $"Interval should grow across empty polls, but stayed at {helper.CurrentInterval}.");
        Assert.True(
            helper.CurrentInterval <= TimeSpan.FromMilliseconds(50),
            $"Interval must never exceed the configured maximum, but was {helper.CurrentInterval}.");
    }

    [Fact]
    public async Task WaitAsync_NeverExceedsMaximum()
    {
        BackoffPollingHelper helper = CreateHelper(minMs: 1, maxMs: 20, deltaMs: 10);

        for (int i = 0; i < 25; i++)
        {
            await helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None);
            Assert.True(
                helper.CurrentInterval <= TimeSpan.FromMilliseconds(20),
                $"Interval exceeded the maximum on iteration {i}: {helper.CurrentInterval}.");
        }

        Assert.Equal(TimeSpan.FromMilliseconds(20), helper.CurrentInterval);
    }

    [Fact]
    public async Task Reset_WakesAPendingWaiter()
    {
        // Reset is what keeps the backoff from adding latency to real work: a checkpoint that
        // enqueues an activity must wake the activity poller instead of letting it sit out the
        // full interval.
        BackoffPollingHelper helper = CreateHelper(minMs: 10_000, maxMs: 10_000, deltaMs: 0);

        var stopwatch = Stopwatch.StartNew();
        Task<bool> wait = helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None);

        // Give the waiter a moment to actually enqueue itself before signaling.
        await Task.Delay(50);
        helper.Reset();

        bool signaled = await wait.WaitAsync(TimeSpan.FromSeconds(5));
        stopwatch.Stop();

        Assert.True(signaled, "Reset() should wake the waiter rather than letting it time out.");
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"Reset() should return promptly, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task Reset_CollapsesIntervalBackToMinimum()
    {
        BackoffPollingHelper helper = CreateHelper(minMs: 5, maxMs: 500, deltaMs: 50);

        // Back off a few times so the interval has grown past the minimum.
        for (int i = 0; i < 5; i++)
        {
            await helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None);
        }
        Assert.True(helper.CurrentInterval > TimeSpan.FromMilliseconds(5));

        // A wait that ends via Reset() means work appeared, so the next poll goes back to min.
        // The generation must be captured before Reset(), mirroring a dispatcher that reads it
        // before polling; reading it afterwards would hide the bump the wait needs to observe.
        ulong generation = helper.CurrentGeneration;
        helper.Reset();
        bool signaled = await helper.WaitAsync(generation, CancellationToken.None);

        Assert.True(signaled);
        Assert.Equal(TimeSpan.FromMilliseconds(5), helper.CurrentInterval);
    }

    [Fact]
    public async Task Reset_BeforeWait_IsNotLost()
    {
        // A Reset() raised with nobody waiting (the common case: the dispatchers are mid-query)
        // must still be observed by the wait that follows, otherwise a checkpoint that lands
        // between polls is dropped and the work waits out a full interval.
        BackoffPollingHelper helper = CreateHelper(minMs: 10_000, maxMs: 10_000, deltaMs: 0);

        ulong generation = helper.CurrentGeneration;
        helper.Reset();

        var stopwatch = Stopwatch.StartNew();
        bool signaled = await helper.WaitAsync(generation, CancellationToken.None);
        stopwatch.Stop();

        Assert.True(signaled, "A Reset() issued before the wait must be observed by that wait.");
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Pre-armed wait should return immediately, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task WaitAsync_HonorsCancellation()
    {
        // Shutdown must abort the wait promptly instead of sitting out the backoff interval.
        BackoffPollingHelper helper = CreateHelper(minMs: 30_000, maxMs: 30_000, deltaMs: 0);
        using var cts = new CancellationTokenSource();

        var stopwatch = Stopwatch.StartNew();
        Task<bool> wait = helper.WaitAsync(helper.CurrentGeneration, cts.Token);
        await Task.Delay(50);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        stopwatch.Stop();

        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"Cancellation should abort the wait promptly, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task Reset_WakesAllPendingWaiters()
    {
        // DTFx runs several concurrent poll loops per dispatcher. A single Reset() must release
        // all of them: waking only one leaves the rest parked at the backed-off interval while
        // work is already sitting in the queue, which showed up as a latency regression
        // (~250ms -> ~850ms for the first runs after an idle period).
        BackoffPollingHelper helper = CreateHelper(minMs: 10_000, maxMs: 10_000, deltaMs: 0);

        Task<bool>[] waiters = Enumerable.Range(0, 10)
            .Select(_ => helper.WaitAsync(helper.CurrentGeneration, CancellationToken.None))
            .ToArray();

        // Let every waiter enqueue itself before signaling.
        await Task.Delay(100);
        helper.Reset();

        bool[] results = await Task.WhenAll(waiters).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.All(results, signaled => Assert.True(signaled));
    }

    [Fact]
    public async Task WaitAsync_AlreadyCancelledToken_DoesNotWait()
    {
        // StopAsync can land before a dispatcher enters its wait; that must surface as
        // cancellation immediately rather than sleeping out the interval first.
        BackoffPollingHelper helper = CreateHelper(minMs: 30_000, maxMs: 30_000, deltaMs: 0);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => helper.WaitAsync(helper.CurrentGeneration, cts.Token));
        stopwatch.Stop();

        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"An already-cancelled wait should return at once, but took {stopwatch.Elapsed}.");
    }

    [Fact]
    public void Constructor_RejectsMinimumGreaterThanMaximum()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateHelper(minMs: 500, maxMs: 100));
    }

    [Fact]
    public void Constructor_RejectsNegativeIntervals()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateHelper(minMs: -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateHelper(deltaMs: -1));
    }
}
