namespace DurableTask.PostgreSQL;

/// <summary>
/// Randomized exponential backoff for work-item polling.
/// </summary>
/// <remarks>
/// <para>
/// A dispatcher calls <see cref="WaitAsync"/> after a poll that found no work, and
/// <see cref="Reset"/> after a poll that found work (or when it has just written work that
/// another dispatcher is waiting for). Without the wait, an idle task hub is polled in a tight
/// loop: every empty <c>lock_next_orchestration</c> / <c>lock_next_task</c> call is still an
/// implicit transaction that commits, so an idle worker produces thousands of commits per second
/// against an empty queue.
/// </para>
/// <para>
/// Ported from the SQL Server provider (<c>DurableTask.SqlServer/BackoffPollingHelper.cs</c>);
/// the backoff strategy itself originates in the Azure WebJobs SDK's
/// <c>RandomizedExponentialBackoffStrategy</c>.
/// </para>
/// </remarks>
internal sealed class BackoffPollingHelper
{
    private readonly RandomizedExponentialBackoffStrategy _backoffStrategy;
    private readonly PollingSignal _resetEvent = new();

    /// <summary>
    /// Guards <see cref="_backoffStrategy"/>. Its <c>CurrentInterval</c> / <c>_backoffExponent</c>
    /// are read-modify-written on every wait, and <see cref="PollingSignal.Set"/> releases all
    /// waiters at once, so several <see cref="WaitAsync"/> calls can run concurrently.
    /// </summary>
    private readonly object _strategyLock = new();

    public BackoffPollingHelper(TimeSpan minimumInterval, TimeSpan maximumInterval, TimeSpan deltaBackoff)
    {
        _backoffStrategy = new RandomizedExponentialBackoffStrategy(minimumInterval, maximumInterval, deltaBackoff);
    }

    /// <summary>
    /// The interval the next <see cref="WaitAsync"/> call will wait for. Exposed for tests.
    /// </summary>
    internal TimeSpan CurrentInterval
    {
        get
        {
            lock (_strategyLock)
            {
                return _backoffStrategy.CurrentInterval;
            }
        }
    }

    /// <summary>
    /// Signals that work may be available, waking every waiter. Each of them collapses its
    /// backoff interval back to the minimum as its wait completes.
    /// </summary>
    public void Reset() => _resetEvent.Set();

    /// <summary>
    /// Reads the current signal generation. A dispatcher captures this <em>before</em> polling
    /// and passes it to <see cref="WaitAsync"/>, so a <see cref="Reset"/> that lands while the
    /// poll is in flight is not lost.
    /// </summary>
    public ulong CurrentGeneration => _resetEvent.CurrentGeneration;

    /// <summary>
    /// Waits for the current backoff interval, or returns early if <see cref="Reset"/> was
    /// called since <paramref name="observedGeneration"/> was captured. Grows the interval
    /// toward the configured maximum when the wait times out.
    /// </summary>
    /// <param name="observedGeneration">
    /// Value of <see cref="CurrentGeneration"/> captured before the caller's last poll.
    /// </param>
    /// <param name="cancellationToken">Aborts the wait on shutdown.</param>
    /// <returns><c>true</c> when woken by <see cref="Reset"/>; <c>false</c> on timeout.</returns>
    public async Task<bool> WaitAsync(ulong observedGeneration, CancellationToken cancellationToken)
    {
        TimeSpan interval;
        lock (_strategyLock)
        {
            interval = _backoffStrategy.CurrentInterval;
        }

        bool signaled = await _resetEvent
            .WaitAsync(interval, observedGeneration, cancellationToken)
            .ConfigureAwait(false);

        lock (_strategyLock)
        {
            _backoffStrategy.UpdateDelay(signaled);
        }

        return signaled;
    }

    private sealed class RandomizedExponentialBackoffStrategy
    {
        private const double RandomizationFactor = 0.2;

        private readonly TimeSpan _minimumInterval;
        private readonly TimeSpan _maximumInterval;
        private readonly TimeSpan _deltaBackoff;

        private uint _backoffExponent;

        public RandomizedExponentialBackoffStrategy(
            TimeSpan minimumInterval,
            TimeSpan maximumInterval,
            TimeSpan deltaBackoff)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(minimumInterval, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfLessThan(maximumInterval, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfLessThan(deltaBackoff, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumInterval, maximumInterval);

            _minimumInterval = minimumInterval;
            _maximumInterval = maximumInterval;
            _deltaBackoff = deltaBackoff;

            // Start at the minimum so the first idle poll already waits. Leaving this at
            // TimeSpan.Zero (the MSSQL reference's implicit default) makes the first wait a
            // no-op busy spin.
            this.CurrentInterval = minimumInterval;
        }

        public TimeSpan CurrentInterval { get; private set; }

        public TimeSpan UpdateDelay(bool executionSucceeded)
        {
            if (executionSucceeded)
            {
                this.CurrentInterval = _minimumInterval;
                _backoffExponent = 1;
            }
            else if (this.CurrentInterval != _maximumInterval)
            {
                TimeSpan backoffInterval = _minimumInterval;

                if (_backoffExponent > 0)
                {
                    // Jitter in [1 - RandomizationFactor, 1 + RandomizationFactor] so replicas
                    // that started together do not re-align on the same poll instant.
                    double jitter = 1.0 - RandomizationFactor +
                        (Random.Shared.NextDouble() * 2.0 * RandomizationFactor);
                    double incrementMsec =
                        jitter * Math.Pow(2.0, _backoffExponent - 1) * _deltaBackoff.TotalMilliseconds;
                    backoffInterval += TimeSpan.FromMilliseconds(incrementMsec);
                }

                if (backoffInterval < _maximumInterval)
                {
                    this.CurrentInterval = backoffInterval;
                    _backoffExponent++;
                }
                else
                {
                    this.CurrentInterval = _maximumInterval;
                }
            }

            // else: already at the maximum, keep it there.
            return this.CurrentInterval;
        }
    }
}
