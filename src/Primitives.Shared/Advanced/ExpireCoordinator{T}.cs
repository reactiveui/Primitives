// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates timeout delivery with one active timer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ExpireCoordinator: Done = {_done}, DueTime = {_dueTime}, Deadline = {_deadline}")]
public sealed class ExpireCoordinator<T> : IObserver<T>, IDisposable
{
    /// <summary>The synchronization gate for downstream observer calls.</summary>
    private readonly Lock _gate = new();

    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The timeout period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer that schedules the timeout.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The active source subscription.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed through Interlocked.Exchange in Dispose.")]
    private IDisposable? _subscription;

    /// <summary>The active timeout timer.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed through Interlocked.Exchange in Dispose.")]
    private IDisposable? _timer;

    /// <summary>A value indicating whether the timeout or source has terminated.</summary>
    private int _done;

    /// <summary>Monotonic version that suppresses timeouts superseded by a newer value.</summary>
    private long _epoch;

    /// <summary>
    /// The instant on the sequencer's clock at which the current inactivity window closes, read and written under
    /// <see cref="_gate"/>. It starts at <see cref="DateTimeOffset.MaxValue"/> so an unpublished window never
    /// expires a value.
    /// </summary>
    private DateTimeOffset _deadline = DateTimeOffset.MaxValue;

    /// <summary>Initializes a new instance of the <see cref="ExpireCoordinator{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer that schedules the timeout.</param>
    /// <param name="observer">The downstream observer.</param>
    public ExpireCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        ArgumentExceptionHelper.ThrowIfNull(observer);

        _source = source;
        _dueTime = dueTime;
        _sequencer = sequencer;
        _observer = observer;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Interlocked.Exchange(ref _timer, null)?.Dispose();

        Interlocked.Exchange(ref _subscription, null)?.Dispose();
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        var shouldDispose = false;
        try
        {
            lock (_gate)
            {
                if (_done != 0)
                {
                    return;
                }

                _done = 1;
                shouldDispose = true;
                _observer.OnCompleted();
            }
        }
        finally
        {
            if (shouldDispose)
            {
                Dispose();
            }
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        var shouldDispose = false;
        try
        {
            lock (_gate)
            {
                if (_done != 0)
                {
                    return;
                }

                _done = 1;
                shouldDispose = true;
                _observer.OnError(error);
            }
        }
        finally
        {
            if (shouldDispose)
            {
                Dispose();
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A value is on time only when it arrives before the inactivity window closes on the sequencer's clock, not
    /// merely before the armed timer has run. A value that arrives past its deadline — which a saturated thread-pool
    /// sequencer can allow — terminates the sequence with <see cref="TimeoutException"/> instead of being forwarded.
    /// </remarks>
    public void OnNext(T value)
    {
        long epoch;
        var shouldDispose = false;
        try
        {
            lock (_gate)
            {
                if (_done != 0)
                {
                    return;
                }

                if (_sequencer.Now >= _deadline)
                {
                    _done = 1;
                    shouldDispose = true;
                    _observer.OnError(new TimeoutException());
                    return;
                }

                epoch = ++_epoch;
                _observer.OnNext(value);
            }
        }
        finally
        {
            if (shouldDispose)
            {
                Dispose();
            }
        }

        ArmTimer(epoch);
    }

    /// <summary>Starts observing the source and timeout timer.</summary>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    public ExpireCoordinator<T> Run()
    {
        long epoch;
        lock (_gate)
        {
            epoch = ++_epoch;
        }

        ArmTimer(epoch);
        Volatile.Write(ref _subscription, _source.Subscribe(this));
        if (Volatile.Read(ref _done) == 0)
        {
            return this;
        }

        Dispose();
        return this;
    }

    /// <summary>Schedules a fresh inactivity timer for the given epoch and discards the in-flight one.</summary>
    /// <param name="epoch">The version this timer must match to fire.</param>
    /// <remarks>Scheduling happens outside the gate so a synchronous sequencer cannot re-enter <see cref="Lock"/>.
    /// The publish is re-checked under the gate, so neither a terminal notification nor a newer value's window can be
    /// overwritten by a superseded arm.</remarks>
    private void ArmTimer(long epoch)
    {
        var deadline = Deadline();
        var timer = _sequencer.Schedule(
            (Coordinator: this, Epoch: epoch),
            _dueTime,
            static (_, state) => state.Coordinator.EmitTimeout(state.Epoch));

        IDisposable? previous;
        lock (_gate)
        {
            if (_done != 0 || epoch != _epoch)
            {
                timer.Dispose();
                return;
            }

            _deadline = deadline;
            previous = Interlocked.Exchange(ref _timer, timer);
        }

        previous?.Dispose();
    }

    /// <summary>Computes the closing instant of an inactivity window opened at the current time.</summary>
    /// <returns>The deadline on the sequencer's clock, saturated instead of overflowing.</returns>
    /// <remarks>
    /// A due time that normalizes to zero is queued as immediate work, so it opens no clock window and a synchronous
    /// value arriving before the queue drains wins; only a positive due time yields a real deadline.
    /// </remarks>
    private DateTimeOffset Deadline()
    {
        var dueTime = Sequencer.Normalize(_dueTime);
        if (dueTime == TimeSpan.Zero)
        {
            return DateTimeOffset.MaxValue;
        }

        var now = _sequencer.Now;
        return DateTimeOffset.MaxValue - now <= dueTime ? DateTimeOffset.MaxValue : now + dueTime;
    }

    /// <summary>Emits the timeout error when the firing timer is the current one.</summary>
    /// <param name="epoch">The version captured when the firing timer was armed.</param>
    /// <returns>An empty disposable.</returns>
    private EmptyDisposable EmitTimeout(long epoch)
    {
        var shouldDispose = false;
        try
        {
            lock (_gate)
            {
                if (_done != 0 || epoch != _epoch)
                {
                    return EmptyDisposable.Instance;
                }

                _done = 1;
                shouldDispose = true;
                _observer.OnError(new TimeoutException());
            }
        }
        finally
        {
            if (shouldDispose)
            {
                Dispose();
            }
        }

        return EmptyDisposable.Instance;
    }
}
