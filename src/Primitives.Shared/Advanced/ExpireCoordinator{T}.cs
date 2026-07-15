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
public sealed class ExpireCoordinator<T> : IObserver<T>, IDisposable
{
    /// <summary>The synchronization gate for downstream observer calls.</summary>
    private readonly Lock _gate = new();

    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The timeout period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule the timeout.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The active source subscription.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification =
            "Disposed via the thread-safe Interlocked.Exchange teardown in Dispose; CA2213 does not recognize disposal of a field through Interlocked.Exchange.")]
    private IDisposable? _subscription;

    /// <summary>The active timeout timer.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification =
            "Disposed via the thread-safe Interlocked.Exchange teardown in Dispose; CA2213 does not recognize disposal of a field through Interlocked.Exchange.")]
    private IDisposable? _timer;

    /// <summary>A value indicating whether the timeout or source has terminated.</summary>
    private int _done;

    /// <summary>Monotonic version used to suppress timeouts superseded by a newer value.</summary>
    private long _epoch;

    /// <summary>
    /// The instant, on the sequencer's own clock, at which the current inactivity window closes. Read and written
    /// under <see cref="_gate"/>. Starts at <see cref="DateTimeOffset.MaxValue"/> so a window that has not been
    /// published yet can never expire a value.
    /// </summary>
    private DateTimeOffset _deadline = DateTimeOffset.MaxValue;

    /// <summary>Initializes a new instance of the <see cref="ExpireCoordinator{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer used to schedule the timeout.</param>
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
    /// A value is on time only when it arrives before the current inactivity window closes, which is a question for
    /// the sequencer's clock — not for whether the armed timer has run yet. The timer is dispatched by the sequencer,
    /// and a thread-pool sequencer whose pool is saturated can dispatch it arbitrarily late while a source on another
    /// thread keeps producing. Forwarding a value in that gap would deliver a value the operator has already promised
    /// to time out, so a value that arrives after its deadline expires the sequence here instead.
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
    /// <param name="epoch">The version this timer must still match to fire.</param>
    /// <remarks>Scheduled outside the gate to avoid reentrant <see cref="Lock"/> acquisition on a synchronous
    /// sequencer; the publish is re-checked under the gate so a timer never survives a terminal notification, and
    /// so a superseded arm cannot publish its older deadline and timer over a newer value's.</remarks>
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

    /// <summary>Computes the instant the inactivity window opened now would close at.</summary>
    /// <returns>The deadline on the sequencer's clock, saturated instead of overflowing.</returns>
    /// <remarks>
    /// A due time that normalizes to zero is scheduled as immediate work rather than timed work, so it has no clock
    /// window: the timeout is ordered by the sequencer's queue, and a synchronous value that arrives before the queue
    /// drains still wins. That branch mirrors the scheduling extension's own zero-due-time path, keeping the deadline
    /// in lockstep with how the timer was actually scheduled. Only a positive due time opens a window on the clock.
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

    /// <summary>Emits the timeout error when the firing timer is still current.</summary>
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
