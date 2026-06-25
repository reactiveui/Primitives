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
        Justification = "Disposed via the thread-safe Interlocked.Exchange teardown in Dispose; CA2213 does not recognize disposal of a field through Interlocked.Exchange.")]
    private IDisposable? _subscription;

    /// <summary>The active timeout timer.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed via the thread-safe Interlocked.Exchange teardown in Dispose; CA2213 does not recognize disposal of a field through Interlocked.Exchange.")]
    private IDisposable? _timer;

    /// <summary>A value indicating whether the timeout or source has terminated.</summary>
    private int _done;

    /// <summary>Monotonic version used to suppress timeouts superseded by a newer value.</summary>
    private long _epoch;

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
    public void OnNext(T value)
    {
        long epoch;
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            epoch = ++_epoch;
            _observer.OnNext(value);
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
        _subscription = _source.Subscribe(this);
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
    /// sequencer; the publish is re-checked under the gate so a timer never survives a terminal notification.</remarks>
    private void ArmTimer(long epoch)
    {
        var timer = _sequencer.Schedule((Coordinator: this, Epoch: epoch), _dueTime, static (_, state) => state.Coordinator.EmitTimeout(state.Epoch));

        IDisposable? previous;
        lock (_gate)
        {
            if (_done != 0)
            {
                timer.Dispose();
                return;
            }

            previous = Interlocked.Exchange(ref _timer, timer);
        }

        previous?.Dispose();
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
