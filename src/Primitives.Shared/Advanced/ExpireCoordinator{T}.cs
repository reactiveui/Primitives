// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates timeout delivery with one active timer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <remarks>
/// The gate only guards the deadline, the epoch and the termination flag. Deliveries are serialized by a
/// <see cref="SerializedDelivery{T}"/>, so no lock is held while the observer runs and a timeout raised during a value's
/// delivery follows that value. A terminal notification raised before <see cref="Dispose"/> is still delivered.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("ExpireCoordinator: Done = {_done}, DueTime = {_dueTime}, Deadline = {_deadline}")]
public sealed class ExpireCoordinator<T> : IObserver<T>, IDisposable
{
    /// <summary>Guards the deadline, the epoch and the termination flag; never held while the observer runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The timeout period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer that schedules the timeout.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Whether a value restarts the timeout window, which is what separates an inactivity timeout from a deadline.</summary>
    private readonly bool _restartOnValue;

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

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

    /// <summary>A value indicating whether a terminal notification has been queued.</summary>
    private int _done;

    /// <summary>Monotonic version that suppresses timeouts superseded by a newer value.</summary>
    private long _epoch;

    /// <summary>Deadline guarded by the gate; MaxValue prevents expiration until the first window is armed.</summary>
    private DateTimeOffset _deadline = DateTimeOffset.MaxValue;

    /// <summary>Initializes a new instance of the <see cref="ExpireCoordinator{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer that schedules the timeout.</param>
    /// <param name="observer">The downstream observer.</param>
    public ExpireCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, IObserver<T> observer)
        : this(source, dueTime, sequencer, observer, restartOnValue: true)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ExpireCoordinator{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer that schedules the timeout.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="restartOnValue">When <see langword="true"/> each value restarts the window; when <see langword="false"/> the window runs once from subscription.</param>
    internal ExpireCoordinator(
        IObservable<T> source,
        TimeSpan dueTime,
        ISequencer sequencer,
        IObserver<T> observer,
        bool restartOnValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        ArgumentExceptionHelper.ThrowIfNull(observer);

        _source = source;
        _dueTime = dueTime;
        _sequencer = sequencer;
        _observer = observer;
        _restartOnValue = restartOnValue;
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
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _done = 1;
            _ = _delivery.PostCompleted();
        }

        FlushThenDispose();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _done = 1;
            _ = _delivery.PostError(error);
        }

        FlushThenDispose();
    }

    /// <inheritdoc/>
    /// <remarks>Values arriving at or after the clock deadline fail with TimeoutException, even if the timer callback has not run.</remarks>
    public void OnNext(T value)
    {
        long epoch = 0;
        var expired = false;
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            if (_sequencer.Now >= _deadline)
            {
                _done = 1;
                expired = true;
                _ = _delivery.PostError(new TimeoutException());
            }
            else if (_restartOnValue)
            {
                epoch = ++_epoch;
            }
        }

        if (expired)
        {
            FlushThenDispose();
            return;
        }

        _delivery.OnNext(_observer, value, new PendingDrain(this));

        // A deadline keeps the window armed at subscription; only an inactivity timeout restarts it.
        if (_restartOnValue)
        {
            ArmTimer(epoch);
        }
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

    /// <summary>Schedules outside the gate and publishes the timer only if its window remains current.</summary>
    /// <param name="epoch">The version this timer must match to fire.</param>
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

    /// <summary>Captures a deadline for positive delays; zero delays remain queued immediate work.</summary>
    /// <returns>The deadline on the sequencer's clock, saturated instead of overflowing.</returns>
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

    /// <summary>Queues the timeout error when the firing timer is the current one.</summary>
    /// <param name="epoch">The version captured when the firing timer was armed.</param>
    /// <returns>An empty disposable.</returns>
    private EmptyDisposable EmitTimeout(long epoch)
    {
        lock (_gate)
        {
            if (_done != 0 || epoch != _epoch)
            {
                return EmptyDisposable.Instance;
            }

            _done = 1;
            _ = _delivery.PostError(new TimeoutException());
        }

        FlushThenDispose();
        return EmptyDisposable.Instance;
    }

    /// <summary>Delivers the queued terminal notification, then releases the source subscription and the timer.</summary>
    private void FlushThenDispose()
    {
        try
        {
            _delivery.Flush(new PendingDrain(this));
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(ExpireCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }
}
