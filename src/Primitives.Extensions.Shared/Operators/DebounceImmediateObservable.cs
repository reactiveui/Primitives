// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Emits the first value inline, then the most recent value after <paramref name="dueTime"/> of quiet.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="dueTime">The debounce duration.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
public sealed class DebounceImmediateObservable<T>(
    IObservable<T> source,
    TimeSpan dueTime,
    ISequencer scheduler) : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="DebounceImmediateObservable{T}"/> class using <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The debounce duration.</param>
    public DebounceImmediateObservable(IObservable<T> source, TimeSpan dueTime)
        : this(source, dueTime, Sequencer.Default)
    {
    }

    /// <summary>Creates a debounce-immediate sequence, defaulting a <see langword="null"/> <paramref name="scheduler"/> to <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The debounce duration.</param>
    /// <param name="scheduler">Optional scheduler to use for timing.</param>
    /// <returns>A debounced sequence whose first emission is immediate.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DebounceImmediateObservable<T> Create(
        IObservable<T> source,
        TimeSpan dueTime,
        ISequencer? scheduler) =>
        new(source, dueTime, scheduler ?? Sequencer.Default);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DebounceImmediateSink sink = new(observer, dueTime, scheduler);
        sink.Attach(source.Subscribe(sink));
        return sink;
    }

    /// <summary>Sink that forwards the first value inline and debounces every later value by the due time.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="dueTime">The debounce duration.</param>
    /// <param name="scheduler">The scheduler to use for timing.</param>
    /// <remarks>Emissions and terminals are queued in order under the gate and delivered after it is released.</remarks>
    private sealed class DebounceImmediateSink(
        IObserver<T> downstream,
        TimeSpan dueTime,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the pending value, the flags and the order notifications are queued in; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>The pending debounce timer, replaced whenever a newer value arrives.</summary>
        private readonly SwapDisposable _timer = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Upstream subscription handle.</summary>
        private IDisposable? _subscription;

        /// <summary>Whether the first value has yet to be emitted.</summary>
        private bool _isFirst = true;

        /// <summary>The last value received.</summary>
        private T? _lastValue;

        /// <summary>Whether a value is pending.</summary>
        private bool _hasValue;

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            bool isFirst;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                isFirst = _isFirst;
                _isFirst = false;
                if (isFirst)
                {
                    _ = _delivery.Post(value);
                }
                else
                {
                    _lastValue = value;
                    _hasValue = true;
                }
            }

            if (isFirst)
            {
                Flush();
                return;
            }

            _timer.Disposable = scheduler.Schedule(this, dueTime, static s => s.Emit());
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                QueuePendingLocked();
                _timer.Dispose();
                _ = _delivery.PostError(error);
            }

            SinkSubscription.Dispose(ref _subscription);
            Flush();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                QueuePendingLocked();
                _timer.Dispose();
                _ = _delivery.PostCompleted();
            }

            SinkSubscription.Dispose(ref _subscription);
            Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _lastValue = default;
                _hasValue = false;
                _timer.Dispose();
            }

            SinkSubscription.Dispose(ref _subscription);
        }

        /// <summary>Attaches the upstream subscription handle.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        internal void Attach(IDisposable? subscription)
        {
            if (subscription is not null)
            {
                SinkSubscription.Set(ref _subscription, subscription);
            }
        }

        /// <summary>Queues and delivers the waiting value, if there is one.</summary>
        private void Emit()
        {
            lock (_gate)
            {
                QueuePendingLocked();
            }

            Flush();
        }

        /// <summary>Queues the waiting value, if there is one, and clears it while the caller holds the gate.</summary>
        private void QueuePendingLocked()
        {
            if (!_hasValue)
            {
                return;
            }

            _hasValue = false;
            _ = _delivery.Post(_lastValue!);
        }

        /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        private void DrainPending()
        {
            try
            {
                if (_delivery.DrainTo(downstream))
                {
                    SinkSubscription.Dispose(ref _subscription);
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(DebounceImmediateSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
