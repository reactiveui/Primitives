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

/// <summary>Throttles a sequence until a predicate becomes true for an element.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="throttle">The throttle duration.</param>
/// <param name="predicate">The predicate to determine if an element should be emitted immediately or throttled.</param>
/// <param name="sequencer">The sequencer timing throttled emissions; <c>null</c> uses the default sequencer.</param>
internal sealed class ThrottleUntilTrueObservable<T>(
    IObservable<T> source,
    TimeSpan throttle,
    Func<T, bool> predicate,
    ISequencer? sequencer = null) : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = InvalidOperationExceptionHelper.Check(source);

    /// <summary>The throttle duration.</summary>
    private readonly TimeSpan _throttle = throttle;

    /// <summary>The predicate to determine if an element should be emitted immediately or throttled.</summary>
    private readonly Func<T, bool> _predicate = InvalidOperationExceptionHelper.Check(predicate);

    /// <summary>The sequencer timing throttled emissions.</summary>
    private readonly ISequencer _sequencer = sequencer ?? Sequencer.Default;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ThrottleUntilTrueSink sink = new(observer, _throttle, _predicate, _sequencer);
        var subscription = _source.Subscribe(sink);
        return new DisposableBag(subscription, sink);
    }

    /// <summary>Sink that forwards a value inline when the predicate holds and otherwise after the throttle window.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="throttle">The throttle duration.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="scheduler">The scheduler used to time throttled emissions.</param>
    /// <remarks>
    /// Notifications are queued in order under the gate and delivered after it is released, so neither the observer nor the
    /// predicate runs while the gate is held.
    /// </remarks>
    private sealed class ThrottleUntilTrueSink(
        IObserver<T> downstream,
        TimeSpan throttle,
        Func<T, bool> predicate,
        ISequencer scheduler) : IObserver<T>, IDisposable
    {
        /// <summary>Guards the terminal flag and the order notifications are queued in.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream = downstream;

        /// <summary>The throttle duration.</summary>
        private readonly TimeSpan _throttle = throttle;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <summary>The timer for throttling.</summary>
        private readonly SwapDisposable _timer = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the sequence is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (!_predicate(value))
            {
                _timer.Disposable = scheduler.Schedule(
                    (Sink: this, Value: value),
                    _throttle,
                    static (_, state) =>
                    {
                        state.Sink.EmitThrottled(state.Value);
                        return EmptyDisposable.Instance;
                    });
                return;
            }

            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _timer.Disposable = null;
                _ = _delivery.Post(value);
            }

            Flush();
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
                _timer.Dispose();
                _ = _delivery.PostError(error);
            }

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
                _timer.Dispose();
                _ = _delivery.PostCompleted();
            }

            Flush();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _done = true;
                _timer.Dispose();
            }
        }

        /// <summary>Queues and delivers a throttled value; the delivery refuses it once a terminal notification is queued.</summary>
        /// <param name="value">The throttled value.</param>
        private void EmitThrottled(T value)
        {
            _ = _delivery.Post(value);
            Flush();
        }

        /// <summary>Delivers the queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Drains this sink's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The sink.</param>
        private readonly record struct PendingDrain(ThrottleUntilTrueSink Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._downstream);
        }
    }
}
