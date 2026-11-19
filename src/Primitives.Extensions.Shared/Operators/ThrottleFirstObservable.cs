// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Throttles a sequence by only emitting the first element in each window.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="window">The window duration.</param>
/// <param name="scheduler">The scheduler to use for timing.</param>
public sealed class ThrottleFirstObservable<T>(
    IObservable<T> source,
    TimeSpan window,
    ISequencer scheduler) : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ThrottleFirstObservable{T}"/> class using <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="window">The window duration.</param>
    public ThrottleFirstObservable(IObservable<T> source, TimeSpan window)
        : this(source, window, Sequencer.Default)
    {
    }

    /// <summary>Creates a throttle-first sequence, defaulting a <see langword="null"/> <paramref name="scheduler"/> to <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="window">The window duration.</param>
    /// <param name="scheduler">Optional scheduler to use for timing.</param>
    /// <returns>A throttle-first sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ThrottleFirstObservable<T> Create(
        IObservable<T> source,
        TimeSpan window,
        ISequencer? scheduler) =>
        new(source, window, scheduler ?? Sequencer.Default);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new ThrottleFirstWitness(observer, window, scheduler));
    }

    /// <summary>Observer that implements the throttle first logic.</summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="window">The window duration.</param>
    /// <param name="scheduler">The scheduler to use for timing.</param>
    /// <remarks>Deliveries are serialized, and no lock is held while the observer runs.</remarks>
    private sealed class ThrottleFirstWitness(
        IObserver<T> downstream,
        TimeSpan window,
        ISequencer scheduler) : IObserver<T>
    {
        /// <summary>Guards the window state and the terminal flag; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The last time an element was emitted.</summary>
        private DateTimeOffset _last;

        /// <summary>Whether at least one element has been emitted.</summary>
        private bool _hasLast;

        /// <summary>Whether the observer has finished.</summary>
        private bool _done;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            var now = scheduler.Now;
            bool emit;

            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                emit = !_hasLast || now - _last >= window;
                if (emit)
                {
                    _last = now;
                    _hasLast = true;
                }
            }

            if (!emit)
            {
                return;
            }

            _delivery.OnNext(downstream, value, new PendingDrain(this));
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
            }

            _delivery.OnError(error, new PendingDrain(this));
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
            }

            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Delivers the queued notifications to the downstream observer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DrainPending() => _ = _delivery.DrainTo(downstream);

        /// <summary>Drains this observer's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The observer.</param>
        private readonly record struct PendingDrain(ThrottleFirstWitness Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => Owner.DrainPending();
        }
    }
}
