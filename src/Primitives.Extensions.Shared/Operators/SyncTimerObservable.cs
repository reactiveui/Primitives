// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Caches and shares one running timer per <c>(TimeSpan, ISequencer)</c> key.</summary>
internal static class SyncTimerObservable
{
    /// <summary>The timer cache, keyed by <c>(TimeSpan, ISequencer)</c>.</summary>
    private static readonly ConcurrentDictionary<(TimeSpan TimeSpan, ISequencer Scheduler), SharedTimer>
        _timerList = [];

    /// <summary>Static factory passed to <c>ConcurrentDictionary.GetOrAdd</c>; avoids a per-call delegate allocation.</summary>
    private static readonly Func<(TimeSpan TimeSpan, ISequencer Scheduler), SharedTimer> _create =
        static key => new(key.TimeSpan, key.Scheduler);

    /// <summary>Gets a shared timer for the specified period and scheduler.</summary>
    /// <param name="timeSpan">The period.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>A shared observable sequence of timer ticks.</returns>
    internal static IObservable<DateTime> Get(TimeSpan timeSpan, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return _timerList.GetOrAdd((timeSpan, scheduler), _create);
    }

    /// <summary>
    /// Connectable timer that fans each tick out to its observers: the tick path reads a swap-on-write observer
    /// array lock-free, while subscribe and unsubscribe take the gate and publish a fresh array.
    /// </summary>
    /// <param name="timeSpan">The period.</param>
    /// <param name="scheduler">The scheduler.</param>
    private sealed class SharedTimer(TimeSpan timeSpan, ISequencer scheduler) : IObservable<DateTime>
    {
        /// <summary>Sentinel empty observer array, shared so unsubscribing the last observer doesn't allocate.</summary>
        private static readonly IObserver<DateTime>[] _emptyObservers = [];

        /// <summary>The gate for subscribe/unsubscribe writes.</summary>
        private readonly Lock _gate = new();

        /// <summary>
        /// Snapshot of active observers, replaced rather than mutated on subscribe and unsubscribe under
        /// <see cref="_gate"/> so the tick path can read it without the lock.
        /// </summary>
        private IObserver<DateTime>[] _observers = _emptyObservers;

        /// <summary>The active timer subscription, or <see langword="null"/> when no observers are attached.</summary>
        private IDisposable? _timerSubscription;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<DateTime> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            lock (_gate)
            {
                var current = _observers;
                var copy = new IObserver<DateTime>[current.Length + 1];
                for (var i = 0; i < current.Length; i++)
                {
                    copy[i] = current[i];
                }

                copy[current.Length] = observer;
                Volatile.Write(ref _observers, copy);

                _timerSubscription ??= scheduler.SchedulePeriodic(TimeSpan.Zero, timeSpan, Tick);
            }

            return new TimerSubscription(this, observer);
        }

        /// <summary>Ticks every currently-subscribed observer with the scheduler's current time.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Tick() =>
            ObserverArrayHelpers.Broadcast(Volatile.Read(ref _observers), scheduler.Now.DateTime);

        /// <summary>Removes <paramref name="observer"/> from the observer set, stopping the timer when the set becomes empty.</summary>
        /// <param name="observer">The observer to remove.</param>
        private void Remove(IObserver<DateTime> observer)
        {
            lock (_gate)
            {
                // Never null: Dispose's Interlocked guard admits one Remove per subscription, and the
                // observer was added under this same lock before the disposable was handed out.
                var updated = ObserverArrayHelpers.RemoveOrNull(_observers, observer, _emptyObservers)!;

                Volatile.Write(ref _observers, updated);
                if (ReferenceEquals(updated, _emptyObservers))
                {
                    // Never null: reaching an empty set means Subscribe ran, which arms the timer.
                    _timerSubscription!.Dispose();
                    _timerSubscription = null;
                }
            }
        }

        /// <summary>Per-subscribe disposable that detaches its observer from the owning timer exactly once.</summary>
        /// <param name="parent">The owning timer.</param>
        /// <param name="observer">The observer to remove on dispose.</param>
        private sealed class TimerSubscription(SharedTimer parent, IObserver<DateTime> observer) : IDisposable
        {
            /// <summary>0 = active, 1 = disposed.</summary>
            private int _disposed;

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 1)
                {
                    return;
                }

                parent.Remove(observer);
            }
        }
    }
}
