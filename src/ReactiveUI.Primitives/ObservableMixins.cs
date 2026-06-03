// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Threading;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>
/// Internal observable helpers for operators that ReactiveUI.Primitives needs but does not expose directly.
/// </summary>
internal static class ObservableMixins
{
    /// <summary>
    /// Forwards source values until <paramref name="other"/> emits a value. Completion of <paramref name="other"/> without
    /// a value does not stop the source.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TOther">The cancellation value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="other">The observable that stops the source when it emits.</param>
    /// <returns>An observable that completes when the source completes or <paramref name="other"/> emits.</returns>
    public static IObservable<T> TakeUntil<T, TOther>(this IObservable<T> source, IObservable<TOther> other)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(other);
#else
        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (other is null)
        {
            throw new ArgumentNullException(nameof(other));
        }
#endif

        return Signal.Create<T>(observer =>
        {
            var coordinator = new TakeUntilCoordinator<T>(observer);

            coordinator.Add(other.Subscribe(_ => coordinator.Complete(), coordinator.Error));
            if (coordinator.IsStopped)
            {
                return coordinator;
            }

            coordinator.Add(source.Subscribe(coordinator.Next, coordinator.Error, coordinator.Complete));

            return coordinator;
        });
    }

    /// <summary>
    /// Coordinates serialized observer callbacks and subscription lifetime for <see cref="TakeUntil{T, TOther}"/>.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class TakeUntilCoordinator<T> : IDisposable
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// Serializes downstream observer callbacks.
        /// </summary>
        private readonly Lock _gate = new();

        /// <summary>
        /// Tracks the source and cancellation subscriptions.
        /// </summary>
        private readonly MultipleDisposable _subscriptions = new();

        /// <summary>
        /// Indicates whether the sequence has already stopped.
        /// </summary>
        private int _stopped;

        /// <summary>
        /// Initializes a new instance of the <see cref="TakeUntilCoordinator{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        public TakeUntilCoordinator(IObserver<T> observer) => _observer = observer;

        /// <summary>
        /// Gets a value indicating whether the sequence has stopped.
        /// </summary>
        public bool IsStopped => Volatile.Read(ref _stopped) != 0;

        /// <summary>
        /// Adds a subscription to the coordinator lifetime.
        /// </summary>
        /// <param name="subscription">The subscription to add.</param>
        public void Add(IDisposable subscription) => _subscriptions.Add(subscription);

        /// <summary>
        /// Completes the downstream observer once and disposes all subscriptions.
        /// </summary>
        public void Complete()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            lock (_gate)
            {
                _observer.OnCompleted();
            }

            _subscriptions.Dispose();
        }

        /// <summary>
        /// Sends an error to the downstream observer once and disposes all subscriptions.
        /// </summary>
        /// <param name="exception">The exception to forward.</param>
        public void Error(Exception exception)
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            lock (_gate)
            {
                _observer.OnError(exception);
            }

            _subscriptions.Dispose();
        }

        /// <summary>
        /// Forwards a source value when the sequence has not stopped.
        /// </summary>
        /// <param name="value">The source value.</param>
        public void Next(T value)
        {
            lock (_gate)
            {
                if (!IsStopped)
                {
                    _observer.OnNext(value);
                }
            }
        }

        /// <inheritdoc/>
        public void Dispose() => _subscriptions.Dispose();
    }
}
