// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Delay extension methods for asynchronous observable sequences.</summary>
/// <remarks>Delay time-shifts the observable sequence by the specified time span. Each element is
/// emitted after a relative delay from the time it was produced by the source. Errors and completion
/// are not delayed.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Delay operators that time-shift an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Time-shifts the observable sequence by the specified time span. Each element notification
        /// is delayed by the specified duration.
        /// </summary>
        /// <param name="delayInterval">The time span by which to delay each element notification. Must be non-negative.</param>
        /// <returns>An observable sequence with element notifications time-shifted by the specified duration.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="delayInterval"/> is negative.</exception>
        public IObservableAsync<T> Shift(TimeSpan delayInterval)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(delayInterval, TimeSpan.Zero);

            return delayInterval == TimeSpan.Zero
                ? @this
                : new DelaySignal<T>(@this, delayInterval, TimeProvider.System);
        }

    }

    /// <summary>An observable that delays each element notification by a specified duration.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="delayInterval">The time span by which to delay each element notification.</param>
    /// <param name="timeProvider">The time provider used to control timing.</param>
    internal sealed class DelaySignal<T>(IObservableAsync<T> source, TimeSpan delayInterval, TimeProvider timeProvider)
        : IObservableAsync<T>
    {
        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DelayWitness delayObserver = new(observer, delayInterval, timeProvider, cancellationToken);
            return source.SubscribeAsync(delayObserver, cancellationToken);
        }

        /// <summary>A witness that delays each element by waiting before forwarding to the downstream witness.</summary>
        /// <param name="observer">The downstream observer to forward delayed notifications to.</param>
        /// <param name="delayInterval">The time span by which to delay each element notification.</param>
        /// <param name="timeProvider">The time provider used to control timing.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class DelayWitness(
            IObserverAsync<T> observer,
            TimeSpan delayInterval,
            TimeProvider timeProvider,
            CancellationToken subscribeToken)
            : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                await DelayAsync(delayInterval, timeProvider, cancellationToken).ConfigureAwait(false);
                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(
                Exception error,
                CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                observer.OnCompletedAsync(result);
        }
    }
}
