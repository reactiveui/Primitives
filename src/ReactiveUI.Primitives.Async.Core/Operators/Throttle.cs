// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Throttle (debounce) extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Throttle (debounce) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Forwards the latest element after a full quiet period.</summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        public IObservableAsync<T> Throttle(TimeSpan dueTime)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(dueTime, TimeSpan.Zero);

            return new ThrottleSignal<T>(source, dueTime, TimeProvider.System);
        }

        /// <summary>Forwards the latest element after a full quiet period.</summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        public IObservableAsync<T> Throttle(TimeSpan dueTime, TimeProvider? timeProvider)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(dueTime, TimeSpan.Zero);

            return new ThrottleSignal<T>(source, dueTime, timeProvider ?? TimeProvider.System);
        }
    }

    /// <summary>Delay operators for a relative duration.</summary>
    /// <param name="delay">The duration to delay.</param>
    extension(TimeSpan delay)
    {
        /// <summary>Creates a cancellable delay using the supplied time provider.</summary>
        /// <param name="timeProvider">The time provider to use for the delay.</param>
        /// <param name="cancellationToken">A token to cancel the delay.</param>
        /// <returns>A <see cref="ValueTask"/> that completes after the specified delay.</returns>
        internal ValueTask DelayAsync(
            TimeProvider timeProvider,
            CancellationToken cancellationToken) =>
            timeProvider == TimeProvider.System
                ? DelayOnSystemClockAsync(delay, cancellationToken)
                : PooledDelaySource.Rent().BeginAsync(delay, timeProvider, cancellationToken);
    }

    /// <summary>Waits for the system clock or cancellation.</summary>
    /// <param name="delay">The duration to wait.</param>
    /// <param name="cancellationToken">Cancellation for the wait.</param>
    /// <returns>The delay operation.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static ValueTask DelayOnSystemClockAsync(TimeSpan delay, CancellationToken cancellationToken) =>
        new(Task.Delay(delay, cancellationToken));

    /// <summary>Async observable that debounces the source sequence, only forwarding elements that are not followed by another element within the specified due time.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence to throttle.</param>
    /// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
    /// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
    internal sealed class ThrottleSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <summary>Subscribes the specified observer with throttle behavior applied.</summary>
        /// <param name="observer">The observer to receive throttled elements.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down the subscription when disposed.</returns>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            ThrottleWitness throttleObserver = new(observer, dueTime, timeProvider);
            return source.SubscribeAsync(throttleObserver, cancellationToken);
        }

        /// <summary>Delays each value and forwards it only if no newer value supersedes it.</summary>
        /// <param name="observer">The downstream observer to forward debounced elements to.</param>
        /// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
        /// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
        [DebuggerDisplay("ThrottleWitness: {_witness}")]
        internal sealed class ThrottleWitness(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider) : IWitnessAsync<T>
        {
            /// <summary>The synchronization gate protecting shared throttle state.</summary>
            private readonly Lock _gate = new();

            /// <summary>A monotonically increasing identifier used to detect whether a newer element has superseded the current timer.</summary>
            private long _id;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <summary>Invalidates pending values before releasing the observer.</summary>
            /// <returns>A completed task.</returns>
            public ValueTask DisposeAsync()
            {
                lock (_gate)
                {
                    _id++;
                }

                return WitnessAsync.DisposeStateAsync(this);
            }

            /// <summary>Starts a debounce delay with a fresh identifier.</summary>
            /// <param name="value">The value to forward if it remains current.</param>
            /// <param name="cancellationToken">Cancellation for the delay.</param>
            /// <returns>The delay and notification operation.</returns>
            internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
            {
                long currentId;
                lock (_gate)
                {
                    currentId = ++_id;
                }

                return FireAfterDelayAsync(value, currentId, cancellationToken);
            }

            /// <summary>Waits for the debounce delay and then forwards the value if it has not been superseded.</summary>
            /// <param name="value">The value to forward after the delay.</param>
            /// <param name="id">The identifier of this timer; if superseded by a newer id, the value is discarded.</param>
            /// <param name="cancellationToken">A token to cancel the delay.</param>
            /// <returns>A task representing the asynchronous delay and forwarding operation.</returns>
            internal async Task FireAfterDelayAsync(T value, long id, CancellationToken cancellationToken)
            {
                try
                {
                    await DelayAsync(dueTime, timeProvider, cancellationToken).ConfigureAwait(false);

                    lock (_gate)
                    {
                        if (_id != id)
                        {
                            return;
                        }
                    }

                    await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>Schedules the value's delay and discards completion if a newer value supersedes it.</summary>
            /// <param name="value">The element to potentially forward after the debounce period.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A completed task; the actual forwarding happens asynchronously after the delay.</returns>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                _ = StartDelayAsync(value, cancellationToken);
                return default;
            }

            /// <summary>Marks any in-flight delay as superseded and forwards the error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _id++;
                }

                return observer.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <summary>Marks any in-flight delay as superseded and forwards completion to the downstream observer.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return observer.OnCompletedAsync(result);
            }
        }
    }
}
