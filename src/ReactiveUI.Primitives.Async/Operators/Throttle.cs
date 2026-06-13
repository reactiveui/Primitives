// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Throttle (debounce) extension methods for asynchronous observable sequences.</summary>
/// <remarks>Throttle ignores elements from the source sequence that are followed by another element
/// within a specified time span. Only values that are not followed by another value within the due time
/// are forwarded to observers. This is commonly used to suppress rapid bursts of events such as keystrokes
/// or mouse movements.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Throttle (debounce) operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Ignores elements from the source sequence that are followed by another element within
        /// the specified time span. Only the last element in each burst is forwarded.
        /// </summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        public IObservableAsync<T> Throttle(TimeSpan dueTime)
            => @this.Throttle(dueTime, (TimeProvider?)null);

        /// <summary>
        /// Ignores elements from the source sequence that are followed by another element within
        /// the specified time span. Only the last element in each burst is forwarded.
        /// </summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        public IObservableAsync<T> Throttle(TimeSpan dueTime, TimeProvider? timeProvider)
        {
#if NET8_0_OR_GREATER
            ArgumentOutOfRangeException.ThrowIfLessThan(dueTime, TimeSpan.Zero);
#else
            if (dueTime < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(dueTime));
            }
#endif

            return new ThrottleSignal<T>(@this, dueTime, timeProvider ?? TimeProvider.System);
        }
    }

    /// <summary>Asynchronously delays for the specified duration using the provided time provider.</summary>
    /// <param name="delay">The duration to delay.</param>
    /// <param name="timeProvider">The time provider to use for the delay.</param>
    /// <param name="cancellationToken">A token to cancel the delay.</param>
    /// <returns>A <see cref="ValueTask"/> that completes after the specified delay.</returns>
    /// <remarks>
    /// For <see cref="TimeProvider.System"/> the result is a wrapper around
    /// <see cref="Task.Delay(TimeSpan, CancellationToken)"/>; for custom providers the call rents a
    /// pooled <see cref="PooledDelaySource"/> so the per-call <see cref="TaskCompletionSource{TResult}"/>
    /// + <see cref="Task{TResult}"/> + <see cref="CancellationTokenRegistration"/> allocation chain
    /// from the legacy implementation collapses to zero on the steady path.
    /// </remarks>
    internal static ValueTask DelayAsync(
        TimeSpan delay,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        if (timeProvider == TimeProvider.System)
        {
            return new(Task.Delay(delay, cancellationToken));
        }

        return PooledDelaySource.Rent().BeginAsync(delay, timeProvider, cancellationToken);
    }

    /// <summary>
    /// Async observable that debounces the source sequence, only forwarding elements that are not
    /// followed by another element within the specified due time.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence to throttle.</param>
    /// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
    /// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
    internal sealed class ThrottleSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider)
        : SignalAsync<T>
    {
        /// <summary>Subscribes the specified observer with throttle behavior applied.</summary>
        /// <param name="observer">The observer to receive throttled elements.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down the subscription when disposed.</returns>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            ThrottleWitness throttleObserver = new(observer, dueTime, timeProvider);
            return source.SubscribeAsync(throttleObserver, cancellationToken);
        }

        /// <summary>
        /// Observer that implements throttle/debounce logic by starting a timer on each element
        /// and only forwarding the element if no newer element supersedes it before the timer fires.
        /// </summary>
        /// <param name="observer">The downstream observer to forward debounced elements to.</param>
        /// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
        /// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
        internal sealed class ThrottleWitness(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider)
            : WitnessAsync<T>
        {
            /// <summary>The synchronization gate protecting shared throttle state.</summary>
            private readonly Lock _gate = new();

            /// <summary>A monotonically increasing identifier used to detect whether a newer element has superseded the current timer.</summary>
            private long _id;

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
                    // UnhandledExceptionHandler filters OperationCanceledException internally so
                    // a separate OCE-only catch would just duplicate the silent-drop behavior.
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>
            /// Starts a new debounce timer for the received element, identifying it by a fresh id.
            /// Supersession is detected post-delay via the id check rather than via a per-emission
            /// linked CTS — eliminating the <c>Linked1CancellationTokenSource</c> allocation that
            /// dominated the operator's GC profile. A superseded delay still runs to completion
            /// (waiting the full <c>dueTime</c>) but its result is discarded, which trades a small
            /// amount of transient state-machine retention for zero per-emission allocation.
            /// </summary>
            /// <param name="value">The element to potentially forward after the debounce period.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A completed task; the actual forwarding happens asynchronously after the delay.</returns>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                long currentId;
                lock (_gate)
                {
                    currentId = ++_id;
                }

                _ = FireAfterDelayAsync(value, currentId, cancellationToken);
                return default;
            }

            /// <summary>Marks any in-flight delay as superseded and forwards the error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
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
            protected override ValueTask OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return observer.OnCompletedAsync(result);
            }

            /// <summary>
            /// Marks any in-flight delay as superseded during disposal. The dispose token threaded
            /// through <see cref="DelayAsync"/> by the base observer also unblocks the awaits.
            /// </summary>
            /// <returns>A completed task.</returns>
            protected override ValueTask DisposeAsyncCore()
            {
                lock (_gate)
                {
                    _id++;
                }

                return base.DisposeAsyncCore();
            }
        }
    }
}
