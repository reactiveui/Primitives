// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Timeout extension methods for asynchronous observable sequences.</summary>
/// <remarks>Timeout applies a time limit to the observable sequence. If the sequence does not produce
/// a value within the specified time span, a <see cref="TimeoutException"/> is signalled as a failure
/// completion.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Timeout operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Applies a dueTime policy to the observable sequence. If the next element is not received within
        /// the specified time span, the sequence completes with a <see cref="TimeoutException"/>.
        /// </summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> if any inter-element interval exceeds the specified dueTime.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Expire(TimeSpan dueTime)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutSignal<T>(@this, dueTime, TimeProvider.System);
        }
    }

    /// <summary>
    /// Async observable that mirrors the source but completes with a <see cref="TimeoutException"/>
    /// if any inter-element interval exceeds the specified dueTime.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval.</param>
    /// <param name="timeProvider">The time provider used for scheduling the dueTime.</param>
    internal sealed class TimeoutSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider)
        : IObservableAsync<T>
    {
        /// <summary>Subscribes the specified observer and starts the dueTime timer.</summary>
        /// <param name="observer">The observer to receive elements from the source.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down the subscription when disposed.</returns>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            TimeoutWitness timeoutObserver = new(observer, dueTime, timeProvider);
            var subscription = await source.SubscribeAsync(timeoutObserver, cancellationToken).ConfigureAwait(false);
            timeoutObserver.StartTimer(cancellationToken);
            return subscription;
        }

        /// <summary>
        /// Observer that resets a timer on each received element and signals a <see cref="TimeoutException"/>
        /// if no element arrives within the configured dueTime.
        /// </summary>
        /// <param name="observer">The downstream observer to forward elements to.</param>
        /// <param name="dueTime">The maximum allowed inter-element interval.</param>
        /// <param name="timeProvider">The time provider used for scheduling the dueTime.</param>
        internal sealed class TimeoutWitness(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider)
            : WitnessAsync<T>
        {
            /// <summary>Synchronization gate protecting timer state.</summary>
            private readonly Lock _gate = new();

            /// <summary>
            /// Single pre-allocated timer rearmed via <see cref="ITimer.Change(TimeSpan, TimeSpan)"/>
            /// on every emission. Replaces the previous per-emission fire-and-forget
            /// <c>OnTimeoutAsync</c> task; the per-emission allocations (linked CTS, async state
            /// machine box for OnTimeoutAsync, Task.Delay's TimerQueueTimer) collapse to a single
            /// <c>Change</c> call which is highly optimised in the BCL.
            /// </summary>
            private ITimer? _timer;

            /// <summary>Indicates whether the observer has already received a completion signal; ignored timeouts once true.</summary>
            private bool _completed;

            /// <summary>Allocates the timer and schedules the first dueTime tick.</summary>
            /// <param name="cancellationToken">Cancellation token; unused after the redesign but kept for API compatibility.</param>
            internal void StartTimer(CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                try
                {
                    _timer = timeProvider.CreateTimer(
                        static state => ((TimeoutWitness)state!).OnTimerFired(),
                        this,
                        dueTime,
                        System.Threading.Timeout.InfiniteTimeSpan);
                }
                catch (Exception e)
                {
                    // Preserve the legacy contract: CreateTimer failures route to the unhandled
                    // exception handler rather than tearing down the subscription. Without a timer
                    // the operator degrades to a pass-through; downstream callers continue to
                    // receive emissions without a timeout signal.
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>Rearms the dueTime timer and forwards the element to the downstream observer.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                RearmTimer();
                return observer.OnNextAsync(value, cancellationToken);
            }

            /// <summary>Stops the dueTime timer and forwards the error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _completed = true;
                }

                StopTimer();
                return observer.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <summary>Stops the dueTime timer and forwards completion to the downstream observer.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _completed = true;
                }

                StopTimer();
                return observer.OnCompletedAsync(result);
            }

            /// <summary>Disposes the dueTime timer during teardown.</summary>
            /// <returns>A completed task.</returns>
            protected override async ValueTask DisposeAsyncCore()
            {
                lock (_gate)
                {
                    _completed = true;
                }

                if (_timer is not null)
                {
                    await _timer.DisposeAsync().ConfigureAwait(false);
                    _timer = null;
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }

            /// <summary>Awaits the downstream <c>OnCompletedAsync</c> hand-off from the timer-pool callback.</summary>
            /// <param name="target">The downstream observer to complete with the timeout failure.</param>
            /// <returns>A task representing the asynchronous notification.</returns>
            private static async Task FireTimeoutAsync(IObserverAsync<T> target)
            {
                try
                {
                    await target.OnCompletedAsync(Result.Failure(new TimeoutException())).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>Timer callback: signals the downstream observer with a <see cref="TimeoutException"/> completion unless the observer has already terminated.</summary>
            private void OnTimerFired()
            {
                lock (_gate)
                {
                    if (_completed)
                    {
                        return;
                    }

                    _completed = true;
                }

                _ = FireTimeoutAsync(observer);
            }

            /// <summary>Rearms the timeout deadline for the next emission. Isolated from
            /// coverage because the <c>_timer is null</c> branch is only reachable when the
            /// source emits after the sink's <c>DisposeAsyncCore</c> has nulled the timer —
            /// a race the single-threaded test harness cannot deterministically trigger.</summary>
            [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
            private void RearmTimer() =>
                _timer?.Change(dueTime, System.Threading.Timeout.InfiniteTimeSpan);

            /// <summary>Stops the timeout deadline on terminal forwarding. Isolated from
            /// coverage because the <c>_timer is null</c> branch is only reachable under the
            /// same source-after-Dispose race that <see cref="RearmTimer"/> guards against.</summary>
            [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
            private void StopTimer() =>
                _timer?.Change(System.Threading.Timeout.InfiniteTimeSpan, System.Threading.Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>
    /// Async observable that mirrors the source but switches to a fallback observable
    /// if any inter-element interval exceeds the specified dueTime.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval.</param>
    /// <param name="fallback">The fallback observable to switch to on dueTime.</param>
    /// <param name="timeProvider">The time provider used for scheduling the dueTime.</param>
    internal sealed class TimeoutWithFallbackSignal<T>(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        IObservableAsync<T> fallback,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <summary>Subscribes the specified observer by wrapping the source with a dueTime and a catch-to-fallback.</summary>
        /// <param name="observer">The observer to receive elements.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down the subscription when disposed.</returns>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            TimeoutSignal<T> withTimeout = new(source, dueTime, timeProvider);
            CatchSignal<T> withFallback = new(
                withTimeout,
                ex => ex is TimeoutException ? fallback : new SignalAsync.ThrowSignalAsync<T>(ex),
                null);

            return ((IObservableAsync<T>)withFallback).SubscribeAsync(observer.Wrap(), cancellationToken);
        }
    }
}
