// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Timeout extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Timeout operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Completes with a TimeoutException when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> once an inter-element gap exceeds <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Expire(TimeSpan dueTime)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutSignal<T>(source, dueTime, TimeProvider.System);
        }
    }

    /// <summary>Async observable that mirrors the source but completes with a <see cref="TimeoutException"/> once an inter-element gap exceeds the configured interval.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval.</param>
    /// <param name="timeProvider">The time provider used to schedule the deadline.</param>
    internal sealed class TimeoutSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <summary>Subscribes the specified observer and starts the deadline timer.</summary>
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

        /// <summary>Observer that resets a timer on each received element and signals a <see cref="TimeoutException"/> if no element arrives within the configured interval.</summary>
        /// <param name="observer">The downstream observer to forward elements to.</param>
        /// <param name="dueTime">The maximum allowed inter-element interval.</param>
        /// <param name="timeProvider">The time provider used to schedule the deadline.</param>
        internal sealed class TimeoutWitness(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider) : WitnessAsync<T>
        {
            /// <summary>Synchronization gate protecting timer state.</summary>
            private readonly Lock _gate = new();

            /// <summary>
            /// The one timer for this subscription, rearmed via <see cref="ITimer.Change(TimeSpan, TimeSpan)"/> on
            /// every emission so that tracking the deadline costs no per-emission allocation.
            /// </summary>
            private ITimer? _timer;

            /// <summary>Set once the observer has been terminated; suppresses any later timeout signal.</summary>
            private bool _completed;

            /// <summary>Allocates the timer and schedules the first deadline tick.</summary>
            /// <param name="cancellationToken">Unused; the timer carries its own deadline.</param>
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
                    // If timeout setup fails, values continue without timeout enforcement.
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>Rearms the deadline unless disposal has removed the timer.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void RearmTimer() =>
                _timer?.Change(dueTime, System.Threading.Timeout.InfiniteTimeSpan);

            /// <summary>Stops the deadline unless disposal has removed the timer.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void StopTimer() =>
                _timer?.Change(System.Threading.Timeout.InfiniteTimeSpan, System.Threading.Timeout.InfiniteTimeSpan);

            /// <summary>Rearms the deadline timer and forwards the element to the downstream observer.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                RearmTimer();
                return observer.OnNextAsync(value, cancellationToken);
            }

            /// <summary>Stops the deadline timer and forwards the error to the downstream observer.</summary>
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

            /// <summary>Stops the deadline timer and forwards completion to the downstream observer.</summary>
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

            /// <summary>Disposes the deadline timer during teardown.</summary>
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

            /// <summary>Timer callback: completes the downstream observer with a <see cref="TimeoutException"/> unless it has terminated.</summary>
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
        }
    }

    /// <summary>Async observable that mirrors the source but switches to a fallback observable once an inter-element gap exceeds the configured interval.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval.</param>
    /// <param name="fallback">The observable to switch to when the interval elapses.</param>
    /// <param name="timeProvider">The time provider used to schedule the deadline.</param>
    internal sealed class TimeoutWithFallbackSignal<T>(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        IObservableAsync<T> fallback,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <summary>Subscribes the specified observer by wrapping the source with a deadline and a catch-to-fallback.</summary>
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
