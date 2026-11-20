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

/// <summary>Re-subscribes after an error using the policy's backoff delays, forwarding the error once its budget is spent.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="policy">The retry / backoff configuration.</param>
public sealed class RetryWithBackoffObservable<T>(
    IObservable<T> source,
    RetryBackoffPolicy policy) : IObservable<T>
{
    /// <summary>Default backoff factor where each retry doubles the previous delay.</summary>
    private const double DefaultBackoffFactor = 2.0;

    /// <summary>Creates a sequence that invokes <paramref name="onError"/> on matching exceptions and retries indefinitely without delay.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onError">Callback invoked on each matching exception.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateOnErrorRetry<TException>(
        IObservable<T> source,
        Action<TException> onError)
        where TException : Exception =>
        new(source, RetryPolicyFor(onError, int.MaxValue, TimeSpan.Zero, Sequencer.Default));

    /// <summary>Creates a sequence that invokes <paramref name="onError"/> on matching exceptions and retries indefinitely after <paramref name="delay"/>.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onError">Callback invoked on each matching exception.</param>
    /// <param name="delay">The delay before each retry.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateOnErrorRetry<TException>(
        IObservable<T> source,
        Action<TException> onError,
        TimeSpan delay)
        where TException : Exception =>
        new(source, RetryPolicyFor(onError, int.MaxValue, delay, Sequencer.Default));

    /// <summary>Creates a sequence that invokes <paramref name="onError"/> on matching exceptions and retries up to <paramref name="retryCount"/> times without delay.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onError">Callback invoked on each matching exception.</param>
    /// <param name="retryCount">The maximum number of retries.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateOnErrorRetry<TException>(
        IObservable<T> source,
        Action<TException> onError,
        int retryCount)
        where TException : Exception =>
        new(source, RetryPolicyFor(onError, retryCount, TimeSpan.Zero, Sequencer.Default));

    /// <summary>Creates a sequence that invokes <paramref name="onError"/> on matching exceptions and retries up to <paramref name="retryCount"/> times after <paramref name="delay"/>.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onError">Callback invoked on each matching exception.</param>
    /// <param name="retryCount">The maximum number of retries.</param>
    /// <param name="delay">The delay before each retry.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateOnErrorRetry<TException>(
        IObservable<T> source,
        Action<TException> onError,
        int retryCount,
        TimeSpan delay)
        where TException : Exception =>
        new(source, RetryPolicyFor(onError, retryCount, delay, Sequencer.Default));

    /// <summary>Creates a sequence that invokes <paramref name="onError"/> on matching exceptions and retries up to <paramref name="retryCount"/> times on <paramref name="delayScheduler"/>.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="onError">Callback invoked on each matching exception.</param>
    /// <param name="retryCount">The maximum number of retries.</param>
    /// <param name="delay">The delay before each retry.</param>
    /// <param name="delayScheduler">The scheduler that times each delay.</param>
    /// <returns>A retried observable sequence.</returns>
    public static RetryWithBackoffObservable<T> CreateOnErrorRetry<TException>(
        IObservable<T> source,
        Action<TException> onError,
        int retryCount,
        TimeSpan delay,
        ISequencer delayScheduler)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, RetryPolicyFor(onError, retryCount, delay, delayScheduler));
    }

    /// <summary>Creates an exponential-backoff retry sequence with a doubling delay on <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial backoff delay.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateBackoff(
        IObservable<T> source,
        int maxRetries,
        TimeSpan initialDelay) =>
        new(source, new(maxRetries, initialDelay, DefaultBackoffFactor, null, Sequencer.Default, null));

    /// <summary>Creates an exponential-backoff retry sequence with explicit backoff factor, cap, and optional scheduler.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="maxRetries">Maximum number of retries.</param>
    /// <param name="initialDelay">Initial backoff delay.</param>
    /// <param name="backoffFactor">Multiplier for each retry.</param>
    /// <param name="maxDelay">Optional maximum delay.</param>
    /// <param name="scheduler">Optional scheduler.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateBackoff(
        IObservable<T> source,
        int maxRetries,
        TimeSpan initialDelay,
        double backoffFactor,
        TimeSpan? maxDelay,
        ISequencer? scheduler) =>
        new(source, new(maxRetries, initialDelay, backoffFactor, maxDelay, scheduler ?? Sequencer.Default, null));

    /// <summary>Creates a fixed-delay retry sequence on <see cref="Sequencer.Default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">Maximum number of retries.</param>
    /// <param name="delay">Delay between retries.</param>
    /// <returns>A retried observable sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RetryWithBackoffObservable<T> CreateFixedDelay(
        IObservable<T> source,
        int retryCount,
        TimeSpan delay) =>
        new(source, new(retryCount, delay, 1.0, null, Sequencer.Default, null));

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(policy.Scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        RetryWithBackoffSink sink = new(observer, source, policy);
        sink.Run();
        return sink;
    }

    /// <summary>Builds a policy that retries one exception type and passes every other error straight downstream.</summary>
    /// <typeparam name="TException">The exception type to retry.</typeparam>
    /// <param name="onError">Callback invoked with each retried exception.</param>
    /// <param name="retryCount">The maximum number of retries.</param>
    /// <param name="delay">The delay before each retry.</param>
    /// <param name="scheduler">The sequencer that times each delay.</param>
    /// <returns>The retry policy.</returns>
    private static RetryBackoffPolicy RetryPolicyFor<TException>(
        Action<TException> onError,
        int retryCount,
        TimeSpan delay,
        ISequencer scheduler)
        where TException : Exception
    {
        ArgumentExceptionHelper.ThrowIfNull(onError);

        return new(
            retryCount,
            delay,
            1.0,
            null,
            scheduler,
            error => onError((TException)error),
            static error => error is TException);
    }

    /// <summary>Sink that manages retries with exponential backoff.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="source">The source observable.</param>
    /// <param name="policy">The retry / backoff configuration.</param>
    /// <remarks>
    /// The retry count is advanced only by the source's serialized notifications, so no lock is needed; the observer, the
    /// error callback and the source subscription all run without one.
    /// </remarks>
    internal sealed class RetryWithBackoffSink(
        IObserver<T> downstream,
        IObservable<T> source,
        RetryBackoffPolicy policy) : IObserver<T>, IDisposable
    {
        /// <summary>The subscription to the source sequence; each attempt disposes the one it replaces.</summary>
        private readonly SwapDisposable _subscription = new();

        /// <summary>The pending retry timer, held separately so re-subscribing never displaces a retry that has not fired.</summary>
        private readonly SwapDisposable _retryTimer = new();

        /// <summary>The number of retries attempted so far.</summary>
        private int _retries;

        /// <summary>Whether the sink has been disposed.</summary>
        private bool _disposed;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (policy.ShouldRetry is not null && !policy.ShouldRetry(error))
            {
                downstream.OnError(error);
                return;
            }

            policy.OnError?.Invoke(error);

            if (Volatile.Read(ref _disposed))
            {
                return;
            }

            if (_retries < policy.MaxRetries)
            {
                var delay = TimeSpan.FromTicks((long)(policy.InitialDelay.Ticks
                                                      * Math.Pow(policy.BackoffFactor, _retries)));
                if (policy.MaxDelay.HasValue && delay > policy.MaxDelay.Value)
                {
                    delay = policy.MaxDelay.Value;
                }

                _retries++;

                if (delay == TimeSpan.Zero)
                {
                    SubscribeToSource();
                }
                else
                {
                    _retryTimer.Disposable = policy.Scheduler.Schedule(delay, SubscribeToSource);
                }
            }
            else
            {
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();

        /// <inheritdoc/>
        public void Dispose()
        {
            Volatile.Write(ref _disposed, true);
            _retryTimer.Dispose();
            _subscription.Dispose();
        }

        /// <summary>Subscribes the source for the first attempt.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Run() => SubscribeToSource();

        /// <summary>Subscribes to the source sequence unless the sink has been disposed.</summary>
        internal void SubscribeToSource()
        {
            if (Volatile.Read(ref _disposed))
            {
                return;
            }

            _subscription.Disposable = source.Subscribe(this);
        }
    }
}
