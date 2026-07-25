// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Operators;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive;
#else
namespace ReactiveUI.Primitives.Extensions;
#endif

/// <summary>Extension methods for Reactive objects.</summary>
public static partial class ReactiveExtensions
{
    /// <summary>Default backoff factor for <see cref="RetryWithBackoff{T}(IObservable{T}, int, TimeSpan)"/>: each retry doubles the previous delay.</summary>
    private const double DefaultBackoffFactor = 2.0;

    /// <summary>Retry operators that re-subscribe to a failed source, optionally after a growing delay.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Repeats the source until it terminates successfully (alias of Retry).</summary>
        /// <returns>Retried sequence.</returns>
        public IObservable<T> OnErrorRetry()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            return new RetryForeverObservable<T>(source);
        }

        /// <summary>When caught exception, do onError action and repeat observable sequence.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    int.MaxValue,
                    TimeSpan.Zero,
                    1.0,
                    null,
                    Sequencer.Default,
                    ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence after delay time.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, TimeSpan delay)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    int.MaxValue,
                    delay,
                    1.0,
                    null,
                    Sequencer.Default,
                    ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence during within retryCount.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, int retryCount)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    retryCount,
                    TimeSpan.Zero,
                    1.0,
                    null,
                    Sequencer.Default,
                    ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>When caught exception, do onError action and repeat observable sequence after delay time during within retryCount.</summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(Action<TException> onError, int retryCount, TimeSpan delay)
            where TException : Exception =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    retryCount,
                    delay,
                    1.0,
                    null,
                    Sequencer.Default,
                    ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));

        /// <summary>
        /// When caught exception, do onError action and repeat observable sequence after delay
        /// time(work on delayScheduler) during within retryCount.
        /// </summary>
        /// <typeparam name="TException">The type of the exception.</typeparam>
        /// <param name="onError">The on error.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <param name="delayScheduler">The delay scheduler.</param>
        /// <returns>A sequence that retries on error with optional delay.</returns>
        public IObservable<T> OnErrorRetry<TException>(
            Action<TException> onError,
            int retryCount,
            TimeSpan delay,
            ISequencer delayScheduler)
            where TException : Exception
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new RetryWithBackoffObservable<T>(
                source,
                new(
                    retryCount,
                    delay,
                    1.0,
                    null,
                    delayScheduler,
                    ex =>
                    {
                        if (ex is not TException tex)
                        {
                            return;
                        }

                        onError(tex);
                    }));
        }

        /// <summary>Retries with exponential backoff.</summary>
        /// <param name="maxRetries">Maximum number of retries.</param>
        /// <param name="initialDelay">Initial backoff delay.</param>
        /// <returns>Retried sequence with backoff.</returns>
        public IObservable<T> RetryWithBackoff(int maxRetries, TimeSpan initialDelay) =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    maxRetries,
                    initialDelay,
                    DefaultBackoffFactor,
                    null,
                    Sequencer.Default,
                    null));

        /// <summary>Retries with exponential backoff.</summary>
        /// <param name="maxRetries">Maximum number of retries.</param>
        /// <param name="initialDelay">Initial backoff delay.</param>
        /// <param name="backoffFactor">Multiplier for each retry (default 2).</param>
        /// <param name="maxDelay">Optional maximum delay.</param>
        /// <param name="scheduler">Scheduler (optional).</param>
        /// <returns>Retried sequence with backoff.</returns>
        public IObservable<T> RetryWithBackoff(
            int maxRetries,
            TimeSpan initialDelay,
            double backoffFactor,
            TimeSpan? maxDelay,
            ISequencer? scheduler) =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    maxRetries,
                    initialDelay,
                    backoffFactor,
                    maxDelay,
                    scheduler ?? Sequencer.Default,
                    null));

        /// <summary>Retry with exponential.</summary>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delaySelector">The delay selector.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryWithDelay(int retryCount, Func<int, TimeSpan> delaySelector) =>
            new RetryWithDelayObservable<T>(source, retryCount, delaySelector);

        /// <summary>Retries the forever with delay.</summary>
        /// <param name="delay">The delay.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryForeverWithDelay(TimeSpan delay) =>
            new RetryWithDelayObservable<T>(source, int.MaxValue, _ => delay);

        /// <summary>Retry with fixed backoff.</summary>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="delay">The delay.</param>
        /// <returns>An IObservable of T.</returns>
        public IObservable<T> RetryWithFixedDelay(int retryCount, TimeSpan delay) =>
            new RetryWithBackoffObservable<T>(
                source,
                new(
                    retryCount,
                    delay,
                    1.0,
                    null,
                    Sequencer.Default,
                    null));
    }
}
