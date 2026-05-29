// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides Retry extension methods for asynchronous observable sequences.
/// </summary>
/// <remarks>Retry re-subscribes to the source sequence upon failure, enabling automatic recovery
/// from transient errors. An optional retry count limits the number of re-subscription attempts.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Repeats the source observable sequence indefinitely until it completes successfully, re-subscribing
    /// on each error.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>An observable sequence that mirrors the source and re-subscribes on error until
    /// a successful completion occurs.</returns>
    public static IObservableAsync<T> Retry<T>(this IObservableAsync<T> @this) => @this.Retry(int.MaxValue);

    /// <summary>
    /// Repeats the source observable sequence on error up to the specified number of times.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="retryCount">The maximum number of times to re-subscribe to the source on error.
    /// Must be greater than or equal to zero. A value of 0 means no retries (original sequence only).</param>
    /// <returns>An observable sequence that mirrors the source, re-subscribing on error up to the
    /// specified number of times. If all retries are exhausted, the last error is propagated.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="retryCount"/> is negative.</exception>
    public static IObservableAsync<T> Retry<T>(this IObservableAsync<T> @this, int retryCount)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(retryCount);
#else
        if (retryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryCount));
        }
#endif

        return Create<T>(async (observer, cancellationToken) =>
        {
            var remaining = retryCount;
            SerialDisposableAsync serialDisposable = new();

            async ValueTask SubscribeOnceAsync(Result result)
            {
                if (result.IsSuccess)
                {
                    await observer.OnCompletedAsync(result).ConfigureAwait(false);
                    return;
                }

                if (remaining <= 0)
                {
                    await observer.OnCompletedAsync(result).ConfigureAwait(false);
                    return;
                }

                remaining--;

                try
                {
                    var newSub = await @this.SubscribeAsync(
                        observer.OnNextAsync,
                        observer.OnErrorResumeAsync,
                        SubscribeOnceAsync,
                        cancellationToken).ConfigureAwait(false);
                    await serialDisposable.SetDisposableAsync(newSub).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Subscription cancelled
                }
                catch (Exception e)
                {
                    await observer.OnCompletedAsync(Result.Failure(e)).ConfigureAwait(false);
                }
            }

            var sub = await @this.SubscribeAsync(
                observer.OnNextAsync,
                observer.OnErrorResumeAsync,
                SubscribeOnceAsync,
                cancellationToken).ConfigureAwait(false);
            await serialDisposable.SetDisposableAsync(sub).ConfigureAwait(false);

            return serialDisposable;
        });
    }
}
