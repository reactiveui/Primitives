// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Retry extension methods for asynchronous observable sequences.</summary>
/// <remarks>Retry re-subscribes to the source sequence upon failure, enabling automatic recovery
/// from transient errors. An optional retry count limits the number of re-subscription attempts.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Retry operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Repeats the source observable sequence on error up to the specified number of times.</summary>
        /// <param name="retryCount">The maximum number of times to re-subscribe to the source on error.
        /// Must be greater than or equal to zero. A value of 0 means no retries (original sequence only).</param>
        /// <returns>An observable sequence that mirrors the source, re-subscribing on error up to the
        /// specified number of times. If all retries are exhausted, the last error is propagated.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="retryCount"/> is negative.</exception>
        public IObservableAsync<T> Reattempt(int retryCount)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

            return new ReattemptSignal<T>(@this, retryCount);
        }
    }
}
