// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible retry names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Retry operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Repeats the source observable sequence on error indefinitely.</summary>
        /// <returns>An observable sequence that mirrors the source and re-subscribes on error until
        /// a successful completion occurs.</returns>
        public IObservableAsync<T> Retry()
        {
            const int retryCount = int.MaxValue;
            return new ReattemptSignal<T>(source, retryCount);
        }

        /// <summary>Runs the source up to the specified number of times in total, stopping at the first run that ends without an error.</summary>
        /// <param name="retryCount">The total number of runs. Zero runs the source not at all and completes.</param>
        /// <returns>An observable sequence that mirrors the source and runs it again on error. If every run
        /// fails, the last error is propagated.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="retryCount"/> is negative.</exception>
        /// <remarks>The count is total runs, matching the System.Reactive operator of this name. Use <c>Reattempt</c> to count extra tries instead.</remarks>
        public IObservableAsync<T> Retry(int retryCount)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(retryCount);

            return retryCount == 0
                ? SignalAsync.Empty<T>()
                : new ReattemptSignal<T>(source, retryCount - 1);
        }
    }
}
