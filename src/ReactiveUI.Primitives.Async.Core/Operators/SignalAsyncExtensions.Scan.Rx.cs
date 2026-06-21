// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible scan names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Scan operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Applies an accumulator function over the observable sequence and returns each intermediate result
        /// using the specified asynchronous accumulator.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to be invoked on each element.</param>
        /// <returns>An observable sequence containing the accumulated values produced after each element is processed.</returns>
        public IObservableAsync<TAcc> Scan<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            var scanAccumulator = accumulator;
            return new FoldAsyncSignal<T, TAcc>(@this, seed, scanAccumulator);
        }

        /// <summary>Applies an accumulator function over the observable sequence and returns each intermediate result.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to be invoked on each element.</param>
        /// <returns>An observable sequence containing the accumulated values produced after each element is processed.</returns>
        public IObservableAsync<TAcc> Scan<TAcc>(TAcc seed, Func<TAcc, T, TAcc> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            var scanAccumulator = accumulator;
            return new FoldSyncSignal<T, TAcc>(@this, seed, scanAccumulator);
        }
    }
}
