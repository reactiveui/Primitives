// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Aggregate (fold/reduce) extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Aggregate (fold/reduce) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Applies an asynchronous accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator) =>
            ReduceAsyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator);

        /// <summary>Applies an asynchronous accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
            CancellationToken cancellationToken) =>
            ReduceAsyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, cancellationToken);

        /// <summary>Applies an accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator);

        /// <summary>Applies an accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            CancellationToken cancellationToken) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, cancellationToken);

        /// <summary>Applies an accumulator function over the observable sequence with a seed value, then applies a result selector to the final accumulated value.</summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> AggregateAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, resultSelector);

        /// <summary>Applies an accumulator function over the observable sequence with a seed value, then applies a result selector to the final accumulated value.</summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> AggregateAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector,
            CancellationToken cancellationToken) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, resultSelector, cancellationToken);

        /// <summary>Applies an asynchronous accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator) =>
            ReduceAsyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator);

        /// <summary>Applies an asynchronous accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
            CancellationToken cancellationToken) =>
            ReduceAsyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, cancellationToken);

        /// <summary>Applies an accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAccumulate> ReduceAsync<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            ReduceSyncTaskWitness<T, TAccumulate>.RunAsync(source, seed, accumulator);

        /// <summary>Applies an accumulator function over the observable sequence, returning the final accumulated value when the sequence completes.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            CancellationToken cancellationToken) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, cancellationToken);

        /// <summary>Applies an accumulator function over the observable sequence with a seed value, then applies a result selector to the final accumulated value.</summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> ReduceAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, resultSelector);

        /// <summary>Applies an accumulator function over the observable sequence with a seed value, then applies a result selector to the final accumulated value.</summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accumulator"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> ReduceAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector,
            CancellationToken cancellationToken) =>
            ReduceSyncTaskWitness<T, TAcc>.RunAsync(source, seed, accumulator, resultSelector, cancellationToken);
    }
}
