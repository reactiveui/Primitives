// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Aggregate (fold/reduce) extension methods for asynchronous observable sequences.</summary>
/// <remarks>Aggregate applies an accumulator function over each element of the observable sequence
/// and returns the final accumulated value when the sequence completes. This is equivalent to a fold
/// or reduce operation.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Aggregate (fold/reduce) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Applies an asynchronous accumulator function over the observable sequence, returning the
        /// final accumulated value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator) =>
            source.ReduceAsync(seed, accumulator);

        /// <summary>
        /// Applies an asynchronous accumulator function over the observable sequence, returning the
        /// final accumulated value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
            CancellationToken cancellationToken) =>
            source.ReduceAsync(seed, accumulator, cancellationToken);

        /// <summary>
        /// Applies an accumulator function over the observable sequence, returning the final accumulated
        /// value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator) =>
            source.ReduceAsync(seed, accumulator);

        /// <summary>
        /// Applies an accumulator function over the observable sequence, returning the final accumulated
        /// value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> AggregateAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            CancellationToken cancellationToken) =>
            source.ReduceAsync(seed, accumulator, cancellationToken);

        /// <summary>
        /// Applies an accumulator function over the observable sequence with a seed value, then applies
        /// a result selector to the final accumulated value.
        /// </summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> or
        /// <paramref name="resultSelector"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> AggregateAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector) =>
            source.ReduceAsync(seed, accumulator, resultSelector);

        /// <summary>
        /// Applies an accumulator function over the observable sequence with a seed value, then applies
        /// a result selector to the final accumulated value.
        /// </summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> or
        /// <paramref name="resultSelector"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> AggregateAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector,
            CancellationToken cancellationToken) =>
            source.ReduceAsync(seed, accumulator, resultSelector, cancellationToken);

        /// <summary>
        /// Applies an asynchronous accumulator function over the observable sequence, returning the
        /// final accumulated value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator) =>
            source.ReduceAsync(seed, accumulator, CancellationToken.None);

        /// <summary>
        /// Applies an asynchronous accumulator function over the observable sequence, returning the
        /// final accumulated value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to invoke on each element. Receives the
        /// current accumulated value, the current element, and a cancellation token.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        public async ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);
            cancellationToken.ThrowIfCancellationRequested();

            ReduceAsyncTaskWitness<T, TAcc> observer = new(seed, accumulator, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Applies an accumulator function over the observable sequence, returning the final accumulated
        /// value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TAccumulate> ReduceAsync<TAccumulate>(
            TAccumulate seed,
            Func<TAccumulate, T, TAccumulate> accumulator) =>
            source.ReduceAsync(seed, accumulator, CancellationToken.None);

        /// <summary>
        /// Applies an accumulator function over the observable sequence, returning the final accumulated
        /// value when the sequence completes.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element. Receives the current
        /// accumulated value and the current element.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the final accumulated value.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> is null.</exception>
        public async ValueTask<TAcc> ReduceAsync<TAcc>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);
            cancellationToken.ThrowIfCancellationRequested();

            ReduceSyncTaskWitness<T, TAcc> observer = new(seed, accumulator, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Applies an accumulator function over the observable sequence with a seed value, then applies
        /// a result selector to the final accumulated value.
        /// </summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> or
        /// <paramref name="resultSelector"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TResult> ReduceAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector) =>
            source.ReduceAsync(seed, accumulator, resultSelector, CancellationToken.None);

        /// <summary>
        /// Applies an accumulator function over the observable sequence with a seed value, then applies
        /// a result selector to the final accumulated value.
        /// </summary>
        /// <typeparam name="TAcc">The type of the intermediate accumulated value.</typeparam>
        /// <typeparam name="TResult">The type of the result value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to invoke on each element.</param>
        /// <param name="resultSelector">A function to transform the final accumulated value into the result value.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation, containing the transformed result.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="accumulator"/> or
        /// <paramref name="resultSelector"/> is null.</exception>
        public async ValueTask<TResult> ReduceAsync<TAcc, TResult>(
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            Func<TAcc, TResult> resultSelector,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            var acc = await source.ReduceAsync(seed, accumulator, cancellationToken).ConfigureAwait(false);
            return resultSelector(acc);
        }
    }

    /// <summary>Observer that accumulates values using an asynchronous accumulator function and produces the final result.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The asynchronous accumulator function.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class ReduceAsyncTaskWitness<T, TAcc>(
        TAcc seed,
        Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
        CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, TAcc>(cancellationToken)
    {
        /// <summary>The current accumulated value.</summary>
        private TAcc _acc = seed;

        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            _acc = await accumulator(_acc, value, cancellationToken).ConfigureAwait(false);

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(_acc) : SetExceptionAndDisposeAsync(result.Exception);
    }

    /// <summary>Observer that accumulates values using a synchronous accumulator function and produces the final result.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator function.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class ReduceSyncTaskWitness<T, TAcc>(
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator,
        CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, TAcc>(cancellationToken)
    {
        /// <summary>The current accumulated value.</summary>
        private TAcc _acc = seed;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            _acc = accumulator(_acc, value);
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(_acc) : SetExceptionAndDisposeAsync(result.Exception);
    }
}
