// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous per-element iteration operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNextAsync">A function to invoke for each element in the sequence. The function receives the element and a cancellation
        /// token, and returns a ValueTask that completes when processing is finished.</param>
        /// <returns>A ValueTask that represents the asynchronous operation. The task completes when all elements have been
        /// processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNextAsync"/> is <see langword="null"/>.</exception>
        /// <remarks>The returned task completes once the sequence terminates and the last invocation has finished; an
        /// exception from <paramref name="onNextAsync"/> or from the sequence surfaces on it.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ForEachAsync(Func<T, CancellationToken, ValueTask> onNextAsync) =>
            source.ForEachAsync(onNextAsync, CancellationToken.None);

        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNextAsync">A function to invoke for each element in the sequence. The function receives the element and a cancellation
        /// token, and returns a ValueTask that completes when processing is finished.</param>
        /// <param name="cancellationToken">A token to observe while waiting for the sequence to complete. The operation is canceled if the token is
        /// signaled.</param>
        /// <returns>A ValueTask that represents the asynchronous operation. The task completes when all elements have been
        /// processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNextAsync"/> is <see langword="null"/>.</exception>
        /// <remarks>The returned task completes once the sequence terminates and the last invocation has finished; an
        /// exception from <paramref name="onNextAsync"/> or from the sequence surfaces on it.</remarks>
        public async ValueTask ForEachAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNextAsync);

            cancellationToken.ThrowIfCancellationRequested();
            ForEachAsyncTaskWitness<T> observer = new(onNextAsync, cancellationToken);
            await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
        /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
        /// fully processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ForEachAsync(Action<T> onNext) =>
            source.ForEachAsync(onNext, CancellationToken.None);

        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the iteration.</param>
        /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
        /// fully processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        public async ValueTask ForEachAsync(Action<T> onNext, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);
            cancellationToken.ThrowIfCancellationRequested();

            ForEachSyncTaskWitness<T> observer = new(onNext, cancellationToken);
            await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A witness that invokes an asynchronous callback for each element and signals completion via a task.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="onNextAsync">The asynchronous callback to invoke for each element.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("ForEachAsyncTaskWitness: {_witness}")]
    internal sealed class ForEachAsyncTaskWitness<T>(
        Func<T, CancellationToken, ValueTask> onNextAsync,
        CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <summary>Asynchronously waits for the witness to produce its result value.</summary>
        /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<bool> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            onNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            _completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            _completion.CompleteAndDisposeAsync(result, true, this);
    }

    /// <summary>A witness that invokes a synchronous callback for each element and signals completion via a task.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="onNext">The synchronous callback to invoke for each element.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("ForEachSyncTaskWitness: {_witness}")]
    internal sealed class ForEachSyncTaskWitness<T>(Action<T> onNext, CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

        /// <summary>The synchronous callback invoked for each element in the sequence.</summary>
        private readonly Action<T> _onNext = onNext;

        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <summary>Asynchronously waits for the witness to produce its result value.</summary>
        /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<bool> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _onNext(value);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            _completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            _completion.CompleteAndDisposeAsync(result, true, this);
    }
}
