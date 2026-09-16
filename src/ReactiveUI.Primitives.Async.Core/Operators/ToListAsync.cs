// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous list-materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> CollectListAsync() =>
            source.ToListAsync(CancellationToken.None);

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> ToListAsync() =>
            source.CollectListAsync();

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        public async ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ToListTaskWitness<T> observer = new(cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A task that completes with the collected array of values.</returns>
        public async ValueTask<T[]> CollectArrayAsync()
        {
            var values = await source.CollectListAsync().ConfigureAwait(false);
            return [.. values];
        }
    }

    /// <summary>Witness that collects all elements from a sequence into a list.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("ToListTaskWitness: {_witness}")]
    internal sealed class ToListTaskWitness<T>(CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<List<T>> _completion = new(cancellationToken);

        /// <summary>The list that accumulates all elements received from the source sequence.</summary>
        private readonly List<T> _items = [];

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
        internal ValueTask<List<T>> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _items.Add(value);
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(_items);

        /// <summary>Sets the result value and disposes this witness.</summary>
        /// <param name="value">The result value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetResultAndDisposeAsync(List<T> value) => _completion.SetResultAndDisposeAsync(value, this);

        /// <summary>Faults the result with an exception and disposes this witness.</summary>
        /// <param name="e">The exception that caused the fault.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetExceptionAndDisposeAsync(Exception e) => _completion.SetExceptionAndDisposeAsync(e, this);
    }
}
