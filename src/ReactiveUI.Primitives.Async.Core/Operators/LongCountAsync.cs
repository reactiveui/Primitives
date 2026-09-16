// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous 64-bit element-counting operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
        /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync(Func<T, bool>? predicate) =>
            source.LongCountAsync(predicate, CancellationToken.None);

        /// <summary>Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
        /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
        public async ValueTask<long> LongCountAsync(
            Func<T, bool>? predicate,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LongCountTaskWitness<T> observer = new(predicate, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously returns the total number of elements in the sequence as a 64-bit integer.</summary>
        /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
        /// sequence as a 64-bit integer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync() =>
            source.LongCountAsync(null, CancellationToken.None);

        /// <summary>Asynchronously returns the total number of elements in the sequence as a 64-bit integer.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
        /// sequence as a 64-bit integer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync(CancellationToken cancellationToken) =>
            source.LongCountAsync(null, cancellationToken);
    }

    /// <summary>Witness that counts elements in a sequence as a 64-bit integer, optionally filtered by a predicate.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements. If null, all elements are counted.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("LongCountTaskWitness: {_witness}")]
    internal sealed class LongCountTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<long> _completion = new(cancellationToken);

        /// <summary>The running count of elements that satisfy the predicate.</summary>
        private long _count;

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
        internal ValueTask<long> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is not null && !predicate(value))
            {
                return default;
            }

            _count = checked(_count + 1);

            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(_count);

        /// <summary>Sets the result value and disposes this witness.</summary>
        /// <param name="value">The result value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetResultAndDisposeAsync(long value) => _completion.SetResultAndDisposeAsync(value, this);

        /// <summary>Faults the result with an exception and disposes this witness.</summary>
        /// <param name="e">The exception that caused the fault.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetExceptionAndDisposeAsync(Exception e) => _completion.SetExceptionAndDisposeAsync(e, this);
    }
}
