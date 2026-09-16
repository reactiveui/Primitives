// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous containment operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, IEqualityComparer<T>? comparer) =>
            source.ContainsAsync(value, comparer, CancellationToken.None);

        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        public async ValueTask<bool> ContainsAsync(
            T value,
            IEqualityComparer<T>? comparer,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cmp = comparer ?? EqualityComparer<T>.Default;
            ContainsTaskWitness<T> observer = new(value, cmp, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value) =>
            source.ContainsAsync(value, null, CancellationToken.None);

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, CancellationToken cancellationToken) =>
            source.ContainsAsync(value, null, cancellationToken);
    }

    /// <summary>A witness that determines whether a sequence contains a specified value.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="target">The value to search for.</param>
    /// <param name="comparer">The equality comparer to use for comparison.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("ContainsTaskWitness: {_witness}")]
    internal sealed class ContainsTaskWitness<T>(
        T target,
        IEqualityComparer<T> comparer,
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
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return !comparer.Equals(target, value)
                ? default
                : _completion.SetResultAndDisposeAsync(true, this);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            _completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            _completion.CompleteAndDisposeAsync(result, false, this);
    }
}
