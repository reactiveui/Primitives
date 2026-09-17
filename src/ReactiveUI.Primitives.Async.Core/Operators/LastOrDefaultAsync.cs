// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Last-or-default operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue) =>
            source.LastOrDefaultAsync(predicate, defaultValue, CancellationToken.None);

        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        public async ValueTask<T?> LastOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastOrDefaultTaskWitness<T> observer = new(predicate, defaultValue, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously returns the last element of a sequence, or a default value if the sequence contains no elements.</summary>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync() =>
            source.LastOrDefaultAsync(default, CancellationToken.None);

        /// <summary>Asynchronously returns the last element of a sequence, or a default value if the sequence contains no elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(CancellationToken cancellationToken) =>
            source.LastOrDefaultAsync(default, cancellationToken);

        /// <summary>Asynchronously returns the last element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(T? defaultValue) =>
            source.LastOrDefaultAsync(defaultValue, CancellationToken.None);

        /// <summary>Asynchronously returns the last element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        public async ValueTask<T?> LastOrDefaultAsync(T? defaultValue, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastOrDefaultTaskWitness<T> observer = new(null, defaultValue, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Witness that captures the last element matching an optional predicate, or returns a default value.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="defaultValue">The default value to return if no element matches.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("LastOrDefaultTaskWitness: {_witness}")]
    internal sealed class LastOrDefaultTaskWitness<T>(
        Func<T, bool>? predicate,
        T? defaultValue,
        CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<T> _completion = new(cancellationToken);

        /// <summary>The most recently observed matching element, or the default value if no match has been found.</summary>
        private T? _last = defaultValue;

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
        internal ValueTask<T> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is not null && !predicate(value))
            {
                return default;
            }

            _last = value;

            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            _completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            _completion.CompleteAndDisposeAsync(result, _last!, this);
    }
}
