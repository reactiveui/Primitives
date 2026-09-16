// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>First-element operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the first element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync(Func<T, bool> predicate) =>
            source.FirstAsync(predicate, CancellationToken.None);

        /// <summary>Asynchronously returns the first element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        public async ValueTask<T> FirstAsync(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FirstTaskWitness<T> observer = new(predicate, false, default, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously returns the first element of the sequence.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync() =>
            source.FirstAsync(CancellationToken.None);

        /// <summary>Asynchronously returns the first element of the sequence.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        public async ValueTask<T> FirstAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FirstTaskWitness<T> observer = new(null, false, default, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A witness that captures the first element matching an optional predicate.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="hasDefault">When <see langword="true"/>, an empty sequence yields <paramref name="defaultValue"/> instead of failing.</param>
    /// <param name="defaultValue">The value an empty sequence yields when <paramref name="hasDefault"/> is set.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("FirstTaskWitness: {_witness}")]
    internal sealed class FirstTaskWitness<T>(
        Func<T, bool>? predicate,
        bool hasDefault,
        T? defaultValue,
        CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<T> _completion = new(cancellationToken);

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
            _ = cancellationToken;
            return predicate is not null && !predicate(value)
                ? default
                : _completion.SetResultAndDisposeAsync(value, this);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            _completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
        {
            if (hasDefault)
            {
                return _completion.CompleteAndDisposeAsync(result, defaultValue!, this);
            }

            Exception exception;
            if (result.IsSuccess)
            {
                var message = predicate is null
                    ? "Sequence contains no elements."
                    : "Sequence contains no matching elements.";
                exception = new InvalidOperationException(message);
            }
            else
            {
                exception = result.Exception;
            }

            return _completion.SetExceptionAndDisposeAsync(exception, this);
        }
    }
}
