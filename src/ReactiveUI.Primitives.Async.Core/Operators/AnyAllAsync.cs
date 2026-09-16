// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous quantifier operators that evaluate elements of an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously determines whether any element in the sequence satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, the method checks whether the sequence contains
        /// any elements.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if any
        /// element satisfies the predicate or, if the predicate is null, if the sequence contains any elements;
        /// otherwise, <see langword="false"/>.</returns>
        public async ValueTask<bool> AnyAsync(Func<T, bool>? predicate, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AnyTaskWitness<T> observer = new(predicate, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync() => source.AnyAsync(CancellationToken.None);

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync(CancellationToken cancellationToken) =>
            source.AnyAsync(null, cancellationToken);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AllAsync(Func<T, bool> predicate) => source.AllAsync(predicate, CancellationToken.None);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        public async ValueTask<bool> AllAsync(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);
            cancellationToken.ThrowIfCancellationRequested();

            AllTaskWitness<T> observer = new(predicate, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A witness that determines whether any element in the sequence satisfies a predicate.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="predicate">An optional predicate to test each element. If null, the sequence is checked for any elements.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("AnyTaskWitness: {_witness}")]
    internal sealed class AnyTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
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
            return predicate is not null && !predicate(value) ? default : SetResultAndDisposeAsync(true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(false);

        /// <summary>Sets the result value and disposes this witness.</summary>
        /// <param name="value">The result value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetResultAndDisposeAsync(bool value) => _completion.SetResultAndDisposeAsync(value, this);

        /// <summary>Faults the result with an exception and disposes this witness.</summary>
        /// <param name="e">The exception that caused the fault.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetExceptionAndDisposeAsync(Exception e) => _completion.SetExceptionAndDisposeAsync(e, this);
    }

    /// <summary>A witness that determines whether all elements in the sequence satisfy a predicate.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="predicate">The predicate to test each element against.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("AllTaskWitness: {_witness}")]
    internal sealed class AllTaskWitness<T>(Func<T, bool> predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

        /// <summary>The test applied to every element.</summary>
        private readonly Func<T, bool> _predicate = predicate;

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
            return _predicate(value) ? default : SetResultAndDisposeAsync(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(true);

        /// <summary>Sets the result value and disposes this witness.</summary>
        /// <param name="value">The result value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetResultAndDisposeAsync(bool value) => _completion.SetResultAndDisposeAsync(value, this);

        /// <summary>Faults the result with an exception and disposes this witness.</summary>
        /// <param name="e">The exception that caused the fault.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetExceptionAndDisposeAsync(Exception e) => _completion.SetExceptionAndDisposeAsync(e, this);
    }
}
