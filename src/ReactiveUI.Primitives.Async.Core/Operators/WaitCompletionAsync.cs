// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous completion-await operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The observable sequence to wait for completion.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously waits for the observable sequence to complete without retrieving any values.</summary>
        /// <returns>A ValueTask that represents the asynchronous wait operation.</returns>
        /// <remarks>The source is subscribed once and its values are ignored; the returned task completes when the
        /// sequence completes and faults with the error when it fails.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask WaitCompletionAsync() =>
            source.WaitCompletionAsync(CancellationToken.None);

        /// <summary>Asynchronously waits for the observable sequence to complete without retrieving any values.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the wait operation.</param>
        /// <returns>A ValueTask that represents the asynchronous wait operation.</returns>
        /// <remarks>The source is subscribed once and its values are ignored; the returned task completes when the
        /// sequence completes and faults with the error when it fails.</remarks>
        public async ValueTask WaitCompletionAsync(
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            cancellationToken.ThrowIfCancellationRequested();

            CompletionTaskWitness<T> observer = new(cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Observer that waits for a sequence to complete, ignoring all emitted values.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("CompletionTaskWitness: {_witness}")]
    internal sealed class CompletionTaskWitness<T>(CancellationToken cancellationToken) : IWitnessAsync<T>
    {
        /// <summary>Produces and cancels the witness's single result value.</summary>
        private readonly TaskResultCompletionSource<object?> _completion = new(cancellationToken);

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
        internal ValueTask<object?> AwaitResultAsync() => _completion.AwaitResultAsync(this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(null);

        /// <summary>Sets the result value and disposes this witness.</summary>
        /// <param name="value">The result value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetResultAndDisposeAsync(object? value) => _completion.SetResultAndDisposeAsync(value, this);

        /// <summary>Faults the result with an exception and disposes this witness.</summary>
        /// <param name="e">The exception that caused the fault.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask SetExceptionAndDisposeAsync(Exception e) => _completion.SetExceptionAndDisposeAsync(e, this);
    }
}
