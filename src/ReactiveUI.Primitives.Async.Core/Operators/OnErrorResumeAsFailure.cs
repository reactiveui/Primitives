// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Error-handling operators that convert source errors into failure completion results for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The source asynchronous observable sequence to monitor for errors.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Creates a new observable sequence that converts any error encountered in the source sequence into a failure
        /// result, allowing the sequence to complete without propagating exceptions.
        /// </summary>
        /// <returns>An observable sequence that emits the same elements as the source, but represents errors as failure results
        /// instead of throwing exceptions.</returns>
        /// <remarks>The completion result carries the error, so an observer inspects that result to tell success from
        /// failure.</remarks>
        public IObservableAsync<T> OnErrorResumeAsFailure()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new OnErrorResumeAsFailureSignal<T>(source);
        }
    }

    /// <summary>An observable that converts resumable errors from the source into failure completion results.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable to monitor for errors.</param>
    internal sealed class OnErrorResumeAsFailureSignal<T>(IObservableAsync<T> source) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            source.SubscribeAsync(new OnErrorResumeAsFailureWitness(observer), cancellationToken);

        /// <summary>A witness that forwards values and completion, but converts resumable errors into failure completions.</summary>
        /// <param name="observer">The downstream observer to forward notifications to.</param>
        [DebuggerDisplay("OnErrorResumeAsFailureWitness: {_witness}")]
        internal sealed class OnErrorResumeAsFailureWitness(IObserverAsync<T> observer) : IWitnessAsync<T>
        {
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

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                observer.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnCompletedAsync(Result.Failure(error));

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                observer.OnCompletedAsync(result);
        }
    }
}
