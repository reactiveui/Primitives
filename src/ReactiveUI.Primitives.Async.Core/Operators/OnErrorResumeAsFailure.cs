// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling advanced composition and error handling scenarios. These methods are intended to be used with
/// types that implement asynchronous push-based notification patterns.</remarks>
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
        /// <remarks>This method enables error handling by transforming exceptions into failure notifications
        /// within the sequence, rather than terminating the sequence with an error. Consumers can inspect the result to
        /// determine whether an operation succeeded or failed.</remarks>
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
        internal sealed class OnErrorResumeAsFailureWitness(IObserverAsync<T> observer) : WitnessAsync<T>
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                observer.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnCompletedAsync(Result.Failure(error));

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                observer.OnCompletedAsync(result);
        }
    }
}
