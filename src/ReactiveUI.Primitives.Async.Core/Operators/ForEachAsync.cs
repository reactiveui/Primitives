// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
            ForEachAsyncTaskWitness<T>.RunAsync(source, onNextAsync);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ForEachAsync(
            Func<T, CancellationToken, ValueTask> onNextAsync,
            CancellationToken cancellationToken) =>
            ForEachAsyncTaskWitness<T>.RunAsync(source, onNextAsync, cancellationToken);

        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
        /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
        /// fully processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ForEachAsync(Action<T> onNext) =>
            ForEachSyncTaskWitness<T>.RunAsync(source, onNext);

        /// <summary>Asynchronously invokes the specified action for each element in the sequence as elements are received.</summary>
        /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the iteration.</param>
        /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
        /// fully processed or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ForEachAsync(Action<T> onNext, CancellationToken cancellationToken) =>
            ForEachSyncTaskWitness<T>.RunAsync(source, onNext, cancellationToken);
    }
}
