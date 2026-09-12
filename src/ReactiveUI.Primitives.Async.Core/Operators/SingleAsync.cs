// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for asynchronous observable sequences, enabling operations such as retrieving a single
/// element that matches a specified condition.
/// </summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Single-element operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Asynchronously returns the single element of a sequence that satisfies a specified condition, or throws an
        /// exception if more than one such element exists.
        /// </summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
        /// the predicate.</returns>
        /// <remarks>Both no match and more than one match throw.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(Func<T, bool> predicate) =>
            source.SingleAsync(predicate, CancellationToken.None);

        /// <summary>
        /// Asynchronously returns the single element of a sequence that satisfies a specified condition, or throws an
        /// exception if more than one such element exists.
        /// </summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
        /// the predicate.</returns>
        /// <remarks>Both no match and more than one match throw.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            SingleCoreAsync(source, predicate, cancellationToken);

        /// <summary>
        /// Asynchronously returns the single element of the sequence, and throws an exception if the sequence does not
        /// contain exactly one element.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence.</returns>
        /// <remarks>Both an empty sequence and a sequence of more than one element throw.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync() =>
            source.SingleAsync(CancellationToken.None);

        /// <summary>
        /// Asynchronously returns the single element of the sequence, and throws an exception if the sequence does not
        /// contain exactly one element.
        /// </summary>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence.</returns>
        /// <remarks>Both an empty sequence and a sequence of more than one element throw.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(CancellationToken cancellationToken) =>
            SingleCoreAsync(source, null, cancellationToken);
    }

    /// <summary>Shared body for the <c>SingleAsync</c> overloads; subscribes the shared observer and unwraps the result.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    /// <returns>The single matching element.</returns>
    private static async ValueTask<T> SingleCoreAsync<T>(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SingleElementWitness<T> observer = new(predicate, true, default, cancellationToken);
        await using var subscription = await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        var result = await observer.AwaitResultAsync().ConfigureAwait(false);
        return result!;
    }
}
