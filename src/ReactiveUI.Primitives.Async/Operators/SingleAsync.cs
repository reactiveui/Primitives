// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for asynchronous observable sequences, enabling operations such as retrieving a single
/// element that matches a specified condition.
/// </summary>
/// <remarks>The methods in this class support querying and consuming asynchronous observables in a manner similar
/// to LINQ, but adapted for asynchronous and reactive scenarios. These extensions are intended for use with types
/// implementing the SignalAsync pattern, allowing developers to perform operations such as filtering and retrieving
/// elements in an asynchronous context.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously returns the single element of a sequence that satisfies a specified condition, or throws an
    /// exception if more than one such element exists.
    /// </summary>
    /// <remarks>If no element satisfies the condition, or if more than one element satisfies the
    /// condition, an exception is thrown. Use this method when exactly one element is expected to match the
    /// predicate.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
    /// returns <see langword="true"/>.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
    /// the predicate.</returns>
    public static ValueTask<T> SingleAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate)
        => @this.SingleAsync(predicate, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the single element of a sequence that satisfies a specified condition, or throws an
    /// exception if more than one such element exists.
    /// </summary>
    /// <remarks>If no element satisfies the condition, or if more than one element satisfies the
    /// condition, an exception is thrown. Use this method when exactly one element is expected to match the
    /// predicate.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
    /// returns <see langword="true"/>.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
    /// the predicate.</returns>
    public static ValueTask<T> SingleAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate, CancellationToken cancellationToken)
        => SingleCoreAsync(@this, predicate, cancellationToken);

    /// <summary>
    /// Asynchronously returns the single element of the sequence, and throws an exception if the sequence does not
    /// contain exactly one element.
    /// </summary>
    /// <remarks>Use this method when you expect the sequence to contain exactly one element. If the
    /// sequence is empty or contains more than one element, an exception is thrown.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence.</returns>
    public static ValueTask<T> SingleAsync<T>(this IObservableAsync<T> @this)
        => @this.SingleAsync(CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the single element of the sequence, and throws an exception if the sequence does not
    /// contain exactly one element.
    /// </summary>
    /// <remarks>Use this method when you expect the sequence to contain exactly one element. If the
    /// sequence is empty or contains more than one element, an exception is thrown.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence.</returns>
    public static ValueTask<T> SingleAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken)
        => SingleCoreAsync(@this, predicate: null, cancellationToken);

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
        var observer = new SingleElementObserver<T>(predicate, requireExactlyOne: true, defaultValue: default, cancellationToken);
        await using var subscription = await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        var result = await observer.WaitValueAsync().ConfigureAwait(false);
        return result!;
    }
}
