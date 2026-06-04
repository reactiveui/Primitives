// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The SignalAsync class contains static extension methods that operate on instances of
/// SignalAsync{T}. These methods enable querying and manipulation of asynchronous observable sequences in a manner
/// similar to LINQ, supporting scenarios such as retrieving single elements or default values asynchronously.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously returns the only element of a sequence that satisfies a specified condition, or a default
    /// value if no such element exists; this operation throws if more than one matching element is found.
    /// </summary>
    /// <remarks>If more than one element satisfies the condition, an exception is thrown. If no
    /// elements satisfy the condition, the specified default value is returned. The operation observes the provided
    /// cancellation token.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
    /// returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element in the sequence satisfies the condition specified by <paramref
    /// name="predicate"/>.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the single element that matches
    /// the predicate, the specified default value if no such element is found, or throws an exception if more than
    /// one matching element exists.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue) =>
        @this.SingleOrDefaultAsync(predicate, defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the only element of a sequence that satisfies a specified condition, or a default
    /// value if no such element exists; this operation throws if more than one matching element is found.
    /// </summary>
    /// <remarks>If more than one element satisfies the condition, an exception is thrown. If no
    /// elements satisfy the condition, the specified default value is returned. The operation observes the provided
    /// cancellation token.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
    /// returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element in the sequence satisfies the condition specified by <paramref
    /// name="predicate"/>.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the single element that matches
    /// the predicate, the specified default value if no such element is found, or throws an exception if more than
    /// one matching element exists.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken) =>
        SingleOrDefaultCoreAsync(@this, predicate, defaultValue, cancellationToken);

    /// <summary>
    /// Asynchronously returns the only element of a sequence, or a default value if the sequence is empty; this
    /// operation throws an exception if more than one element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence, or the default value of <typeparamref name="T"/> if the sequence is empty.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(this IObservableAsync<T> @this) =>
        @this.SingleOrDefaultAsync(default, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the only element of a sequence, or a default value if the sequence is empty; this
    /// operation throws an exception if more than one element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence, or the default value of <typeparamref name="T"/> if the sequence is empty.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken) =>
        @this.SingleOrDefaultAsync(default, cancellationToken);

    /// <summary>
    /// Asynchronously returns the single element of the sequence, or a specified default value if the sequence is
    /// empty. Throws an exception if the sequence contains more than one element.
    /// </summary>
    /// <remarks>Use this method when you expect the sequence to contain zero or one element. If the
    /// sequence contains more than one element, an exception is thrown. If the sequence is empty, the specified
    /// default value is returned.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence contains no elements.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence, the specified default value if the sequence is empty, or throws if more than one element is
    /// present.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue) =>
        @this.SingleOrDefaultAsync(defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the single element of the sequence, or a specified default value if the sequence is
    /// empty. Throws an exception if the sequence contains more than one element.
    /// </summary>
    /// <remarks>Use this method when you expect the sequence to contain zero or one element. If the
    /// sequence contains more than one element, an exception is thrown. If the sequence is empty, the specified
    /// default value is returned.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence contains no elements.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
    /// sequence, the specified default value if the sequence is empty, or throws if more than one element is
    /// present.</returns>
    public static ValueTask<T?> SingleOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue, CancellationToken cancellationToken) =>
        SingleOrDefaultCoreAsync(@this, predicate: null, defaultValue, cancellationToken);

    /// <summary>Shared body for the <c>SingleOrDefaultAsync</c> overloads; subscribes the shared observer.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="defaultValue">The value to return on empty.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    /// <returns>The single matching element, or the default value on empty.</returns>
    private static async ValueTask<T?> SingleOrDefaultCoreAsync<T>(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new SingleElementObserver<T>(predicate, requireExactlyOne: false, defaultValue, cancellationToken);
        await using var subscription = await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }
}
