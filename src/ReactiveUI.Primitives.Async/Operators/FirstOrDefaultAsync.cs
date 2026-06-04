// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and retrieving elements from asynchronous observables, such
/// as obtaining the first element that matches a condition or a default value if no such element exists. These
/// extensions are designed to be used with types implementing asynchronous observable patterns and support cancellation
/// via cancellation tokens.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously returns the first element that matches the specified predicate, or a default value if no such
    /// element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element satisfies the predicate.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the first element that matches
    /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue) =>
        @this.FirstOrDefaultAsync(predicate, defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the first element that matches the specified predicate, or a default value if no such
    /// element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element satisfies the predicate.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the first element that matches
    /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
    public static async ValueTask<T?> FirstOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new FirstOrDefaultTaskWitness<T>(predicate, defaultValue, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a default value if the sequence contains no
    /// elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence, or the default value for type T if the sequence is empty.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync<T>(this IObservableAsync<T> @this) =>
        @this.FirstOrDefaultAsync(default, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a default value if the sequence contains no
    /// elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence, or the default value for type T if the sequence is empty.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken) =>
        @this.FirstOrDefaultAsync(default, cancellationToken);

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a specified default value if the sequence
    /// contains no elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence is empty.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue) =>
        @this.FirstOrDefaultAsync(defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the first element of the sequence, or a specified default value if the sequence
    /// contains no elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence is empty.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
    public static async ValueTask<T?> FirstOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new FirstOrDefaultTaskWitness<T>(null, defaultValue, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Observer that captures the first element matching an optional predicate, or returns a default value.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="defaultValue">The default value to return if no element matches.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class FirstOrDefaultTaskWitness<T>(
        Func<T, bool>? predicate,
        T? defaultValue,
        CancellationToken cancellationToken) : TaskWitnessAsyncBase<T, T>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is null || predicate(value))
            {
                await SetResultAndDisposeAsync(value).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(defaultValue!) : SetExceptionAndDisposeAsync(result.Exception);
    }
}
