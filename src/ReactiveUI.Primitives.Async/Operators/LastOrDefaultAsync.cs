// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and retrieving elements from asynchronous observables, such
/// as obtaining the last element or a default value if no elements are found. These extensions are designed to support
/// asynchronous and cancellation-aware operations on observable sequences.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default
    /// value if no such element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
    /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
    public static ValueTask<T?> LastOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue) =>
        @this.LastOrDefaultAsync(predicate, defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default
    /// value if no such element is found.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
    /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
    public static async ValueTask<T?> LastOrDefaultAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new LastOrDefaultTaskWitness<T>(predicate, defaultValue, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the last element of a sequence, or a default value if the sequence contains no
    /// elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence, or the default value for type T if the sequence is empty.</returns>
    public static ValueTask<T?> LastOrDefaultAsync<T>(this IObservableAsync<T> @this) =>
        @this.LastOrDefaultAsync(default, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the last element of a sequence, or a default value if the sequence contains no
    /// elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence, or the default value for type T if the sequence is empty.</returns>
    public static ValueTask<T?> LastOrDefaultAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken) =>
        @this.LastOrDefaultAsync(default, cancellationToken);

    /// <summary>
    /// Asynchronously returns the last element of the sequence, or a specified default value if the sequence
    /// contains no elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence is empty.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
    public static ValueTask<T?> LastOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue) =>
        @this.LastOrDefaultAsync(defaultValue, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the last element of the sequence, or a specified default value if the sequence
    /// contains no elements.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="defaultValue">The value to return if the sequence is empty.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
    public static async ValueTask<T?> LastOrDefaultAsync<T>(this IObservableAsync<T> @this, T? defaultValue, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new LastOrDefaultTaskWitness<T>(null, defaultValue, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Observer that captures the last element matching an optional predicate, or returns a default value.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="defaultValue">The default value to return if no element matches.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class LastOrDefaultTaskWitness<T>(
        Func<T, bool>? predicate,
        T? defaultValue,
        CancellationToken cancellationToken) : TaskWitnessAsyncBase<T, T>(cancellationToken)
    {
        /// <summary>
        /// The most recently observed matching element, or the default value if no match has been found.
        /// </summary>
        private T? _last = defaultValue;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is not null && !predicate(value))
            {
                return default;
            }

            _last = value;

            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(_last!) : SetExceptionAndDisposeAsync(result.Exception);
    }
}
