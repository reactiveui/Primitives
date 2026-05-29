// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and manipulation of asynchronous observables, such as
/// retrieving the first element that matches a specified condition. These extensions are designed to be used with types
/// that implement asynchronous observable patterns.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Asynchronously returns the first element in the sequence that satisfies the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
    /// the predicate.</returns>
    public static ValueTask<T> FirstAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate)
        => @this.FirstAsync(predicate, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the first element in the sequence that satisfies the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
    /// the predicate.</returns>
    public static async ValueTask<T> FirstAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new FirstAsyncObserver<T>(predicate, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the first element of the sequence.
    /// </summary>
    /// <remarks>If the sequence is empty, the behavior depends on the implementation and may result
    /// in an exception being thrown.</remarks>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence.</returns>
    public static ValueTask<T> FirstAsync<T>(this IObservableAsync<T> @this)
        => @this.FirstAsync(CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the first element of the sequence.
    /// </summary>
    /// <remarks>If the sequence is empty, the behavior depends on the implementation and may result
    /// in an exception being thrown.</remarks>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
    /// sequence.</returns>
    public static async ValueTask<T> FirstAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new FirstAsyncObserver<T>(null, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Observer that captures the first element matching an optional predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class FirstAsyncObserver<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskObserverAsyncBase<T, T>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is null || predicate(value))
            {
                await TrySetCompleted(value).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            TrySetException(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result)
        {
            Exception exception;
            if (result.IsSuccess)
            {
                var message = predicate is null
                    ? "Sequence contains no elements."
                    : "Sequence contains no matching elements.";
                exception = new InvalidOperationException(message);
            }
            else
            {
                exception = result.Exception;
            }

            return TrySetException(exception);
        }
    }
}
