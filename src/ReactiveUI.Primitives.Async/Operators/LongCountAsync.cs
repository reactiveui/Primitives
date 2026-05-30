// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and aggregating data from asynchronous observables in a
/// manner similar to LINQ operators. These extensions are designed to be used with types that implement asynchronous
/// observable patterns, allowing for efficient and composable asynchronous data processing.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
    /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
    public static ValueTask<long> LongCountAsync<T>(this IObservableAsync<T> @this, Func<T, bool>? predicate)
        => @this.LongCountAsync(predicate, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
    /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
    public static async ValueTask<long> LongCountAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new LongCountAsyncObserver<T>(predicate, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the total number of elements in the sequence as a 64-bit integer.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
    /// sequence as a 64-bit integer.</returns>
    public static ValueTask<long> LongCountAsync<T>(this IObservableAsync<T> @this)
        => @this.LongCountAsync(null, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the total number of elements in the sequence as a 64-bit integer.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
    /// sequence as a 64-bit integer.</returns>
    public static ValueTask<long> LongCountAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken)
        => @this.LongCountAsync(null, cancellationToken);

    /// <summary>
    /// Observer that counts elements in a sequence as a 64-bit integer, optionally filtered by a predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements. If null, all elements are counted.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class LongCountAsyncObserver<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskObserverAsyncBase<T, long>(cancellationToken)
    {
        /// <summary>
        /// The running count of elements that satisfy the predicate.
        /// </summary>
        private long _count;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is not null && !predicate(value))
            {
                return default;
            }

            _count = checked(_count + 1);

            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            TrySetException(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? TrySetException(result.Exception) : TrySetCompleted(_count);
    }
}
