// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and manipulation of asynchronous observables, such as
/// retrieving the last element of a sequence. These extensions are designed to support asynchronous and reactive
/// programming patterns.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Asynchronously returns the last element in the sequence that satisfies the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the last element that matches
    /// the predicate.</returns>
    public static ValueTask<T> LastAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate)
        => @this.LastAsync(predicate, CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the last element in the sequence that satisfies the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
    /// predicate returns <see langword="true"/>.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the last element that matches
    /// the predicate.</returns>
    public static async ValueTask<T> LastAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new LastAsyncObserver<T>(predicate, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the last element of the sequence.
    /// </summary>
    /// <remarks>If the sequence is empty, the behavior depends on the implementation and may result
    /// in an exception being thrown. The operation is performed asynchronously and may not complete
    /// immediately.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence.</returns>
    public static ValueTask<T> LastAsync<T>(this IObservableAsync<T> @this)
        => @this.LastAsync(CancellationToken.None);

    /// <summary>
    /// Asynchronously returns the last element of the sequence.
    /// </summary>
    /// <remarks>If the sequence is empty, the behavior depends on the implementation and may result
    /// in an exception being thrown. The operation is performed asynchronously and may not complete
    /// immediately.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the last element of the
    /// sequence.</returns>
    public static async ValueTask<T> LastAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new LastAsyncObserver<T>(null, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Observer that captures the last element matching an optional predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class LastAsyncObserver<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskObserverAsyncBase<T, T>(cancellationToken)
    {
        /// <summary>
        /// A value indicating whether any matching element has been observed.
        /// </summary>
        private bool _hasValue;

        /// <summary>
        /// The most recently observed matching element.
        /// </summary>
        private T? _last;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is not null && !predicate(value))
            {
                return default;
            }

            _hasValue = true;
            _last = value;

            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            TrySetException(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result)
        {
            if (!result.IsSuccess)
            {
                return TrySetException(result.Exception);
            }

            if (_hasValue)
            {
                return TrySetCompleted(_last!);
            }

            var message = predicate is null
                ? "Sequence contains no elements."
                : "Sequence contains no matching elements.";
            return TrySetException(new InvalidOperationException(message));
        }
    }
}
