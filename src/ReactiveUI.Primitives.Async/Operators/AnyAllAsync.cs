// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable querying and evaluating asynchronous observable sequences, such as
/// determining whether any or all elements satisfy a condition. These methods are designed to be used with types that
/// implement asynchronous observation patterns.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Asynchronously determines whether any element in the sequence satisfies the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. If null, the method checks whether the sequence contains
    /// any elements.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if any
    /// element satisfies the predicate or, if the predicate is null, if the sequence contains any elements;
    /// otherwise, <see langword="false"/>.</returns>
    public static async ValueTask<bool> AnyAsync<T>(this IObservableAsync<T> @this, Func<T, bool>? predicate, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var observer = new AnyAsyncObserver<T>(predicate, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously determines whether the source contains any elements.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
    /// source contains any elements; otherwise, <see langword="false"/>.</returns>
    public static ValueTask<bool> AnyAsync<T>(this IObservableAsync<T> @this) => @this.AnyAsync(CancellationToken.None);

    /// <summary>
    /// Asynchronously determines whether the source contains any elements.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
    /// source contains any elements; otherwise, <see langword="false"/>.</returns>
    public static ValueTask<bool> AnyAsync<T>(this IObservableAsync<T> @this, CancellationToken cancellationToken)
        => @this.AnyAsync(null, cancellationToken);

    /// <summary>
    /// Asynchronously determines whether all elements in the sequence satisfy the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
    /// sequence.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
    /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
    /// <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static ValueTask<bool> AllAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate) => @this.AllAsync(predicate, CancellationToken.None);

    /// <summary>
    /// Asynchronously determines whether all elements in the sequence satisfy the specified predicate.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
    /// sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
    /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
    /// <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static async ValueTask<bool> AllAsync<T>(this IObservableAsync<T> @this, Func<T, bool> predicate, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        cancellationToken.ThrowIfCancellationRequested();

        var observer = new AllAsyncObserver<T>(predicate, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.WaitValueAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// An observer that determines whether any element in the sequence satisfies a predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    internal sealed class AnyAsyncObserver<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskObserverAsyncBase<T, bool>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is null || predicate(value))
            {
                await TrySetCompleted(true).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            TrySetException(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? TrySetException(result.Exception) : TrySetCompleted(false);
    }

    /// <summary>
    /// An observer that determines whether all elements in the sequence satisfy a predicate.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    internal sealed class AllAsyncObserver<T>(Func<T, bool> predicate, CancellationToken cancellationToken)
        : TaskObserverAsyncBase<T, bool>(cancellationToken)
    {
        /// <summary>
        /// The predicate function used to test each element in the sequence.
        /// </summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (!_predicate(value))
            {
                await TrySetCompleted(false).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            TrySetException(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? TrySetException(result.Exception) : TrySetCompleted(true);
    }
}
