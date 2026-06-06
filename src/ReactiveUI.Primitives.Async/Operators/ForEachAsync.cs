// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The SignalAsync class contains utility methods that enable consumers to process items emitted by
/// asynchronous observables in a convenient and idiomatic way. These methods are designed to simplify common patterns
/// when interacting with IAsyncObservable or similar asynchronous push-based data sources.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously invokes the specified action for each element in the sequence as elements are received.
    /// </summary>
    /// <remarks>If the sequence completes or is canceled, the method returns when all in-flight
    /// actions have finished. Exceptions thrown by the action or during enumeration will propagate to the returned
    /// task.</remarks>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="onNextAsync">A function to invoke for each element in the sequence. The function receives the element and a cancellation
    /// token, and returns a ValueTask that completes when processing is finished.</param>
    /// <returns>A ValueTask that represents the asynchronous operation. The task completes when all elements have been
    /// processed or the operation is canceled.</returns>
    public static ValueTask ForEachAsync<T>(this IObservableAsync<T> @this, Func<T, CancellationToken, ValueTask> onNextAsync)
        => @this.ForEachAsync(onNextAsync, CancellationToken.None);

    /// <summary>
    /// Asynchronously invokes the specified action for each element in the sequence as elements are received.
    /// </summary>
    /// <remarks>If the sequence completes or is canceled, the method returns when all in-flight
    /// actions have finished. Exceptions thrown by the action or during enumeration will propagate to the returned
    /// task.</remarks>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="onNextAsync">A function to invoke for each element in the sequence. The function receives the element and a cancellation
    /// token, and returns a ValueTask that completes when processing is finished.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the sequence to complete. The operation is canceled if the token is
    /// signaled.</param>
    /// <returns>A ValueTask that represents the asynchronous operation. The task completes when all elements have been
    /// processed or the operation is canceled.</returns>
    public static async ValueTask ForEachAsync<T>(
        this IObservableAsync<T> @this,
        Func<T, CancellationToken, ValueTask> onNextAsync,
        CancellationToken cancellationToken)
    {
        if (onNextAsync is null)
        {
            throw new ArgumentNullException(
                nameof(onNextAsync),
                "Cannot invoke a null action for each element in the sequence.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        var observer = new ForEachAsyncTaskWitness<T>(onNextAsync, cancellationToken);
        await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously invokes the specified action for each element in the sequence as elements are received.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
    /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
    /// fully processed or the operation is canceled.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="onNext"/> is null.</exception>
    public static ValueTask ForEachAsync<T>(this IObservableAsync<T> @this, Action<T> onNext)
        => @this.ForEachAsync(onNext, CancellationToken.None);

    /// <summary>
    /// Asynchronously invokes the specified action for each element in the sequence as elements are received.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="onNext">The action to invoke for each element in the sequence. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the iteration.</param>
    /// <returns>A task that represents the asynchronous iteration operation. The task completes when the sequence has been
    /// fully processed or the operation is canceled.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="onNext"/> is null.</exception>
    public static async ValueTask ForEachAsync<T>(this IObservableAsync<T> @this, Action<T> onNext, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        cancellationToken.ThrowIfCancellationRequested();

        var observer = new ForEachSyncTaskWitness<T>(onNext, cancellationToken);
        await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// A witness that invokes an asynchronous callback for each element and signals completion via a task.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    internal sealed class ForEachAsyncTaskWitness<T>(
        Func<T, CancellationToken, ValueTask> onNextAsync,
        CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, bool>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            onNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(true) : SetExceptionAndDisposeAsync(result.Exception);
    }

    /// <summary>
    /// A witness that invokes a synchronous callback for each element and signals completion via a task.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    internal sealed class ForEachSyncTaskWitness<T>(Action<T> onNext, CancellationToken cancellationToken)
        : TaskResultWitnessAsyncBase<T, bool>(cancellationToken)
    {
        /// <summary>
        /// The synchronous callback invoked for each element in the sequence.
        /// </summary>
        private readonly Action<T> _onNext = onNext;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _onNext(value);
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(true) : SetExceptionAndDisposeAsync(result.Exception);
    }
}
