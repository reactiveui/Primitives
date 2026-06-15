// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for performing asynchronous operations on observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observable sequences, enabling operations such as counting elements that satisfy a specified condition. These
/// methods are designed to work with types that implement asynchronous observation patterns.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous element-counting operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Asynchronously counts the number of elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <returns>A task that represents the asynchronous count operation. The task result contains the number of elements
        /// that match the predicate.</returns>
        public ValueTask<int> CountAsync(Func<T, bool>? predicate)
            => @this.CountAsync(predicate, CancellationToken.None);

        /// <summary>Asynchronously counts the number of elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous count operation. The task result contains the number of elements
        /// that match the predicate.</returns>
        public async ValueTask<int> CountAsync(Func<T, bool>? predicate, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CountTaskWitness<T> observer = new(predicate, cancellationToken);
            await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously returns the total number of elements in the data source.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements in the
        /// data source.</returns>
        public ValueTask<int> CountAsync()
            => @this.CountAsync(null, CancellationToken.None);

        /// <summary>Asynchronously returns the total number of elements in the data source.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous count operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements in the
        /// data source.</returns>
        public ValueTask<int> CountAsync(CancellationToken cancellationToken)
            => @this.CountAsync(null, cancellationToken);
    }

    /// <summary>A witness that counts elements in a sequence, optionally filtered by a predicate.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="predicate">An optional predicate to filter elements. If null, all elements are counted.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class CountTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskResultWitnessAsyncBase<T, int>(cancellationToken)
    {
        /// <summary>The running count of elements that satisfy the predicate.</summary>
        private int _count;

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
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(_count);
    }
}
