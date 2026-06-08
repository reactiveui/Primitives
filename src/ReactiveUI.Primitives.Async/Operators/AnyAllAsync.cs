// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable querying and evaluating asynchronous observable sequences, such as
/// determining whether any or all elements satisfy a condition. These methods are designed to be used with types that
/// implement asynchronous observation patterns.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous quantifier operators that evaluate elements of an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Asynchronously determines whether any element in the sequence satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, the method checks whether the sequence contains
        /// any elements.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if any
        /// element satisfies the predicate or, if the predicate is null, if the sequence contains any elements;
        /// otherwise, <see langword="false"/>.</returns>
        public async ValueTask<bool> AnyAsync(Func<T, bool>? predicate, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var observer = new AnyTaskWitness<T>(predicate, cancellationToken);
            await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        public ValueTask<bool> AnyAsync() => @this.AnyAsync(CancellationToken.None);

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        public ValueTask<bool> AnyAsync(CancellationToken cancellationToken)
            => @this.AnyAsync(null, cancellationToken);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public ValueTask<bool> AllAsync(Func<T, bool> predicate) => @this.AllAsync(predicate, CancellationToken.None);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public async ValueTask<bool> AllAsync(Func<T, bool> predicate, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);
            cancellationToken.ThrowIfCancellationRequested();

            var observer = new AllTaskWitness<T>(predicate, cancellationToken);
            await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A witness that determines whether any element in the sequence satisfies a predicate.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="predicate">An optional predicate to test each element. If null, the sequence is checked for any elements.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class AnyTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken)
        : TaskResultWitnessAsyncBase<T, bool>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (predicate is null || predicate(value))
            {
                await SetResultAndDisposeAsync(true).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(false);
    }

    /// <summary>A witness that determines whether all elements in the sequence satisfy a predicate.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="predicate">The predicate to test each element against.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class AllTaskWitness<T>(Func<T, bool> predicate, CancellationToken cancellationToken)
        : TaskResultWitnessAsyncBase<T, bool>(cancellationToken)
    {
        /// <summary>The predicate function used to test each element in the sequence.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <inheritdoc/>
        protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            if (!_predicate(value))
            {
                await SetResultAndDisposeAsync(false).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(true);
    }
}
