// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling operations such as materializing the sequence into a list asynchronously. These methods are
/// intended to simplify common tasks when consuming asynchronous observable streams.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous list-materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> CollectListAsync() =>
            source.ToListAsync(CancellationToken.None);

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> ToListAsync() =>
            source.CollectListAsync();

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        public async ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ToListTaskWitness<T> observer = new(cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A task that completes with the collected array of values.</returns>
        public async ValueTask<T[]> CollectArrayAsync()
        {
            var values = await source.CollectListAsync().ConfigureAwait(false);
            return [.. values];
        }
    }

    /// <summary>Witness that collects all elements from a sequence into a list.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class ToListTaskWitness<T>(CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, List<T>>(cancellationToken)
    {
        /// <summary>The list that accumulates all elements received from the source sequence.</summary>
        private readonly List<T> _items = [];

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _items.Add(value);
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(_items);
    }
}
