// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable querying and manipulation of asynchronous observables, such as
/// determining whether a sequence contains a specified element. These extensions are designed to integrate with the
/// SignalAsync{T} pattern for asynchronous, push-based data streams.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous containment operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, IEqualityComparer<T>? comparer) =>
            source.ContainsAsync(value, comparer, CancellationToken.None);

        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        public async ValueTask<bool> ContainsAsync(
            T value,
            IEqualityComparer<T>? comparer,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cmp = comparer ?? EqualityComparer<T>.Default;
            ContainsTaskWitness<T> observer = new(value, cmp, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value) =>
            source.ContainsAsync(value, null, CancellationToken.None);

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, CancellationToken cancellationToken) =>
            source.ContainsAsync(value, null, cancellationToken);
    }

    /// <summary>A witness that determines whether a sequence contains a specified value.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="target">The value to search for.</param>
    /// <param name="comparer">The equality comparer to use for comparison.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class ContainsTaskWitness<T>(
        T target,
        IEqualityComparer<T> comparer,
        CancellationToken cancellationToken) : TaskResultWitnessAsyncBase<T, bool>(cancellationToken)
    {
        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return !comparer.Equals(target, value) ? default : SetResultAndDisposeAsync(true);
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result) =>
            result.IsSuccess ? SetResultAndDisposeAsync(false) : SetExceptionAndDisposeAsync(result.Exception);
    }
}
