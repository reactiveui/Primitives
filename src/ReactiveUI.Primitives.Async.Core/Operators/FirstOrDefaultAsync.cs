// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>First-or-default operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the first element that matches the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element satisfies the predicate.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the first element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> FirstOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue) =>
            source.FirstOrDefaultAsync(predicate, defaultValue, CancellationToken.None);

        /// <summary>Asynchronously returns the first element that matches the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element satisfies the predicate.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the first element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        public async ValueTask<T?> FirstOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FirstTaskWitness<T> observer = new(predicate, true, defaultValue, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously returns the first element of the sequence, or a default value if the sequence contains no elements.</summary>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> FirstOrDefaultAsync() =>
            source.FirstOrDefaultAsync(default, CancellationToken.None);

        /// <summary>Asynchronously returns the first element of the sequence, or a default value if the sequence contains no elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken) =>
            source.FirstOrDefaultAsync(default, cancellationToken);

        /// <summary>Asynchronously returns the first element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> FirstOrDefaultAsync(T? defaultValue) =>
            source.FirstOrDefaultAsync(defaultValue, CancellationToken.None);

        /// <summary>Asynchronously returns the first element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        public async ValueTask<T?> FirstOrDefaultAsync(T? defaultValue, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FirstTaskWitness<T> observer = new(null, true, defaultValue, cancellationToken);
            await using var subscription =
                await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
            return await observer.AwaitResultAsync().ConfigureAwait(false);
        }
    }
}
