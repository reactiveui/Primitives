// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides CombineLatest overloads for enumerable collections of asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>SyncLatest/CombineLatest operators for an enumerable collection of observable source sequences.</summary>
    /// <typeparam name="TSource">The element type produced by the source sequences.</typeparam>
    /// <param name="sources">The source sequences to combine.</param>
    extension<TSource>(IEnumerable<IObservableAsync<TSource>> sources)
    {
        /// <summary>Combines the latest value from each asynchronous observable sequence in the supplied collection.</summary>
        /// <returns>An observable sequence that emits a snapshot of the latest values whenever any source produces a new value,
        /// after all sources have produced at least one value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="sources"/> is <see langword="null"/>.</exception>
        /// <remarks>Each emitted <see cref="IReadOnlyList{T}"/> is a reference to one buffer owned by the subscription,
        /// not a fresh allocation. An observer must consume the snapshot inside its <c>OnNextAsync</c> handler: the buffer
        /// is overwritten under the operator's gate before each emit, so a retained reference surfaces the next emission's
        /// values. For a stable copy, use the projecting <c>CombineLatest</c> overload or
        /// <c>.Select(static s =&gt; s.ToArray())</c>.</remarks>
        public IObservableAsync<IReadOnlyList<TSource>> SyncLatest()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            // An identity selector lets one subscription implementation back both shapes.
            return new SyncLatestEnumerableSignal<TSource, IReadOnlyList<TSource>>(sources, static s => s);
        }

        /// <summary>
        /// Combines the latest value from each asynchronous observable sequence in the supplied collection and projects the
        /// resulting snapshot into a result value.
        /// </summary>
        /// <typeparam name="TResult">The projected result type.</typeparam>
        /// <param name="resultSelector">A selector that projects the current snapshot of latest values into a result value.</param>
        /// <returns>An observable sequence that emits projected results whenever any source produces a new value, after all
        /// sources have produced at least one value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="sources"/> or <paramref name="resultSelector"/>
        /// is <see langword="null"/>.</exception>
        public IObservableAsync<TResult> SyncLatest<TResult>(
            Func<IReadOnlyList<TSource>, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new SyncLatestEnumerableSignal<TSource, TResult>(sources, resultSelector);
        }
    }
}
