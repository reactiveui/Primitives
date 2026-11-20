// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>System.Reactive-named CombineLatest parity operators over a collection of same-typed sources.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named latest-value combination operators for a collection of sources.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources whose latest values are combined.</param>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Combines the latest value of every source into a list, one list per source notification.</summary>
        /// <returns>
        /// An observable sequence of latest-value lists. Each notification carries its own list, so a subscriber
        /// may keep it. An empty source collection produces an empty sequence.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> or one of its elements is <see langword="null"/>.</exception>
        /// <remarks>The collection is enumerated once when the operator is called.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> CombineLatest() => CombineLatestSignal.CreateList(sources);

        /// <summary>Projects the latest value of every source through a selector, once per source notification.</summary>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="resultSelector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results. An empty source collection produces an empty sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/>, one of its elements, or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        /// <remarks>The collection is enumerated once when the operator is called.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> CombineLatest<TResult>(Func<IList<T>, TResult> resultSelector) =>
            CombineLatestSignal<TResult>.CreateProjected(sources, resultSelector);
    }

    /// <summary>Combines the latest value of every source into a list, one list per source notification.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources whose latest values are combined.</param>
    /// <returns>
    /// An observable sequence of latest-value lists. Each notification carries its own list, so a subscriber
    /// may keep it. An empty source collection produces an empty sequence.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> or one of its elements is <see langword="null"/>.</exception>
    /// <remarks>
    /// Inline calls with two through sixteen same-typed sources select tuple overloads; larger calls and arrays select this overload.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(-2)]
    public static IObservable<IList<T>> CombineLatest<T>(params IObservable<T>[] sources) =>
        CombineLatestSignal.CreateList(sources);
}
