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
        /// <remarks>The collection is enumerated once, when the operator is called, not on each subscription.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IList<T>> CombineLatest() => CombineLatestOf(CombineLatestSources(sources));

        /// <summary>Projects the latest value of every source through a selector, once per source notification.</summary>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="resultSelector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results. An empty source collection produces an empty sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/>, one of its elements, or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        /// <remarks>The collection is enumerated once, when the operator is called, not on each subscription.</remarks>
        public IObservable<TResult> CombineLatest<TResult>(Func<IList<T>, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            var materialized = CombineLatestSources(sources);
            return materialized.Length == 0
                ? Signal.Empty<TResult>()
                : CombineLatestSignal<TResult>.Create(materialized, slots => resultSelector(Snapshot(slots)));
        }
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
    /// Ranked below the tuple overloads, which are themselves ranked below the selector overloads. Two to
    /// sixteen same-typed sources listed inline keep binding to the tuple overload that names each of them;
    /// this one takes over past that arity, and whenever the sources arrive as an array.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [OverloadResolutionPriority(-2)]
    public static IObservable<IList<T>> CombineLatest<T>(params IObservable<T>[] sources) =>
        CombineLatestOf(CombineLatestSources(sources));

    /// <summary>Builds the list-valued combine-latest signal for already-validated sources.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The validated source array.</param>
    /// <returns>The combine-latest signal.</returns>
    private static IObservable<IList<T>> CombineLatestOf<T>(IObservable<T>[] sources) =>
        sources.Length == 0
            ? Signal.Empty<IList<T>>()
            : CombineLatestSignal<IList<T>>.Create(sources, Snapshot);

    /// <summary>Copies the subscription's latest-value slots into a list the subscriber owns.</summary>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="slots">The subscription's latest-value slots, reused across notifications.</param>
    /// <returns>A list holding the latest value of every source.</returns>
    private static T[] Snapshot<TResult, T>(CombineLatestSlot<TResult, T>[] slots)
    {
        var snapshot = new T[slots.Length];
        for (var i = 0; i < slots.Length; i++)
        {
            snapshot[i] = slots[i].Value;
        }

        return snapshot;
    }

    /// <summary>Materializes and validates the sources a collection-based combine-latest operator was given.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources whose latest values are combined.</param>
    /// <returns>The source array.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> or one of its elements is <see langword="null"/>.</exception>
    private static IObservable<T>[] CombineLatestSources<T>(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        if (sources is not IObservable<T>[] materialized)
        {
            List<IObservable<T>> buffer = [];
            buffer.AddRange(sources);
            materialized = buffer.ToArray();
        }

        for (var i = 0; i < materialized.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(materialized[i]);
        }

        return materialized;
    }

    /// <summary>A combine-latest signal, carrying the factory for a variable number of same-typed sources.</summary>
    private sealed partial class CombineLatestSignal<TResult>
    {
        /// <summary>Creates a combine-latest signal over a variable number of same-typed sources.</summary>
        /// <typeparam name="T">The element type shared by every source.</typeparam>
        /// <param name="sources">The source observables.</param>
        /// <param name="selector">The selector that projects the subscription's latest-value slots.</param>
        /// <returns>The combine-latest signal.</returns>
        internal static CombineLatestSignal<TResult> Create<T>(
            IObservable<T>[] sources,
            Func<CombineLatestSlot<TResult, T>[], TResult> selector) =>
            new(coordinator =>
            {
                var slots = new CombineLatestSlot<TResult, T>[sources.Length];
                for (var i = 0; i < sources.Length; i++)
                {
                    slots[i] = coordinator.Attach(sources[i]);
                }

                return () => selector(slots);
            });
    }
}
