// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>A combine-latest signal, carrying the factory for a variable number of same-typed sources.</summary>
[System.Diagnostics.DebuggerDisplay("CombineLatestSignal: Connect = {_connect}")]
public sealed partial class CombineLatestSignal<TResult>
{
    /// <summary>Creates a combine-latest signal over a variable number of same-typed sources.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The source observables.</param>
    /// <param name="selector">The selector that projects the subscription's latest-value slots.</param>
    /// <returns>The combine-latest signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/>, one of its elements, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static CombineLatestSignal<TResult> Create<T>(
        IObservable<T>[] sources,
        Func<CombineLatestSlot<TResult, T>[], TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        for (var i = 0; i < sources.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources[i]);
        }

        return new(coordinator =>
        {
            var slots = new CombineLatestSlot<TResult, T>[sources.Length];
            for (var i = 0; i < sources.Length; i++)
            {
                slots[i] = coordinator.Attach(sources[i]);
            }

            return () => selector(slots);
        });
    }

    /// <summary>Projects the latest value of every source in <paramref name="sources"/> through <paramref name="resultSelector"/>.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources whose latest values are combined.</param>
    /// <param name="resultSelector">Projects the latest value of every source into a result.</param>
    /// <returns>An observable sequence of projected results, or an empty sequence when <paramref name="sources"/> is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/>, one of its elements, or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> CreateProjected<T>(
        IEnumerable<IObservable<T>> sources,
        Func<IList<T>, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        var materialized = CombineLatestSignal.MaterializeSources(sources);
        return materialized.Length == 0
            ? ImmutableEmptySignal<TResult>.Instance
            : Create(materialized, slots => resultSelector(Snapshot(slots)));
    }

    /// <summary>Copies the subscription's latest-value slots into an array owned by the subscriber.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="slots">The subscription's latest-value slots.</param>
    /// <returns>An array holding the latest value of every source.</returns>
    internal static T[] Snapshot<T>(CombineLatestSlot<TResult, T>[] slots)
    {
        var snapshot = new T[slots.Length];
        for (var i = 0; i < slots.Length; i++)
        {
            snapshot[i] = slots[i].Value;
        }

        return snapshot;
    }
}
