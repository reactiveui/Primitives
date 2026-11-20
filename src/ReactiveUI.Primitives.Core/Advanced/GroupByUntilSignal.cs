// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Factory methods for <see cref="GroupByUntilSignal{TSource, TKey, TElement, TDuration}"/>.</summary>
public static class GroupByUntilSignal
{
    /// <summary>Creates a duration-bounded grouping signal that stores unprojected source values in each group.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="durationSelector">Supplies the signal that ends a group.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupByUntilSignal<TSource, TKey, TSource, TDuration> Create<TSource, TKey, TDuration>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector)
        where TKey : notnull =>
        new(source, keySelector, static value => value, durationSelector, null, 0);

    /// <summary>Creates a duration-bounded grouping signal that stores unprojected source values in each group, comparing keys with <paramref name="comparer"/>.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="durationSelector">Supplies the signal that ends a group.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupByUntilSignal<TSource, TKey, TSource, TDuration> Create<TSource, TKey, TDuration>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
        IEqualityComparer<TKey>? comparer)
        where TKey : notnull =>
        new(source, keySelector, static value => value, durationSelector, comparer, 0);

    /// <summary>Creates a duration-bounded grouping signal that stores unprojected source values in each group, with the initial table capacity.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="durationSelector">Supplies the signal that ends a group.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupByUntilSignal<TSource, TKey, TSource, TDuration> Create<TSource, TKey, TDuration>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
        int capacity)
        where TKey : notnull =>
        new(source, keySelector, static value => value, durationSelector, null, capacity);

    /// <summary>Creates a duration-bounded grouping signal that stores unprojected source values in each group, with the initial table capacity and a key comparer.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="durationSelector">Supplies the signal that ends a group.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupByUntilSignal<TSource, TKey, TSource, TDuration> Create<TSource, TKey, TDuration>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
        int capacity,
        IEqualityComparer<TKey>? comparer)
        where TKey : notnull =>
        new(source, keySelector, static value => value, durationSelector, comparer, capacity);
}
