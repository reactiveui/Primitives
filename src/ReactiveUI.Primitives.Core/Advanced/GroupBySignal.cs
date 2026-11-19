// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Factory methods for <see cref="GroupBySignal{TSource, TKey, TElement}"/>.</summary>
public static class GroupBySignal
{
    /// <summary>Creates a grouping signal that stores unprojected source values in each group.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupBySignal<TSource, TKey, TSource> Create<TSource, TKey>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector)
        where TKey : notnull =>
        new(source, keySelector, static value => value, null, 0);

    /// <summary>Creates a grouping signal that stores unprojected source values in each group, comparing keys with <paramref name="comparer"/>.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupBySignal<TSource, TKey, TSource> Create<TSource, TKey>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey>? comparer)
        where TKey : notnull =>
        new(source, keySelector, static value => value, comparer, 0);

    /// <summary>Creates a grouping signal that stores unprojected source values in each group, with the initial table capacity.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupBySignal<TSource, TKey, TSource> Create<TSource, TKey>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        int capacity)
        where TKey : notnull =>
        new(source, keySelector, static value => value, null, capacity);

    /// <summary>Creates a grouping signal that stores unprojected source values in each group, with the initial table capacity and a key comparer.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <returns>The grouping signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GroupBySignal<TSource, TKey, TSource> Create<TSource, TKey>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        int capacity,
        IEqualityComparer<TKey>? comparer)
        where TKey : notnull =>
        new(source, keySelector, static value => value, comparer, capacity);
}
