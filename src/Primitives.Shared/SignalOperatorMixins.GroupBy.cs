// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Grouping operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Grouping operators for an observable source sequence.</summary>
    /// <typeparam name="TSource">The type of the source values.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Splits the source into groups by key. Each group receives the values that share its key.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupBy<TKey>(Func<TSource, TKey> keySelector)
            where TKey : notnull =>
            GroupByCore(source, keySelector, static value => value, null, 0);

        /// <summary>Splits the source into groups by key, comparing keys with a comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupBy<TKey>(
            Func<TSource, TKey> keySelector,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByCore(source, keySelector, static value => value, comparer, 0);

        /// <summary>Splits the source into groups by key, with the initial capacity of the group table.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupBy<TKey>(Func<TSource, TKey> keySelector, int capacity)
            where TKey : notnull =>
            GroupByCore(source, keySelector, static value => value, null, capacity);

        /// <summary>Splits the source into groups by key, with the initial capacity of the group table and a key comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupBy<TKey>(
            Func<TSource, TKey> keySelector,
            int capacity,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByCore(source, keySelector, static value => value, comparer, capacity);

        /// <summary>Splits the source into groups by key and projects each value before it enters its group.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupBy<TKey, TElement>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector)
            where TKey : notnull =>
            GroupByCore(source, keySelector, elementSelector, null, 0);

        /// <summary>Splits the source into groups by key, projecting each value and comparing keys with a comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupBy<TKey, TElement>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByCore(source, keySelector, elementSelector, comparer, 0);

        /// <summary>Splits the source into groups by key, projecting each value, with the initial capacity of the group table.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupBy<TKey, TElement>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            int capacity)
            where TKey : notnull =>
            GroupByCore(source, keySelector, elementSelector, null, capacity);

        /// <summary>Splits the source into groups by key, projecting each value, with the initial capacity of the group table and a key comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups, each delivered when its first value arrives.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupBy<TKey, TElement>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            int capacity,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByCore(source, keySelector, elementSelector, comparer, capacity);

        /// <summary>Splits the source into groups by key; each group ends when its duration signal emits or completes.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="durationSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupByUntil<TKey, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, static value => value, durationSelector, null, 0);

        /// <summary>Splits the source into groups by key that end with a duration signal, comparing keys with a comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="durationSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupByUntil<TKey, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, static value => value, durationSelector, comparer, 0);

        /// <summary>Splits the source into groups by key that end with a duration signal, with the initial capacity of the group table.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="durationSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupByUntil<TKey, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
            int capacity)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, static value => value, durationSelector, null, capacity);

        /// <summary>Splits the source into groups by key that end with a duration signal, with the initial capacity of the group table and a key comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="durationSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TSource>> GroupByUntil<TKey, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<GroupedSignal<TKey, TSource>, IObservable<TDuration>> durationSelector,
            int capacity,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, static value => value, durationSelector, comparer, capacity);

        /// <summary>Splits the source into groups by key, projecting each value; each group ends when its duration signal emits or completes.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupByUntil<TKey, TElement, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, elementSelector, durationSelector, null, 0);

        /// <summary>Splits the source into groups by key, projecting each value, with a duration signal per group and a key comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupByUntil<TKey, TElement, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, elementSelector, durationSelector, comparer, 0);

        /// <summary>Splits the source into groups by key, projecting each value, with a duration signal per group and the initial capacity of the group table.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupByUntil<TKey, TElement, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector,
            int capacity)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, elementSelector, durationSelector, null, capacity);

        /// <summary>Splits the source into groups by key, projecting each value, with a duration signal per group, the initial capacity of the group table and a key comparer.</summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
        /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
        /// <param name="keySelector">Extracts the key of a value.</param>
        /// <param name="elementSelector">Projects the value stored in its group.</param>
        /// <param name="durationSelector">Supplies the signal that ends a group.</param>
        /// <param name="capacity">The initial capacity of the group table.</param>
        /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
        /// <returns>A sequence of groups; a key that appears after its group ended opens a new group.</returns>
        /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<GroupedSignal<TKey, TElement>> GroupByUntil<TKey, TElement, TDuration>(
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> elementSelector,
            Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector,
            int capacity,
            IEqualityComparer<TKey>? comparer)
            where TKey : notnull =>
            GroupByUntilCore(source, keySelector, elementSelector, durationSelector, comparer, capacity);
    }

    /// <summary>Builds the signal shared by every <c>GroupBy</c> overload.</summary>
    /// <typeparam name="TSource">The type of the source values.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="elementSelector">Projects the value stored in its group.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <returns>The grouping signal.</returns>
    private static GroupBySignal<TSource, TKey, TElement> GroupByCore<TSource, TKey, TElement>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TElement> elementSelector,
        IEqualityComparer<TKey>? comparer,
        int capacity)
        where TKey : notnull =>
        new(source, keySelector, elementSelector, comparer, capacity);

    /// <summary>Builds the signal shared by every <c>GroupByUntil</c> overload.</summary>
    /// <typeparam name="TSource">The type of the source values.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TElement">The type of the values inside each group.</typeparam>
    /// <typeparam name="TDuration">The value type of the duration signals.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="keySelector">Extracts the key of a value.</param>
    /// <param name="elementSelector">Projects the value stored in its group.</param>
    /// <param name="durationSelector">Supplies the signal that ends a group.</param>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    /// <param name="capacity">The initial capacity of the group table.</param>
    /// <returns>The grouping signal.</returns>
    private static GroupByUntilSignal<TSource, TKey, TElement, TDuration> GroupByUntilCore<TSource, TKey, TElement, TDuration>(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TElement> elementSelector,
        Func<GroupedSignal<TKey, TElement>, IObservable<TDuration>> durationSelector,
        IEqualityComparer<TKey>? comparer,
        int capacity)
        where TKey : notnull =>
        new(source, keySelector, elementSelector, durationSelector, comparer, capacity);
}
