// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Cold signal that splits a source into keyed groups.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TElement">The value type inside each group.</typeparam>
[System.Diagnostics.DebuggerDisplay("GroupBySignal: Source = {_source}")]
public sealed class GroupBySignal<TSource, TKey, TElement> : IObservable<GroupedSignal<TKey, TElement>>
    where TKey : notnull
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The selector that extracts the key of a value.</summary>
    private readonly Func<TSource, TKey> _keySelector;

    /// <summary>The selector that extracts the value stored in a group.</summary>
    private readonly Func<TSource, TElement> _elementSelector;

    /// <summary>The key comparer, or <see langword="null"/> for the default comparer.</summary>
    private readonly IEqualityComparer<TKey>? _comparer;

    /// <summary>The initial capacity of the group table.</summary>
    private readonly int _capacity;

    /// <summary>Initializes a new instance of the <see cref="GroupBySignal{TSource, TKey, TElement}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The selector that extracts the key of a value.</param>
    /// <param name="elementSelector">The selector that extracts the value stored in a group.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default comparer.</param>
    /// <param name="capacity">The initial capacity of the group table; zero uses the default.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
    public GroupBySignal(
        IObservable<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TElement> elementSelector,
        IEqualityComparer<TKey>? comparer,
        int capacity)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(capacity);
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _elementSelector = elementSelector ?? throw new ArgumentNullException(nameof(elementSelector));
        _comparer = comparer;
        _capacity = capacity;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<GroupedSignal<TKey, TElement>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        GroupByWitness<TSource, TKey, TElement> sink = new(observer, _keySelector, _elementSelector, _comparer, _capacity);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink.Subscription;
    }
}
