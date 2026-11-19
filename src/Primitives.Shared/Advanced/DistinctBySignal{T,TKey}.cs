// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Forwards the first source value for each distinct key.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DistinctBySignal: Source = {_source}")]
public sealed class DistinctBySignal<T, TKey> : IRequireCurrentThread<T>, ICountSource
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The key selector.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The key comparer.</summary>
    private readonly IEqualityComparer<TKey>? _comparer;

    /// <summary>Initializes a new instance of the <see cref="DistinctBySignal{T, TKey}"/> class using the default key comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public DistinctBySignal(IObservable<T> source, Func<T, TKey> keySelector)
        : this(source, keySelector, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DistinctBySignal{T, TKey}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public DistinctBySignal(IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);

        _source = source;
        _keySelector = keySelector;
        _comparer = comparer;
    }

    /// <summary>Creates a distinct-by-key sequence using the default key comparer, with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <returns>The distinct-by-key observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source, Func<T, TKey> keySelector) =>
        Create(source, keySelector, null);

    /// <summary>Creates a distinct-by-key sequence using <paramref name="comparer"/>, with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <returns>The distinct-by-key observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(
        IObservable<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new DistinctBySignal<T, TKey>(source, keySelector, comparer);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DistinctByWitness<T, TKey> sink = new(observer, _keySelector, _comparer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    IDisposable ICountSource.SubscribeCount(IObserver<int> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range && typeof(T) == typeof(int))
        {
            observer.OnNext(CountDistinctRange(range, _keySelector, _comparer));
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        AggregateWitness<T, int, DistinctByCountAggregator<T, TKey>> sink = new(
            observer,
            new(_keySelector, _comparer));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    IDisposable ICountSource.SubscribeLongCount(IObserver<long> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range && typeof(T) == typeof(int))
        {
            observer.OnNext(CountDistinctRange(range, _keySelector, _comparer));
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        AggregateWitness<T, long, DistinctByLongCountAggregator<T, TKey>> sink = new(
            observer,
            new(_keySelector, _comparer));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Counts distinct selected keys directly over a range source.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <returns>The number of distinct keys.</returns>
    private static int CountDistinctRange(
        RangeSignal range,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer)
    {
        HashSet<TKey> seen = comparer is null ? [] : [with(comparer)];
        var typedSelector = (Func<int, TKey>)(object)keySelector;
        for (var i = 0; i < range.Count; i++)
        {
            _ = seen.Add(typedSelector(range.Start + i));
        }

        return seen.Count;
    }
}
