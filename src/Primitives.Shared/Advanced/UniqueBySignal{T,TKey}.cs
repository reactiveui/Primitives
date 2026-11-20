// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>UniqueBy</c> (adjacent distinct by key).</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UniqueBySignal: Source = {_source}")]
public sealed class UniqueBySignal<T, TKey> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The key projection.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The comparer used to compare adjacent keys.</summary>
    private readonly IEqualityComparer<TKey> _comparer;

    /// <summary>Initializes a new instance of the <see cref="UniqueBySignal{T, TKey}"/> class using the default key comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    public UniqueBySignal(IObservable<T> source, Func<T, TKey> keySelector)
        : this(source, keySelector, EqualityComparer<TKey>.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UniqueBySignal{T, TKey}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The comparer used to compare adjacent keys.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="keySelector"/>, or <paramref name="comparer"/> is <see langword="null"/>.</exception>
    public UniqueBySignal(IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        ArgumentExceptionHelper.ThrowIfNull(comparer);

        _source = source;
        _keySelector = keySelector;
        _comparer = comparer;
    }

    /// <summary>Creates an adjacent-distinct-by-key sequence using the default key comparer, with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <returns>The filtered observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source, Func<T, TKey> keySelector) =>
        Create(source, keySelector, null);

    /// <summary>Creates an adjacent-distinct-by-key sequence using <paramref name="comparer"/>, with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default.</param>
    /// <returns>The filtered observable sequence.</returns>
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
            : new UniqueBySignal<T, TKey>(source, keySelector, comparer ?? EqualityComparer<TKey>.Default);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (typeof(T) == typeof(int) && _source is RangeSignal range)
        {
            EmitRange(range, _keySelector, _comparer, observer);
            return EmptyDisposable.Instance;
        }

        if (_source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, _keySelector, _comparer, observer);
            return EmptyDisposable.Instance;
        }

        UniqueByWitness<T, TKey> sink = new(observer, _keySelector, _comparer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Filters adjacent duplicate keys from a synchronous integer range.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRange(RangeSignal range, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer, IObserver<T> observer)
    {
        var intKeySelector = Unsafe.As<Func<int, TKey>>(keySelector);
        var intObserver = Unsafe.As<IObserver<int>>(observer);
        var hasLast = false;
        var last = default(TKey);
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            if (!TryAcceptKey(value, intKeySelector, comparer, ref hasLast, ref last, observer, out var accepted))
            {
                return;
            }

            if (accepted)
            {
                intObserver.OnNext(value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Filters adjacent duplicate keys from an indexable synchronous list.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer, IObserver<T> observer)
    {
        var hasLast = false;
        var last = default(TKey);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!TryAcceptKey(value, keySelector, comparer, ref hasLast, ref last, observer, out var accepted))
            {
                return;
            }

            if (accepted)
            {
                observer.OnNext(value);
            }
        }

        observer.OnCompleted();
    }

    /// <summary>Projects and compares a key for <paramref name="value"/>, forwarding non-fatal callback failures to <paramref name="observer"/>.</summary>
    /// <typeparam name="TItem">The source item type.</typeparam>
    /// <param name="value">The candidate item.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The key comparer.</param>
    /// <param name="hasLast">Tracks whether a previous key has been observed.</param>
    /// <param name="last">Stores the previous key.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="accepted">Receives whether <paramref name="value"/> should be emitted.</param>
    /// <returns><see langword="true"/> when enumeration should continue; <see langword="false"/> when faulted.</returns>
    private static bool TryAcceptKey<TItem>(
        TItem value,
        Func<TItem, TKey> keySelector,
        IEqualityComparer<TKey> comparer,
        ref bool hasLast,
        ref TKey? last,
        IObserver<T> observer,
        out bool accepted)
    {
        TKey key;
        bool duplicate;
        try
        {
            key = keySelector(value);
            duplicate = hasLast && comparer.Equals(last!, key);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            accepted = false;
            observer.OnError(error);
            return false;
        }

        if (duplicate)
        {
            accepted = false;
            return true;
        }

        hasLast = true;
        last = key;
        accepted = true;
        return true;
    }
}
