// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Single-observer-layer <c>DistinctUntilChangedBy</c>; key is extracted once per emission and compared against the most-recently-forwarded key.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UniqueBySignal: Source = {_source}")]
public sealed class UniqueBySignal<T, TKey> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The key selector.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The key equality comparer.</summary>
    private readonly IEqualityComparer<TKey> _comparer;

    /// <summary>Initializes a new instance of the <see cref="UniqueBySignal{T, TKey}"/> class using <see cref="EqualityComparer{TKey}.Default"/>.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="keySelector">The key selector.</param>
    public UniqueBySignal(IObservableAsync<T> source, Func<T, TKey> keySelector)
        : this(source, keySelector, EqualityComparer<TKey>.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UniqueBySignal{T, TKey}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key equality comparer.</param>
    public UniqueBySignal(
        IObservableAsync<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey> comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        ArgumentExceptionHelper.ThrowIfNull(comparer);
        _source = source;
        _keySelector = keySelector;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new UniqueByWitness<T, TKey>(observer, _keySelector, _comparer, cancellationToken),
            observer,
            cancellationToken);
}
