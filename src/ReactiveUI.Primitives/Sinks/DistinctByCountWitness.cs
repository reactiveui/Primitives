// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for counting distinct keys.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
public sealed class DistinctByCountWitness<T, TKey> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<int> _observer;

    /// <summary>The key selector.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The observed keys.</summary>
    private readonly HashSet<TKey> _seen;

    /// <summary>The running count.</summary>
    private int _count;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="DistinctByCountWitness{T,TKey}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer.</param>
    public DistinctByCountWitness(IObserver<int> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
    {
        _observer = observer;
        _keySelector = keySelector;
        _seen = comparer is null ? [] : new(comparer);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_done || !_seen.Add(_keySelector(value)))
        {
            return;
        }

        _count = checked(_count + 1);
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, _count, this, ref _done);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
