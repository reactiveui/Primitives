// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer for distinct-by.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Done = {_done}, SeenKeys = {_seen.Count}")]
public sealed class DistinctByWitness<T, TKey> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The key selector.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The observed keys.</summary>
    private readonly HashSet<TKey> _seen;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="DistinctByWitness{T,TKey}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer.</param>
    public DistinctByWitness(IObserver<T> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
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

        try
        {
            _observer.OnNext(value);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => SinkTerminal.Complete(_observer, this, ref _done);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
