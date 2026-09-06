// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that suppresses adjacent values whose projected key matches the previous one.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="keySelector">The key projection.</param>
/// <param name="comparer">The comparer used to compare adjacent keys.</param>
[System.Diagnostics.DebuggerDisplay("UniqueByWitness: HasLast = {_hasLast}, Last = {_last}")]
public sealed class UniqueByWitness<T, TKey>(
    IObserver<T> observer,
    Func<T, TKey> keySelector,
    IEqualityComparer<TKey> comparer) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The key projection.</summary>
    private readonly Func<T, TKey> _keySelector = keySelector;

    /// <summary>The comparer used to compare adjacent keys.</summary>
    private readonly IEqualityComparer<TKey> _comparer = comparer;

    /// <summary>A value indicating whether a previous key has been observed.</summary>
    private bool _hasLast;

    /// <summary>The most recently observed key.</summary>
    private TKey? _last;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        var key = _keySelector(value);
        if (_hasLast && _comparer.Equals(_last!, key))
        {
            return;
        }

        _hasLast = true;
        _last = key;
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
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
