// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that suppresses adjacent values whose projected key matches the previous one.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
public sealed class UniqueByWitness<T, TKey> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The key projection.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The comparer used to compare adjacent keys.</summary>
    private readonly IEqualityComparer<TKey> _comparer;

    /// <summary>A value indicating whether a previous key has been observed.</summary>
    private bool _hasLast;

    /// <summary>The most recently observed key.</summary>
    private TKey? _last;

    /// <summary>Initializes a new instance of the <see cref="UniqueByWitness{T, TKey}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The comparer used to compare adjacent keys.</param>
    public UniqueByWitness(IObserver<T> observer, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer)
    {
        _observer = observer;
        _keySelector = keySelector;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
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
    public override void OnError(Exception error)
    {
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
