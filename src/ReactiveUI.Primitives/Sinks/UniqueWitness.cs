// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that suppresses adjacent duplicate values.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class UniqueWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The comparer used to compare adjacent values.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>A value indicating whether a previous value has been observed.</summary>
    private bool _hasLast;

    /// <summary>The most recently forwarded value.</summary>
    private T? _last;

    /// <summary>Initializes a new instance of the <see cref="UniqueWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="comparer">The comparer used to compare adjacent values.</param>
    public UniqueWitness(IObserver<T> observer, IEqualityComparer<T> comparer)
    {
        _observer = observer;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_hasLast && _comparer.Equals(_last!, value))
        {
            return;
        }

        _hasLast = true;
        _last = value;
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
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        SinkTerminal.Complete(_observer, this);
    }
}
