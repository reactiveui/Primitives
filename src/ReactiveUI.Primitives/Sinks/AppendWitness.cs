// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for append.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AppendWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The appended value.</summary>
    private readonly T _value;

    /// <summary>Initializes a new instance of the <see cref="AppendWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The appended value.</param>
    public AppendWitness(IObserver<T> observer, T value)
    {
        _observer = observer;
        _value = value;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
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
        SinkTerminal.Complete(_observer, _value, this);
    }
}
