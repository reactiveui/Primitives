// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether any value is present.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AnyWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="AnyWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public AnyWitness(IObserver<bool> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        SinkTerminal.Complete(_observer, true, this);
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        if (_done)
        {
            return;
        }

        _done = true;
        SinkTerminal.Complete(_observer, false, this);
    }
}
