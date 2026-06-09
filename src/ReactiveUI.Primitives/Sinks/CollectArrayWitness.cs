// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that buffers values and emits them as an array on completion.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class CollectArrayWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T[]> _observer;

    /// <summary>The accumulated values.</summary>
    private readonly List<T> _values = [];

    /// <summary>Initializes a new instance of the <see cref="CollectArrayWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CollectArrayWitness(IObserver<T[]> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value) => _values.Add(value);

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        SinkTerminal.Complete(_observer, [.. _values], this);
    }
}
