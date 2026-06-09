// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that drops values and forwards only terminal notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class IgnoreValuesWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Initializes a new instance of the <see cref="IgnoreValuesWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public IgnoreValuesWitness(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        // Values are intentionally ignored; only terminal notifications are forwarded.
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
