// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives;

/// <summary>Sink that materializes notifications into <see cref="Spark{T}"/> values.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SparkWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<Spark<T>> _observer;

    /// <summary>Initializes a new instance of the <see cref="SparkWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public SparkWitness(IObserver<Spark<T>> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value) => _observer.OnNext(Spark.CreateOnNext(value));

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        SinkTerminal.Complete(_observer, Spark.CreateOnError<T>(error), this);
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        SinkTerminal.Complete(_observer, Spark.CreateOnCompleted<T>(), this);
    }
}
