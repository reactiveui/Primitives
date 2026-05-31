// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives;

/// <summary>Sink that dematerializes <see cref="Spark{T}"/> values into notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class UnsparkObserver<T> : SingleSourceObserver<Spark<T>>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Initializes a new instance of the <see cref="UnsparkObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public UnsparkObserver(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(Spark<T> spark) => spark.Accept(_observer);

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
