// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for long-counting all values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class LongCountWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<long> _observer;

    /// <summary>The running count.</summary>
    private long _count;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="LongCountWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public LongCountWitness(IObserver<long> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_done)
        {
            return;
        }

        _count = checked(_count + 1L);
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        if (_done)
        {
            return;
        }

        _done = true;
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
        if (_done)
        {
            return;
        }

        _done = true;
        try
        {
            _observer.OnNext(_count);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
