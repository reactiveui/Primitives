// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether any value is present.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AnyObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="AnyObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public AnyObserver(IObserver<bool> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        try
        {
            _observer.OnNext(true);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
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
            _observer.OnNext(false);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
