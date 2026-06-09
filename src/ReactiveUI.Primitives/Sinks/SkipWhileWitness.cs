// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that drops leading values while the predicate holds, then mirrors the source.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SkipWhileWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The predicate that determines whether to keep skipping values.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>A value indicating whether the skipping phase is still active.</summary>
    private bool _skipping = true;

    /// <summary>Initializes a new instance of the <see cref="SkipWhileWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
    public SkipWhileWitness(IObserver<T> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_skipping && _predicate(value))
        {
            return;
        }

        _skipping = false;
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
