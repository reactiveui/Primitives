// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Sink that forwards the first occurrence of each value.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class DistinctObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The set of values already observed.</summary>
    private readonly HashSet<T> _seen;

    /// <summary>
    /// Initializes a new instance of the <see cref="DistinctObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="seen">The set used to track already-observed values.</param>
    public DistinctObserver(IObserver<T> observer, HashSet<T> seen)
    {
        _observer = observer;
        _seen = seen;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (!_seen.Add(value))
        {
            return;
        }

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
