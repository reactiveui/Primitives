// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Sink that drops the first <c>count</c> values, then forwards the rest.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SkipObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The remaining number of values to drop.</summary>
    private int _remaining;

    /// <summary>
    /// Initializes a new instance of the <see cref="SkipObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="count">The number of leading values to drop.</param>
    public SkipObserver(IObserver<T> observer, int count)
    {
        _observer = observer;
        _remaining = count;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (_remaining > 0)
        {
            _remaining--;
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
