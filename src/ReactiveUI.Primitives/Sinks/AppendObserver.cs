// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for append.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AppendObserver<T> : SingleSourceObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The appended value.</summary>
    private readonly T _value;

    /// <summary>Initializes a new instance of the <see cref="AppendObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The appended value.</param>
    public AppendObserver(IObserver<T> observer, T value)
    {
        _observer = observer;
        _value = value;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
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
            _observer.OnNext(_value);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
