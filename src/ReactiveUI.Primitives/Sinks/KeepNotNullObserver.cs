// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that forwards only non-null values.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class KeepNotNullObserver<T> : SingleSourceObserver<T?>
    where T : class
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Initializes a new instance of the <see cref="KeepNotNullObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public KeepNotNullObserver(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public override void OnNext(T? value)
    {
        if (value is null)
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
