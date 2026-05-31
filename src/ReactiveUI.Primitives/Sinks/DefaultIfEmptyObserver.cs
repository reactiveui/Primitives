// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Observer for default-if-empty.
/// </summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class DefaultIfEmptyObserver<T> : SingleSourceObserver<T>
{
    /// <summary>
    /// The downstream observer.
    /// </summary>
    private readonly IObserver<T> _observer;

    /// <summary>
    /// Value emitted for an empty source.
    /// </summary>
    private readonly T _defaultValue;

    /// <summary>
    /// A value indicating whether the source produced any values.
    /// </summary>
    private bool _seen;

    /// <summary>
    /// Initializes a new instance of the <see cref="DefaultIfEmptyObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="defaultValue">Value emitted for an empty source.</param>
    public DefaultIfEmptyObserver(IObserver<T> observer, T defaultValue)
    {
        _observer = observer;
        _defaultValue = defaultValue;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        _seen = true;
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
            if (!_seen)
            {
                _observer.OnNext(_defaultValue);
            }

            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
