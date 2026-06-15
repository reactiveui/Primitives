// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer for default-if-empty.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class DefaultIfEmptyWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Value emitted for an empty source.</summary>
    private readonly T _defaultValue;

    /// <summary>A value indicating whether the source produced any values.</summary>
    private bool _seen;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="DefaultIfEmptyWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="defaultValue">Value emitted for an empty source.</param>
    public DefaultIfEmptyWitness(IObserver<T> observer, T defaultValue)
    {
        _observer = observer;
        _defaultValue = defaultValue;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
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
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    public void OnCompleted()
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

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
