// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that drops the first <c>count</c> values, then forwards the rest.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SkipWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The remaining number of values to drop.</summary>
    private int _remaining;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="SkipWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="count">The number of leading values to drop.</param>
    public SkipWitness(IObserver<T> observer, int count)
    {
        _observer = observer;
        _remaining = count;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
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
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
