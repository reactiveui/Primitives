// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that forwards values while the predicate holds, then completes and unsubscribes.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TakeWhileWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The predicate that determines whether to keep taking values.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>A value indicating whether completion has been emitted.</summary>
    private bool _completed;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="TakeWhileWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
    public TakeWhileWitness(IObserver<T> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_completed)
        {
            return;
        }

        if (!_predicate(value))
        {
            Complete();
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
    public void OnError(Exception error)
    {
        if (_completed)
        {
            return;
        }

        _completed = true;
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public void OnCompleted() => Complete();

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Completes the downstream observer once and releases the upstream subscription.</summary>
    private void Complete()
    {
        if (_completed)
        {
            return;
        }

        _completed = true;
        SinkTerminal.Complete(_observer, this);
    }
}
